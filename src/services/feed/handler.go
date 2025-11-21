package main

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/models"
	"Gosnip/src/rpc/comment"
	"Gosnip/src/rpc/favorite"
	"Gosnip/src/rpc/feed"
	"Gosnip/src/rpc/recommend"
	"Gosnip/src/rpc/user"
	"Gosnip/src/storage/cached"
	"Gosnip/src/storage/database"
	"Gosnip/src/storage/file"
	grpc2 "Gosnip/src/utils/grpc"
	"Gosnip/src/utils/logging"
	"Gosnip/src/utils/rabbitmq"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"

	"github.com/sirupsen/logrus"
)

type FeedServiceImpl struct {
	feed.FeedServiceServer
}

const (
	VideoCount = 3
)

var UserClient user.UserServiceClient
var CommentClient comment.CommentServiceClient
var FavoriteClient favorite.FavoriteServiceClient
var RecommendClient recommend.RecommendServiceClient

var conn *amqp.Connection

var channel *amqp.Channel

func exitOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func (s FeedServiceImpl) New() {
	userRpcConn := grpc2.Connect(config.UserRpcServerName)
	UserClient = user.NewUserServiceClient(userRpcConn)
	commentRpcConn := grpc2.Connect(config.CommentRpcServerName)
	CommentClient = comment.NewCommentServiceClient(commentRpcConn)
	favoriteRpcConn := grpc2.Connect(config.FavoriteRpcServerName)
	FavoriteClient = favorite.NewFavoriteServiceClient(favoriteRpcConn)
	recommendRpcConn := grpc2.Connect(config.RecommendRpcServiceName)
	RecommendClient = recommend.NewRecommendServiceClient(recommendRpcConn)

	var err error

	conn, err = amqp.Dial(rabbitmq.BuildMQConnAddr())
	exitOnError(err)

	channel, err = conn.Channel()
	exitOnError(err)

	err = channel.ExchangeDeclare(
		strings.EventExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	exitOnError(err)
}

func CloseMQConn() {
	if err := conn.Close(); err != nil {
		panic(err)
	}

	if err := channel.Close(); err != nil {
		panic(err)
	}
}

// produceFeed 将获取的推荐视频列表发给event总线
func produceFeed(ctx context.Context, event models.RecommendEvent) {
	ctx, span := tracing.Tracer.Start(ctx, "FeedPublisher")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("FeedService.FeedPublisher").WithContext(ctx)

	data, err := json.Marshal(event)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Error when marshal the event model")
		logging.SetSpanError(span, err)
		return
	}

	headers := rabbitmq.InjectAMQPHeaders(ctx)

	err = channel.PublishWithContext(ctx,
		strings.EventExchange,
		strings.VideoGetEvent,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
			Headers:     headers,
		})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Error when publishing the event model")
		logging.SetSpanError(span, err)
		return
	}
}

// ListVideosByRecommend  向GORSE推荐系统获取推荐视频
func (s FeedServiceImpl) ListVideosByRecommend(ctx context.Context, request *feed.ListFeedRequest) (resp *feed.ListFeedResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "ListVideosService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("FeedService.ListVideos").WithContext(ctx)

	now := time.Now().UnixMilli()
	latestTime := now // 若传入的时间戳无效，则使用默认的时间：当前时间
	if request.LatestTime != nil && *request.LatestTime != "" && *request.LatestTime != "0" {
		// Check if request .LatestTime is a timestamp
		t, ok := isUnixMilliTimestamp(*request.LatestTime)
		if ok {
			latestTime = t
		} else {
			logger.WithFields(logrus.Fields{
				"latestTime": request.LatestTime,
			}).Errorf("The latestTime is not a unix timestamp")
			logging.SetSpanError(span, errors.New("the latestTime is not a unit timestamp"))
		}
	}
	// 调用recommend服务，获取推荐视频
	recommendResponse, err := RecommendClient.GetRecommendInformation(ctx, &recommend.RecommendRequest{
		UserId: *request.ActorId,
		Offset: -1,
		Number: VideoCount,
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":             err,
			"recommenVideoId": recommendResponse.VideoList,
		}).Errorf("Error when trying to connect with RecommendService")
		logging.SetSpanError(span, err)
		resp = &feed.ListFeedResponse{
			StatusCode: strings.RecommendServiceInnerErrorCode,
			StatusMsg:  strings.RecommendServiceInnerError,
			NextTime:   nil,
			VideoList:  nil,
		}
		return resp, err
	}
	recommendVideoIds := recommendResponse.VideoList
	find, err := findRecommendVideos(ctx, recommendVideoIds)

	nextTimeStamp := uint64(latestTime)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"find": find,
		}).Warnf("func findRecommendVideos meet trouble.")
		logging.SetSpanError(span, err)

		// ，latestTime 在这个函数中主要是为了保持与 API
		//  接口的兼容性而存在，它并没有直接影响推荐视频的获取逻辑。
		resp = &feed.ListFeedResponse{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
			NextTime:   &nextTimeStamp,
			VideoList:  nil,
		}
		return resp, err
	}
	if len(find) == 0 {
		resp = &feed.ListFeedResponse{
			StatusCode: strings.ServiceOKCode,
			StatusMsg:  strings.ServiceOK,
			NextTime:   nil,
			VideoList:  nil,
		}
		return resp, err
	}

	var actorId uint32 = 0
	if request.ActorId != nil {
		actorId = *request.ActorId
	}
	// 从model.Video 转换为 feed.Video，其中feed.Video中的多个字段需要请求多个服务才能填充完
	videos := queryDetailed(ctx, logger, actorId, find)
	if videos == nil {
		logger.WithFields(logrus.Fields{
			"videos": videos,
		}).Warnf("func queryDetailed meet trouble.")
		logging.SetSpanError(span, err)
		resp = &feed.ListFeedResponse{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
			NextTime:   nil,
			VideoList:  nil,
		}
		return resp, err
	}

	// 异步将推荐视频列表发送给event总线
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		var videoLists []uint32
		for _, item := range videos {
			videoLists = append(videoLists, item.Id)
		}
		produceFeed(ctx, models.RecommendEvent{
			ActorId: *request.ActorId,
			VideoId: videoLists,
			// 类型1表示视频已观看
			Type:   1,
			Source: config.FeedRpcServerName,
		})
	}()
	wg.Wait()
	resp = &feed.ListFeedResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		NextTime:   &nextTimeStamp,
		VideoList:  videos,
	}
	return resp, err
}

// ListVideos 根据传入的最新时间获取视频列表
func (s FeedServiceImpl) ListVideos(ctx context.Context, request *feed.ListFeedRequest) (resp *feed.ListFeedResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "ListVideosService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("FeedService.ListVideos").WithContext(ctx)

	now := time.Now().UnixMilli()
	latestTime := now
	if request.LatestTime != nil && *request.LatestTime != "" {
		// Check if request .LatestTime is a timestamp
		t, ok := isUnixMilliTimestamp(*request.LatestTime)
		if ok {
			latestTime = t
		} else {
			logger.WithFields(logrus.Fields{
				"latestTime": request.LatestTime,
			}).Errorf("The latestTime is not a unix timestamp")
			logging.SetSpanError(span, errors.New("the latestTime is not a unit timestamp"))
		}
	}

	find, nextTime, err := findVideos(ctx, latestTime)
	nextTimeStamp := uint64(nextTime.UnixMilli())
	if err != nil {
		logger.WithFields(logrus.Fields{
			"find": find,
		}).Warnf("func findVideos meet trouble.")
		logging.SetSpanError(span, err)

		resp = &feed.ListFeedResponse{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
			NextTime:   &nextTimeStamp,
			VideoList:  nil,
		}
		return resp, err
	}
	if len(find) == 0 {
		resp = &feed.ListFeedResponse{
			StatusCode: strings.ServiceOKCode,
			StatusMsg:  strings.ServiceOK,
			NextTime:   nil,
			VideoList:  nil,
		}
		return resp, err
	}

	var actorId uint32 = 0
	if request.ActorId != nil {
		actorId = *request.ActorId
	}
	videos := queryDetailed(ctx, logger, actorId, find)
	if videos == nil {
		logger.WithFields(logrus.Fields{
			"videos": videos,
		}).Warnf("func queryDetailed meet trouble.")
		logging.SetSpanError(span, err)
		resp = &feed.ListFeedResponse{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
			NextTime:   nil,
			VideoList:  nil,
		}
		return resp, err
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		var videoLists []uint32
		for _, item := range videos {
			videoLists = append(videoLists, item.Id)
		}
		// todo 游客访问为什么还要发给推荐系统？
		produceFeed(ctx, models.RecommendEvent{
			ActorId: *request.ActorId,
			VideoId: videoLists,
			Type:    1,
			Source:  config.FeedRpcServerName,
		})
	}()
	wg.Wait()
	resp = &feed.ListFeedResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		NextTime:   &nextTimeStamp,
		VideoList:  videos,
	}
	return resp, err
}

// QueryVideos 聚合查询，返回个性化的视频信息，feed.Model，查询指定视频ID的视频详情
func (s FeedServiceImpl) QueryVideos(ctx context.Context, req *feed.QueryVideosRequest) (resp *feed.QueryVideosResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "QueryVideosService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("FeedService.QueryVideos").WithContext(ctx)

	rst, err := query(ctx, logger, req.ActorId, req.VideoIds)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"rst": rst,
		}).Warnf("func query meet trouble.")
		logging.SetSpanError(span, err)
		resp = &feed.QueryVideosResponse{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
			VideoList:  rst,
		}
		return resp, err
	}

	resp = &feed.QueryVideosResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		VideoList:  rst,
	}
	return resp, err
}

func (s FeedServiceImpl) QueryVideoExisted(ctx context.Context, req *feed.VideoExistRequest) (resp *feed.VideoExistResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "QueryVideoExistedService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("FeedService.QueryVideoExisted").WithContext(ctx)

	var video models.Video
	// 先查二级缓存，查不到就查数据库
	_, err = cached.GetWithFunc(ctx, fmt.Sprintf("VideoExistedCached-%d", req.VideoId), func(ctx context.Context, key string) (string, error) {
		row := database.Client.WithContext(ctx).Where("id = ?", req.VideoId).First(&video)
		if row.Error != nil {
			return "false", row.Error
		}
		return "true", nil
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			logger.WithFields(logrus.Fields{
				"video_id": req.VideoId,
			}).Warnf("gorm.ErrRecordNotFound")
			logging.SetSpanError(span, err)
			resp = &feed.VideoExistResponse{
				StatusCode: strings.ServiceOKCode,
				StatusMsg:  strings.ServiceOK,
				Existed:    false,
			}
			return resp, nil
		} else {
			logger.WithFields(logrus.Fields{
				"video_id": req.VideoId,
			}).Warnf("Error occurred while querying database")
			logging.SetSpanError(span, err)
			resp = &feed.VideoExistResponse{
				StatusCode: strings.FeedServiceInnerErrorCode,
				StatusMsg:  strings.FeedServiceInnerError,
				Existed:    false,
			}
			return resp, err
		}
	}
	resp = &feed.VideoExistResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		Existed:    true,
	}
	return
}

func (s FeedServiceImpl) QueryVideoSummaryAndKeywords(ctx context.Context, req *feed.QueryVideoSummaryAndKeywordsRequest) (resp *feed.QueryVideoSummaryAndKeywordsResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "QueryVideoSummaryAndKeywordsService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("FeedService.QueryVideoSummaryAndKeywords").WithContext(ctx)

	videoExistRes, err := s.QueryVideoExisted(ctx, &feed.VideoExistRequest{
		VideoId: req.VideoId,
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"VideoId": req.VideoId,
		}).Errorf("Cannot check if the video exists")
		logging.SetSpanError(span, err)

		resp = &feed.QueryVideoSummaryAndKeywordsResponse{
			StatusCode: strings.VideoServiceInnerErrorCode,
			StatusMsg:  strings.VideoServiceInnerError,
		}
		return
	}

	if !videoExistRes.Existed {
		resp = &feed.QueryVideoSummaryAndKeywordsResponse{
			StatusCode: strings.UnableToQueryVideoErrorCode,
			StatusMsg:  strings.UnableToQueryVideoError,
		}
		return
	}

	video := models.Video{}
	result := database.Client.WithContext(ctx).Where("id = ?", req.VideoId).First(&video)
	if result.Error != nil {
		logger.WithFields(logrus.Fields{
			"VideoId": req.VideoId,
		}).Errorf("Cannot get video from database")
		logging.SetSpanError(span, err)

		resp = &feed.QueryVideoSummaryAndKeywordsResponse{
			StatusCode: strings.VideoServiceInnerErrorCode,
			StatusMsg:  strings.VideoServiceInnerError,
		}
		return
	}

	resp = &feed.QueryVideoSummaryAndKeywordsResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		Summary:    video.Summary,
		Keywords:   video.Keywords,
	}

	return
}

// findVideos获取发布时间早于latestTime的视频
func findVideos(ctx context.Context, latestTime int64) ([]*models.Video, time.Time, error) {
	logger := logging.LogService("ListVideos.findVideos").WithContext(ctx)

	nextTime := time.UnixMilli(latestTime)

	var videos []*models.Video
	result := database.Client.Where("created_at < ?", nextTime).
		Order("created_at DESC").
		Limit(VideoCount).
		Find(&videos)

	if result.Error != nil {
		logger.WithFields(logrus.Fields{
			"videos": videos,
		}).Warnf("database.Client.Where meet trouble")
		return nil, nextTime, result.Error
	}

	if len(videos) != 0 {
		nextTime = videos[len(videos)-1].CreatedAt
	}

	logger.WithFields(logrus.Fields{
		"latestTime":  time.UnixMilli(latestTime),
		"VideosCount": len(videos),
		"NextTime":    nextTime,
	}).Debugf("Find videos")
	return videos, nextTime, nil
}

func findRecommendVideos(ctx context.Context, recommendVideoId []uint32) ([]*models.Video, error) {
	logger := logging.LogService("ListVideos.findVideos").WithContext(ctx)
	var videos []*models.Video
	var ids []interface{}
	for _, id := range recommendVideoId {
		ids = append(ids, id)
	}
	// GORM 需要 []interface{} 类型来处理 IN 查询的参数
	result := database.Client.WithContext(ctx).Where("id IN ?", ids).Find(&videos)

	if result.Error != nil {
		logger.WithFields(logrus.Fields{
			"videos": videos,
		}).Warnf("database.Client.Where meet trouble")
		return nil, result.Error
	}

	return videos, nil
}

func queryDetailed(ctx context.Context, logger *logrus.Entry, actorId uint32, videos []*models.Video) (respVideoList []*feed.Video) {
	ctx, span := tracing.Tracer.Start(ctx, "queryDetailed")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger = logging.LogService("ListVideos.queryDetailed").WithContext(ctx)

	respVideoList = make([]*feed.Video, len(videos))
	// Init respVideoList
	for i, v := range videos {
		respVideoList[i] = &feed.Video{
			Id:     v.ID,
			Title:  v.Title,
			Author: &user.User{Id: v.UserId},
		}
	}

	// Create userid -> user map to reduce duplicate user info query：简历userID到user的映射来去重
	userMap := make(map[uint32]*user.User)
	for _, video := range videos {
		userMap[video.UserId] = &user.User{}
	}

	userWg := sync.WaitGroup{}
	userWg.Add(len(userMap))
	for userId := range userMap {
		go func(userId uint32) {
			defer userWg.Done()
			userResponse, localErr := UserClient.GetUserInfo(ctx, &user.UserRequest{
				UserId:  userId,
				ActorId: actorId,
			})
			if localErr != nil || userResponse.StatusCode != strings.ServiceOKCode {
				logger.WithFields(logrus.Fields{
					"UserId": userId,
					"cause":  localErr,
				}).Warning("failed to get user info")
				logging.SetSpanError(span, localErr)
			}
			userMap[userId] = userResponse.User
		}(userId)
	}

	wg := sync.WaitGroup{}
	for i, v := range videos {
		wg.Add(4)
		// fill play url
		go func(i int, v *models.Video) {
			defer wg.Done()
			playUrl, localErr := file.GetLink(ctx, v.FileName, v.UserId)
			if localErr != nil {
				logger.WithFields(logrus.Fields{
					"video_id":  v.ID,
					"file_name": v.FileName,
					"err":       localErr,
				}).Warning("failed to fetch play url")
				logging.SetSpanError(span, localErr)
				return
			}
			respVideoList[i].PlayUrl = playUrl
		}(i, v)

		// fill cover url
		go func(i int, v *models.Video) {
			defer wg.Done()
			coverUrl, localErr := file.GetLink(ctx, v.CoverName, v.UserId)
			if localErr != nil {
				logger.WithFields(logrus.Fields{
					"video_id":   v.ID,
					"cover_name": v.CoverName,
					"err":        localErr,
				}).Warning("failed to fetch cover url")
				logging.SetSpanError(span, localErr)
				return
			}
			respVideoList[i].CoverUrl = coverUrl
		}(i, v)

		// fill favorite count
		go func(i int, v *models.Video) {
			defer wg.Done()
			favoriteCount, localErr := FavoriteClient.CountFavorite(ctx, &favorite.CountFavoriteRequest{
				VideoId: v.ID,
			})
			if localErr != nil {
				logger.WithFields(logrus.Fields{
					"video_id": v.ID,
					"err":      localErr,
				}).Warning("failed to fetch favorite count")
				logging.SetSpanError(span, localErr)
				return
			}
			respVideoList[i].FavoriteCount = favoriteCount.Count
		}(i, v)

		// fill comment count
		go func(i int, v *models.Video) {
			defer wg.Done()
			commentCount, localErr := CommentClient.CountComment(ctx, &comment.CountCommentRequest{
				ActorId: actorId,
				VideoId: v.ID,
			})
			if localErr != nil {
				logger.WithFields(logrus.Fields{
					"video_id": v.ID,
					"err":      localErr,
				}).Warning("failed to fetch comment count")
				logging.SetSpanError(span, localErr)
				return
			}
			respVideoList[i].CommentCount = commentCount.CommentCount
		}(i, v)

		// fill is favorite
		if actorId != 0 {
			wg.Add(1)
			go func(i int, v *models.Video) {
				defer wg.Done()
				isFavorite, localErr := FavoriteClient.IsFavorite(ctx, &favorite.IsFavoriteRequest{
					ActorId: actorId,
					VideoId: v.ID,
				})
				if localErr != nil {
					logger.WithFields(logrus.Fields{
						"video_id": v.ID,
						"err":      localErr,
					}).Warning("failed to fetch favorite status")
					logging.SetSpanError(span, localErr)
					return
				}
				respVideoList[i].IsFavorite = isFavorite.Result
			}(i, v)
		} else {
			respVideoList[i].IsFavorite = false
		}
	}
	userWg.Wait()
	wg.Wait()

	for i, respVideo := range respVideoList {
		authorId := respVideo.Author.Id
		respVideoList[i].Author = userMap[authorId]
	}

	return
}

func query(ctx context.Context, logger *logrus.Entry, actorId uint32, videoIds []uint32) (resp []*feed.Video, err error) {
	var videos []*models.Video
	//Gorm的操作，以后不需要在单独开span，通过传ctx的方式完成 "WithContext(ctx)"，如果在函数需要这样写，但是这个的目的是为了获取子 Span 的 ctx
	err = database.Client.WithContext(ctx).Where("Id IN ?", videoIds).Find(&videos).Error
	if err != nil {
		return nil, err
	}
	return queryDetailed(ctx, logger, actorId, videos), nil
}

// todo 转换逻辑是什么
func isUnixMilliTimestamp(s string) (int64, bool) {
	timestamp, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}

	startTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Now().AddDate(100, 0, 0)

	t := time.UnixMilli(timestamp)
	res := t.After(startTime) && t.Before(endTime)

	return timestamp, res
}
