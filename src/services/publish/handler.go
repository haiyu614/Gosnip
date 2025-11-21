package main

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/models"
	"Gosnip/src/rpc/feed"
	"Gosnip/src/rpc/publish"
	"Gosnip/src/rpc/user"
	"Gosnip/src/storage/cached"
	"Gosnip/src/storage/database"
	"Gosnip/src/storage/file"
	"Gosnip/src/storage/redis"
	grpc2 "Gosnip/src/utils/grpc"
	"Gosnip/src/utils/logging"
	"Gosnip/src/utils/pathgen"
	"Gosnip/src/utils/rabbitmq"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/go-redis/redis_rate/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

type PublishServiceImpl struct {
	publish.PublishServiceServer
}

var conn *amqp.Connection

var channel *amqp.Channel

var FeedClient feed.FeedServiceClient
var userClient user.UserServiceClient

func exitOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func CloseMQConn() {
	if err := conn.Close(); err != nil {
		panic(err)
	}

	if err := channel.Close(); err != nil {
		panic(err)
	}
}

var createVideoLimitKeyPrefix = config.EnvCfg.RedisPrefix + "publish_freq_limit"

const createVideoMaxQPS = 3

// Return redis key to record the amount of CreateVideo query of an actor, e.g., publish_freq_limit-1-1669524458
func createVideoLimitKey(userId uint32) string {
	return fmt.Sprintf("%s-%d", createVideoLimitKeyPrefix, userId)
}

func (a PublishServiceImpl) New() {
	FeedRpcConn := grpc2.Connect(config.FeedRpcServerName)
	FeedClient = feed.NewFeedServiceClient(FeedRpcConn)

	userRpcConn := grpc2.Connect(config.UserRpcServerName)
	userClient = user.NewUserServiceClient(userRpcConn)

	var err error

	conn, err = amqp.Dial(rabbitmq.BuildMQConnAddr())
	exitOnError(err)

	channel, err = conn.Channel()
	exitOnError(err)

	// 延迟消息交换机
	exchangeArgs := amqp.Table{
		"x-delayed-type": "topic",
	}
	err = channel.ExchangeDeclare(
		strings.VideoExchange,
		"x-delayed-message", //"topic",
		true,
		false,
		false,
		false,
		exchangeArgs,
	)
	exitOnError(err)

	// 视频信息采集队列（处理封面/水印等）
	_, err = channel.QueueDeclare(
		strings.VideoPicker, //视频信息采集(封面/水印)
		true,
		false,
		false,
		false,
		nil,
	)
	exitOnError(err)

	// 视频摘要生成队列
	_, err = channel.QueueDeclare(
		strings.VideoSummary,
		true,
		false,
		false,
		false,
		nil,
	)
	exitOnError(err)

	// 将 VideoPicker 队列绑定到交换机
	err = channel.QueueBind(
		strings.VideoPicker,
		strings.VideoPicker,
		strings.VideoExchange,
		false,
		nil,
	)
	exitOnError(err)

	// 将 VideoSummary 队列绑定到交换机
	err = channel.QueueBind(
		strings.VideoSummary,
		strings.VideoSummary,
		strings.VideoExchange,
		false,
		nil,
	)
	exitOnError(err)
}

func (a PublishServiceImpl) ListVideo(ctx context.Context, req *publish.ListVideoRequest) (resp *publish.ListVideoResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "ListVideoService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("PublishServiceImpl.ListVideo").WithContext(ctx)

	// Check if user exist
	userExistResp, err := userClient.GetUserExistInformation(ctx, &user.UserExistRequest{
		UserId: req.UserId,
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Query user existence happens error")
		logging.SetSpanError(span, err)
		resp = &publish.ListVideoResponse{
			StatusCode: strings.UserServiceInnerErrorCode,
			StatusMsg:  strings.UserServiceInnerError,
		}
		return
	}

	if !userExistResp.Existed {
		logger.WithFields(logrus.Fields{
			"UserID": req.UserId,
		}).Errorf("User ID does not exist")
		logging.SetSpanError(span, err)
		resp = &publish.ListVideoResponse{
			StatusCode: strings.UserDoNotExistedCode,
			StatusMsg:  strings.UserDoNotExisted,
		}
		return
	}

	var videos []models.Video
	err = database.Client.WithContext(ctx).
		Where("user_id = ?", req.UserId).
		Order("created_at DESC").
		Find(&videos).Error
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("failed to query video")
		logging.SetSpanError(span, err)
		resp = &publish.ListVideoResponse{
			StatusCode: strings.PublishServiceInnerErrorCode,
			StatusMsg:  strings.PublishServiceInnerError,
		}
		return
	}
	videoIds := make([]uint32, 0, len(videos))
	for _, video := range videos {
		videoIds = append(videoIds, video.ID)
	}

	// 返回个性化的某用户的视频信息
	queryVideoResp, err := FeedClient.QueryVideos(ctx, &feed.QueryVideosRequest{
		ActorId:  req.ActorId,
		VideoIds: videoIds,
	})
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("queryVideoResp failed to obtain")
		logging.SetSpanError(span, err)
		resp = &publish.ListVideoResponse{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
		}
		return
	}

	logger.WithFields(logrus.Fields{
		"response": resp,
	}).Debug("all process done, ready to launch response")
	resp = &publish.ListVideoResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		VideoList:  queryVideoResp.VideoList,
	}
	return
}

func (a PublishServiceImpl) CountVideo(ctx context.Context, req *publish.CountVideoRequest) (resp *publish.CountVideoResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "CountVideoService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("PublishServiceImpl.CountVideo").WithContext(ctx)

	countStringKey := fmt.Sprintf("VideoCount-%d", req.UserId)
	countString, err := cached.GetWithFunc(ctx, countStringKey,
		func(ctx context.Context, key string) (string, error) {
			rCount, err := count(ctx, req.UserId)
			return strconv.FormatInt(rCount, 10), err
		})

	if err != nil {
		cached.TagDelete(ctx, "VideoCount")
		logger.WithFields(logrus.Fields{
			"err":     err,
			"user_id": req.UserId,
		}).Errorf("failed to count video")
		logging.SetSpanError(span, err)

		resp = &publish.CountVideoResponse{
			StatusCode: strings.PublishServiceInnerErrorCode,
			StatusMsg:  strings.PublishServiceInnerError,
		}
		return
	}
	rCount, _ := strconv.ParseUint(countString, 10, 64)

	resp = &publish.CountVideoResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		Count:      uint32(rCount),
	}
	return
}

func count(ctx context.Context, userId uint32) (count int64, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "CountVideo")
	defer span.End()
	logger := logging.LogService("PublishService.CountVideo").WithContext(ctx)

	result := database.Client.Model(&models.Video{}).WithContext(ctx).Where("user_id = ?", userId).Count(&count)

	if result.Error != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Error when counting video")
		logging.SetSpanError(span, err)
	}
	return count, result.Error
}

func (a PublishServiceImpl) CreateVideo(ctx context.Context, request *publish.CreateVideoRequest) (resp *publish.CreateVideoResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "CreateVideoService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("PublishService.CreateVideo").WithContext(ctx)

	logger.WithFields(logrus.Fields{
		"ActorId": request.ActorId,
		"Title":   request.Title,
	}).Infof("Create video requested.")

	// Rate limiting
	limiter := redis_rate.NewLimiter(redis.Client)
	limiterKey := createVideoLimitKey(request.ActorId)
	// 检查是否允许当前请求通过，限制为每秒最多 createVideoMaxQPS 个请求。
	limiterRes, err := limiter.Allow(ctx, limiterKey, redis_rate.PerSecond(createVideoMaxQPS))
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":     err,
			"ActorId": request.ActorId,
		}).Errorf("CreateVideo limiter error")

		resp = &publish.CreateVideoResponse{
			StatusCode: strings.VideoServiceInnerErrorCode,
			StatusMsg:  strings.VideoServiceInnerError,
		}
		return
	}
	if limiterRes.Allowed == 0 {
		logger.WithFields(logrus.Fields{
			"err":     err,
			"ActorId": request.ActorId,
		}).Errorf("Create video query too frequently by user %d", request.ActorId)

		resp = &publish.CreateVideoResponse{
			StatusCode: strings.PublishVideoLimitedCode,
			StatusMsg:  strings.PublishVideoLimited,
		}
		return
	}

	// 检测视频格式
	detectedContentType := http.DetectContentType(request.Data)
	if detectedContentType != "video/mp4" {
		logger.WithFields(logrus.Fields{
			"content_type": detectedContentType,
		}).Debug("invalid content type")
		resp = &publish.CreateVideoResponse{
			StatusCode: strings.InvalidContentTypeCode,
			StatusMsg:  strings.InvalidContentType,
		}
		return
	}
	// byte[] -> reader
	//! 使用io.Reader流式读取数据，避免将整个文件加载到内存中。
	reader := bytes.NewReader(request.Data)

	// 创建一个新的随机数生成器
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 生成一个随机的32位无符号整数作为视频的唯一标识符。
	// todo 直接使用32位随机数不能完全保证唯一性，考虑使用自增id或者uuid？
	videoId := r.Uint32()
	fileName := pathgen.GenerateRawVideoName(request.ActorId, request.Title, videoId)
	coverName := pathgen.GenerateCoverName(request.ActorId, request.Title, videoId)
	//* 上传视频，将文件保存到文件系统
	//todo 需要异步上传吗？
	_, err = file.Upload(ctx, fileName, reader)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"file_name": fileName,
			"err":       err,
		}).Debug("failed to upload video")
		resp = &publish.CreateVideoResponse{
			StatusCode: strings.VideoServiceInnerErrorCode,
			StatusMsg:  strings.VideoServiceInnerError,
		}
		return
	}
	logger.WithFields(logrus.Fields{
		"file_name": fileName,
	}).Debug("uploaded video")

	raw := &models.RawVideo{
		ActorId:   request.ActorId,
		VideoId:   videoId,
		Title:     request.Title,
		FileName:  fileName,
		CoverName: coverName,
	}
	result := database.Client.Create(&raw)
	if result.Error != nil {
		logger.WithFields(logrus.Fields{
			"file_name":  raw.FileName,
			"cover_name": raw.CoverName,
			"err":        err,
		}).Errorf("Error when updating rawVideo information to database")
		logging.SetSpanError(span, result.Error)
	}

	marshal, err := json.Marshal(raw)
	if err != nil {
		resp = &publish.CreateVideoResponse{
			StatusCode: strings.VideoServiceInnerErrorCode,
			StatusMsg:  strings.VideoServiceInnerError,
		}
		return
	}

	// Context 注入到 RabbitMQ 中
	headers := rabbitmq.InjectAMQPHeaders(ctx)

	//? 将消息发送到不同的队列中，视频信息采集队列（处理封面/水印等），视频摘要生成队列
	routingKeys := []string{strings.VideoPicker, strings.VideoSummary}
	for _, key := range routingKeys {
		// Send raw video to all queues bound the exchange
		err = channel.PublishWithContext(ctx, strings.VideoExchange, key, false, false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent, // 持久化消息
				ContentType:  "text/plain",    // 消息体（视频信息）
				Body:         marshal,         // 消息体（视频信息）
				Headers:      headers,         // 追踪头信息，包含注入的上下文信息
			})

		if err != nil {
			resp = &publish.CreateVideoResponse{
				StatusCode: strings.VideoServiceInnerErrorCode,
				StatusMsg:  strings.VideoServiceInnerError,
			}
			return
		}
	}

	// 当发布了新的视频，旧的记录了视频数量的缓存就失效了，因此在这里删除缓存，使缓存失效，下次调用count重新计算
	countStringKey := fmt.Sprintf("VideoCount-%d", request.ActorId)
	cached.TagDelete(ctx, countStringKey)
	resp = &publish.CreateVideoResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
	}
	return
}
