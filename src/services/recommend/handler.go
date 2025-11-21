package main

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/gorse"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/rpc/recommend"
	"Gosnip/src/storage/redis"
	"Gosnip/src/utils/logging"
	"context"
	"errors"
	"fmt"
	"strconv"

	redis2 "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type RecommendServiceImpl struct {
	recommend.RecommendServiceServer
}

func (a RecommendServiceImpl) New() {
	gorseClient = gorse.NewGorseClient(config.EnvCfg.GorseAddr, config.EnvCfg.GorseApiKey)
}

var gorseClient *gorse.GorseClient

// GetRecommendInformation 获取推荐视频列表
// - offset == -1：
//   - 业务场景：1、用户下拉刷新获取新的推荐内容 2、用户首次访问推荐页面 3、系统需要为用户生成一批全新的推荐视频
//   - 特点：1、需要防止重复推荐（通过 Redis Set 集合记录已推荐的视频 ID） 2、从offset=0开始获取推荐视频
//
// - offset != -1：
//   - 业务场景：1、用户上拉加载更多推荐内容 2、基于已有推荐位置继续获取后续内容 3、分页浏览推荐视频列表
//   - 特点：1、直接使用 offset 参数进行分页 2、不进行重复推荐检查（可能重复推荐已看过的视频）
func (a RecommendServiceImpl) GetRecommendInformation(ctx context.Context, request *recommend.RecommendRequest) (resp *recommend.RecommendResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "GetRecommendService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("RecommendService.GetRecommend").WithContext(ctx)

	var offset int
	if request.Offset == -1 {
		ids, err := getVideoIds(ctx, strconv.Itoa(int(request.UserId)), int(request.Number))

		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when getting recommend user item with default logic")
			logging.SetSpanError(span, err)
			resp = &recommend.RecommendResponse{
				StatusCode: strings.RecommendServiceInnerErrorCode,
				StatusMsg:  strings.RecommendServiceInnerError,
				VideoList:  nil,
			}
			return resp, err
		}

		resp = &recommend.RecommendResponse{
			StatusCode: strings.ServiceOKCode,
			StatusMsg:  strings.ServiceOK,
			VideoList:  ids,
		}
		return resp, nil

	} else {
		offset = int(request.Offset)
	}

	videos, err := gorseClient.GetItemRecommend(ctx, strconv.Itoa(int(request.UserId)), []string{}, "read", "5m", int(request.Number), offset)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Error when getting recommend user item")
		logging.SetSpanError(span, err)
		resp = &recommend.RecommendResponse{
			StatusCode: strings.RecommendServiceInnerErrorCode,
			StatusMsg:  strings.RecommendServiceInnerError,
			VideoList:  nil,
		}
		return
	}

	var videoIds []uint32
	for _, id := range videos {
		parseUint, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when getting recommend user item")
			logging.SetSpanError(span, err)
			resp = &recommend.RecommendResponse{
				StatusCode: strings.RecommendServiceInnerErrorCode,
				StatusMsg:  strings.RecommendServiceInnerError,
				VideoList:  nil,
			}
			return resp, err
		}
		videoIds = append(videoIds, uint32(parseUint))
	}

	logger.WithFields(logrus.Fields{
		"offset":   offset,
		"videoIds": videoIds,
	}).Infof("Get recommend with offset")
	resp = &recommend.RecommendResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
		VideoList:  videoIds,
	}
	return
}

// RegisterRecommendUser 为用户订阅推荐服务
func (a RecommendServiceImpl) RegisterRecommendUser(ctx context.Context, request *recommend.RecommendRegisterRequest) (resp *recommend.RecommendRegisterResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "RegisterRecommendService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("RecommendService.RegisterRecommend").WithContext(ctx)

	_, err = gorseClient.InsertUsers(ctx, []gorse.User{
		{
			UserId:  strconv.Itoa(int(request.UserId)),
			Comment: request.Username,
		},
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Error when creating recommend user")
		logging.SetSpanError(span, err)
		resp = &recommend.RecommendRegisterResponse{
			StatusCode: strings.RecommendServiceInnerErrorCode,
			StatusMsg:  strings.RecommendServiceInnerError,
		}
		return
	}

	resp = &recommend.RecommendRegisterResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
	}
	return
}

// getVideoIds 获取推荐视频，且只会返回不在缓存中的新视频，返回数量为num（可能更少）
func getVideoIds(ctx context.Context, actorId string, num int) (ids []uint32, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "GetRecommendAutoService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("RecommendService.GetRecommendAuto").WithContext(ctx)

	key := fmt.Sprintf("%s-RecommendAutoService-%s", config.EnvCfg.RedisPrefix, actorId)
	offset := 0

	for len(ids) < num {
		// 对 gorse 推荐系统发请求
		vIds, err := gorseClient.GetItemRecommend(ctx, actorId, []string{}, "read", "5m", num, offset)
		logger.WithFields(logrus.Fields{
			"vIds": vIds,
		}).Debugf("Fetch data from Gorse")

		if err != nil {
			logger.WithFields(logrus.Fields{
				"err":     err,
				"actorId": actorId,
				"num":     num,
			}).Errorf("Error when getting item recommend")
			return nil, err
		}

		for _, id := range vIds {
			// 用于检查一个元素是否是一个集合(Set)的成员
			res := redis.Client.SIsMember(ctx, key, id)
			// 发生错误，且不是不在集合中的错误
			if res.Err() != nil && !errors.Is(res.Err(), redis2.Nil) {
				logger.WithFields(logrus.Fields{
					// todo err 应该是 res.Err() 才对？
					"err":     err,
					"actorId": actorId,
					"num":     num,
				}).Errorf("Error when getting item recommend")
				return nil, err
			}

			logger.WithFields(logrus.Fields{
				"id":  id,
				"res": res,
			}).Debugf("Get id in redis information")

			// 推荐视频不在缓存中，将其加入缓存
			if !res.Val() {
				uintId, err := strconv.ParseUint(id, 10, 32)
				if err != nil {
					logger.WithFields(logrus.Fields{
						"err":     err,
						"actorId": actorId,
						"num":     num,
						"uint":    id,
					}).Errorf("Error when parsing uint")
					return nil, err
				}
				// ids 是不断append累加的，多次请求videoIds不会将ids覆盖
				// 只会将不在缓存中的id加入响应ids
				ids = append(ids, uint32(uintId))
			}
		}

		// 转换成string类型方便加入redis的集合中
		var idsStr []interface{}
		for _, id := range ids {
			idsStr = append(idsStr, strconv.FormatUint(uint64(id), 10))
		}

		logger.WithFields(logrus.Fields{
			"actorId": actorId,
			"ids":     idsStr,
		}).Infof("Get recommend information")

		if len(idsStr) != 0 {
			// SAdd将切片的每个元素都添加到集合中，非string类型会被自动转换为string
			res := redis.Client.SAdd(ctx, key, idsStr)
			if res.Err() != nil {
				// todo 这里的err哪来的？应该多余了吧
				if err != nil {
					logger.WithFields(logrus.Fields{
						"err":     err,
						"actorId": actorId,
						"num":     num,
						"ids":     idsStr,
					}).Errorf("Error when locking redis ids read state")
					return nil, err
				}
			}
		}

		// vIds是直接向GORSE推荐系统请求到的数量，按照api响应应该准确等于请求的数量，如果不等于（即小于），只能说明没有数据了，此时应该结束请求
		if len(vIds) != num {
			break
		}
		offset += num
	}
	return
}
