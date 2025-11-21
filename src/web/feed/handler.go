package feed

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/rpc/feed"
	grpc2 "Gosnip/src/utils/grpc"
	"Gosnip/src/utils/logging"
	"Gosnip/src/web/models"
	"Gosnip/src/web/utils"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	_ "github.com/mbobakov/grpc-consul-resolver"
	"github.com/sirupsen/logrus"
)

var Client feed.FeedServiceClient

func ListVideosByRecommendHandle(c *gin.Context) {
	var req models.ListVideosReq
	_, span := tracing.Tracer.Start(c.Request.Context(), "Feed-ListVideosByRecommendHandle")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("GateWay.Videos").WithContext(c.Request.Context())

	if err := c.ShouldBindQuery(&req); err != nil {
		logger.WithFields(logrus.Fields{
			"latestTime": req.LatestTime,
			"err":        err,
		}).Warnf("Error when trying to bind query")
		c.JSON(http.StatusOK, models.ListVideosRes{
			StatusCode: strings.GateWayParamsErrorCode,
			StatusMsg:  strings.GateWayParamsError,
			NextTime:   nil,
			VideoList:  nil,
		})
		return
	}

	latestTime := req.LatestTime
	actorId := uint32(req.ActorId)
	var res *feed.ListFeedResponse
	var err error
	anonymity, err := strconv.ParseUint(config.EnvCfg.AnonymityUser, 10, 32) // 匿名用户
	if err != nil {
		c.JSON(http.StatusOK, models.ListVideosRes{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
			NextTime:   nil,
			VideoList:  nil,
		})
		return
	}
	if actorId == uint32(anonymity) {
		// 游客用户返回默认的视频列表
		res, err = Client.ListVideos(c.Request.Context(), &feed.ListFeedRequest{
			LatestTime: &latestTime,
			ActorId:    &actorId,
		})
	} else {
		// 注册用户返回推荐视频列表
		res, err = Client.ListVideosByRecommend(c.Request.Context(), &feed.ListFeedRequest{
			LatestTime: &latestTime,
			ActorId:    &actorId,
		})
	}
	if err != nil {
		logger.WithFields(logrus.Fields{
			"LatestTime": latestTime,
			"Err":        err,
		}).Warnf("Error when trying to connect with FeedService")
		c.JSON(http.StatusOK, models.ListVideosRes{
			StatusCode: strings.FeedServiceInnerErrorCode,
			StatusMsg:  strings.FeedServiceInnerError,
			NextTime:   nil,
			VideoList:  nil,
		})
		return
	}
	c.Render(http.StatusOK, utils.CustomJSON{Data: res, Context: c})
}

func init() {
	conn := grpc2.Connect(config.FeedRpcServerName)
	Client = feed.NewFeedServiceClient(conn)
}
