package user

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/rpc/user"
	grpc2 "Gosnip/src/utils/grpc"
	"Gosnip/src/utils/logging"
	"Gosnip/src/web/models"
	"Gosnip/src/web/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

var userClient user.UserServiceClient

func init() {
	userConn := grpc2.Connect(config.UserRpcServerName)
	userClient = user.NewUserServiceClient(userConn)
}

func UserHandler(c *gin.Context) {
	var req models.UserReq
	_, span := tracing.Tracer.Start(c.Request.Context(), "UserInfoHandler")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("GateWay.UserInfo").WithContext(c.Request.Context())

	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusOK, models.UserRes{
			StatusCode: strings.GateWayParamsErrorCode,
			StatusMsg:  strings.GateWayParamsError,
		})
		logging.SetSpanError(span, err)
		return
	}

	resp, err := userClient.GetUserInfo(c.Request.Context(), &user.UserRequest{
		UserId:  req.UserId,
		ActorId: req.ActorId,
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Error when gateway get info from UserInfo Service")
		logging.SetSpanError(span, err)
		// c.Render 作用是将服务返回的 protobuf 响应数据 (resp) 以 JSON 格式返回给客户端。
		c.Render(http.StatusOK, utils.CustomJSON{Data: resp, Context: c})
		return
	}

	c.Render(http.StatusOK, utils.CustomJSON{Data: resp, Context: c})
}
