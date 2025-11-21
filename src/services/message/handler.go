package main

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/models"
	"Gosnip/src/rpc/chat"
	"Gosnip/src/rpc/feed"
	"Gosnip/src/rpc/recommend"
	"Gosnip/src/rpc/relation"
	"Gosnip/src/rpc/user"
	"Gosnip/src/storage/database"
	"Gosnip/src/storage/redis"
	grpc2 "Gosnip/src/utils/grpc"
	"Gosnip/src/utils/logging"
	"Gosnip/src/utils/ptr"
	"Gosnip/src/utils/rabbitmq"
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/robfig/cron/v3"

	"time"

	"github.com/go-redis/redis_rate/v10"
	"gorm.io/gorm"

	"github.com/sirupsen/logrus"
)

var userClient user.UserServiceClient
var recommendClient recommend.RecommendServiceClient
var relationClient relation.RelationServiceClient
var feedClient feed.FeedServiceClient
var chatClient chat.ChatServiceClient

type MessageServiceImpl struct {
	chat.ChatServiceServer
}

// 连接
var conn *amqp.Connection
var channel *amqp.Channel

// 输出
func failOnError(err error, msg string) {
	//打日志
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf(msg)
	}
}

func (c MessageServiceImpl) New() {
	var err error

	conn, err = amqp.Dial(rabbitmq.BuildMQConnAddr())
	failOnError(err, "Failed to connect to RabbitMQ")

	channel, err = conn.Channel()
	failOnError(err, "Failed to open a channel")

	err = channel.ExchangeDeclare(
		strings.MessageExchange,
		"x-delayed-message",
		true, false, false, false,
		amqp.Table{
			"x-delayed-type": "topic",
		},
	)
	failOnError(err, "Failed to get exchange")

	_, err = channel.QueueDeclare(
		strings.MessageCommon,
		true, false, false, false,
		nil,
	)
	failOnError(err, "Failed to define queue")

	_, err = channel.QueueDeclare(
		strings.MessageGPT,
		true, false, false, false,
		nil,
	)

	failOnError(err, "Failed to define queue")
	_, err = channel.QueueDeclare(
		strings.MessageES,
		true, false, false, false,
		nil,
	)

	failOnError(err, "Failed to define queue")

	err = channel.QueueBind(
		strings.MessageCommon,
		"message.#",
		strings.MessageExchange,
		false,
		nil,
	)
	failOnError(err, "Failed to bind queue to exchange")

	err = channel.QueueBind(
		strings.MessageES,
		"message.#",
		strings.MessageExchange,
		false,
		nil,
	)
	failOnError(err, "Failed to bind queue to exchange")

	err = channel.QueueBind(
		strings.MessageGPT,
		strings.MessageGptActionEvent,
		strings.MessageExchange,
		false,
		nil,
	)
	failOnError(err, "Failed to bind queue to exchange")

	userRpcConn := grpc2.Connect(config.UserRpcServerName)
	userClient = user.NewUserServiceClient(userRpcConn)

	recommendRpcConn := grpc2.Connect(config.RecommendRpcServiceName)
	recommendClient = recommend.NewRecommendServiceClient(recommendRpcConn)

	relationRpcConn := grpc2.Connect(config.RelationRpcServerName)
	relationClient = relation.NewRelationServiceClient(relationRpcConn)

	feedRpcConn := grpc2.Connect(config.FeedRpcServerName)
	feedClient = feed.NewFeedServiceClient(feedRpcConn)

	//todo 自己调用自己的客户端，何意味？
	chatRpcConn := grpc2.Connect(config.MessageRpcServerName)
	chatClient = chat.NewChatServiceClient(chatRpcConn)

	//
	cronRunner := cron.New(cron.WithSeconds())

	//_, err = cronRunner.AddFunc("0 0 18 * * *", sendMagicMessage) // execute every 18:00
	// 定时任务
	_, err = cronRunner.AddFunc("@every 5m", sendMagicMessage) // execute every minute [for test]

	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Cannot start SendMagicMessage cron job")
	}

	cronRunner.Start()

}

func CloseMQConn() {
	if err := channel.Close(); err != nil {
		failOnError(err, "close channel error")
	}
	if err := conn.Close(); err != nil {
		failOnError(err, "close conn error")
	}
}

//发送消息

var chatActionLimitKeyPrefix = config.EnvCfg.RedisPrefix + "chat_freq_limit"

const chatActionMaxQPS = 3

func chatActionLimitKey(userId uint32) string {
	return fmt.Sprintf("%s-%d", chatActionLimitKeyPrefix, userId)
}

// ChatAction 将消息发送给消息队列，然后由消费者存储到数据库
func (c MessageServiceImpl) ChatAction(ctx context.Context, request *chat.ActionRequest) (res *chat.ActionResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "ChatActionService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("ChatService.ActionMessage").WithContext(ctx)

	logger.WithFields(logrus.Fields{
		"ActorId":      request.ActorId,
		"user_id":      request.UserId,
		"action_type":  request.ActionType,
		"content_text": request.Content,
	}).Debugf("Process start")

	// Rate limiting
	limiter := redis_rate.NewLimiter(redis.Client)
	limiterKey := chatActionLimitKey(request.ActorId)
	limiterRes, err := limiter.Allow(ctx, limiterKey, redis_rate.PerSecond(chatActionMaxQPS))
	if err != nil {
		logger.WithFields(logrus.Fields{
			"ActorId":      request.ActorId,
			"user_id":      request.UserId,
			"action_type":  request.ActionType,
			"content_text": request.Content,
		}).Errorf("ChatAction limiter error")

		res = &chat.ActionResponse{
			StatusCode: strings.UnableToAddMessageErrorCode,
			StatusMsg:  strings.UnableToAddMessageError,
		}
		return
	}
	if limiterRes.Allowed == 0 {
		logger.WithFields(logrus.Fields{
			"ActorId":      request.ActorId,
			"user_id":      request.UserId,
			"action_type":  request.ActionType,
			"content_text": request.Content,
		}).Errorf("Chat action query too frequently by user %d", request.ActorId)

		res = &chat.ActionResponse{
			StatusCode: strings.ChatActionLimitedCode,
			StatusMsg:  strings.ChatActionLimitedError,
		}
		return
	}

	userResponse, err := userClient.GetUserExistInformation(ctx, &user.UserExistRequest{
		UserId: request.UserId,
	})

	if err != nil || userResponse.StatusCode != strings.ServiceOKCode {
		logger.WithFields(logrus.Fields{
			"err":          err,
			"ActorId":      request.ActorId,
			"user_id":      request.UserId,
			"action_type":  request.ActionType,
			"content_text": request.Content,
		}).Errorf("User service error")
		logging.SetSpanError(span, err)

		return &chat.ActionResponse{
			StatusCode: strings.UnableToAddMessageErrorCode,
			StatusMsg:  strings.UnableToAddMessageError,
		}, err
	}

	if !userResponse.Existed {
		return &chat.ActionResponse{
			StatusCode: strings.UserDoNotExistedCode,
			StatusMsg:  strings.UserNotExisted,
		}, nil
	}

	res, err = addMessage(ctx, request.ActorId, request.UserId, request.Content)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err":          err,
			"user_id":      request.UserId,
			"action_type":  request.ActionType,
			"content_text": request.Content,
		}).Errorf("database insert error")
		logging.SetSpanError(span, err)
		return res, err
	}

	logger.WithFields(logrus.Fields{
		"response": res,
	}).Debugf("Process done.")

	return res, err
}

// Chat 从数据库获取聊天记录并返回给客户端，ActorID的用户查询和UserID的用户的聊天记录
func (c MessageServiceImpl) Chat(ctx context.Context, request *chat.ChatRequest) (resp *chat.ChatResponse, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "ChatService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("ChatService.chat").WithContext(ctx)
	logger.WithFields(logrus.Fields{
		"user_id":      request.UserId,
		"ActorId":      request.ActorId,
		"pre_msg_time": request.PreMsgTime,
	}).Debugf("Process start")

	userResponse, err := userClient.GetUserExistInformation(ctx, &user.UserExistRequest{
		UserId: request.UserId,
	})

	if err != nil || userResponse.StatusCode != strings.ServiceOKCode {
		logger.WithFields(logrus.Fields{
			"err":     err,
			"ActorId": request.ActorId,
			"user_id": request.UserId,
		}).Errorf("User service error")
		logging.SetSpanError(span, err)

		resp = &chat.ChatResponse{
			StatusCode: strings.UnableToQueryMessageErrorCode,
			StatusMsg:  strings.UnableToQueryMessageError,
		}
		return
	}

	if !userResponse.Existed {
		return &chat.ChatResponse{
			StatusCode: strings.UserDoNotExistedCode,
			StatusMsg:  strings.UserNotExisted,
		}, nil
	}

	toUserId := request.UserId
	fromUserId := request.ActorId

	conversationId := fmt.Sprintf("%d_%d", toUserId, fromUserId)

	if toUserId > fromUserId {
		conversationId = fmt.Sprintf("%d_%d", fromUserId, toUserId)
	}
	//这个地方应该取出多少条消息？
	//TODO 看怎么需要改一下

	var pMessageList []models.Message
	var result *gorm.DB
	//todo 初次加载返回全部消息？应该怎么设计？
	if request.PreMsgTime == 0 {
		result = database.Client.WithContext(ctx).
			Where("conversation_id=?", conversationId).
			Order("created_at").
			Find(&pMessageList)
	} else {
		//todo 这是历史聊天记录查询？
		result = database.Client.WithContext(ctx).
			Where("conversation_id=?", conversationId).
			Where("created_at > ?", time.UnixMilli(int64(request.PreMsgTime)).Add(100*time.Millisecond)).
			Order("created_at").
			Find(&pMessageList)
	}

	if result.Error != nil {
		logger.WithFields(logrus.Fields{
			"err":          result.Error,
			"user_id":      request.UserId,
			"ActorId":      request.ActorId,
			"pre_msg_time": request.PreMsgTime,
		}).Errorf("ChatServiceImpl list chat failed to response when listing message, database err")
		logging.SetSpanError(span, err)

		resp = &chat.ChatResponse{
			StatusCode: strings.UnableToQueryMessageErrorCode,
			StatusMsg:  strings.UnableToQueryMessageError,
		}
		return
	}

	rMessageList := make([]*chat.Message, 0, len(pMessageList))
	if request.PreMsgTime == 0 {
		for _, pMessage := range pMessageList {
			// 正常加载返回双方消息
			rMessageList = append(rMessageList, &chat.Message{
				Id:         pMessage.ID,
				Content:    pMessage.Content,
				CreateTime: uint64(pMessage.CreatedAt.UnixMilli()),
				FromUserId: ptr.Ptr(pMessage.FromUserId),
				ToUserId:   ptr.Ptr(pMessage.ToUserId),
			})
		}
	} else {
		// todo 分页历史查询只返回给查询者自己的消息？双方的消息都返回不是更合理吗？
		for _, pMessage := range pMessageList {
			if pMessage.ToUserId == request.ActorId {
				rMessageList = append(rMessageList, &chat.Message{
					Id:         pMessage.ID,
					Content:    pMessage.Content,
					CreateTime: uint64(pMessage.CreatedAt.UnixMilli()),
					FromUserId: ptr.Ptr(pMessage.FromUserId),
					ToUserId:   ptr.Ptr(pMessage.ToUserId),
				})
			}
		}
	}

	resp = &chat.ChatResponse{
		StatusCode:  strings.ServiceOKCode,
		StatusMsg:   strings.ServiceOK,
		MessageList: rMessageList,
	}

	logger.WithFields(logrus.Fields{
		"response": resp,
	}).Debugf("Process done.")

	return
}

// addMessage 将消息发送给消息队列：由消费者接受持久化到数据库
func addMessage(ctx context.Context, fromUserId uint32, toUserId uint32, Context string) (resp *chat.ActionResponse, err error) {
	conversationId := fmt.Sprintf("%d_%d", toUserId, fromUserId)

	if toUserId > fromUserId {
		conversationId = fmt.Sprintf("%d_%d", fromUserId, toUserId)
	}
	message := models.Message{
		ToUserId:       toUserId,
		FromUserId:     fromUserId,
		Content:        Context,
		ConversationId: conversationId,
	}
	message.Model = gorm.Model{
		CreatedAt: time.Now(),
	}

	body, err := json.Marshal(message)
	if err != nil {
		resp = &chat.ActionResponse{
			StatusCode: strings.UnableToAddMessageErrorCode,
			StatusMsg:  strings.UnableToAddMessageError,
		}
		return
	}

	headers := rabbitmq.InjectAMQPHeaders(ctx)
	// 如果是GPT消息，发给GPT的消息队列，之后会被消费者接受进行实际的API调用，调用完的结果会和普通消息一样再次发送给消息队列，只不过是MessageActionEvent普通的队列，由这个队列进行持久化存储
	if message.ToUserId == config.EnvCfg.MagicUserId {
		logging.Logger.WithFields(logrus.Fields{
			"routing": strings.MessageGptActionEvent,
			"message": message,
		}).Debugf("Publishing message to %s", strings.MessageGptActionEvent)
		err = channel.PublishWithContext(
			ctx,
			strings.MessageExchange,
			strings.MessageGptActionEvent,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         body,
				Headers:      headers,
			})
		// 这里产生的err不会会直接return
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Failed to publish message to %s", strings.MessageGptActionEvent)
		}

	} else {
		logging.Logger.WithFields(logrus.Fields{
			"routing": strings.MessageActionEvent,
			"message": message,
		}).Debugf("Publishing message to %s", strings.MessageActionEvent)
		err = channel.PublishWithContext(
			ctx,
			strings.MessageExchange,
			strings.MessageActionEvent,
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         body,
				Headers:      headers,
			})
		// 这里产生的err不会会直接return
		if err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Failed to publish message to %s", strings.MessageActionEvent)
		}
	}

	// result := database.Client.WithContext(ctx).Create(&message)

	// 检查上面是否有产生err
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorf("Error when publishing the message to mq")
		resp = &chat.ActionResponse{
			StatusCode: strings.UnableToAddMessageErrorCode,
			StatusMsg:  strings.UnableToAddMessageError,
		}
		return
	}

	resp = &chat.ActionResponse{
		StatusCode: strings.ServiceOKCode,
		StatusMsg:  strings.ServiceOK,
	}
	return

}

// sendMagicMessage 作为定时任务被调用
func sendMagicMessage() {
	ctx, span := tracing.Tracer.Start(context.Background(), "SendMagicMessageService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("ChatService.SendMessageService").WithContext(ctx)

	logger.Debugf("Start ChatService.SendMessageService at %s", time.Now())

	// Get all friends of magic user
	friendsResponse, err := relationClient.GetFriendList(ctx, &relation.FriendListRequest{
		ActorId: config.EnvCfg.MagicUserId,
		UserId:  config.EnvCfg.MagicUserId,
	})

	if err != nil {
		logger.WithFields(logrus.Fields{
			"ActorId": config.EnvCfg.MagicUserId,
			"Err":     err,
		}).Errorf("Cannot get friend list of magic user")
		logging.SetSpanError(span, err)
		return
	}

	// Send magic message to every friends
	friends := friendsResponse.UserList
	videoMap := make(map[uint32]*feed.Video)
	for _, friend := range friends {
		// Get recommend video id
		recommendResponse, err := recommendClient.GetRecommendInformation(ctx, &recommend.RecommendRequest{
			UserId: friend.Id,
			Offset: 0,
			Number: 1, // 只返回第一条推荐视频
		})

		if err != nil || len(recommendResponse.VideoList) < 1 {
			logger.WithFields(logrus.Fields{
				"UserId": friend.Id,
				"Err":    err,
			}).Errorf("Cannot get recommend video of user")
			logging.SetSpanError(span, err)
			continue
		}

		// Get video by video id：只推荐一条视频
		videoId := recommendResponse.VideoList[0]
		video, ok := videoMap[videoId]
		if !ok {
			// todo 这里传入actorID的意义是？  -->  这里返回的视频列表是feed.Video，里面的部分字段比如isFavorite是需要ActorID来获取的
			videoQueryResponse, err := feedClient.QueryVideos(ctx, &feed.QueryVideosRequest{
				ActorId:  config.EnvCfg.MagicUserId,
				VideoIds: []uint32{videoId},
			})
			if err != nil || len(videoQueryResponse.VideoList) < 1 {
				logger.WithFields(logrus.Fields{
					"UserId":  friend.Id,
					"VideoId": videoId,
					"Err":     err,
				}).Errorf("Cannot get video info of %d", videoId)
				logging.SetSpanError(span, err)
				continue
			}
			video = videoQueryResponse.VideoList[0]
			videoMap[videoId] = video
		}

		// Chat to every friend
		content := fmt.Sprintf("今日视频推荐：%s；\n视频链接：%s", video.Title, video.PlayUrl)
		// 由ChatAction将消息发给GPT消息队列
		_, err = chatClient.ChatAction(ctx, &chat.ActionRequest{
			ActorId:    config.EnvCfg.MagicUserId,
			UserId:     friend.Id,
			ActionType: 1,
			Content:    content,
		})

		if err != nil {
			logger.WithFields(logrus.Fields{
				"UserId":  friend.Id,
				"VideoId": videoId,
				"Content": content,
				"Err":     err,
			}).Errorf("Cannot send magic message to user %d", friend.Id)
			logging.SetSpanError(span, err)
			continue
		}

		logger.WithFields(logrus.Fields{
			"UserId":  friend.Id,
			"VideoId": videoId,
			"Content": content,
		}).Infof("Successfully send the magic message")
	}
}
