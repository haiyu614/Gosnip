package main

import (
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/models"
	"Gosnip/src/storage/es"
	"Gosnip/src/utils/logging"
	"Gosnip/src/utils/rabbitmq"
	"bytes"
	"context"
	"encoding/json"

	"github.com/elastic/go-elasticsearch/esapi"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// todo ES写入失败怎么没有重试机制，幂等性也没有保证？

func esSaveMessage(channel *amqp.Channel) {

	msg, err := channel.Consume(strings.MessageES, "",
		false, false, false, false, nil,
	)
	failOnError(err, "Failed to Consume")

	var message models.Message
	for body := range msg {
		ctx := rabbitmq.ExtractAMQPHeaders(context.Background(), body.Headers)
		ctx, span := tracing.Tracer.Start(ctx, "MessageSendService")
		logger := logging.LogService("MessageSend").WithContext(ctx)

		if err := json.Unmarshal(body.Body, &message); err != nil {
			logger.WithFields(logrus.Fields{
				"from_id": message.FromUserId,
				"to_id":   message.ToUserId,
				"content": message.Content,
				"err":     err,
			}).Errorf("Error when unmarshaling the prepare json body.")
			logging.SetSpanError(span, err)
			err = body.Nack(false, true)
			if err != nil {
				logger.WithFields(
					logrus.Fields{
						"from_id": message.FromUserId,
						"to_id":   message.ToUserId,
						"content": message.Content,
						"err":     err,
					},
				).Errorf("Error when nack the message")
				logging.SetSpanError(span, err)
			}
			span.End()
			continue
		}

		// 转换为ES的文档格式
		EsMessage := models.EsMessage{
			ToUserId:       message.ToUserId,
			FromUserId:     message.FromUserId,
			ConversationId: message.ConversationId,
			Content:        message.Content,
			CreateTime:     message.CreatedAt,
		}
		data, _ := json.Marshal(EsMessage)

		// 索引到Elasticsearch
		// todo 这里应该指定DocumentID来保证幂等性吗
		req := esapi.IndexRequest{
			Index:   "message", // 索引名
			Refresh: "true",
			Body:    bytes.NewReader(data),
		}
		//返回值close
		// 这里执行实际的ES插入操作
		res, err := req.Do(ctx, es.EsClient)

		if err != nil {
			logger.WithFields(logrus.Fields{
				"from_id": message.FromUserId,
				"to_id":   message.ToUserId,
				"content": message.Content,
				"err":     err,
			}).Errorf("Error when insert message to database.")
			logging.SetSpanError(span, err)

			span.End()
			continue
		}
		res.Body.Close()

		err = body.Ack(false)

		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when dealing with the message...3")
			logging.SetSpanError(span, err)
		}

	}
}
