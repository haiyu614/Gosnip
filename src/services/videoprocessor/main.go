package main

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/constant/strings"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/models"
	"Gosnip/src/storage/database"
	"Gosnip/src/storage/file"
	"Gosnip/src/utils/logging"
	"Gosnip/src/utils/pathgen"
	"Gosnip/src/utils/rabbitmq"
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/golang/freetype/truetype"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm/clause"

	"github.com/golang/freetype"
)

func exitOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	conn, err := amqp.Dial(rabbitmq.BuildMQConnAddr())
	exitOnError(err)

	defer func(conn *amqp.Connection) {
		err := conn.Close()
		exitOnError(err)
	}(conn)

	tp, err := tracing.SetTraceProvider(config.VideoPicker)
	if err != nil {
		logging.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Panicf("Error to set the trace")
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logging.Logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error to set the trace")
		}
	}()

	ch, err := conn.Channel()
	exitOnError(err)

	defer func(ch *amqp.Channel) {
		err := ch.Close()
		exitOnError(err)
	}(ch)

	exchangeArgs := amqp.Table{
		"x-delayed-type": "topic",
	}
	err = ch.ExchangeDeclare(
		strings.VideoExchange,
		"x-delayed-message", //"topic",
		true,
		false,
		false,
		false,
		exchangeArgs,
	)
	exitOnError(err)

	_, err = ch.QueueDeclare(
		strings.VideoPicker, //视频信息采集(封面/水印)
		true,
		false,
		false,
		false,
		nil,
	)
	exitOnError(err)

	_, err = ch.QueueDeclare(
		strings.VideoSummary, //视频摘要
		true,
		false,
		false,
		false,
		nil,
	)
	exitOnError(err)

	err = ch.QueueBind(
		strings.VideoPicker,
		strings.VideoPicker,
		strings.VideoExchange,
		false,
		nil,
	)
	exitOnError(err)

	err = ch.QueueBind(
		strings.VideoSummary,
		strings.VideoSummary,
		strings.VideoExchange,
		false,
		nil,
	)
	exitOnError(err)

	// 设置 RabbitMQ 消费者的 QoS（Quality of Service） 参数，用于控制消息的预取和处理。
	//第一个参数 1：prefetchCount
	//每次预取的消息数量
	//设置为 1 表示每次只预取一条消息
	//第二个参数 0：prefetchSize
	//每次预取的消息大小（字节数）
	//设置为 0 表示不限制消息大小
	//第三个参数 false：global
	//是否对整个连接生效
	//false 表示只对当前 channel 生效
	//true 表示对整个 connection 生效
	err = ch.Qos(1, 0, false)
	exitOnError(err)

	// 异步处理封面和水印
	go Consume(ch)
	logger := logging.LogService("VideoPicker")
	logger.Infof(strings.VideoPicker + " is running now")

	// 异步生成摘要和关键字
	go SummaryConsume(ch)
	logger = logging.LogService("VideoSummary")
	logger.Infof(strings.VideoSummary + " is running now")

	ConnectServiceClient()
	defer CloseMQConn()

	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

// Consume 获取封面和水印
func Consume(channel *amqp.Channel) {
	// 返回一个只读chan，<-chan amqp.Delivery，这个返回的通道连接到 RabbitMQ 的 VideoPicker 队列，当队列中有新消息时，通道会接收到消息，供消费者处理
	msg, err := channel.Consume(strings.VideoPicker, "", false, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	for d := range msg {
		//解包 Otel Context，从 RabbitMQ 消息头中提取分布式追踪上下文。
		ctx := rabbitmq.ExtractAMQPHeaders(context.Background(), d.Headers)
		ctx, span := tracing.Tracer.Start(ctx, "VideoPickerService")
		logger := logging.LogService("VideoPicker.Picker").WithContext(ctx)
		logging.SetSpanWithHostname(span)

		var raw models.RawVideo
		if err := json.Unmarshal(d.Body, &raw); err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when unmarshaling the prepare json body.")
			continue
		}

		// 截取封面
		err := extractVideoCover(ctx, &raw)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when extracting video cover.")
			logging.SetSpanError(span, err)
		}
		// 获取视频水印
		watermarkPNGName, err := textWatermark(ctx, &raw)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when generate watermark png.")
			logging.SetSpanError(span, err)
		}
		// 添加水印逻辑
		err = addWatermarkToVideo(ctx, &raw, watermarkPNGName)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when adding watermark to video.")
			logging.SetSpanError(span, err)
		}
		// 保存到数据库
		finalFileName := pathgen.GenerateFinalVideoName(raw.ActorId, raw.Title, raw.VideoId)
		video := &models.Video{
			ID:        raw.VideoId,
			UserId:    raw.ActorId,
			Title:     raw.Title,
			FileName:  finalFileName,
			CoverName: raw.CoverName,
		}
		//* 不冲突创建，冲突则只更新指定字段
		result := database.Client.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns([]string{"user_id", "title", "file_name", "cover_name"}),
		}).Create(&video)
		if result.Error != nil {
			logger.WithFields(logrus.Fields{
				"file_name":  raw.FileName,
				"cover_name": raw.CoverName,
				"err":        err,
			}).Errorf("Error when updating file information to database")
			logging.SetSpanError(span, result.Error)
			err = d.Nack(false, true)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"err": err,
				}).Errorf("Error when resending the video to queue...")
				logging.SetSpanError(span, err)
			}
			span.End()
			continue
		}
		logger.WithFields(logrus.Fields{
			"entry": video,
		}).Debug("saved db entry")

		span.End()
		err = d.Ack(false)
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err": err,
			}).Errorf("Error when dealing with the video...")
		}
	}
}

// extractVideoCover 从视频中截取封面，保存到封面名对应的路径
func extractVideoCover(ctx context.Context, video *models.RawVideo) error {
	ctx, span := tracing.Tracer.Start(ctx, "ExtractVideoCoverService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("VideoPicker.Picker").WithContext(ctx)
	logger.Debug("Extracting video cover...")

	RawFileName := video.FileName
	CoverFileName := video.CoverName
	RawFilePath := file.GetLocalPath(ctx, RawFileName)
	cmdArgs := []string{
		// -vframes 1: 只提取1帧 --> 获取封面
		"-i", RawFilePath, "-vframes", "1", "-an", "-f", "image2pipe", "-",
	}
	// 创建执行FFmpeg命令的实例。
	cmd := exec.Command("ffmpeg", cmdArgs...)
	// Create a bytes.Buffer to capture stdout
	var buf bytes.Buffer
	// 创建缓冲区捕获命令输出。让FFmpeg执行实例的标准输出指向刚刚申请的buffer
	cmd.Stdout = &buf
	//执行FFmpeg命令。
	err := cmd.Run()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("cmd.Run() failed with %s\n", err)
		logging.SetSpanError(span, err)
		return err
	}
	// buf.Bytes() now contains the image data. You can use it to write to a file or send it to an output stream.
	_, err = file.Upload(ctx, CoverFileName, bytes.NewReader(buf.Bytes()))
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("failed to upload video cover")
		logging.SetSpanError(span, err)
		return err
	}
	return nil
}

func textWatermark(ctx context.Context, video *models.RawVideo) (string, error) {
	ctx, span := tracing.Tracer.Start(ctx, "NicknameWatermarkService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("VideoPicker.Picker").WithContext(ctx)

	// 加载字体文件
	fontName := filepath.Join("static", "font.ttf")
	fontBytes, err := os.ReadFile(fontName)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("Read FontFile failed.")
		logging.SetSpanError(span, err)
		return "", err
	}

	// 解析字体文件
	font, err := truetype.Parse(fontBytes)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("Parse font failed.")
		logging.SetSpanError(span, err)
		return "", err
	}

	// 设置字体大小
	fontSize := 40

	// 设置图片大小
	imgWidth := 800
	imgHeight := 60

	// 设置文本内容
	var user models.User
	err = database.Client.Where("id = ?", video.ActorId).First(&user).Error
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("Find userName failed.")
		logging.SetSpanError(span, err)
		return "", err
	}
	text := user.UserName

	// 设置文本颜色
	textColor := color.RGBA{R: 255, G: 255, B: 255, A: 128}

	// 创建一个新的RGBA图片
	img := image.NewRGBA(image.Rect(0, 0, imgWidth, imgHeight))

	// 将背景颜色设置为透明，将整个目标图像 img 的背景填充为透明色，为后续的水印文字绘制提供透明背景画布。
	// image.Uniform 是一个特殊的图像类型，它会生成单一颜色的无限大图像
	// color.Transparent 是 Go 标准库中预定义的透明颜色常量
	draw.Draw(img, img.Bounds(), &image.Uniform{C: color.Transparent}, image.Point{}, draw.Src)

	// 创建一个新的freetype上下文
	c := freetype.NewContext()
	c.SetDPI(72)
	c.SetFont(font)
	c.SetFontSize(float64(fontSize))
	c.SetClip(img.Bounds())
	c.SetDst(img)
	c.SetSrc(image.NewUniform(textColor))

	// 计算文本的位置
	textX := 10
	textY := 50

	// 在图片上绘制文本
	pt := freetype.Pt(textX, textY)
	_, err = c.DrawString(text, pt)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("DrawString failed.")
		logging.SetSpanError(span, err)
		return "", err
	}

	// 将图像保存到内存中
	var buf bytes.Buffer
	err = png.Encode(&buf, img)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("Encode image failed.")
		logging.SetSpanError(span, err)
		return "", err
	}

	WatermarkPNGName := pathgen.GenerateNameWatermark(video.ActorId, text)
	// 将图片保存到文件
	_, err = file.Upload(ctx, WatermarkPNGName, bytes.NewReader(buf.Bytes()))
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("Create output.png failed.")
		logging.SetSpanError(span, err)
		return "", err
	}
	return WatermarkPNGName, nil
}

func addWatermarkToVideo(ctx context.Context, video *models.RawVideo, WatermarkPNGName string) error {
	ctx, span := tracing.Tracer.Start(ctx, "AddWatermarkToVideoService")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("VideoPicker.Picker").WithContext(ctx)
	logger.Debug("Adding watermark to video...")

	RawFileName := video.FileName
	FinalFileName := pathgen.GenerateFinalVideoName(video.ActorId, video.Title, video.VideoId)
	RawFilePath := file.GetLocalPath(ctx, RawFileName)
	WatermarkPath := file.GetLocalPath(ctx, WatermarkPNGName)
	cmdArgs := []string{
		"-i", RawFilePath,
		"-i", WatermarkPath,
		"-filter_complex", "[0:v][1:v]overlay=10:10",
		"-f", "matroska", "-",
	}

	cmd := exec.Command("ffmpeg", cmdArgs...)
	var buf bytes.Buffer
	cmd.Stdout = &buf

	// Execute the command
	err := cmd.Run()
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("cmd.Run() failed with %s\n", err)
		logging.SetSpanError(span, err)
	}

	// Write the captured stdout to a file
	_, err = file.Upload(ctx, FinalFileName, bytes.NewReader(buf.Bytes()))
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Warnf("failed to upload video with watermark")
		logging.SetSpanError(span, err)
		return err
	}
	return nil
}
