package file

import (
	"Gosnip/src/constant/config"
	"Gosnip/src/extra/tracing"
	"Gosnip/src/utils/logging"
	"context"
	"io"
	"net/url"
	"os"
	"path"

	"github.com/sirupsen/logrus"
)

type FSStorage struct {
}

func (f FSStorage) GetLocalPath(ctx context.Context, fileName string) string {
	_, span := tracing.Tracer.Start(ctx, "FSStorage-GetLocalPath")
	defer span.End()
	logging.SetSpanWithHostname(span)
	return path.Join(config.EnvCfg.FileSystemStartPath, fileName)
}

func (f FSStorage) Upload(ctx context.Context, fileName string, content io.Reader) (output *PutObjectOutput, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "FSStorage-Upload")
	defer span.End()
	logging.SetSpanWithHostname(span)
	logger := logging.LogService("FSStorage.Upload").WithContext(ctx)

	logger = logger.WithFields(logrus.Fields{
		"file_name": fileName,
	})
	logger.Debugf("Process start")

	all, err := io.ReadAll(content)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Debug("Failed reading content")
		return nil, err
	}

	filePath := path.Join(config.EnvCfg.FileSystemStartPath, fileName)

	dir := path.Dir(filePath)
	// 0755:
	// 目录所有者可以完全控制目录
	// 其他用户可以读取和进入目录，但不能修改
	err = os.MkdirAll(dir, os.FileMode(0755))

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Debug("Failed creating directory before writing file")
		return nil, err
	}

	err = os.WriteFile(filePath, all, os.FileMode(0755))

	if err != nil {
		logger.WithFields(logrus.Fields{
			"err": err,
		}).Debug("Failed writing content to file")
		return nil, err
	}

	return &PutObjectOutput{}, nil
}

func (f FSStorage) GetLink(ctx context.Context, fileName string) (string, error) {
	_, span := tracing.Tracer.Start(ctx, "FSStorage-GetLink")
	defer span.End()
	logging.SetSpanWithHostname(span)
	return url.JoinPath(config.EnvCfg.FileSystemBaseUrl, fileName)
}

func (f FSStorage) IsFileExist(ctx context.Context, fileName string) (bool, error) {
	_, span := tracing.Tracer.Start(ctx, "FSStorage-IsFileExist")
	defer span.End()
	logging.SetSpanWithHostname(span)

	filePath := path.Join(config.EnvCfg.FileSystemStartPath, fileName)
	// 返回值：os.FileInfo - 文件信息接口
	// 包含文件大小、修改时间、权限等信息
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	// 判断是不是文件不存在的错误
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}
