package minio

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"IM_backend/configs"
	objectport "IM_backend/internal/application/ports/objectstorage"
)

type ObjectStorage struct {
	client        *minio.Client
	core          *minio.Core
	presignClient *minio.Client
	bucket        string
}

func (s *ObjectStorage) Health(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("存储桶不存在：%s", s.bucket)
	}
	return nil
}

func NewObjectStorage(ctx context.Context, cfg configs.MinIOConfig) (objectport.ObjectStorage, error) {
	region := strings.TrimSpace(cfg.Region)

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: cfg.UseSSL,
		Region: region,
	})

	if err != nil {
		return nil, err
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{Region: region}); err != nil {
			return nil, err
		}
	}

	core := &minio.Core{Client: client}
	presignClient := client
	if strings.TrimSpace(cfg.PublicEndpoint) != "" {
		publicEndpoint, publicSecure, err := parseEndpoint(cfg.PublicEndpoint, cfg.UseSSL)
		if err != nil {
			return nil, fmt.Errorf("解析 MinIO public_endpoint 失败：%w", err)
		}

		presignClient, err = minio.New(publicEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
			Secure: publicSecure,
			Region: region,
		})
		if err != nil {
			return nil, fmt.Errorf("创建 MinIO 预签名客户端失败：%w", err)
		}
	}

	return &ObjectStorage{
		client:        client,
		core:          core,
		presignClient: presignClient,
		bucket:        cfg.Bucket,
	}, nil
}

func parseEndpoint(raw string, defaultSecure bool) (string, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", defaultSecure, fmt.Errorf("endpoint 不能为空")
	}

	parsedRaw := raw
	if !strings.Contains(parsedRaw, "://") {
		if defaultSecure {
			parsedRaw = "https://" + parsedRaw
		} else {
			parsedRaw = "http://" + parsedRaw
		}
	}
	parsed, err := url.Parse(parsedRaw)
	if err != nil || parsed.Host == "" {
		return "", false, fmt.Errorf("地址无效：%s", raw)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", false, fmt.Errorf("仅支持 http 或 https：%s", raw)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", false, fmt.Errorf("endpoint 不支持路径：%s", raw)
	}

	return parsed.Host, parsed.Scheme == "https", nil
}

func isInlineContentType(contentType string) bool {
	switch strings.ToLower(contentType) {
	case "image/jpeg",
		"image/png",
		"image/gif",
		"video/mp4":
		return true
	default:
		return false
	}
}

func sanitizeFileName(name string) string {
	name = strings.ReplaceAll(name, "\r", "")
	name = strings.ReplaceAll(name, "\n", "")
	name = strings.ReplaceAll(name, `"`, "")
	name = strings.TrimSpace(name)

	if name == "" {
		return "download"
	}
	return name
}

func (s *ObjectStorage) Bucket() string {
	return s.bucket
}

func (s *ObjectStorage) PresignedPutURL(ctx context.Context, objectKey string, ttl time.Duration) (string, error) {
	u, err := s.presignClient.PresignedPutObject(ctx, s.bucket, objectKey, ttl)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *ObjectStorage) StatObject(ctx context.Context, objectKey string) (int64, error) {
	info, err := s.client.StatObject(ctx, s.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

func (s *ObjectStorage) OpenObject(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
}

func (s *ObjectStorage) DeleteObject(ctx context.Context, objectKey string) error {
	return s.client.RemoveObject(ctx, s.bucket, objectKey, minio.RemoveObjectOptions{})
}

func (s *ObjectStorage) CreateMultipartUpload(ctx context.Context, objectKey string, contentType string) (string, error) {
	return s.core.NewMultipartUpload(ctx, s.bucket, objectKey, minio.PutObjectOptions{
		ContentType: contentType,
	})
}

func (s *ObjectStorage) PresignMultipartPart(ctx context.Context, objectKey string, uploadID string, partNumber int, ttl time.Duration) (string, error) {
	if uploadID == "" || partNumber <= 0 {
		return "", fmt.Errorf("参数错误")
	}
	u, err := s.presignClient.Presign(ctx, http.MethodPut, s.bucket, objectKey, ttl, url.Values{
		"uploadId":   []string{uploadID},
		"partNumber": []string{strconv.Itoa(partNumber)},
	})
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *ObjectStorage) ListMultipartParts(ctx context.Context, objectKey string, uploadID string) ([]objectport.MultipartPart, error) {
	marker := 0
	result := make([]objectport.MultipartPart, 0)
	for {
		listed, err := s.core.ListObjectParts(ctx, s.bucket, objectKey, uploadID, marker, 1000)
		if err != nil {
			return nil, err
		}
		for _, part := range listed.ObjectParts {
			result = append(result, objectport.MultipartPart{PartNumber: part.PartNumber, ETag: part.ETag, Size: part.Size})
		}
		if !listed.IsTruncated {
			return result, nil
		}
		marker = listed.NextPartNumberMarker
	}
}

func (s *ObjectStorage) CompleteMultipartUpload(ctx context.Context, objectKey string, uploadID string, parts []objectport.MultipartPart) error {
	completeParts := make([]minio.CompletePart, 0, len(parts))
	for _, part := range parts {
		completeParts = append(completeParts, minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}
	_, err := s.core.CompleteMultipartUpload(ctx, s.bucket, objectKey, uploadID, completeParts, minio.PutObjectOptions{})
	return err
}

func (s *ObjectStorage) AbortMultipartUpload(ctx context.Context, objectKey string, uploadID string) error {
	return s.core.AbortMultipartUpload(ctx, s.bucket, objectKey, uploadID)
}

func (s *ObjectStorage) PresignedGetURL(ctx context.Context,
	objectKey string,
	ttl time.Duration,
	contentType string,
	fileName string,
) (string, error) {
	params := url.Values{}
	// 对不适合浏览器直接展示的文件强制下载，避免 HTML、SVG、脚本等内容被浏览器执行
	if !isInlineContentType(contentType) {
		params.Set(
			"response-content-disposition", mime.FormatMediaType("attachment", map[string]string{
				"filename": sanitizeFileName(fileName),
			}),
		)
		params.Set("response-content-type", "application/octet-stream")
	}

	u, err := s.presignClient.PresignedGetObject(ctx, s.bucket, objectKey, ttl, params)
	if err != nil {
		return "", err
	}

	return u.String(), nil
}
