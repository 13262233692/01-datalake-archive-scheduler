package repository

import (
	"context"
	"io"
	"time"
)

type OSSRepository interface {
	UploadStream(ctx context.Context, objectKey string, data io.Reader, size int64) error
	UploadFile(ctx context.Context, objectKey string, filePath string) error
	Download(ctx context.Context, objectKey string) (io.ReadCloser, error)
	Exists(ctx context.Context, objectKey string) (bool, error)
	Delete(ctx context.Context, objectKey string) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	GeneratePresignedURL(ctx context.Context, objectKey string, expires time.Duration) (string, error)
	Close() error
}

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
}
