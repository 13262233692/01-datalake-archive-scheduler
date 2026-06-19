package oss

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"datalake-archive-scheduler/internal/domain/repository"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

type MockOSSRepository struct {
	config     *config.OSSConfig
	mu         sync.RWMutex
	objects    map[string][]byte
	metadata   map[string]repository.ObjectInfo
	bucketName string
}

func NewMockOSSRepository(cfg *config.OSSConfig) *MockOSSRepository {
	return &MockOSSRepository{
		config:     cfg,
		objects:    make(map[string][]byte),
		metadata:   make(map[string]repository.ObjectInfo),
		bucketName: cfg.Bucket,
	}
}

func (r *MockOSSRepository) UploadStream(ctx context.Context, objectKey string, data io.Reader, size int64) error {
	buf, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("read stream error: %w", err)
	}

	fullKey := filepath.Join(r.config.PathPrefix, objectKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.objects[fullKey] = buf
	r.metadata[fullKey] = repository.ObjectInfo{
		Key:          fullKey,
		Size:         int64(len(buf)),
		LastModified: time.Now(),
		ETag:         fmt.Sprintf("%x", len(buf)),
	}

	logger.Info("oss upload stream success",
		zap.String("bucket", r.bucketName),
		zap.String("key", fullKey),
		zap.Int("size_bytes", len(buf)),
	)

	return nil
}

func (r *MockOSSRepository) UploadFile(ctx context.Context, objectKey string, filePath string) error {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file error: %w", err)
	}

	fullKey := filepath.Join(r.config.PathPrefix, objectKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.objects[fullKey] = fileData
	r.metadata[fullKey] = repository.ObjectInfo{
		Key:          fullKey,
		Size:         int64(len(fileData)),
		LastModified: time.Now(),
		ETag:         fmt.Sprintf("%x", len(fileData)),
	}

	logger.Info("oss upload file success",
		zap.String("bucket", r.bucketName),
		zap.String("key", fullKey),
		zap.Int("size_bytes", len(fileData)),
	)

	return nil
}

func (r *MockOSSRepository) Download(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	fullKey := filepath.Join(r.config.PathPrefix, objectKey)

	r.mu.RLock()
	defer r.mu.RUnlock()

	data, exists := r.objects[fullKey]
	if !exists {
		return nil, fmt.Errorf("object not found: %s", fullKey)
	}

	reader := bytes.NewReader(data)
	return io.NopCloser(reader), nil
}

func (r *MockOSSRepository) Exists(ctx context.Context, objectKey string) (bool, error) {
	fullKey := filepath.Join(r.config.PathPrefix, objectKey)

	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.objects[fullKey]
	return exists, nil
}

func (r *MockOSSRepository) Delete(ctx context.Context, objectKey string) error {
	fullKey := filepath.Join(r.config.PathPrefix, objectKey)

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.objects, fullKey)
	delete(r.metadata, fullKey)

	logger.Info("oss delete success",
		zap.String("bucket", r.bucketName),
		zap.String("key", fullKey),
	)

	return nil
}

func (r *MockOSSRepository) List(ctx context.Context, prefix string) ([]repository.ObjectInfo, error) {
	fullPrefix := filepath.Join(r.config.PathPrefix, prefix)

	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []repository.ObjectInfo
	for key, info := range r.metadata {
		if len(key) >= len(fullPrefix) && key[:len(fullPrefix)] == fullPrefix {
			result = append(result, info)
		}
	}

	logger.Debug("oss list",
		zap.String("prefix", fullPrefix),
		zap.Int("count", len(result)),
	)

	return result, nil
}

func (r *MockOSSRepository) GeneratePresignedURL(ctx context.Context, objectKey string, expires time.Duration) (string, error) {
	fullKey := filepath.Join(r.config.PathPrefix, objectKey)
	url := fmt.Sprintf("https://%s.%s/%s?Expires=%d",
		r.bucketName,
		r.config.Endpoint,
		fullKey,
		time.Now().Add(expires).Unix(),
	)

	logger.Debug("oss presigned url generated",
		zap.String("key", fullKey),
		zap.Duration("expires", expires),
	)

	return url, nil
}

func (r *MockOSSRepository) Close() error {
	logger.Info("mock oss connection closed")
	return nil
}

func (r *MockOSSRepository) GetObjectCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.objects)
}

func (r *MockOSSRepository) GetTotalSize() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var total int64
	for _, info := range r.metadata {
		total += info.Size
	}
	return total
}
