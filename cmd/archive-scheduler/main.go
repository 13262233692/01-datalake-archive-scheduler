package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"datalake-archive-scheduler/internal/application/service"
	domainservice "datalake-archive-scheduler/internal/domain/service"
	"datalake-archive-scheduler/internal/infrastructure/config"
	"datalake-archive-scheduler/internal/infrastructure/hive"
	"datalake-archive-scheduler/internal/infrastructure/oss"
	"datalake-archive-scheduler/internal/infrastructure/persistance"
	"datalake-archive-scheduler/internal/infrastructure/starrocks"
	router "datalake-archive-scheduler/internal/interfaces/http"
	"datalake-archive-scheduler/pkg/logger"

	"go.uber.org/zap"
)

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := logger.Init(cfg.Log.Level, cfg.Log.Format); err != nil {
		fmt.Printf("Failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	logger.Info("starting datalake archive scheduler",
		zap.Int("port", cfg.Server.Port),
		zap.String("mode", cfg.Server.Mode),
	)

	sourceDB := persistance.NewMockPolarDBRepository(cfg.Archive.TableName)
	defer sourceDB.Close()

	ossRepo := oss.NewMockOSSRepository(&cfg.OSS)
	defer ossRepo.Close()

	hiveRepo := hive.NewMockHiveMetastoreRepository(&cfg.Hive)
	defer hiveRepo.Close()

	starrocksRepo := starrocks.NewMockStarRocksRepository(&cfg.StarRocks)
	defer starrocksRepo.Close()

	cleaningSvc := domainservice.NewDataCleaningService()
	maskingSvc := domainservice.NewDataMaskingService(cfg.Archive.MaskingSalt)
	serialSvc := domainservice.NewJSONSerializationService()

	streamProcessor := service.NewStreamProcessor(
		sourceDB,
		ossRepo,
		cleaningSvc,
		maskingSvc,
		serialSvc,
		cfg.Archive.MemoryLimitBytes(),
		cfg.Archive.BatchSize,
	)

	compensationSvc := service.NewCompensationService(
		sourceDB,
		ossRepo,
		hiveRepo,
		starrocksRepo,
		streamProcessor,
	)

	archiveAppService := service.NewArchiveAppService(
		&cfg.Archive,
		sourceDB,
		ossRepo,
		hiveRepo,
		starrocksRepo,
		streamProcessor,
		compensationSvc,
		cleaningSvc,
		maskingSvc,
	)

	auditCfg := service.AuditConfig{
		Enabled:               cfg.Audit.Enabled,
		SampleRate:            cfg.Audit.SampleRate,
		BatchSize:             cfg.Audit.BatchSize,
		Concurrency:           cfg.Audit.Concurrency,
		QueueCapacity:         cfg.Audit.QueueCapacity,
		WebhookURL:            cfg.Audit.WebhookURL,
		WebhookTimeout:        time.Duration(cfg.Audit.WebhookTimeoutSec) * time.Second,
		MaxRetries:            cfg.Audit.MaxRetries,
		AlertThresholdMiss:    cfg.Audit.AlertThresholdMiss,
		AlertThresholdMismatch: cfg.Audit.AlertThresholdMismatch,
	}

	auditService := service.NewAuditService(auditCfg, sourceDB, starrocksRepo)
	if auditCfg.Enabled {
		auditService.Start()
		defer auditService.Stop()
	}

	archiveAppService.SetAuditService(auditService)

	r := router.SetupRouter(archiveAppService, auditService, cfg.Server.Mode)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  time.Duration(cfg.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WriteTimeout) * time.Second,
	}

	go func() {
		logger.Info("http server listening", zap.Int("port", cfg.Server.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server failed to start", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server...")

	if err := srv.Close(); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}

	logger.Info("server exited")
}
