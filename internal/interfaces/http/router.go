package http

import (
	"datalake-archive-scheduler/internal/application/service"
	"datalake-archive-scheduler/internal/interfaces/http/handler"
	"datalake-archive-scheduler/internal/interfaces/http/middleware"

	"github.com/gin-gonic/gin"
)

func SetupRouter(archiveService *service.ArchiveAppService, auditService *service.AuditService, mode string) *gin.Engine {
	gin.SetMode(mode)

	r := gin.New()

	r.Use(middleware.Recovery())
	r.Use(middleware.RequestLogger())
	r.Use(middleware.CORSMiddleware())

	archiveHandler := handler.NewArchiveHandler(archiveService)
	auditHandler := handler.NewAuditHandler(auditService)

	api := r.Group("/api/v1")
	{
		archive := api.Group("/archive")
		{
			archive.POST("/jobs", archiveHandler.CreateJob)
			archive.GET("/jobs", archiveHandler.ListJobs)
			archive.GET("/jobs/:jobId", archiveHandler.GetJobStatus)
			archive.POST("/jobs/:jobId/start", archiveHandler.StartJob)
			archive.POST("/jobs/:jobId/pause", archiveHandler.PauseJob)
			archive.POST("/jobs/:jobId/resume", archiveHandler.ResumeJob)
			archive.POST("/jobs/:jobId/compensate", archiveHandler.CompensateJob)
			archive.GET("/jobs/:jobId/shards", archiveHandler.GetShardStatus)
		}

		audit := api.Group("/audit")
		{
			audit.GET("/jobs", auditHandler.ListAuditJobs)
			audit.GET("/jobs/:auditId", auditHandler.GetAuditJob)
			audit.GET("/jobs/:auditId/batches", auditHandler.GetAuditBatches)
			audit.POST("/trigger", auditHandler.TriggerAudit)
		}

		stats := api.Group("/stats")
		{
			stats.GET("", archiveHandler.GetStats)
			stats.GET("/audit", auditHandler.GetAuditStats)
			stats.GET("/audit/dashboard", auditHandler.GetDashboard)
		}
	}

	r.GET("/health", archiveHandler.HealthCheck)

	return r
}
