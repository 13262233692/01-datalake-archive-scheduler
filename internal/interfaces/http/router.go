package http

import (
	"datalake-archive-scheduler/internal/application/service"
	"datalake-archive-scheduler/internal/interfaces/http/handler"
	"datalake-archive-scheduler/internal/interfaces/http/middleware"

	"github.com/gin-gonic/gin"
)

func SetupRouter(archiveService *service.ArchiveAppService, mode string) *gin.Engine {
	gin.SetMode(mode)

	r := gin.New()

	r.Use(middleware.Recovery())
	r.Use(middleware.RequestLogger())
	r.Use(middleware.CORSMiddleware())

	archiveHandler := handler.NewArchiveHandler(archiveService)

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

		stats := api.Group("/stats")
		{
			stats.GET("", archiveHandler.GetStats)
		}
	}

	r.GET("/health", archiveHandler.HealthCheck)

	return r
}
