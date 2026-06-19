package handler

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"datalake-archive-scheduler/internal/application/dto"
	"datalake-archive-scheduler/internal/application/service"
	"datalake-archive-scheduler/pkg/logger"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type ArchiveHandler struct {
	archiveService *service.ArchiveAppService
}

func NewArchiveHandler(archiveService *service.ArchiveAppService) *ArchiveHandler {
	return &ArchiveHandler{
		archiveService: archiveService,
	}
}

func (h *ArchiveHandler) CreateJob(c *gin.Context) {
	var req dto.CreateArchiveJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Warn("invalid request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Invalid request: " + err.Error(),
		})
		return
	}

	ctx := c.Request.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	resp, err := h.archiveService.CreateJob(ctx, &req)
	if err != nil {
		logger.Error("create job failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "Create job failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data":    resp,
	})
}

func (h *ArchiveHandler) StartJob(c *gin.Context) {
	jobID := c.Param("jobId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Job ID is required",
		})
		return
	}

	ctx := c.Request.Context()
	if err := h.archiveService.StartJob(ctx, jobID); err != nil {
		logger.Error("start job failed", zap.String("job_id", jobID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "Start job failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "Job started successfully",
		"data": gin.H{
			"job_id": jobID,
		},
	})
}

func (h *ArchiveHandler) GetJobStatus(c *gin.Context) {
	jobID := c.Param("jobId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Job ID is required",
		})
		return
	}

	resp, err := h.archiveService.GetJobStatus(jobID)
	if err != nil {
		logger.Error("get job status failed", zap.String("job_id", jobID), zap.Error(err))
		c.JSON(http.StatusNotFound, gin.H{
			"code":    404,
			"message": "Job not found: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data":    resp,
	})
}

func (h *ArchiveHandler) GetShardStatus(c *gin.Context) {
	jobID := c.Param("jobId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Job ID is required",
		})
		return
	}

	resp, err := h.archiveService.GetShardStatus(jobID)
	if err != nil {
		logger.Error("get shard status failed", zap.String("job_id", jobID), zap.Error(err))
		c.JSON(http.StatusNotFound, gin.H{
			"code":    404,
			"message": "Job not found: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data":    resp,
	})
}

func (h *ArchiveHandler) ListJobs(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "20")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 || limit > 100 {
		limit = 20
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		offset = 0
	}

	resp, err := h.archiveService.ListJobs(limit, offset)
	if err != nil {
		logger.Error("list jobs failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "List jobs failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data":    resp,
	})
}

func (h *ArchiveHandler) PauseJob(c *gin.Context) {
	jobID := c.Param("jobId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Job ID is required",
		})
		return
	}

	if err := h.archiveService.PauseJob(jobID); err != nil {
		logger.Error("pause job failed", zap.String("job_id", jobID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "Pause job failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "Job paused successfully",
	})
}

func (h *ArchiveHandler) ResumeJob(c *gin.Context) {
	jobID := c.Param("jobId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Job ID is required",
		})
		return
	}

	ctx := c.Request.Context()
	if err := h.archiveService.ResumeJob(ctx, jobID); err != nil {
		logger.Error("resume job failed", zap.String("job_id", jobID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "Resume job failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "Job resumed successfully",
	})
}

func (h *ArchiveHandler) CompensateJob(c *gin.Context) {
	jobID := c.Param("jobId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Job ID is required",
		})
		return
	}

	ctx := c.Request.Context()
	resp, err := h.archiveService.CompensateJob(ctx, jobID)
	if err != nil {
		logger.Error("compensate job failed", zap.String("job_id", jobID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "Compensate job failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "Compensation task started",
		"data":    resp,
	})
}

func (h *ArchiveHandler) GetStats(c *gin.Context) {
	resp := h.archiveService.GetStats()

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data":    resp,
	})
}

func (h *ArchiveHandler) HealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"status":  "healthy",
		"time":    time.Now().Format(time.RFC3339),
		"service": "datalake-archive-scheduler",
	})
}
