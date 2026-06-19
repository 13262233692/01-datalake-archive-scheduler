package handler

import (
	"net/http"
	"strconv"
	"time"

	"datalake-archive-scheduler/internal/application/dto"
	"datalake-archive-scheduler/internal/application/service"
	"datalake-archive-scheduler/internal/domain/model"
	"datalake-archive-scheduler/pkg/logger"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type AuditHandler struct {
	auditService *service.AuditService
}

func NewAuditHandler(auditService *service.AuditService) *AuditHandler {
	return &AuditHandler{
		auditService: auditService,
	}
}

func (h *AuditHandler) GetAuditJob(c *gin.Context) {
	jobID := c.Param("auditId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Audit job ID is required",
		})
		return
	}

	job, ok := h.auditService.GetAuditJob(jobID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{
			"code":    404,
			"message": "Audit job not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data":    toAuditJobResponse(job),
	})
}

func (h *AuditHandler) ListAuditJobs(c *gin.Context) {
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

	jobs, total := h.auditService.ListAuditJobs(limit, offset)

	respList := make([]dto.AuditJobResponse, 0, len(jobs))
	for _, job := range jobs {
		respList = append(respList, toAuditJobResponse(job))
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data": dto.AuditListResponse{
			Total: total,
			Jobs:  respList,
		},
	})
}

func (h *AuditHandler) GetAuditBatches(c *gin.Context) {
	jobID := c.Param("auditId")
	if jobID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Audit job ID is required",
		})
		return
	}

	batches, ok := h.auditService.GetAuditBatches(jobID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{
			"code":    404,
			"message": "Audit job not found",
		})
		return
	}

	respList := make([]dto.AuditBatchResponse, 0, len(batches))
	for _, batch := range batches {
		respList = append(respList, toAuditBatchResponse(batch))
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data":    respList,
	})
}

func (h *AuditHandler) GetAuditStats(c *gin.Context) {
	stats := h.auditService.GetStats()

	passRate := 0.0
	if stats.TotalAudits > 0 {
		passRate = float64(stats.PassedAudits) / float64(stats.TotalAudits)
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data": dto.AuditStatsResponse{
			TotalAudits:     stats.TotalAudits,
			PassedAudits:    stats.PassedAudits,
			FailedAudits:    stats.FailedAudits,
			AlertAudits:     stats.AlertAudits,
			TotalSampled:    stats.TotalSampled,
			TotalMismatches: stats.TotalMismatches,
			TotalMissings:   stats.TotalMissings,
			PassRate:        passRate,
		},
	})
}

func (h *AuditHandler) GetDashboard(c *gin.Context) {
	stats := h.auditService.GetStats()

	passRate := 0.0
	if stats.TotalAudits > 0 {
		passRate = float64(stats.PassedAudits) / float64(stats.TotalAudits)
	}

	recentAlerts, _ := h.auditService.ListAuditJobs(10, 0)
	alertList := make([]dto.AuditJobResponse, 0)
	for _, job := range recentAlerts {
		if job.Status == model.AuditStatusAlert || job.Status == model.AuditStatusFailed {
			alertList = append(alertList, toAuditJobResponse(job))
		}
	}

	queueStats := h.auditService.GetQueueStats()

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "success",
		"data": dto.AuditDashboardResponse{
			Stats: dto.AuditStatsResponse{
				TotalAudits:     stats.TotalAudits,
				PassedAudits:    stats.PassedAudits,
				FailedAudits:    stats.FailedAudits,
				AlertAudits:     stats.AlertAudits,
				TotalSampled:    stats.TotalSampled,
				TotalMismatches: stats.TotalMismatches,
				TotalMissings:   stats.TotalMissings,
				PassRate:        passRate,
			},
			RecentAlerts: alertList,
			QueueSize:    queueStats["queue_size"].(int),
			IsRunning:    queueStats["is_running"].(bool),
		},
	})
}

func (h *AuditHandler) TriggerAudit(c *gin.Context) {
	var req dto.CreateAuditRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		logger.Warn("invalid audit request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    400,
			"message": "Invalid request: " + err.Error(),
		})
		return
	}

	targetTable := req.TargetTableName
	if targetTable == "" {
		targetTable = "unicorn_pro_history"
	}

	auditID, err := h.auditService.SubmitAudit(
		req.ArchiveJobID,
		req.TableName,
		targetTable,
		0,
		time.Now().AddDate(-3, 0, 0),
	)
	if err != nil {
		logger.Error("trigger audit failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    500,
			"message": "Trigger audit failed: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "Audit triggered successfully",
		"data": gin.H{
			"audit_job_id": auditID,
		},
	})
}

func toAuditJobResponse(job *model.AuditJob) dto.AuditJobResponse {
	return dto.AuditJobResponse{
		ID:              job.ID,
		ArchiveJobID:    job.ArchiveJobID,
		TableName:       job.TableName,
		Status:          string(job.Status),
		BatchCount:      job.BatchCount,
		SampledRecords:  job.SampledRecords,
		TotalRecords:    job.TotalRecords,
		SourceRootHash:  job.SourceRootHash,
		TargetRootHash:  job.TargetRootHash,
		MatchCount:      job.MatchCount,
		MismatchCount:   job.MismatchCount,
		MissingCount:    job.MissingCount,
		AlertLevel:      job.AlertLevel,
		ErrorMessage:    job.ErrorMessage,
		CreatedAt:       job.CreatedAt,
		StartedAt:       job.StartedAt,
		CompletedAt:     job.CompletedAt,
		DurationMs:      job.DurationMs,
	}
}

func toAuditBatchResponse(batch *model.AuditBatch) dto.AuditBatchResponse {
	return dto.AuditBatchResponse{
		ID:            batch.ID,
		AuditJobID:    batch.AuditJobID,
		BatchIndex:    batch.BatchIndex,
		StartID:       batch.StartID,
		EndID:         batch.EndID,
		RecordCount:   batch.RecordCount,
		SourceHash:    batch.SourceHash,
		TargetHash:    batch.TargetHash,
		Status:        batch.Status,
		IsMatched:     batch.IsMatched,
		MismatchedIDs: batch.MismatchedIDs,
		MissingIDs:    batch.MissingIDs,
	}
}
