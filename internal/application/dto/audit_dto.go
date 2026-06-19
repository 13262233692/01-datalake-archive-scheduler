package dto

import "time"

type AuditJobResponse struct {
	ID              string    `json:"id"`
	ArchiveJobID    string    `json:"archive_job_id"`
	TableName       string    `json:"table_name"`
	Status      string    `json:"status"`
	BatchCount     int       `json:"batch_count"`
	SampledRecords int64     `json:"sampled_records"`
	TotalRecords    int64     `json:"total_records"`
	SourceRootHash string    `json:"source_root_hash"`
	TargetRootHash string    `json:"target_root_hash"`
	MatchCount    int       `json:"match_count"`
	MismatchCount int       `json:"mismatch_count"`
	MissingCount  int       `json:"missing_count"`
	AlertLevel    string    `json:"alert_level"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	DurationMs   int64     `json:"duration_ms"`
}

type AuditBatchResponse struct {
	ID            string   `json:"id"`
	AuditJobID   string `json:"audit_job_id"`
	BatchIndex  int      `json:"batch_index"`
	StartID    int64    `json:"start_id"`
	EndID     int64    `json:"end_id"`
	RecordCount int    `json:"record_count"`
	SourceHash  string   `json:"source_hash"`
	TargetHash  string   `json:"target_hash"`
	Status      string   `json:"status"`
	IsMatched    bool     `json:"is_matched"`
	MismatchedIDs []int64  `json:"mismatched_ids,omitempty"`
	MissingIDs  []int64  `json:"missing_ids,omitempty"`
}

type AuditListResponse struct {
	Total int                 `json:"total"`
	Jobs  []AuditJobResponse `json:"jobs"`
}

type AuditStatsResponse struct {
	TotalAudits     int64 `json:"total_audits"`
	PassedAudits    int64 `json:"passed_audits"`
	FailedAudits    int64 `json:"failed_audits"`
	AlertAudits  int64 `json:"alert_audits"`
	TotalSampled   int64 `json:"total_sampled_records"`
	TotalMismatches int64 `json:"total_mismatches"`
	TotalMissings  int64 `json:"total_missing_records"`
	PassRate     float64 `json:"pass_rate"`
}

type CreateAuditRequest struct {
	ArchiveJobID   string `json:"archive_job_id" binding:"required"`
	TableName      string `json:"table_name"`
	TargetTableName string `json:"target_table_name"`
}

type AuditDashboardResponse struct {
	Stats     AuditStatsResponse   `json:"stats"`
	RecentAlerts []AuditJobResponse `json:"recent_alerts"`
	QueueSize   int                 `json:"queue_size"`
	IsRunning   bool                `json:"is_running"`
}
