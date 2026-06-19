package dto

import "time"

type CreateArchiveJobRequest struct {
	TableName   string `json:"table_name" binding:"required"`
	ColdDateStr string `json:"cold_date"`
	ShardCount  int    `json:"shard_count"`
	Concurrency int    `json:"concurrency"`
}

type CreateArchiveJobResponse struct {
	JobID     string    `json:"job_id"`
	JobName   string    `json:"job_name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type JobStatusResponse struct {
	JobID         string    `json:"job_id"`
	JobName       string    `json:"job_name"`
	TableName     string    `json:"table_name"`
	Status        string    `json:"status"`
	TotalRecords  int64     `json:"total_records"`
	ArchivedCount int64     `json:"archived_count"`
	FailedCount   int64     `json:"failed_count"`
	Progress      float64   `json:"progress"`
	ShardCount    int       `json:"shard_count"`
	Concurrency   int       `json:"concurrency"`
	StartTime     time.Time `json:"start_time"`
	EndTime       string    `json:"end_time,omitempty"`
	ErrorMessage  string    `json:"error_message,omitempty"`
}

type ShardStatusResponse struct {
	ShardIndex   int    `json:"shard_index"`
	Status       string `json:"status"`
	RecordCount  int64  `json:"record_count"`
	OSSPath      string `json:"oss_path"`
	RetryCount   int    `json:"retry_count"`
	ErrorMessage string `json:"error_message,omitempty"`
}

type ListJobsResponse struct {
	Total int                 `json:"total"`
	Jobs  []JobStatusResponse `json:"jobs"`
}

type ControlJobRequest struct {
	Action string `json:"action" binding:"required"`
}

type CompensationRequest struct {
	JobID string `json:"job_id" binding:"required"`
}

type CompensationResponse struct {
	CompensationID string `json:"compensation_id"`
	Status         string `json:"status"`
	RetryShardCount int  `json:"retry_shard_count"`
}

type StatsResponse struct {
	TotalJobs        int     `json:"total_jobs"`
	RunningJobs      int     `json:"running_jobs"`
	CompletedJobs    int     `json:"completed_jobs"`
	FailedJobs       int     `json:"failed_jobs"`
	TotalArchivedRecords int64 `json:"total_archived_records"`
	TotalOSSSize     int64   `json:"total_oss_size_bytes"`
}
