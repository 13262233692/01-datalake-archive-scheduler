package model

import (
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	JobStatusPending    JobStatus = "PENDING"
	JobStatusRunning    JobStatus = "RUNNING"
	JobStatusPaused     JobStatus = "PAUSED"
	JobStatusCompleted  JobStatus = "COMPLETED"
	JobStatusFailed     JobStatus = "FAILED"
	JobStatusCompensating JobStatus = "COMPENSATING"
)

type ArchiveJob struct {
	ID            string
	JobName       string
	TableName     string
	ColdDate      time.Time
	Status        JobStatus
	TotalRecords  int64
	ArchivedCount int64
	FailedCount   int64
	ShardCount    int
	Concurrency   int
	StartTime     time.Time
	EndTime       *time.Time
	ErrorMessage  string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

func NewArchiveJob(tableName string, coldDate time.Time, shardCount, concurrency int) *ArchiveJob {
	return &ArchiveJob{
		ID:          uuid.New().String(),
		JobName:     "archive-" + tableName + "-" + coldDate.Format("20060102"),
		TableName:   tableName,
		ColdDate:    coldDate,
		Status:      JobStatusPending,
		ShardCount:  shardCount,
		Concurrency: concurrency,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

func (j *ArchiveJob) Start() {
	j.Status = JobStatusRunning
	j.StartTime = time.Now()
	j.UpdatedAt = time.Now()
}

func (j *ArchiveJob) Complete() {
	j.Status = JobStatusCompleted
	now := time.Now()
	j.EndTime = &now
	j.UpdatedAt = now
}

func (j *ArchiveJob) Fail(errMsg string) {
	j.Status = JobStatusFailed
	j.ErrorMessage = errMsg
	now := time.Now()
	j.EndTime = &now
	j.UpdatedAt = now
}

func (j *ArchiveJob) StartCompensation() {
	j.Status = JobStatusCompensating
	j.UpdatedAt = time.Now()
}

func (j *ArchiveJob) Pause() {
	j.Status = JobStatusPaused
	j.UpdatedAt = time.Now()
}

func (j *ArchiveJob) IsFinished() bool {
	return j.Status == JobStatusCompleted || j.Status == JobStatusFailed
}

func (j *ArchiveJob) Progress() float64 {
	if j.TotalRecords == 0 {
		return 0
	}
	return float64(j.ArchivedCount) / float64(j.TotalRecords) * 100
}
