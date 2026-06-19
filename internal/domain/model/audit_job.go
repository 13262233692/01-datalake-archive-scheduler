package model

import "time"

type AuditStatus string

const (
	AuditStatusPending   AuditStatus = "pending"
	AuditStatusRunning AuditStatus = "running"
	AuditStatusPassed  AuditStatus = "passed"
	AuditStatusFailed  AuditStatus = "failed"
	AuditStatusAlert   AuditStatus = "alert"
)

type AuditJob struct {
	ID              string          `json:"id"`
	ArchiveJobID    string          `json:"archive_job_id"`
	TableName       string          `json:"table_name"`
	TargetTableName string          `json:"target_table_name"`
	Status          AuditStatus     `json:"status"`
	BatchCount      int             `json:"batch_count"`
	SampledRecords  int64           `json:"sampled_records"`
	TotalRecords    int64           `json:"total_records"`
	SourceRootHash  string          `json:"source_root_hash"`
	TargetRootHash  string          `json:"target_root_hash"`
	MatchCount      int             `json:"match_count"`
	MismatchCount   int             `json:"mismatch_count"`
	MissingCount    int             `json:"missing_count"`
	AlertLevel      string          `json:"alert_level"`
	ErrorMessage    string          `json:"error_message,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	DurationMs      int64           `json:"duration_ms"`
}

type AuditBatch struct {
	ID              string      `json:"id"`
	AuditJobID      string    `json:"audit_job_id"`
	BatchIndex      int         `json:"batch_index"`
	StartID         int64       `json:"start_id"`
	EndID           int64       `json:"end_id"`
	RecordCount     int         `json:"record_count"`
	SourceHash      string    `json:"source_hash"`
	TargetHash      string    `json:"target_hash"`
	Status          string    `json:"status"`
	IsMatched       bool      `json:"is_matched"`
	MismatchedIDs    []int64     `json:"mismatched_ids,omitempty"`
	MissingIDs     []int64     `json:"missing_ids,omitempty"`
	ProcessedAt    *time.Time  `json:"processed_at,omitempty"`
}

type AuditRecord struct {
	RecordID  int64  `json:"record_id"`
	Fields  map[string]interface{} `json:"fields"`
	Hash    string `json:"hash"`
}

type MerkleNode struct {
	Hash  string
	Left  *MerkleNode
	Right *MerkleNode
}

type MerkleTree struct {
	Root     *MerkleNode
	Leaves   []*MerkleNode
	RootHash string
}

func (t *MerkleTree) GetRootHash() string {
	return t.RootHash
}

func (t *MerkleTree) GetLeafCount() int {
	return len(t.Leaves)
}
