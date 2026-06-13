package core

import (
	"time"
)

// JobStatus represents the current state of a job.
type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusRetrying  JobStatus = "retrying"
	StatusWaiting   JobStatus = "waiting"   // Waiting for fan-out sub-jobs to complete
	StatusCancelled JobStatus = "cancelled" // Terminated before completion
	StatusPaused    JobStatus = "paused"    // Paused, won't be picked up
)

// AllJobStatuses is the single source of truth that the migration CHECK and
// drift guard derive from.
var AllJobStatuses = []JobStatus{StatusPending, StatusRunning, StatusCompleted, StatusFailed, StatusRetrying, StatusWaiting, StatusCancelled, StatusPaused}

// MetadataMap stores queryable string metadata for jobs and job filters.
type MetadataMap map[string]string

// Job represents a unit of work to be processed.
type Job struct {
	ID    string `gorm:"primaryKey;size:36"`
	Type  string `gorm:"index;size:255;not null"`
	Args  []byte `gorm:"type:bytes"`
	Queue string `gorm:"size:255;default:'default';not null"`
	// Tenant identifies the tenant that owns this job.
	Tenant string `gorm:"size:255;column:tenant"`
	// Metadata stores queryable string tags for this job.
	Metadata       map[string]string `gorm:"serializer:json;column:metadata"`
	Priority       int               `gorm:"default:0;not null"`
	Status         JobStatus         `gorm:"size:20;default:'pending';not null;index:idx_jobs_fan_out_status,priority:2"`
	PreviousStatus JobStatus         `gorm:"size:20"` // Status before pause, for restoration
	Attempt        int               `gorm:"default:0;not null"`
	MaxRetries     int               `gorm:"default:3;not null"`
	Timeout        time.Duration     `gorm:"not null;default:0"`
	// Determinism is the replay strictness mode
	// (0=ExplicitCheckpoints,1=Strict,2=BestEffort).
	// BestEffort relaxes the Call replay type-mismatch guard.
	Determinism      int    `gorm:"not null;default:0"`
	LastError        string `gorm:"type:text"`
	DeadLetteredAt   *time.Time
	DeadLetterReason string `gorm:"type:text"`
	RunAt            *time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
	CreatedAt        time.Time `gorm:"autoCreateTime"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime"`
	LockedBy         string    `gorm:"size:255"`
	LockedUntil      *time.Time
	LastHeartbeatAt  *time.Time // Tracks when the last heartbeat was received
	UniqueKey        string     `gorm:"size:255"` // For job deduplication

	// Parent-child relationship
	ParentJobID *string `gorm:"index;size:36"`
	RootJobID   *string `gorm:"index;size:36"` // Top-level workflow job

	// Fan-out tracking
	FanOutID    *string `gorm:"index:idx_jobs_fan_out_status,priority:1;size:36"` // Groups sibling sub-jobs
	FanOutIndex int     `gorm:"default:0"`                                        // Position in fan-out batch

	// Result storage for parent retrieval
	Result []byte `gorm:"type:bytes"` // Serialized return value

	// Trace context for distributed tracing (serialized span context)
	TraceContext []byte `gorm:"type:bytes"`
}

// Checkpoint stores the result of a durable Call() for replay.
type Checkpoint struct {
	ID        string `gorm:"primaryKey;size:36"`
	JobID     string `gorm:"index;uniqueIndex:idx_checkpoints_job_call,priority:1;size:36;not null"`
	CallIndex int    `gorm:"uniqueIndex:idx_checkpoints_job_call,priority:2;not null"`
	CallType  string `gorm:"uniqueIndex:idx_checkpoints_job_call,priority:3;size:255;not null"`
	Result    []byte `gorm:"type:bytes"`
	Error     string `gorm:"type:text"`
	ErrorKind string `gorm:"size:64"`
	// ErrorCause holds the discriminator-specific reconstruction payload: for
	// no_retry/retry_after it is the inner cause message (so the wrapper is
	// rebuilt without parsing the formatted prefix); for sentinel errors it is
	// the stable sentinel key. Empty for checkpoints written before this column
	// existed — RehydrateCheckpointError falls back to message parsing then.
	ErrorCause      string    `gorm:"type:text"`
	ErrorDelayNanos int64     `gorm:"default:0"`
	CreatedAt       time.Time `gorm:"autoCreateTime"`
}

// FanOutCheckpoint stores fan-out state for job replay.
type FanOutCheckpoint struct {
	FanOutID  string `json:"fan_out_id"`
	CallIndex int    `json:"call_index"`
}
