package core

import (
	"strings"
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

// TerminalJobStatuses are statuses past which a job does no more work.
// Retention may reap terminal jobs, and workflow parent/root refs are cleared.
var TerminalJobStatuses = []JobStatus{StatusCompleted, StatusFailed, StatusCancelled}

// IsTerminal reports whether s is a terminal job status.
func (s JobStatus) IsTerminal() bool {
	for _, status := range TerminalJobStatuses {
		if s == status {
			return true
		}
	}
	return false
}

// ActiveDedupStatuses are the statuses in which a job still HOLDS a
// queue.Unique(key) guard: it has not finished, so a second job with the same key
// is a duplicate.
//
// It is the complement of TerminalJobStatuses (plus retrying, which is a transient
// spelling of pending), and that is the point: Unique documents its guard as
// releasing "as soon as the existing job reaches completed, failed, or cancelled".
// A `waiting` job — parked on a signal or a fan-out — and a `paused` job are
// neither, so both still hold it. The windowed sibling (unique_locks) already
// treats them as in-progress; this is what makes the two mechanisms agree.
var ActiveDedupStatuses = []JobStatus{
	StatusPending, StatusRunning, StatusRetrying, StatusWaiting, StatusPaused,
}

// MetadataMap stores queryable string metadata for jobs and job filters.
type MetadataMap map[string]string

// Job represents a unit of work to be processed.
type Job struct {
	ID    UUID   `gorm:"primaryKey"`
	Type  string `gorm:"index;size:255;not null"`
	Args  []byte `gorm:"type:bytes"`
	Queue string `gorm:"size:255;default:'default';not null"`
	// Tenant identifies the tenant that owns this job.
	Tenant string `gorm:"size:255;column:tenant"`
	// Metadata stores queryable string tags for this job.
	Metadata       map[string]string `gorm:"serializer:json;column:metadata"`
	Priority       int               `gorm:"type:integer;default:0;not null"`
	Status         JobStatus         `gorm:"size:20;default:'pending';not null;index:idx_jobs_fan_out_status,priority:2"`
	PreviousStatus JobStatus         `gorm:"size:20"` // Status before pause, for restoration
	Attempt        int               `gorm:"type:integer;default:0;not null"`
	// No gorm `default:` on purpose. GORM omits a zero-valued field from the
	// INSERT when the field declares a default, so a deliberate Retries(0) was
	// silently replaced by the column default and a job marked do-not-retry ran
	// three times. The Go layer already supplies the default (queue.Options starts
	// at DefaultJobRetries), so the tag was only ever masking the explicit zero.
	// Migration v41 keeps the DB-level default for writers that omit the column.
	MaxRetries int           `gorm:"type:integer;not null"`
	Timeout    time.Duration `gorm:"not null;default:0"`
	// Determinism is the replay strictness mode
	// (0=ExplicitCheckpoints,1=Strict,2=BestEffort).
	// BestEffort relaxes the Call replay type-mismatch guard.
	Determinism      int    `gorm:"type:integer;not null;default:0"`
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
	// DQReady is a performance hint for MySQL dequeue: true iff this is a pending
	// job eligible to run now (run_at is nil or in the past). It lets MySQL use an
	// index that serves the priority-ordered dequeue without a filesort. It is NOT a
	// correctness gate — Dequeue still filters dq_eligible_at <= now, so a stale
	// dq_ready can only cost latency, never cause an incorrect dequeue.
	DQReady bool `gorm:"column:dq_ready;not null;default:true"`

	// Parent-child relationship
	ParentJobID *UUID `gorm:"index"`
	RootJobID   *UUID // Top-level workflow job

	// Fan-out tracking
	FanOutID    *UUID `gorm:"index:idx_jobs_fan_out_status,priority:1"` // Groups sibling sub-jobs
	FanOutIndex int   `gorm:"type:integer;default:0"`                   // Position in fan-out batch

	// WaitingSignalName is the signal name this job most recently suspended on,
	// and is meaningful only while Status is StatusWaiting.
	//
	// The signal-resume poll correlates against it so a pending signal the handler
	// will never consume cannot wake the job on every tick forever. EMPTY means
	// "not recorded", and the poll then falls back to waking on any pending
	// signal: fan-out suspends go through plain MarkWaiting, and a third-party
	// core.Storage need not implement SignalWaitMarker, so permissiveness is what
	// preserves liveness in both cases.
	WaitingSignalName string `gorm:"size:255;not null;default:''"`

	// Result storage for parent retrieval
	Result []byte `gorm:"type:bytes"` // Serialized return value

	// Trace context for distributed tracing (serialized span context)
	TraceContext []byte `gorm:"type:bytes"`
}

// Checkpoint stores the result of a durable Call() for replay.
type Checkpoint struct {
	ID        UUID   `gorm:"primaryKey"`
	JobID     UUID   `gorm:"index;uniqueIndex:idx_checkpoints_job_call,priority:1;not null"`
	CallIndex int    `gorm:"type:integer;uniqueIndex:idx_checkpoints_job_call,priority:2;not null"`
	CallType  string `gorm:"uniqueIndex:idx_checkpoints_job_call,priority:3;size:255;not null"`
	Result    []byte `gorm:"type:bytes"`
	Error     string `gorm:"type:text"`
	ErrorKind string `gorm:"size:64"`
	// ErrorCause holds the discriminator-specific reconstruction payload: for
	// no_retry/retry_after it is the inner cause message (so the wrapper is
	// rebuilt without parsing the formatted prefix); for sentinel errors it is
	// the stable sentinel key. Empty for checkpoints written before this column
	// existed — RehydrateCheckpointError falls back to message parsing then.
	ErrorCause      string `gorm:"type:text"`
	ErrorDelayNanos int64  `gorm:"default:0"`
	// SpanEnd is the value of the handler's call counter immediately after the
	// checkpointed call returned — i.e. one past the LAST index that call and
	// everything nested beneath it consumed.
	//
	// It exists because Call indices come from a single flat counter shared with
	// nested durable operations. On a first run, an outer Call that internally
	// issues its own Call consumes two indices; on replay the outer call is
	// served from its checkpoint WITHOUT re-invoking the handler, so the nested
	// index is never consumed and every later call reads a checkpoint one slot
	// too low — silently returning another call's cached result, or raising a
	// bogus determinism violation when the types happen to differ.
	//
	// Replay therefore jumps the counter to SpanEnd rather than incrementing it.
	// A checkpoint written before this column existed has SpanEnd == 0, which is
	// less than callIndex+1 and so degrades to the historical +1 behaviour —
	// making non-nested workflows bit-for-bit unchanged, and leaving workflows
	// already in flight exactly as (in)correct as they were before the upgrade.
	SpanEnd int `gorm:"type:integer;not null;default:0"`
	// ResultShape fingerprints the JSON SHAPE of the result type this checkpoint
	// was written from, so replay can tell "the handler now returns a different
	// type" from "the stored payload happens to look different". It is structural
	// (field names and kinds), not nominal, so moving or renaming the type does not
	// trip replay while changing its fields does.
	//
	// EMPTY means "not recorded" — every checkpoint written before this column
	// existed — and replay then skips the comparison entirely, exactly as SpanEnd
	// == 0 degrades to the historical behaviour. A workflow already in flight is
	// therefore unaffected by the upgrade.
	ResultShape string    `gorm:"size:32;not null;default:''"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
}

// Checkpoint CallType values reserved for BUILT-IN durable operations rather
// than a user Call(). They share the flat call-index counter with Call, so they
// occupy real (non-negative) call indices, but only Call() records a SpanEnd —
// these are always written with SpanEnd == 0, by every version.
//
// They are defined here, next to the Checkpoint they describe, so the producers
// and the legacy-span detector read from ONE list. Keeping the detector's
// exclusions hand-synced against the producers is how span_end itself came to be
// omitted from a hand-written column list once already.
const (
	// CheckpointTypeFanOut is the CallType of a fan-out's checkpoint.
	CheckpointTypeFanOut = "fanout"
	// CheckpointTypeSignalPrefix prefixes the CallType of a signal wait,
	// followed by the signal name.
	CheckpointTypeSignalPrefix = "signal:"
	// CheckpointTypeSignalTimeoutPrefix prefixes the CallType of a signal wait
	// with a timeout, followed by the signal name.
	CheckpointTypeSignalTimeoutPrefix = "signaltimeout:"
	// CheckpointTypeSignalPeekPrefix prefixes the CallType of a non-blocking
	// signal peek, followed by the signal name.
	CheckpointTypeSignalPeekPrefix = "signalpeek:"
	// CheckpointTypeSignalDrainPrefix prefixes the CallType of a signal drain,
	// followed by the signal name.
	CheckpointTypeSignalDrainPrefix = "signaldrain:"
	// CheckpointTypeSleep is the CallType of a durable sleep. pkg/signal
	// re-exports this as SleepCheckpointType.
	CheckpointTypeSleep = "_sleep"
)

// IsCallCheckpointType reports whether callType belongs to a user Call() rather
// than one of the built-in durable operations above.
//
// This is the discriminator the legacy-span detector needs. SpanEnd == 0 alone
// does not mean "written before span tracking existed": a built-in operation's
// checkpoint has SpanEnd == 0 in every version, including the current one, so
// treating it as legacy flags healthy work as pre-upgrade-corrupt.
//
// Residual, accepted: a user Call() named exactly "fanout", or one whose name
// begins "signal:" or "signaltimeout:", is classified as built-in and so escapes
// the legacy listing. That is a deliberate trade against the alternative — a
// systematic false positive on every workflow using two built-in operations,
// whose documented repair discards completed work.
func IsCallCheckpointType(callType string) bool {
	exact, prefixes := builtinCheckpointTypeMatchers()
	for _, v := range exact {
		if callType == v {
			return false
		}
	}
	for _, p := range prefixes {
		if strings.HasPrefix(callType, p) {
			return false
		}
	}
	return true
}

// builtinCheckpointTypeMatchers is every built-in CallType shape, as an exact
// value or a LIKE prefix. It is the SINGLE source both spellings read:
// IsCallCheckpointType walks it directly and BuiltinCheckpointTypeSQLExclusion
// renders it as SQL, so the Go predicate and the SQL predicate cannot disagree.
//
// This used to be a second list sitting next to a hand-written switch, with a
// comment claiming proximity meant one could not be updated without the other.
// Proximity is not a guarantee: dropping a type from the SQL list alone left
// IsCallCheckpointType correct while the legacy-span query started flagging that
// operation, and no test failed — a job with one non-excluded checkpoint sits at
// count = 1, below the query's `HAVING count > 1` threshold, so the omission was
// invisible until a workflow used the same operation twice.
func builtinCheckpointTypeMatchers() (exact []string, prefixes []string) {
	return []string{CheckpointTypeFanOut, CheckpointTypeSleep},
		[]string{
			CheckpointTypeSignalPrefix,
			CheckpointTypeSignalTimeoutPrefix,
			CheckpointTypeSignalPeekPrefix,
			CheckpointTypeSignalDrainPrefix,
		}
}

// BuiltinCheckpointTypeSQLExclusion renders the built-in checkpoint types as a
// SQL predicate over `col`, together with its bind arguments, so a query can
// select only user Call() checkpoints. The predicate is the SQL twin of
// IsCallCheckpointType and is generated from the same lists.
func BuiltinCheckpointTypeSQLExclusion(col string) (string, []any) {
	exact, prefixes := builtinCheckpointTypeMatchers()
	clauses := make([]string, 0, len(exact)+len(prefixes))
	args := make([]any, 0, len(exact)+len(prefixes))
	for _, v := range exact {
		clauses = append(clauses, col+" <> ?")
		args = append(args, v)
	}
	for _, p := range prefixes {
		clauses = append(clauses, col+" NOT LIKE ?")
		args = append(args, p+"%")
	}
	return strings.Join(clauses, " AND "), args
}

// FanOutCheckpoint stores fan-out state for job replay.
type FanOutCheckpoint struct {
	FanOutID  UUID `json:"fan_out_id"`
	CallIndex int  `json:"call_index"`
}
