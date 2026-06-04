package core

import "time"

// Signal is a durable, named message delivered to a specific job (workflow).
//
// Producers persist a signal with SendSignal; a running or waiting handler
// receives signals via WaitForSignal / CheckSignal / DrainSignals. Signals are
// buffered — one sent before the handler waits for it is not lost — and
// delivered in FIFO (CreatedAt) order per (JobID, Name). A signal is "pending"
// until ConsumedAt is set; CheckSignal peeks without consuming, WaitForSignal
// and DrainSignals consume.
type Signal struct {
	ID    string `gorm:"primaryKey;size:36"`
	JobID string `gorm:"index;index:idx_signals_pending,priority:1;size:36;not null"`
	Name  string `gorm:"index:idx_signals_pending,priority:2;size:255;not null"`
	// Payload is the JSON-encoded signal value.
	Payload []byte `gorm:"type:bytes"`
	// ConsumedAt is nil while the signal is pending; set when consumed. The
	// composite index (job_id, name, consumed_at) supports the
	// oldest-pending-of-(job,name) lookup and the has-pending-signal poll.
	ConsumedAt *time.Time `gorm:"index:idx_signals_pending,priority:3"`
	CreatedAt  time.Time  `gorm:"autoCreateTime"`
}
