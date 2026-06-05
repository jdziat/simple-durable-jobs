package core

import "time"

// ConcurrencySlot is a leased admission slot for a named fleet-wide or per-key
// concurrency cap. The composite primary key makes re-acquiring the same
// (slot_name, job_id) idempotent: the owning worker renews the existing row
// instead of consuming another slot.
type ConcurrencySlot struct {
	SlotName  string    `gorm:"primaryKey;size:255;index:idx_concurrency_slots_live,priority:1"`
	JobID     string    `gorm:"primaryKey;size:36"`
	WorkerID  string    `gorm:"size:255"`
	ExpiresAt time.Time `gorm:"index;index:idx_concurrency_slots_live,priority:2"`
}
