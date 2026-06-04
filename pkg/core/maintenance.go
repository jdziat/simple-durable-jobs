package core

import "time"

// SchemaMigration records one versioned schema migration that has been applied
// to a database. Rows are inserted by the storage migration runner after a
// migration's body succeeds, so each version is applied at most once per
// database. The ledger is what makes index/constraint/data changes safe to ship
// to existing deployments (AutoMigrate only ever adds columns/indexes; it never
// replaces or backfills).
type SchemaMigration struct {
	Version   int       `gorm:"primaryKey"`
	Name      string    `gorm:"size:255"`
	AppliedAt time.Time `gorm:"autoCreateTime"`
}

// Lease is a generic, expiring advisory lease. It is used to elect a single
// worker in a fleet to run periodic, fleet-wide maintenance (e.g. the fan-out
// recovery poll) so that N workers do not all run the same scan every tick.
//
// Acquisition is a conditional upsert keyed on Name: a caller takes the lease
// only when there is no holder or the current lease has expired. ExpiresAt is
// stamped from the database clock on multi-worker backends (Postgres/MySQL) so
// that lease arbitration is immune to per-worker clock skew, exactly like the
// job lock. The holder renews by re-acquiring before ExpiresAt; if it dies, the
// lease simply expires and another worker takes over.
type Lease struct {
	Name      string    `gorm:"primaryKey;size:128"`
	Owner     string    `gorm:"size:255"`
	ExpiresAt time.Time `gorm:"index"`
}
