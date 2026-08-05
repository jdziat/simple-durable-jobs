package core

import "time"

// DeadLetterFilter scopes dead-letter triage queries.
type DeadLetterFilter struct {
	Queue string
	Type  string
	// Tenant matches dead-lettered jobs owned by exactly this tenant.
	Tenant string
	// MetaContains requires every key/value pair to appear in the job metadata.
	MetaContains *MetadataMap
	Search       string
	// DeadLetteredSince/DeadLetteredUntil are the INCLUSIVE bounds of a time
	// window over dead_lettered_at — WHEN THE JOB DIED, not when it was created.
	// A zero value means "no bound" on that side.
	//
	// The column is named in the FIELD rather than left to a doc comment because
	// the two candidate columns give different answers and the difference is not
	// cosmetic: a job created 48h ago and dead-lettered a second ago is exactly
	// what a "what died in the last hour" triage query is looking for, and
	// bounding created_at would hide it. dead_lettered_at is also the column this
	// view is ORDERED by (dead_lettered_at DESC — see ListDeadLettered), so
	// filtering it keeps the window and the sort talking about the same COLUMN,
	// and it is indexed (idx_jobs_dead_lettered_at).
	//
	// Same column is not automatically the same AXIS. On SQLite the window selects
	// by instant (julianday) while the ORDER BY compares wall text, so the two
	// agree only while every row wears one clock face. Every row this release
	// writes does; rows dead-lettered by an EARLIER release keep the local face
	// they were written on and can still sort out of order among themselves. They
	// are never dropped from the window, only mis-placed in the sort, and they
	// drain with retention. Postgres and MySQL store a real instant and have
	// neither concern. See storage.deadLetterOrderColumn.
	//
	// JobFilter.Since/Until bound created_at, and that stays true. ListJobs maps
	// its request-level since/until onto whichever column the branch it selected
	// is about; see ui.jobsService.searchJobs.
	//
	// Both bounds select by INSTANT, whatever timezone they and the stored row are
	// expressed in. On SQLITE ONLY there is one accepted limit: dead_lettered_at is
	// TEXT carrying the offset of whichever process wrote it, and when that offset
	// differs from the bound's the two are normalized through julianday(), which
	// resolves to MILLISECONDS — so a job that died less than 1ms outside the
	// window can still be returned. The error is bounded by 1ms and always returns
	// MORE — a job that died inside the window is never dropped. Matching offsets
	// compare exactly, to the nanosecond. Postgres and MySQL store a real instant
	// and have neither limit.
	//
	// The never-drops half is load-bearing and was WRONG in an earlier release,
	// which normalized to text with strftime(): SQLite renders one instant
	// differently depending on whether its offset is zero, so a job sitting exactly
	// on an inclusive bound was dropped and CountDeadLettered under-counted. See
	// timeBoundPredicate in pkg/storage for the measured bands and the tests that
	// now pin it.
	//
	// These are plain time.Time (comparable) on purpose — DeadLetterFilter is an
	// exported concrete struct and the release-gating api-compat job treats a
	// loss of comparability as an incompatible change.
	DeadLetteredSince time.Time
	DeadLetteredUntil time.Time
	Limit             int
	Offset            int
	// SortKey/SortDir select a whitelisted order column (see GormStorage). When
	// SortKey is empty the default dead-letter order (dead_lettered_at DESC) is
	// kept so the most-recently-dead jobs surface first.
	SortKey string
	SortDir string
}
