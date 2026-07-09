package storage

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// recordingTB is a minimal testing.TB that captures the first Fatalf instead of
// aborting the process, so a test can assert that an isolation-gate helper fails
// loudly (and names the offending table) on a dirty shared DB. Real
// testing.T.Fatalf does not return; we mimic that by unwinding through
// panic(errGateFatal), which runGate recovers.
type recordingTB struct {
	testing.TB
	fataled  bool
	fatalMsg string
}

var errGateFatal = errors.New("recordingTB: gate fatal")

func (r *recordingTB) Helper()               {}
func (r *recordingTB) Logf(string, ...any)   {}
func (r *recordingTB) Errorf(string, ...any) {}
func (r *recordingTB) Fatalf(format string, args ...any) {
	r.fataled = true
	r.fatalMsg = fmt.Sprintf(format, args...)
	panic(errGateFatal)
}

// runGate invokes requireTablesEmpty against rec, absorbing the Goexit-substitute
// panic so a Fatalf is recorded rather than propagated.
func runGate(rec *recordingTB, db *gorm.DB) {
	defer func() {
		if r := recover(); r != nil && r != errGateFatal {
			panic(r)
		}
	}()
	requireTablesEmpty(rec, db)
}

// TestRequireTablesEmpty_DetectsContamination proves the shared-DB isolation
// safety net actually works: a stray row left behind by a (simulated) lost
// cleanup DELETE is DETECTED at the emptiness gate and reported loudly, naming
// the offending table — instead of silently corrupting the next test's dequeue
// ordering, which is the exact CI flake this hardening addresses.
//
// Skipped on the per-test :memory: SQLite path, where each test gets a private
// database and the cross-test contamination this guards against cannot occur.
func TestRequireTablesEmpty_DetectsContamination(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" && os.Getenv("TEST_MYSQL_URL") == "" {
		t.Skip("external shared DB not configured; contamination cannot occur on per-test :memory: sqlite")
	}
	s := newTestStorage(t) // clean start (requireCleanStart) + migrated schema

	// Baseline: a freshly-started, migrated store MUST pass the gate. If this
	// trips, Migrate is seeding a per-test table and the gate's table list needs
	// revisiting — surface that here rather than as a confusing failure elsewhere.
	base := &recordingTB{TB: t}
	runGate(base, s.db)
	require.False(t, base.fataled, "a clean, freshly-migrated store must pass the emptiness gate; got: %s", base.fatalMsg)

	// Simulate a prior test whose cleanup DELETE was lost: one stray row survives.
	require.NoError(t, s.db.Create(&core.Job{
		ID:     core.NewID(),
		Type:   "stray",
		Queue:  "default",
		Status: core.StatusPending,
	}).Error)

	rec := &recordingTB{TB: t}
	runGate(rec, s.db)

	assert.True(t, rec.fataled, "requireTablesEmpty must fail loudly when a table is non-empty")
	assert.Contains(t, rec.fatalMsg, "jobs", "the failure must name the offending table so the leak is diagnosable")

	// Leave the DB clean so this test's own teardown gate is satisfied.
	require.NoError(t, s.db.Exec("DELETE FROM jobs").Error)
}

// TestDeleteAllWithRetry_SucceedsOnHealthyTable is a fast sanity check that the
// retry wrapper clears a populated table on the happy path (no transient error).
// Runs on every backend, including SQLite.
func TestDeleteAllWithRetry_SucceedsOnHealthyTable(t *testing.T) {
	s := newTestStorage(t)
	for i := 0; i < 3; i++ {
		require.NoError(t, s.db.Create(&core.Job{
			ID:     core.NewID(),
			Type:   "work",
			Queue:  "default",
			Status: core.StatusPending,
		}).Error)
	}
	require.NoError(t, deleteAllWithRetry(s.db, "jobs"))

	var n int64
	require.NoError(t, s.db.Raw("SELECT COUNT(*) FROM jobs").Scan(&n).Error)
	assert.Equal(t, int64(0), n, "deleteAllWithRetry must clear the table on the happy path")
}
