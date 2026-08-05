package fanout

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/storage"
)

// A fan-out child's retry count is ALWAYS resolved before the core.Job is built —
// Sub()'s explicit value when RetriesSet, otherwise the fan-out config's — so it is
// always deliberate, including when it resolves to zero. Storage cannot tell a
// deliberate 0 from an untouched int field, so buildSubJobs has to say so; without
// that, `jobs.Sub(..., jobs.Retries(0))` and `WithFanOutRetries(0)` are both
// replaced by the max_retries column default and the child runs FOUR times.
//
// This is checked against the real column rather than the struct field, because
// the struct field being right is exactly what the shipped bug looked like: the
// intent was computed correctly and then dropped one layer below.
func TestFanOutChildRetryIntentReachesTheColumn(t *testing.T) {
	ctx := context.Background()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	store := storage.NewGormStorage(db)
	require.NoError(t, store.Migrate(ctx))

	storedRetries := func(t *testing.T, id core.UUID) int {
		t.Helper()
		var raw int
		require.NoError(t, db.Raw("SELECT max_retries FROM jobs WHERE id = ?", id).Scan(&raw).Error)
		return raw
	}

	// literalTestCtx's parent ID is a readable placeholder, which the binary-UUID
	// primary key rejects on INSERT. This path actually persists, so it needs real
	// IDs.
	jc := literalTestCtx()
	jc.Job = &core.Job{ID: core.NewID(), Queue: "parent-q"}

	cases := []struct {
		name string
		cfg  *config
		sub  SubJob
		want int
	}{
		{
			name: "Sub with an explicit Retries(0) runs once",
			cfg:  &config{queue: "fan-q", retries: queue.DefaultJobRetries},
			sub:  Sub("work", "x", queue.Retries(0)),
			want: 0,
		},
		{
			name: "WithFanOutRetries(0) runs once",
			cfg:  &config{queue: "fan-q", retries: 0},
			sub:  Sub("work", "x"),
			want: 0,
		},
		{
			name: "a non-zero fan-out default still round-trips",
			cfg:  &config{queue: "fan-q", retries: 4},
			sub:  Sub("work", "x"),
			want: 4,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			built, err := buildSubJobs([]SubJob{tc.sub}, tc.cfg, jc, core.NewID(), time.Now())
			require.NoError(t, err)
			require.Len(t, built, 1)

			// The real path: EnqueueBatch is what FanOut uses for children.
			require.NoError(t, store.EnqueueBatch(ctx, built))
			assert.Equal(t, tc.want, storedRetries(t, built[0].ID),
				"the child's resolved retry budget must reach the column, not be replaced "+
					"by the max_retries default")
		})
	}
}
