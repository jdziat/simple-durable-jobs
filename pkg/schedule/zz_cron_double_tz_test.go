package schedule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An expression naming TWO timezones used to be accepted, with one of them
// silently discarded. robfig's parser understands TZ=/CRON_TZ= itself, so it
// stripped the inner name and set the location from it; parseCronIn then
// overwrote that with the OUTER name. The caller got a schedule firing in a
// timezone they did not ask for, with no error — the silent-wrong-answer shape
// this package's timezone handling exists to remove.
func TestCron_RejectsMoreThanOneTimezone(t *testing.T) {
	for _, expr := range []string{
		"CRON_TZ=UTC TZ=Asia/Tokyo 0 9 * * *",
		"TZ=Asia/Tokyo CRON_TZ=UTC 0 9 * * *",
		"CRON_TZ=UTC CRON_TZ=Europe/Berlin 0 9 * * *",
		"TZ=UTC TZ=UTC 0 9 * * *", // even when they agree: still ambiguous input
	} {
		t.Run(expr, func(t *testing.T) {
			_, err := Cron(expr)
			require.Error(t, err,
				"an expression naming two timezones must be rejected, not resolved by picking one and discarding the other")
			require.Contains(t, err.Error(), "more than one timezone",
				"the error must say what is wrong so the caller can fix the expression")
		})
	}
}

// The single-prefix forms must keep working, or the guard has broken the feature
// it protects.
func TestCron_StillAcceptsExactlyOneTimezone(t *testing.T) {
	for _, expr := range []string{
		"CRON_TZ=Asia/Tokyo 0 9 * * *",
		"TZ=Europe/Berlin 0 9 * * *",
		"0 9 * * *", // no prefix at all
	} {
		t.Run(expr, func(t *testing.T) {
			s, err := Cron(expr)
			require.NoError(t, err)
			require.NotNil(t, s)
		})
	}
}
