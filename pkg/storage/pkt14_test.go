package storage

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestRequireMySQLUTCDriverLoc_DetectsNonUTCLoc (PKT-14 / D3) proves the guard
// fails when the MySQL driver DSN uses a non-UTC loc (which silently shifts every
// stored timestamp) and passes for a UTC loc. Skips unless a live MySQL is set.
func TestRequireMySQLUTCDriverLoc_DetectsNonUTCLoc(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_URL")
	if dsn == "" {
		t.Skip("TEST_MYSQL_URL not set — D3 loc guard needs a live MySQL")
	}
	ctx := context.Background()

	// Non-UTC loc must FAIL the guard.
	badDSN := withLoc(dsn, "America/New_York")
	badDB, err := gorm.Open(mysql.Open(badDSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err, "open mysql with non-UTC loc")
	err = requireMySQLUTCDriverLoc(ctx, badDB)
	require.Error(t, err, "a non-UTC driver loc must be detected")
	require.Contains(t, err.Error(), "loc is not UTC")

	// Explicit UTC loc must PASS.
	goodDB, err := gorm.Open(mysql.Open(withLoc(dsn, "UTC")), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, requireMySQLUTCDriverLoc(ctx, goodDB), "a UTC driver loc must pass")
}

// withLoc sets or replaces the loc= DSN parameter (and ensures parseTime=true,
// required for loc to take effect on time.Time binds).
func withLoc(dsn, loc string) string {
	base := dsn
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		base = dsn[:i]
	}
	return base + "?parseTime=true&loc=" + strings.ReplaceAll(loc, "/", "%2F")
}
