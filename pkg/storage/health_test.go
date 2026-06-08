package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGormStorage_HealthPingSuccess(t *testing.T) {
	ctx := context.Background()
	s := NewGormStorage(openTestDB(t))

	require.NoError(t, s.Ping(ctx))
}

func TestGormStorage_HealthPingContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := NewGormStorage(openTestDB(t))

	err := s.Ping(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGormStorage_HealthPingClosedDB(t *testing.T) {
	ctx := context.Background()
	s := NewGormStorage(openTestDB(t))

	sqlDB, err := s.DB().DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	err = s.Ping(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "storage: ping database")
	require.NotNil(t, errors.Unwrap(err))
}
