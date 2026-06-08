package storage

import (
	"context"
	"fmt"
)

// Healther is the optional storage capability for checking backing database
// connectivity.
type Healther interface {
	Ping(context.Context) error
}

var _ Healther = (*GormStorage)(nil)

// Ping checks whether the backing database is reachable.
func (s *GormStorage) Ping(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("storage: get sql db for ping: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return fmt.Errorf("storage: ping database: %w", err)
	}
	return nil
}
