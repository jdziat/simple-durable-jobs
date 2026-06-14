package storage

import "github.com/jdziat/simple-durable-jobs/v3/pkg/core"

var _ TxEnqueuer = (*GormStorage)(nil)
var _ TxUniqueLockEnqueuer = (*GormStorage)(nil)
var _ TxCheckpointer = (*GormStorage)(nil)
var _ core.UniqueLockEnqueuer = (*GormStorage)(nil)
