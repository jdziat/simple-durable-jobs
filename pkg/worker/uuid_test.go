package worker

import (
	"github.com/google/uuid"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

func workerTestUUID(name string) core.UUID {
	return core.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("worker:"+name)).String())
}
