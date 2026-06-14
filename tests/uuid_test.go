package jobs_test

import (
	"github.com/google/uuid"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
)

func integrationTestUUID(name string) jobs.UUID {
	return jobs.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("integration:"+name)).String())
}
