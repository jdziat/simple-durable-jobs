package ui

import (
	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
)

func uiTestUUID(name string) core.UUID {
	return core.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte("ui:"+name)).String())
}

func uiTestID(name string) string {
	return string(uiTestUUID(name))
}
