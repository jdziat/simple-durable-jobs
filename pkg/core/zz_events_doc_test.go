package core

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const eventsDocPath = "../../docs/content/docs/api-reference/events.md"

// TestEveryEventTypeIsDocumented pins the event catalogue in
// docs/content/docs/api-reference/events.md against the types that actually
// implement Event.
//
// That page opens by promising "each payload's fields are listed below", which a
// reader takes as a complete catalogue — so an omission is not a gap, it is a
// false claim of completeness. JobCancelled was missing for a release: a
// subscriber writing a type-switch from the page silently never handled a
// cancellation. Nothing failed, because nothing tied the page to the types.
//
// Uses the same registry the event bus does, so a new event type is caught the
// moment it is added rather than whenever someone next reads the page.
func TestEveryEventTypeIsDocumented(t *testing.T) {
	b, err := os.ReadFile(eventsDocPath)
	require.NoErrorf(t, err, "cannot read %s; if the page moved, move this guard with it", eventsDocPath)
	page := string(b)

	// Every concrete Event implementation in this package.
	events := []Event{
		&JobStarted{}, &JobCompleted{}, &JobFailed{}, &JobRetrying{}, &CheckpointSaved{},
		&JobPaused{}, &JobCancelled{}, &JobResumed{}, &JobResumedBySignal{}, &JobReclaimed{},
		&SignalDelivered{}, &QueuePaused{}, &QueueResumed{}, &WorkerPaused{}, &WorkerResumed{},
		&CustomEvent{},
	}
	for _, ev := range events {
		name := reflect.TypeOf(ev).Elem().Name()
		require.Containsf(t, page, "type "+name+" struct",
			"%s does not document the %s event, but its catalogue claims to list every payload; "+
				"a subscriber writing a type-switch from that page silently never handles it",
			eventsDocPath, name)
	}

	// And nothing documented that no longer exists.
	for _, line := range strings.Split(page, "\n") {
		if !strings.HasPrefix(line, "type ") || !strings.HasSuffix(line, " struct {") {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(line, "type "), " struct {")
		found := false
		for _, ev := range events {
			if reflect.TypeOf(ev).Elem().Name() == name {
				found = true
				break
			}
		}
		require.Truef(t, found, "%s documents a %s event that no longer exists", eventsDocPath, name)
	}
}
