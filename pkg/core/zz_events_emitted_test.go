package core

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// notEmittedMarker is the exact phrase every artifact must carry for an event
// type that is declared but never published to Queue.Events() subscribers.
const notEmittedMarker = "DECLARED BUT NOT CURRENTLY EMITTED"

// TestEveryDocumentedEventIsEmitted closes the hole its sibling
// TestEveryEventTypeIsDocumented left open.
//
// That guard ties docs/content/docs/api-reference/events.md to the set of types
// that IMPLEMENT Event, in both directions. It never checks whether anything
// ever CONSTRUCTS one. CheckpointSaved had shipped for releases with three
// artifacts — its godoc, the jobs.CheckpointSaved facade alias godoc, and the
// API reference page — all telling a user the event arrives when a checkpoint is
// written, while no Emit site existed anywhere in the module. A user's
// `case *jobs.CheckpointSaved:` arm was dead code, and the round-44 guard
// certified the false claim rather than catching it: it pinned the type's
// PRESENCE on the page in both directions and stopped there.
//
// So this guard asks the other question. For every Event implementation:
//
//   - if the module constructs it (an `Emit(&Foo{` / `Emit(&core.Foo{` site in
//     non-test source), it must NOT be marked as unemitted anywhere; and
//   - if the module does not, all three artifacts must carry notEmittedMarker,
//     so a reader is told before they write the type-switch.
//
// Either fix therefore satisfies it: emit the event and drop the notices, or
// keep the notices. What it forbids is the state that shipped — a documented
// event with no emitter and no notice.
func TestEveryDocumentedEventIsEmitted(t *testing.T) {
	root := moduleRoot(t)

	events := []Event{
		&JobStarted{}, &JobCompleted{}, &JobFailed{}, &JobRetrying{}, &CheckpointSaved{},
		&JobPaused{}, &JobCancelled{}, &JobResumed{}, &JobResumedBySignal{}, &JobReclaimed{},
		&SignalDelivered{}, &QueuePaused{}, &QueueResumed{}, &WorkerPaused{}, &WorkerResumed{},
		&CustomEvent{},
	}

	source := nonTestGoSources(t, root)
	page := readFileString(t, filepath.Join(root, "docs", "content", "docs", "api-reference", "events.md"))
	eventsGo := readFileString(t, filepath.Join(root, "pkg", "core", "events.go"))
	facade := readFileString(t, filepath.Join(root, "jobs.go"))

	for _, ev := range events {
		name := reflect.TypeOf(ev).Elem().Name()
		emitted := hasEmitSite(source, name)

		// The three artifacts a user reads: the type's own godoc, the facade
		// alias godoc (what a facade-only user sees on pkg.go.dev), and the
		// shipped API reference page.
		godocMarked := markerNear(eventsGo, "type "+name+" struct", notEmittedMarker)
		facadeMarked := markerNear(facade, name+" = core."+name, notEmittedMarker)
		pageMarked := markerNear(page, "type "+name+" struct", notEmittedMarker)

		if emitted {
			require.Falsef(t, godocMarked || facadeMarked || pageMarked,
				"%s IS emitted, but an artifact still marks it %q — remove the notice", name, notEmittedMarker)
			continue
		}
		require.Truef(t, godocMarked,
			"no Emit site constructs %s anywhere in the module, so a subscriber's `case *%s:` arm is dead code, "+
				"but pkg/core/events.go still presents it as a live event. Either emit it or mark its godoc %q.",
			name, name, notEmittedMarker)
		require.Truef(t, facadeMarked,
			"no Emit site constructs %s, but the jobs.%s facade alias godoc — the one a facade-only user reads on "+
				"pkg.go.dev — still presents it as a live event. Either emit it or mark that alias %q.",
			name, name, notEmittedMarker)
		require.Truef(t, pageMarked,
			"no Emit site constructs %s, but docs/content/docs/api-reference/events.md lists it in a catalogue that "+
				"opens by promising every payload a subscriber can type-switch on. Either emit it or mark it %q on that page.",
			name, notEmittedMarker)
	}
}

// hasEmitSite reports whether any non-test source PRODUCES the named event.
//
// It accepts two shapes, and the second one is there to avoid a false FIRE
// rather than to catch anything extra. The strict form — the literal inside the
// Emit call, which is how all 18 current emitters are written — would report
// "not emitted" the moment someone refactored to
//
//	ev := &core.JobStarted{...}
//	q.Emit(ev)
//
// and this guard would then demand a "not emitted" notice on a perfectly live
// event. A false fire on a healthy tree is worse than a miss here: it wedges a
// correct change and teaches the next person to weaken the guard. So a bare
// construction in non-test source counts too. Constructing an event value and
// never publishing it is not a thing that happens; a type with NO construction
// anywhere — which is exactly CheckpointSaved's state — is.
//
// Both spellings are checked: pkg/queue builds core.Foo values by qualified
// name, an in-package emitter would use the bare name.
func hasEmitSite(sources []string, name string) bool {
	for _, src := range sources {
		if strings.Contains(src, "Emit(&core."+name+"{") || strings.Contains(src, "Emit(&"+name+"{") {
			return true
		}
		if strings.Contains(src, "&core."+name+"{") || strings.Contains(src, "&"+name+"{") {
			return true
		}
	}
	return false
}

// markerNear reports whether marker appears in the contiguous `//` comment block
// IMMEDIATELY above anchor — the declaration's own doc comment, and on the API
// reference page the comment line attached to the same struct.
//
// The block must be delimited, not merely nearby: a fixed character window
// reaches back past a short type into a long-winded neighbour's notice and
// reports the neighbour as marked, which is a false PASS for the neighbour and a
// false FAIL for the emitted type below it.
func markerNear(text, anchor, marker string) bool {
	idx := strings.Index(text, anchor)
	if idx < 0 {
		return false
	}
	lines := strings.Split(text[:idx], "\n")
	// The anchor's own (partial) line is last; the doc comment is above it.
	for i := len(lines) - 2; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(line, "//") {
			return false
		}
		if strings.Contains(line, marker) {
			return true
		}
	}
	return false
}

// nonTestGoSources reads every non-test .go file belonging to THIS module.
//
// The "belonging to this module" part is load-bearing and was learned the hard
// way. A developer working tree can contain a whole second copy of the repo
// (.gitignore carries a `/simple-durable-jobs` entry for exactly that), and a
// naive walk reads its sources too — so a guard like this one reports an emitter
// that the shipped code no longer has, and passes while the real tree regressed.
// Any directory carrying its own go.mod is therefore a different module and is
// skipped whole, along with dot-directories and the non-Go trees.
func nonTestGoSources(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			out = append(out, string(b))
			return nil
		}
		if path == root {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") || d.Name() == "node_modules" || d.Name() == "docs" {
			return fs.SkipDir
		}
		// A nested go.mod means a different module — a vendored dependency or, in
		// practice here, a stale second checkout of this very repo.
		if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
			return fs.SkipDir
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, out, "walked the module and found no non-test Go sources; the walk root is wrong")
	return out
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoErrorf(t, err, "cannot read %s; if it moved, move this guard with it", path)
	return string(b)
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "go.mod"))
	require.NoErrorf(t, err, "expected the module root at %s", root)
	return root
}
