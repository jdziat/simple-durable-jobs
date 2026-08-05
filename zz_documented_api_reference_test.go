package jobs

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/typed"
)

// This file guards docs/content/docs/api-reference/* — the pages a reader copies
// signatures out of.
//
// Round 40 found two families of defect there, both invisible to reading:
//
//  1. Job IDs are core.UUID (a DEFINED string type, so not interchangeable with
//     string), yet ten documented signatures still said `string`. Code written
//     from the reference did not compile. The sharpest was
//     events.md's OnJobReclaimed: a reader who writes the documented callback
//     literal gets a hard type error on the function VALUE, not a fixable
//     assignment.
//  2. typed.Define was documented with an inferable `fn func(context.Context, A)
//     (R, error)` parameter. The real one is `fn any`, so A and R can NEVER be
//     inferred and every call must spell the type parameters out — yet all four
//     documented call sites omitted them, including typed-api.md's whole
//     `package main` "Complete Example".
//
// TestDocumentedAPIReferenceSignaturesStillCompile is the type check: it writes
// each documented signature as a func value and lets the compiler decide. The
// scanning tests below are the prose check, since a compile assertion here
// cannot notice a page that has quietly drifted back.

// TestDocumentedAPIReferenceSignaturesStillCompile compile-asserts the exact
// shapes these pages print:
//
//	api-reference/queue.md, signals.md, events.md, worker.md, workflows.md,
//	api-reference/types.md, api-reference/typed-api.md
//
// The body is unreachable on purpose: this is a TYPE check, not a run. If one of
// these signatures changes, this stops compiling — update the pages in the same
// commit.
// assertType[T](v) compiles only when v is assignable to T. It is spelled as a
// call rather than `var _ T = v` so the explicit type — which is the entire
// assertion — is not what a linter offers to delete.
func assertType[T any](T) {}

func TestDocumentedAPIReferenceSignaturesStillCompile(t *testing.T) {
	if true {
		t.Skip("compile-time assertion only")
	}
	var (
		q *Queue
		w *Worker
	)

	// queue.md — Enqueue/EnqueueRemote return core.UUID, not string.
	assertType[func(context.Context, string, any, ...queue.Option) (core.UUID, error)](q.Enqueue)
	assertType[func(context.Context, string, any, ...queue.Option) (core.UUID, error)](q.EnqueueRemote)
	// queue.md — JobIDFromContext(ctx) core.UUID
	assertType[func(context.Context) core.UUID](JobIDFromContext)

	// signals.md:27
	assertType[func(context.Context, core.UUID, string, any) error](q.Signal)

	// events.md:20 and :168
	assertType[func(core.UUID, string, map[string]any)](q.EmitCustomEvent)
	assertType[func(func(ctx context.Context, jobID core.UUID, reason string))](q.OnJobReclaimed)

	// worker.md:61
	assertType[func(core.UUID) bool](w.CancelJob)

	// workflows.md:42
	assertType[func(context.Context, *Queue, core.UUID) (int, error)](LoadResult[int])

	// types.md — the JobID-bearing structs the page lists.
	assertType[core.UUID](core.Job{}.ID)
	assertType[core.UUID](fanout.SubJobFailure{}.JobID)

	// events.md "Event Types" — every documented JobID field.
	assertType[core.UUID](core.CheckpointSaved{}.JobID)
	assertType[core.UUID](core.JobResumedBySignal{}.JobID)
	assertType[core.UUID](core.SignalDelivered{}.JobID)
	assertType[core.UUID](core.JobReclaimed{}.JobID)
	assertType[core.UUID](core.CustomEvent{}.JobID)

	// typed-api.md — Define/DefineE take `fn any`, so A and R are never inferred.
	type A struct{ To string }
	type R struct{ MessageID string }
	assertType[func(*queue.Queue, string, any, ...queue.Option) *typed.Def[A, R]](typed.Define[A, R])
	assertType[func(*queue.Queue, string, any, ...queue.Option) (*typed.Def[A, R], error)](typed.DefineE[A, R])
	// DefineVoid, by contrast, takes a TYPED fn, which is why the page says its
	// type parameter may be omitted.
	assertType[func(*queue.Queue, string, func(context.Context, A) error, ...queue.Option) *typed.Def[A, struct{}]](typed.DefineVoid[A])

	// typed-api.md — Def's ID-returning and ID-taking methods.
	var d *typed.Def[A, R]
	assertType[func(context.Context, A, ...queue.Option) (core.UUID, error)](d.Enqueue)
	assertType[func(context.Context, A, ...queue.Option) (core.UUID, error)](d.EnqueueRemote)
	assertType[func(context.Context, core.UUID) (R, error)](d.Load)
	assertType[func(context.Context, *queue.Queue, core.UUID, string, any) error](typed.Signal)
}

// docsDir is the api-reference directory, relative to the repo root (go test runs
// with cwd = the package dir, and this package is the root). docs/ is a nested
// Hugo module excluded from the published module zip, so this is a
// repo-checkout guard; CI runs it because test.Dockerfile COPYs the whole tree.
const docsDir = "docs/content/docs"

func readDocsFile(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(docsDir, rel))
	if err != nil {
		t.Fatalf("cannot read %s: %v; if the page moved, move this guard with it rather than deleting it", rel, err)
	}
	return string(b)
}

// TestDocumentedJobIDsAreNotTypedAsString scans the api-reference pages for
// signatures that give a job ID the type `string`. Every one of these was a
// real, non-compiling documented signature before round 40.
func TestDocumentedJobIDsAreNotTypedAsString(t *testing.T) {
	// Two scopes, because the patterns have different blast radii.
	//
	// apiRefOnly patterns match shapes that are unambiguous in a REFERENCE page
	// but ordinary elsewhere. Running them repo-wide produced two false positives
	// immediately: `type Order struct { ID string }` in an examples.md snippet is
	// a user's OWN args struct, and migration-v1-to-v2.md legitimately prints
	// `jobID, workerID string` because job IDs only became core.UUID in v3 — that
	// page documents a v1→v2 rename and its signatures were right for that era.
	apiRefOnly := map[string]*regexp.Regexp{
		// jobID's OWN type only: "jobID string", "jobID, name string". A later
		// `string` parameter (e.g. "jobID core.UUID, kind string") must not match.
		"a jobID parameter typed as string": regexp.MustCompile(`\bjobID\b(\s*,\s*\w+)*\s+string\b`),
		"a JobID struct field typed string": regexp.MustCompile(`(?m)^\s*JobID\s+string\b`),
		"Job.ID typed string":               regexp.MustCompile(`(?m)^\s*ID\s+string\b`),
	}

	// everyPage patterns name a specific exported symbol, so they cannot collide
	// with a reader's own code and are safe to run across all documentation.
	everyPage := map[string]*regexp.Regexp{
		"an ID-returning method giving string": regexp.MustCompile(
			`### ` + "`" + `[^` + "`" + `\n]*(Enqueue|EnqueueRemote|EnqueueTx)\([^)]*\) \(string, error\)`),
		// A skeptic restored `JobIDFromContext(ctx context.Context) string` — one of
		// the ten sites this guard's docstring claims to cover — and the whole suite
		// stayed green. The jobID pattern is case-sensitive, so it never saw the
		// capitalised identifier, and the compile assertion pins the CODE's return
		// type rather than the page.
		"a JobID-returning function giving string": regexp.MustCompile(`\b\w*JobID\w*\([^)]*\)\s+string\b`),
		// The sharpest shape: a hook registered with an untyped parameter group, so
		// every parameter silently becomes string. `queue.OnJobReclaimed(func(ctx,
		// jobID, reason string))` is a hard type error at the call site. Scoping
		// this guard to api-reference is what let advanced/stale-lock-reaper.md keep
		// printing it after the identical claim was corrected on
		// api-reference/events.md, which that page cross-links to.
		"a callback with an untyped ctx/jobID group": regexp.MustCompile(`func\(ctx\s*,\s*jobID\b`),
	}

	report := func(page, what, match string) {
		t.Errorf("%s documents %s (%q). Job IDs are core.UUID, a defined string "+
			"type: code copied from a `string` signature does not compile.",
			page, what, strings.TrimSpace(match))
	}

	for _, page := range apiReferencePages(t) {
		body := readDocsFile(t, filepath.Join("api-reference", page))
		for what, re := range apiRefOnly {
			if m := re.FindString(body); m != "" {
				report("api-reference/"+page, what, m)
			}
		}
	}
	for _, page := range allDocsPages(t) {
		body := readDocsFile(t, page)
		for what, re := range everyPage {
			if m := re.FindString(body); m != "" {
				report(page, what, m)
			}
		}
	}
}

func allDocsPages(t *testing.T) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(docsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".md") {
			rel, relErr := filepath.Rel(docsDir, path)
			if relErr != nil {
				return relErr
			}
			out = append(out, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("cannot walk docs: %v", err)
	}
	sort.Strings(out)
	if len(out) == 0 {
		t.Fatal("no docs pages found; this guard would be vacuous")
	}
	return out
}

func apiReferencePages(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(docsDir, "api-reference"))
	if err != nil {
		t.Fatalf("cannot list api-reference: %v", err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	if len(out) == 0 {
		t.Fatal("no api-reference pages found; this guard would be vacuous")
	}
	return out
}

// TestDocumentedTypedDefineCallsSpellOutTheirTypeParameters scans every markdown
// page in the repo. typed.Define's third parameter is `any`, so a call without
// explicit type parameters fails with "cannot infer A" — a reader copying the
// first typed-API snippet they meet cannot proceed at all.
func TestDocumentedTypedDefineCallsSpellOutTheirTypeParameters(t *testing.T) {
	// `typed.Define(` / `typed.DefineE(` with no `[` before the paren.
	bare := regexp.MustCompile(`typed\.DefineE?\(`)

	var scanned int
	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if name := info.Name(); name == "node_modules" || name == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".md") {
			return nil
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		scanned++
		if loc := bare.FindIndex(b); loc != nil {
			t.Errorf("%s calls typed.Define/DefineE without explicit type parameters (%q). "+
				"Its fn parameter is `any`, so A and R can never be inferred: the snippet does "+
				"not compile. Write typed.Define[Args, Result](...).",
				path, string(b[loc[0]:loc[1]]))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking markdown: %v", err)
	}
	if scanned == 0 {
		t.Fatal("no markdown scanned; this guard would be vacuous")
	}
}

// TestTypedAPIDocPrintsDefinesRealParameter pins the one word that made all four
// call sites wrong: the page must print `fn any`, not an inferable func type.
func TestTypedAPIDocPrintsDefinesRealParameter(t *testing.T) {
	page := readDocsFile(t, "api-reference/typed-api.md")
	if !strings.Contains(page, "`Define[A any, R any](q *queue.Queue, name string, fn any, opts ...queue.Option) *Def[A, R]`") {
		t.Error("typed-api.md must print Define's real signature, whose third parameter is `fn any`; " +
			"an inferable func type there is what made every documented call site omit the type parameters")
	}
	if !strings.Contains(page, "cannot infer A") {
		t.Error("typed-api.md must quote the compiler error a reader hits when the type parameters " +
			"are omitted (`cannot infer A`), or the next reader will drop them again")
	}
}
