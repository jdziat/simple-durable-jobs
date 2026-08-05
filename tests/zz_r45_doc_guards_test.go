package jobs_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4"
	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
)

// ---------------------------------------------------------------------------
// Timeout: the queue DOES cancel handlers.
// ---------------------------------------------------------------------------

// TestR45_TimeoutCancelsTheHandlerContext pins the behaviour the API reference
// now describes. docs/content/docs/api-reference/job-options.md used to say
// "the queue does not cancel handlers automatically", and README.md's options
// snippet said "Recorded on the job; enforce via ctx". Both were false:
// pkg/worker runs every handler under context.WithTimeout(effectiveTimeout).
//
// A user who believed the page and shipped a long ETL job had its handler
// context cancelled at the deadline with nothing they had read telling them the
// deadline was live. What that costs them is pinned separately by
// TestR45_TimeoutOutcomeMatchesTheDocumentedTwoCases; this test asserts only the
// cancellation itself, which is the claim the page denied.
func TestR45_TimeoutCancelsTheHandlerContext(t *testing.T) {
	queue, _ := openIntegrationQueue(t)

	observed := make(chan error, 1)
	queue.Register("r45timeout", func(ctx context.Context, args map[string]string) error {
		select {
		case <-ctx.Done():
			observed <- ctx.Err()
		case <-time.After(10 * time.Second):
			observed <- nil
		}
		return nil
	})

	_, err := queue.Enqueue(context.Background(), "r45timeout", map[string]string{},
		jobs.Timeout(200*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	worker := jobs.NewWorker(queue, jobs.Concurrency(1), jobs.WithPollInterval(10*time.Millisecond))
	go func() { _ = worker.Start(ctx) }()

	select {
	case got := <-observed:
		require.ErrorIs(t, got, context.DeadlineExceeded,
			"jobs.Timeout must cancel the handler context at the deadline, as job-options.md and the godoc both say")
	case <-time.After(12 * time.Second):
		t.Fatal("handler never ran")
	}
}

// TestR45_TimeoutOutcomeMatchesTheDocumentedTwoCases pins the SECOND half of the
// corrected page: what the live deadline actually does to a job.
//
// The first draft of that correction said a handler that ignores ctx "keeps
// running while the attempt is already recorded as failed with context deadline
// exceeded". Probing it showed that is false in both halves — the worker WAITS
// for the handler, and a handler that returns nil after the deadline is recorded
// COMPLETED. Cancelling a context does not kill a goroutine, so the outcome is
// still the handler's return value. Replacing one false sentence with another is
// exactly the failure this campaign keeps hitting, so both branches are pinned
// here and the page states both.
func TestR45_TimeoutOutcomeMatchesTheDocumentedTwoCases(t *testing.T) {
	for _, tc := range []struct {
		name       string
		propagates bool
		wantStatus core.JobStatus
		wantErr    string
	}{
		{"propagates the cancellation", true, core.StatusFailed, "context deadline exceeded"},
		{"ignores ctx and returns nil", false, core.StatusCompleted, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue, store := openIntegrationQueue(t)
			queue.Register("r45timeoutoutcome", func(ctx context.Context, args map[string]string) error {
				<-ctx.Done()
				if tc.propagates {
					return ctx.Err()
				}
				return nil
			})
			id, err := queue.Enqueue(context.Background(), "r45timeoutoutcome", map[string]string{},
				jobs.Timeout(150*time.Millisecond), jobs.Retries(0))
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			w := jobs.NewWorker(queue, jobs.Concurrency(1), jobs.WithPollInterval(10*time.Millisecond))
			go func() { _ = w.Start(ctx) }()

			require.Eventually(t, func() bool {
				j, err := store.GetJob(context.Background(), id)
				return err == nil && j.Status.IsTerminal()
			}, 15*time.Second, 25*time.Millisecond, "job never reached a terminal status")

			j, err := store.GetJob(context.Background(), id)
			require.NoError(t, err)
			require.Equalf(t, tc.wantStatus, j.Status,
				"docs/content/docs/api-reference/job-options.md documents this outcome for a handler that %s; last_error=%q", tc.name, j.LastError)
			if tc.wantErr == "" {
				require.Empty(t, j.LastError)
			} else {
				require.Contains(t, j.LastError, tc.wantErr)
			}
		})
	}
}

// TestR45_TimeoutDocsDoNotDenyEnforcement is the guard on the prose. The
// behaviour above has always been true; what shipped broken was the page.
func TestR45_TimeoutDocsDoNotDenyEnforcement(t *testing.T) {
	root := r45ModuleRoot(t)
	for _, rel := range []string{
		filepath.Join("docs", "content", "docs", "api-reference", "job-options.md"),
		"README.md",
	} {
		lower := strings.ToLower(r45ReadFile(t, filepath.Join(root, rel)))
		// Checked as booleans, not require.NotContains on the body: a failure
		// there prints the whole file and buries the one sentence at fault.
		require.Falsef(t, strings.Contains(lower, "the queue does not cancel handlers"),
			"%s denies that jobs.Timeout is enforced; pkg/worker cancels the handler context at the deadline", rel)
		require.Falsef(t, strings.Contains(lower, "recorded on the job; enforce via ctx"),
			"%s presents jobs.Timeout as an inert label; the worker enforces it", rel)
	}

	// And the page must positively state the enforcement, so deleting the false
	// sentence without replacing it does not count as fixed.
	page := strings.ToLower(r45ReadFile(t, filepath.Join(root, "docs", "content", "docs", "api-reference", "job-options.md")))
	idx := strings.Index(page, "### `timeout(d time.duration) option`")
	require.GreaterOrEqual(t, idx, 0, "job-options.md no longer documents Timeout; move this guard with it")
	section := page[idx:]
	if end := strings.Index(section[3:], "\n### "); end >= 0 {
		section = section[:end+3]
	}
	// Two SPECIFIC phrases, not a bare "cancel". The Timeout section already ends
	// with a paragraph about checkpoints surviving "a handler's own deadline or
	// cancellation", so a substring test for "cancel" is satisfied by prose that
	// says nothing about who enforces the deadline — verified: deleting the whole
	// enforcement statement and leaving "Sets a per-job deadline." passed that
	// weaker check.
	for _, phrase := range []string{"the queue enforces", "cancels the handler"} {
		require.Containsf(t, section, phrase,
			"the Timeout section must state that the QUEUE enforces the deadline by cancelling the handler's "+
				"context (missing %q). Deleting the old false sentence is not enough — a reader who is told "+
				"nothing still assumes the label is advisory.\nsection was:\n%s", phrase, section)
	}
}

// ---------------------------------------------------------------------------
// Register / RegisterE: SIX accepted handler signatures, not one.
// ---------------------------------------------------------------------------

type r45Args struct {
	N int `json:"n"`
}

// r45AcceptedHandlerForms is the set of signatures Register/RegisterE accept,
// paired with the exact spelling every godoc and the API reference page must
// list. The pairing is the point: the func value is run through RegisterE (so a
// listed-but-rejected form fails) and the spelling is looked for in the prose (so
// an accepted-but-unlisted form fails). Neither half alone catches a drift.
//
// The finder reported FOUR forms, which is what docs/content/docs/api-reference/
// queue.md listed. Probing handler.NewHandler showed there are six: the args
// value is optional too, so func(ctx) error and func(ctx) (R, error) are
// accepted and run end to end. Writing "one of these four" would have replaced
// one too-narrow contract with another.
var r45AcceptedHandlerForms = []struct {
	spelling string
	fn       any
}{
	{"func(ctx context.Context, args T) error", func(ctx context.Context, a r45Args) error { return nil }},
	{"func(ctx context.Context, args T) (R, error)", func(ctx context.Context, a r45Args) (int, error) { return a.N, nil }},
	{"func(args T) error", func(a r45Args) error { return nil }},
	{"func(args T) (R, error)", func(a r45Args) (int, error) { return a.N, nil }},
	{"func(ctx context.Context) error", func(ctx context.Context) error { return nil }},
	{"func(ctx context.Context) (R, error)", func(ctx context.Context) (int, error) { return 0, nil }},
}

// TestR45_RegisterAcceptsEverySignature is the "does the code do what the prose
// says" half: every listed form must actually register.
func TestR45_RegisterAcceptsEverySignature(t *testing.T) {
	queue, _ := openIntegrationQueue(t)
	for i, form := range r45AcceptedHandlerForms {
		require.NoErrorf(t, queue.RegisterE(fmt.Sprintf("r45sig%d", i), form.fn),
			"the godoc and docs/content/docs/api-reference/queue.md list %q as an accepted handler signature, "+
				"but RegisterE rejects it", form.spelling)
	}
	// The one shape that is genuinely rejected — pinned so "all six" cannot quietly
	// become "anything goes" without this guard noticing.
	require.Error(t, queue.RegisterE("r45sigNoParams", func() error { return nil }),
		"a zero-parameter handler must still be rejected; the docs say so")
}

// TestR45_RegisterGodocListsEverySignature is the other half: the godoc on
// Register, RegisterE and handler.NewHandler each stated a NARROWER contract
// than the code accepts — Register and RegisterE named ONE signature, NewHandler
// named two. A stranger implementing to that contract would rewrite working
// (R, error) handlers for no reason, or conclude that Define[A, R] (which
// registers through RegisterE and REQUIRES the (R, error) form) needs some other
// registration path.
func TestR45_RegisterGodocListsEverySignature(t *testing.T) {
	root := r45ModuleRoot(t)
	for _, target := range []struct{ file, symbol string }{
		{filepath.Join("pkg", "queue", "queue.go"), "func (q *Queue) Register(name string, fn any, opts ...Option)"},
		{filepath.Join("pkg", "queue", "queue.go"), "func (q *Queue) RegisterE(name string, fn any, opts ...Option) error"},
		{filepath.Join("pkg", "internal", "handler", "handler.go"), "func NewHandler(fn any) (*Handler, error)"},
	} {
		doc := r45DocCommentAbove(t, r45ReadFile(t, filepath.Join(root, target.file)), target.symbol)
		for _, form := range r45AcceptedHandlerForms {
			require.Containsf(t, doc, form.spelling,
				"%s godoc omits the accepted signature %q; TestR45_RegisterAcceptsEverySignature proves RegisterE takes it.\ngodoc was:\n%s",
				target.symbol, form.spelling, doc)
		}
	}

	// The shipped API reference is the third artifact a user reads, and it listed
	// the same four.
	page := r45ReadFile(t, filepath.Join(root, "docs", "content", "docs", "api-reference", "queue.md"))
	for _, form := range r45AcceptedHandlerForms {
		require.Containsf(t, page, form.spelling,
			"docs/content/docs/api-reference/queue.md omits the accepted signature %q", form.spelling)
	}
}

// ---------------------------------------------------------------------------
// Deprecation notices must not promise a removal that already came and went.
// ---------------------------------------------------------------------------

var r45RemovalPromise = regexp.MustCompile(`will be (?:unexported|removed|dropped) in v(\d+)`)

// TestR45_NoDeprecationPromisesAPastMajor catches a deprecation notice whose
// promised major has already shipped without the promise being kept.
//
// Five exported helpers (ValidateJobTypeName, ValidateQueueName,
// SanitizeErrorMessage, ClampRetries, ClampConcurrency) carried
// "Deprecated: internal helper; will be unexported in v3." while the module
// declares .../v4 and all five are still exported. A consumer planning an
// upgrade reads "v3" and either believes their code already broke (it did not)
// or that the removal is behind them (it is not). The sibling annotation from
// the same v2-era packet had already been corrected in pkg/queue/options.go;
// these five were missed because nothing tied the notice to the module major.
func TestR45_NoDeprecationPromisesAPastMajor(t *testing.T) {
	root := r45ModuleRoot(t)
	major := r45ModuleMajor(t, root)

	for path, body := range r45ModuleGoSources(t, root) {
		for _, m := range r45RemovalPromise.FindAllStringSubmatch(body, -1) {
			promised, err := strconv.Atoi(m[1])
			require.NoError(t, err)
			require.Greaterf(t, promised, major,
				"%s promises %q, but the module is already at v%d and the symbol is still exported; "+
					"a notice naming a shipped major is a false statement in published godoc",
				path, m[0], major)
		}
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func r45ModuleRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "go.mod"))
	require.NoErrorf(t, err, "expected the module root at %s", root)
	return root
}

func r45ReadFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoErrorf(t, err, "cannot read %s; if it moved, move this guard with it", path)
	return string(b)
}

func r45ModuleMajor(t *testing.T, root string) int {
	t.Helper()
	for _, line := range strings.Split(r45ReadFile(t, filepath.Join(root, "go.mod")), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "module ") {
			continue
		}
		parts := strings.Split(strings.TrimSpace(strings.TrimPrefix(line, "module ")), "/")
		last := parts[len(parts)-1]
		if strings.HasPrefix(last, "v") {
			if n, err := strconv.Atoi(strings.TrimPrefix(last, "v")); err == nil {
				return n
			}
		}
		return 1 // no /vN suffix means v0/v1
	}
	t.Fatal("go.mod has no module line")
	return 0
}

// r45ModuleGoSources returns every non-test .go file of THIS module, keyed by
// path. Directories carrying their own go.mod are skipped: a working tree can
// hold a whole second checkout of this repo (.gitignore has a
// `/simple-durable-jobs` entry for exactly that), and reading it would let a
// stale copy satisfy — or falsely trip — a guard about the shipped tree.
func r45ModuleGoSources(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				b, rerr := os.ReadFile(path)
				if rerr != nil {
					return rerr
				}
				rel, _ := filepath.Rel(root, path)
				out[rel] = string(b)
			}
			return nil
		}
		if path == root {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") || d.Name() == "node_modules" || d.Name() == "docs" {
			return fs.SkipDir
		}
		if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
			return fs.SkipDir
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, out, "walked the module and found no non-test Go sources; the walk root is wrong")
	return out
}

// r45DocCommentAbove returns the contiguous `//` block immediately above decl.
func r45DocCommentAbove(t *testing.T, body, decl string) string {
	t.Helper()
	idx := strings.Index(body, decl)
	require.GreaterOrEqualf(t, idx, 0, "declaration %q not found; move this guard with it", decl)
	lines := strings.Split(body[:idx], "\n")
	var doc []string
	for i := len(lines) - 2; i >= 0; i-- {
		if !strings.HasPrefix(strings.TrimSpace(lines[i]), "//") {
			break
		}
		doc = append([]string{lines[i]}, doc...)
	}
	return strings.Join(doc, "\n")
}
