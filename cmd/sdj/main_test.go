package main

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v4"
)

func TestRunMigrateAndQueuesSQLite(t *testing.T) {
	tests := []struct {
		name        string
		seed        []*jobs.Job
		wantQueue   string
		wantPending string
	}{
		{
			name: "pending jobs by queue",
			seed: []*jobs.Job{
				{
					ID:         "00000000-0000-7000-8000-000000000101",
					Type:       "SendEmail",
					Queue:      "email",
					Status:     jobs.StatusPending,
					Priority:   3,
					MaxRetries: 3,
					Args:       []byte(`{}`),
					CreatedAt:  time.Now().Add(-2 * time.Hour),
				},
				{
					ID:         "00000000-0000-7000-8000-000000000102",
					Type:       "SendEmail",
					Queue:      "email",
					Status:     jobs.StatusPending,
					Priority:   1,
					MaxRetries: 3,
					Args:       []byte(`{}`),
					CreatedAt:  time.Now().Add(-time.Hour),
				},
			},
			wantQueue:   "email",
			wantPending: "2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "jobs.db")

			var stdout, stderr bytes.Buffer
			code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("migrate exit code = %d, stderr = %q", code, stderr.String())
			}
			if !strings.Contains(stdout.String(), "migrations applied successfully") {
				t.Fatalf("migrate stdout = %q, want success line", stdout.String())
			}
			stdout.Reset()
			stderr.Reset()
			code = run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("second migrate exit code = %d, stderr = %q", code, stderr.String())
			}

			store := openSQLiteStoreForTest(t, dbPath)
			for _, job := range tt.seed {
				if err := store.Enqueue(context.Background(), job); err != nil {
					t.Fatalf("seed job %s: %v", job.ID, err)
				}
			}

			stdout.Reset()
			stderr.Reset()
			code = run([]string{"--driver", "sqlite", "--dsn", dbPath, "queues"}, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("queues exit code = %d, stderr = %q", code, stderr.String())
			}
			out := stdout.String()
			for _, want := range []string{"QUEUE", "PENDING", "DLQ", "OLDEST_PENDING", "BACKLOG_AGE", tt.wantQueue, tt.wantPending} {
				if !strings.Contains(out, want) {
					t.Fatalf("queues stdout = %q, want %q", out, want)
				}
			}
		})
	}
}

func TestRunEmptyStatesSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	var stdout, stderr bytes.Buffer
	code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("migrate exit code = %d, stderr = %q", code, stderr.String())
	}

	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "queues",
			args: []string{"--driver", "sqlite", "--dsn", dbPath, "queues"},
			want: "No queues found. Pending and dead-letter counts are both zero.",
		},
		{
			name: "dlq list",
			args: []string{"--driver", "sqlite", "--dsn", dbPath, "dlq", "list"},
			want: "No dead-lettered jobs found.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout.Reset()
			stderr.Reset()
			code := run(tt.args, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
			}
			if !strings.Contains(stdout.String(), tt.want) {
				t.Fatalf("stdout = %q, want %q", stdout.String(), tt.want)
			}
		})
	}
}

func TestRunDLQListTenantAndMetadataSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	var stdout, stderr bytes.Buffer
	code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("migrate exit code = %d, stderr = %q", code, stderr.String())
	}

	store := openSQLiteStoreForTest(t, dbPath)
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000201", "emails", "tenant-a", jobs.MetadataMap{"env": "prod"})
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000202", "emails", "tenant-a", jobs.MetadataMap{"env": "dev"})
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000203", "emails", "tenant-b", jobs.MetadataMap{"env": "prod"})

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--driver", "sqlite", "--dsn", dbPath, "dlq", "list", "--tenant", "tenant-a", "--metadata", "env=prod", "--ids-only"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dlq list exit code = %d, stderr = %q", code, stderr.String())
	}
	got := stdout.String()
	if !strings.Contains(got, "00000000-0000-7000-8000-000000000201") {
		t.Fatalf("stdout = %q, want filtered job", got)
	}
	for _, notWant := range []string{"00000000-0000-7000-8000-000000000202", "00000000-0000-7000-8000-000000000203"} {
		if strings.Contains(got, notWant) {
			t.Fatalf("stdout = %q, did not want %q", got, notWant)
		}
	}
}

func TestRunDLQRequeueBulkSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	var stdout, stderr bytes.Buffer
	code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("migrate exit code = %d, stderr = %q", code, stderr.String())
	}

	store := openSQLiteStoreForTest(t, dbPath)
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000211", "emails", "tenant-a", nil)
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000212", "emails", "tenant-a", nil)
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000213", "emails", "tenant-b", nil)
	// The queue-scope bystander: SAME tenant, DIFFERENT queue. Without it the only
	// non-matching row also differed by tenant, so the tenant clause alone
	// distinguished it and deleting the --queue predicate entirely
	// (storage.deadLetterQuery) left this test green while `dlq requeue --queue
	// emails` silently drained every other queue.
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000214", "billing", "tenant-a", nil)

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--driver", "sqlite", "--dsn", dbPath, "dlq", "requeue", "--queue", "emails", "--tenant", "tenant-a"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dlq requeue exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "requeued 2 jobs") {
		t.Fatalf("stdout = %q, want bulk count", stdout.String())
	}

	dead, err := store.ListDeadLettered(context.Background(), jobs.DeadLetterFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list dead-lettered: %v", err)
	}
	remaining := make([]string, 0, len(dead))
	for _, job := range dead {
		remaining = append(remaining, string(job.ID))
	}
	sort.Strings(remaining)
	want := []string{
		"00000000-0000-7000-8000-000000000213", // other tenant, same queue
		"00000000-0000-7000-8000-000000000214", // same tenant, other queue
	}
	if !reflect.DeepEqual(remaining, want) {
		t.Fatalf("remaining dead-lettered = %v, want %v (both scope predicates must apply)", remaining, want)
	}
}

// TestRunDLQRequeueBulkExitCodesSQLite pins that the three bulk outcomes are
// distinguishable from a shell. `sdj dlq requeue --queue "$Q" && clear-alert`
// used to clear the alert when the filter matched nothing at all and when rows
// were skipped and left dead-lettered.
func TestRunDLQRequeueBulkExitCodesSQLite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr); code != 0 {
		t.Fatalf("migrate exit code = %d, stderr = %q", code, stderr.String())
	}
	store := openSQLiteStoreForTest(t, dbPath)

	requeue := func(queue string) (int, string) {
		t.Helper()
		stdout.Reset()
		stderr.Reset()
		code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "dlq", "requeue", "--queue", queue}, &stdout, &stderr)
		return code, stdout.String()
	}

	// (1) Nothing matches. This must still exit 0: "nothing matched" includes the
	// ordinary already-drained queue, and the runbook's own example is
	// `sdj dlq requeue --queue "$Q" && clear-alert`. Changing that to non-zero
	// would stop clearing the alert on a healthy queue and abort a `set -e` cron —
	// a user-visible CLI break in a patch release. Distinguishability lives on
	// STDERR instead, where a script can capture it and a human can read it.
	code, out := requeue("no-such-queue")
	if code != exitOK {
		t.Fatalf("empty filter exit code = %d, want %d (already-drained is not a failure); stdout = %q", code, exitOK, out)
	}
	if !strings.Contains(out, "requeued 0 jobs") {
		t.Fatalf("stdout = %q, want the zero count", out)
	}

	// (2) A clean drain.
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000221", "clean", "", nil)
	code, out = requeue("clean")
	if code != exitOK {
		t.Fatalf("clean drain exit code = %d, want %d; stdout = %q", code, exitOK, out)
	}
	if !strings.Contains(out, "requeued 1 jobs") {
		t.Fatalf("stdout = %q, want the requeued count", out)
	}

	// (3) A partial run: a dead-lettered fan-out sub-job is rejected by Requeue by
	// design, so it stays dead-lettered and the operator must not be told the
	// queue is clear.
	seedCLIDeadLetterJob(t, store, "00000000-0000-7000-8000-000000000231", "mixed", "", nil)
	seedCLIDeadLetterSubJob(t, store, "00000000-0000-7000-8000-000000000232", "mixed")
	code, out = requeue("mixed")
	if code != exitPartial {
		t.Fatalf("partial run exit code = %d, want %d; stdout = %q", code, exitPartial, out)
	}
	if !strings.Contains(out, "skipped 1 fan-out sub-jobs") {
		t.Fatalf("stdout = %q, want the skipped sub-job count", out)
	}
	dead, err := store.ListDeadLettered(context.Background(), jobs.DeadLetterFilter{Queue: "mixed", Limit: 10})
	if err != nil {
		t.Fatalf("list dead-lettered: %v", err)
	}
	if len(dead) != 1 || string(dead[0].ID) != "00000000-0000-7000-8000-000000000232" {
		t.Fatalf("remaining dead-lettered = %v, want the sub-job still queued", dead)
	}
}

func TestRunHelpExitCodes(t *testing.T) {
	tests := [][]string{
		{"-h"},
		{"--help"},
		{"migrate", "-h"},
		{"queues", "-h"},
		{"dlq", "-h"},
		{"dlq", "list", "-h"},
		{"dlq", "requeue", "-h"},
		{"health", "-h"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(args, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
			}
			if !strings.Contains(stdout.String()+stderr.String(), "Usage:") {
				t.Fatalf("stdout = %q stderr = %q, want usage", stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunMissingDSNActionable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"queues"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("exit code = 0, want non-zero")
	}
	errOut := stderr.String()
	for _, want := range []string{"--dsn is required", "sqlite", "./jobs.db"} {
		if !strings.Contains(errOut, want) {
			t.Fatalf("stderr = %q, want %q", errOut, want)
		}
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestRunUnknownDriverActionable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--driver", "oracle", "--dsn", "ignored", "health"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("exit code = 0, want non-zero")
	}
	errOut := stderr.String()
	for _, want := range []string{"unknown --driver", "sqlite", "postgres", "mysql"} {
		if !strings.Contains(errOut, want) {
			t.Fatalf("stderr = %q, want %q", errOut, want)
		}
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
}

func TestRunHealthHonorsCanceledContext(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	var stdout, stderr bytes.Buffer
	code := run([]string{"--driver", "sqlite", "--dsn", dbPath, "migrate"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("migrate exit code = %d, stderr = %q", code, stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	a := app{stdout: &stdout, stderr: &stderr}
	code = a.run(canceled, []string{"--driver", "sqlite", "--dsn", dbPath, "health"})
	if code == 0 {
		t.Fatalf("exit code = 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "storage health check failed") {
		t.Fatalf("stderr = %q, want health failure", stderr.String())
	}
}

func TestRunVersion(t *testing.T) {
	oldVersion := version
	version = "vtest"
	defer func() { version = oldVersion }()

	tests := []struct {
		name string
		args []string
	}{
		{name: "flag", args: []string{"--version"}},
		{name: "subcommand", args: []string{"version"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
			}
			if got := strings.TrimSpace(stdout.String()); got != "sdj vtest" {
				t.Fatalf("stdout = %q, want sdj vtest", got)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func openSQLiteStoreForTest(t *testing.T, path string) *jobs.GormStorage {
	t.Helper()
	opened, err := openStore(globalOptions{driver: "sqlite", dsn: path})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { closeStore(opened) })
	return opened.store
}

// seedCLIDeadLetterSubJob seeds a dead-lettered FAN-OUT SUB-JOB: Requeue rejects
// it with ErrCannotRequeueSubJob by design (replay goes through the parent), so
// it is what a bulk run skips and leaves in the DLQ.
func seedCLIDeadLetterSubJob(t *testing.T, store *jobs.GormStorage, id, queue string) {
	t.Helper()
	now := time.Now()
	fanOutID := jobs.UUID("00000000-0000-7000-8000-0000000009f0")
	err := store.DB().Create(&jobs.Job{
		ID:               jobs.UUID(id),
		Type:             "send-email",
		Queue:            queue,
		Status:           jobs.StatusFailed,
		Attempt:          1,
		MaxRetries:       1,
		LastError:        "boom",
		FanOutID:         &fanOutID,
		DeadLetteredAt:   &now,
		DeadLetterReason: "max retries exhausted: boom",
		CompletedAt:      &now,
	}).Error
	if err != nil {
		t.Fatalf("seed dlq sub-job %s: %v", id, err)
	}
}

func seedCLIDeadLetterJob(t *testing.T, store *jobs.GormStorage, id, queue, tenant string, metadata jobs.MetadataMap) {
	t.Helper()
	now := time.Now()
	err := store.DB().Create(&jobs.Job{
		ID:               jobs.UUID(id),
		Type:             "send-email",
		Queue:            queue,
		Tenant:           tenant,
		Metadata:         metadata,
		Status:           jobs.StatusFailed,
		Attempt:          1,
		MaxRetries:       1,
		LastError:        "boom",
		DeadLetteredAt:   &now,
		DeadLetterReason: "max retries exhausted: boom",
		CompletedAt:      &now,
	}).Error
	if err != nil {
		t.Fatalf("seed dlq job %s: %v", id, err)
	}
}
