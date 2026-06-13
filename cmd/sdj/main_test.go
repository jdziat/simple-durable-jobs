package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jobs "github.com/jdziat/simple-durable-jobs/v3"
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
					ID:         "job-email-1",
					Type:       "SendEmail",
					Queue:      "email",
					Status:     jobs.StatusPending,
					Priority:   3,
					MaxRetries: 3,
					Args:       []byte(`{}`),
					CreatedAt:  time.Now().Add(-2 * time.Hour),
				},
				{
					ID:         "job-email-2",
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
	seedCLIDeadLetterJob(t, store, "dlq-acme-prod", "emails", "tenant-a", jobs.MetadataMap{"env": "prod"})
	seedCLIDeadLetterJob(t, store, "dlq-acme-dev", "emails", "tenant-a", jobs.MetadataMap{"env": "dev"})
	seedCLIDeadLetterJob(t, store, "dlq-globex-prod", "emails", "tenant-b", jobs.MetadataMap{"env": "prod"})

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--driver", "sqlite", "--dsn", dbPath, "dlq", "list", "--tenant", "tenant-a", "--metadata", "env=prod", "--ids-only"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("dlq list exit code = %d, stderr = %q", code, stderr.String())
	}
	got := stdout.String()
	if !strings.Contains(got, "dlq-acme-prod") {
		t.Fatalf("stdout = %q, want filtered job", got)
	}
	for _, notWant := range []string{"dlq-acme-dev", "dlq-globex-prod"} {
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
	seedCLIDeadLetterJob(t, store, "dlq-acme-1", "emails", "tenant-a", nil)
	seedCLIDeadLetterJob(t, store, "dlq-acme-2", "emails", "tenant-a", nil)
	seedCLIDeadLetterJob(t, store, "dlq-globex-1", "emails", "tenant-b", nil)

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
	if len(dead) != 1 || dead[0].ID != "dlq-globex-1" {
		t.Fatalf("remaining dead-lettered = %v, want only dlq-globex-1", dead)
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

func seedCLIDeadLetterJob(t *testing.T, store *jobs.GormStorage, id, queue, tenant string, metadata jobs.MetadataMap) {
	t.Helper()
	now := time.Now()
	err := store.DB().Create(&jobs.Job{
		ID:               id,
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
