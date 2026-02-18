package jobs_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jdziat/simple-durable-jobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var securityTestCounter int

func setupSecurityTestQueue(t *testing.T) (*jobs.Queue, jobs.Storage) {
	securityTestCounter++
	dbPath := fmt.Sprintf("/tmp/jobs_security_test_%d_%d.db", os.Getpid(), securityTestCounter)
	t.Cleanup(func() {
		_ = os.Remove(dbPath)
	})

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)

	store := jobs.NewGormStorage(db)
	err = store.Migrate(context.Background())
	require.NoError(t, err)

	queue := jobs.New(store)
	return queue, store
}

func TestSecurity_ValidateJobTypeName_Valid(t *testing.T) {
	validNames := []string{
		"send-email",
		"processOrder",
		"task_1",
		"MyJob",
		"a",
		"job.subtask",
		"Send_Email_V2",
	}

	for _, name := range validNames {
		err := jobs.ValidateJobTypeName(name)
		assert.NoError(t, err, "Expected %q to be valid", name)
	}
}

func TestSecurity_ValidateJobTypeName_Invalid(t *testing.T) {
	invalidNames := []string{
		"",                          // empty
		"123-task",                  // starts with number
		"-task",                     // starts with hyphen
		"task with spaces",          // contains spaces
		"task@email",                // contains special char
		"task/subtask",              // contains slash
		strings.Repeat("a", 300),    // too long
	}

	for _, name := range invalidNames {
		err := jobs.ValidateJobTypeName(name)
		assert.Error(t, err, "Expected %q to be invalid", name)
	}
}

func TestSecurity_SanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "normal message",
			input:    "connection refused",
			expected: "connection refused",
		},
		{
			name:     "message with newlines",
			input:    "error on\nline 2",
			expected: "error on\nline 2",
		},
		{
			name:     "message with null bytes",
			input:    "error\x00with\x00nulls",
			expected: "errorwithnulls",
		},
		{
			name:     "empty message",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := jobs.SanitizeErrorMessage(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSecurity_SanitizeErrorMessage_Truncation(t *testing.T) {
	longMessage := strings.Repeat("a", 5000)
	result := jobs.SanitizeErrorMessage(longMessage)

	assert.LessOrEqual(t, len(result), jobs.MaxErrorMessageLength)
	assert.True(t, strings.HasSuffix(result, "..."))
}

func TestSecurity_ClampRetries(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{-1, 0},
		{0, 0},
		{5, 5},
		{50, 50},
		{100, 100},
		{101, 100},
		{1000, 100},
	}

	for _, tt := range tests {
		result := jobs.ClampRetries(tt.input)
		assert.Equal(t, tt.expected, result, "ClampRetries(%d)", tt.input)
	}
}

func TestSecurity_ClampConcurrency(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{-1, 1},
		{0, 1},
		{1, 1},
		{10, 10},
		{500, 500},
		{1000, 1000},
		{1001, 1000},
		{5000, 1000},
	}

	for _, tt := range tests {
		result := jobs.ClampConcurrency(tt.input)
		assert.Equal(t, tt.expected, result, "ClampConcurrency(%d)", tt.input)
	}
}

func TestSecurity_Register_InvalidName_Panics(t *testing.T) {
	queue, _ := setupSecurityTestQueue(t)

	assert.Panics(t, func() {
		queue.Register("123-invalid", func(ctx context.Context, _ struct{}) error {
			return nil
		})
	})

	assert.Panics(t, func() {
		queue.Register("", func(ctx context.Context, _ struct{}) error {
			return nil
		})
	})
}

func TestSecurity_Enqueue_InvalidQueueName(t *testing.T) {
	queue, _ := setupSecurityTestQueue(t)

	queue.Register("test-job", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	_, err := queue.Enqueue(context.Background(), "test-job", struct{}{}, jobs.QueueOpt("invalid queue"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, jobs.ErrInvalidQueueName)
}

func TestSecurity_Enqueue_ArgsTooLarge(t *testing.T) {
	queue, _ := setupSecurityTestQueue(t)

	queue.Register("test-job", func(ctx context.Context, data []byte) error {
		return nil
	})

	// Create args larger than MaxJobArgsSize
	largeData := make([]byte, jobs.MaxJobArgsSize+1)
	_, err := queue.Enqueue(context.Background(), "test-job", largeData)
	assert.Error(t, err)
	assert.ErrorIs(t, err, jobs.ErrJobArgsTooLarge)
}

func TestSecurity_JobOwnership_CompleteWrongWorker(t *testing.T) {
	_, store := setupSecurityTestQueue(t)
	ctx := context.Background()

	// Create and dequeue job as worker-1
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	_, err = store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)

	// Try to complete as worker-2 (should fail)
	err = store.Complete(ctx, job.ID, "worker-2")
	assert.ErrorIs(t, err, jobs.ErrJobNotOwned)
}

func TestSecurity_JobOwnership_FailWrongWorker(t *testing.T) {
	_, store := setupSecurityTestQueue(t)
	ctx := context.Background()

	// Create and dequeue job as worker-1
	job := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.Enqueue(ctx, job)
	require.NoError(t, err)

	_, err = store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)

	// Try to fail as worker-2 (should fail)
	err = store.Fail(ctx, job.ID, "worker-2", "error", nil)
	assert.ErrorIs(t, err, jobs.ErrJobNotOwned)
}

func TestSecurity_UniqueKey_Deduplication(t *testing.T) {
	_, store := setupSecurityTestQueue(t)
	ctx := context.Background()

	// Create first job with unique key
	job1 := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.EnqueueUnique(ctx, job1, "unique-123")
	require.NoError(t, err)

	// Try to create second job with same unique key (should fail)
	job2 := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err = store.EnqueueUnique(ctx, job2, "unique-123")
	assert.ErrorIs(t, err, jobs.ErrDuplicateJob)
}

func TestSecurity_UniqueKey_AllowsAfterCompletion(t *testing.T) {
	_, store := setupSecurityTestQueue(t)
	ctx := context.Background()

	// Create first job with unique key
	job1 := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err := store.EnqueueUnique(ctx, job1, "unique-456")
	require.NoError(t, err)

	// Dequeue and complete it
	dequeued, err := store.Dequeue(ctx, []string{"default"}, "worker-1")
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	err = store.Complete(ctx, job1.ID, "worker-1")
	require.NoError(t, err)

	// Now should be able to create new job with same unique key
	job2 := &jobs.Job{
		Type:  "test-job",
		Queue: "default",
	}
	err = store.EnqueueUnique(ctx, job2, "unique-456")
	assert.NoError(t, err)
}

func TestSecurity_RetriesAreClamped(t *testing.T) {
	queue, store := setupSecurityTestQueue(t)
	ctx := context.Background()

	queue.Register("test-job", func(ctx context.Context, _ struct{}) error {
		return nil
	})

	// Try to set excessive retries
	jobID, err := queue.Enqueue(ctx, "test-job", struct{}{}, jobs.Retries(1000))
	require.NoError(t, err)

	job, err := store.GetJob(ctx, jobID)
	require.NoError(t, err)

	// Should be clamped to MaxRetries
	assert.Equal(t, jobs.MaxRetries, job.MaxRetries)
}
