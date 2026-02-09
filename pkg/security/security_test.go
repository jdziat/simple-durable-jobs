package security

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateJobTypeName_Valid(t *testing.T) {
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
		err := ValidateJobTypeName(name)
		assert.NoError(t, err, "Expected %q to be valid", name)
	}
}

func TestValidateJobTypeName_Invalid(t *testing.T) {
	invalidNames := []string{
		"",                       // empty
		"123-task",               // starts with number
		"-task",                  // starts with hyphen
		"task with spaces",       // contains spaces
		"task@email",             // contains special char
		"task/subtask",           // contains slash
		strings.Repeat("a", 300), // too long
	}

	for _, name := range invalidNames {
		err := ValidateJobTypeName(name)
		assert.Error(t, err, "Expected %q to be invalid", name)
	}
}

func TestValidateQueueName_Valid(t *testing.T) {
	validNames := []string{
		"default",
		"high-priority",
		"emails_v2",
	}

	for _, name := range validNames {
		err := ValidateQueueName(name)
		assert.NoError(t, err, "Expected %q to be valid", name)
	}
}

func TestValidateQueueName_Invalid(t *testing.T) {
	invalidNames := []string{
		"",
		"queue with spaces",
		strings.Repeat("q", 300),
	}

	for _, name := range invalidNames {
		err := ValidateQueueName(name)
		assert.Error(t, err, "Expected %q to be invalid", name)
	}
}

func TestSanitizeErrorMessage(t *testing.T) {
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
			result := SanitizeErrorMessage(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeErrorMessage_Truncation(t *testing.T) {
	longMessage := strings.Repeat("a", 5000)
	result := SanitizeErrorMessage(longMessage)

	assert.LessOrEqual(t, len(result), MaxErrorMessageLength)
	assert.True(t, strings.HasSuffix(result, "..."))
}

func TestClampRetries(t *testing.T) {
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
		result := ClampRetries(tt.input)
		assert.Equal(t, tt.expected, result, "ClampRetries(%d)", tt.input)
	}
}

func TestClampConcurrency(t *testing.T) {
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
		result := ClampConcurrency(tt.input)
		assert.Equal(t, tt.expected, result, "ClampConcurrency(%d)", tt.input)
	}
}

func TestConstants(t *testing.T) {
	assert.Equal(t, 255, MaxJobTypeNameLength)
	assert.Equal(t, 1<<20, MaxJobArgsSize) // 1MB
	assert.Equal(t, 100, MaxRetries)
	assert.Equal(t, 1000, MaxConcurrency)
	assert.Equal(t, 4096, MaxErrorMessageLength)
	assert.Equal(t, 255, MaxQueueNameLength)
	assert.Equal(t, 255, MaxUniqueKeyLength)
}
