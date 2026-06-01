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

func TestSanitizeErrorMessage_RedactsSecrets(t *testing.T) {
	msg := "request failed: bearer abcdefghijklmnopqrstuvwxyz012345 and dsn postgres://app:secret@db.example/jobs"

	result := SanitizeErrorMessage(msg)

	assert.Contains(t, result, "request failed:")
	assert.Contains(t, result, "bearer [REDACTED]")
	assert.Contains(t, result, "postgres://app:[REDACTED]@db.example/jobs")
	assert.NotContains(t, result, "abcdefghijklmnopqrstuvwxyz012345")
	assert.NotContains(t, result, ":secret@")
}

func TestSanitizeErrorMessage_RedactsPasswordKeysAndOpaqueTokens(t *testing.T) {
	msg := "query failed password=hunter2 token=0123456789abcdef0123456789abcdef"

	result := SanitizeErrorMessage(msg)

	assert.Contains(t, result, "password=[REDACTED]")
	assert.Contains(t, result, "token=[REDACTED]")
	assert.NotContains(t, result, "hunter2")
	assert.NotContains(t, result, "0123456789abcdef0123456789abcdef")
}

func TestRedactSecrets_ProviderKeys(t *testing.T) {
	tests := []struct {
		name   string
		secret string
	}{
		{
			name:   "stripe live secret key",
			secret: "sk_live_1234567890abcdef",
		},
		{
			name:   "stripe test secret key",
			secret: "sk_test_abcdef1234567890",
		},
		{
			name:   "stripe restricted live key",
			secret: "rk_live_1234567890abcdef",
		},
		{
			name:   "github token",
			secret: "ghp_1234567890abcdefghij",
		},
		{
			name:   "slack bot token",
			secret: "xoxb-1234567890-ABCDEFGHIJabcdefghijkl",
		},
		{
			name:   "google api key",
			secret: "AIza" + strings.Repeat("A", 35),
		},
		{
			name:   "aws access key id",
			secret: "AKIA1234567890ABCDEF",
		},
		{
			name:   "jwt",
			secret: "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.sgn-x_y",
		},
		{
			name:   "base64url opaque token",
			secret: "abcDEF1234567890_-abcDEF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := redactSecrets("request failed with token " + tt.secret + " while processing")

			assert.Contains(t, out, "[REDACTED]")
			assert.NotContains(t, out, tt.secret)
		})
	}
}

func TestRedactSecrets_PreservesDiagnostics(t *testing.T) {
	tests := []string{
		"git revision 0123456789abcdef0123456789abcdef01234567",
		"failed to read /var/lib/pg/12345/base/67890",
		"correlation id 550e8400-e29b-41d4-a716-446655440000",
		"ordinary connection refused after retrying twice",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			out := redactSecrets(input)

			assert.Equal(t, input, out)
			assert.NotContains(t, out, "[REDACTED]")
		})
	}
}

func TestSanitizeErrorMessage_BoundedForHugeInput(t *testing.T) {
	secret := "sk_live_1234567890abcdef"
	hugeInput := strings.Repeat("a", maxErrorMessageInputLength+MaxErrorMessageLength) + secret

	result := SanitizeErrorMessage(hugeInput)

	assert.LessOrEqual(t, len(result), MaxErrorMessageLength)
	assert.NotContains(t, result, secret)

	inputWithSecretInPrefix := "request failed " + secret + " " + strings.Repeat("b", maxErrorMessageInputLength*2)
	result = SanitizeErrorMessage(inputWithSecretInPrefix)

	assert.LessOrEqual(t, len(result), MaxErrorMessageLength)
	assert.Contains(t, result, "[REDACTED]")
	assert.NotContains(t, result, secret)
}

func TestRedactSecrets_ExportedRedactsProviderKey(t *testing.T) {
	secret := "sk_live_1234567890abcdef"
	input := "payload contains " + secret + " here"

	result := RedactSecrets(input)

	assert.Contains(t, result, "payload contains [REDACTED] here")
	assert.NotContains(t, result, secret)
}

func TestRedactSecrets_DoesNotTruncateLargePayload(t *testing.T) {
	secret := "ghp_1234567890abcdefghij"
	sentinel := "TAIL_SENTINEL"
	input := strings.Repeat("a", MaxErrorMessageLength+100) + " " + secret + " " + sentinel

	result := RedactSecrets(input)

	assert.Contains(t, result, "[REDACTED]")
	assert.NotContains(t, result, secret)
	assert.Contains(t, result, sentinel)
	assert.Greater(t, len(result), MaxErrorMessageLength)
}

func TestRedactSecrets_ExportedLeavesSecretFreeStringUnchanged(t *testing.T) {
	input := "ordinary connection refused after retrying twice"

	result := RedactSecrets(input)

	assert.Equal(t, input, result)
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
	assert.Equal(t, 1<<20, MaxResultSize)  // 1MB
	assert.Equal(t, 100, MaxRetries)
	assert.Equal(t, 1000, MaxConcurrency)
	assert.Equal(t, 4096, MaxErrorMessageLength)
	assert.Equal(t, 255, MaxQueueNameLength)
	assert.Equal(t, 255, MaxUniqueKeyLength)
}

// ──────────────────────────────────────────────────────────────────────────────
// ValidateUniqueKey
// ──────────────────────────────────────────────────────────────────────────────

func TestValidateUniqueKey_ValidKey(t *testing.T) {
	// Any non-empty string within the length limit is accepted.
	validKeys := []string{
		"user:42",
		"email:user@example.com",
		"order-123",
		"key_with_underscores",
		"key with spaces",        // spaces are allowed
		"key!@#$%^&*()",          // special characters are allowed
		strings.Repeat("a", 255), // exactly at limit
		"",                       // empty string is always valid (zero-length)
	}
	for _, key := range validKeys {
		err := ValidateUniqueKey(key)
		assert.NoError(t, err, "expected key %q to be valid", key)
	}
}

func TestValidateUniqueKey_TooLong(t *testing.T) {
	// One character over the 255-byte limit.
	tooLong := strings.Repeat("x", MaxUniqueKeyLength+1)

	err := ValidateUniqueKey(tooLong)

	assert.Error(t, err, "key longer than MaxUniqueKeyLength should be rejected")
}

func TestValidateUniqueKey_ExactlyAtLimit(t *testing.T) {
	atLimit := strings.Repeat("z", MaxUniqueKeyLength)

	err := ValidateUniqueKey(atLimit)

	assert.NoError(t, err, "key exactly at MaxUniqueKeyLength should be accepted")
}

func TestValidateUniqueKey_Empty(t *testing.T) {
	// Empty key has length 0, which is <= MaxUniqueKeyLength.
	err := ValidateUniqueKey("")

	assert.NoError(t, err, "empty key should be accepted")
}

func TestValidateUniqueKey_SpecialCharacters(t *testing.T) {
	// Special characters, unicode, and control-looking chars are all accepted
	// as long as the byte length is within bounds.
	keys := []string{
		"key\twith\ttabs",
		"key\nwith\nnewlines",
		"unicode-key-你好世界",
		"emoji-🚀🎉",
	}
	for _, key := range keys {
		err := ValidateUniqueKey(key)
		assert.NoError(t, err, "expected key %q to be accepted", key)
	}
}
