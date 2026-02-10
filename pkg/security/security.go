// Package security provides validation, sanitization, and limits for the jobs package.
package security

import (
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
)

// Security limits and configuration
const (
	// MaxJobTypeNameLength is the maximum length for job type names
	MaxJobTypeNameLength = 255

	// MaxJobArgsSize is the maximum size in bytes for job arguments (1MB)
	MaxJobArgsSize = 1 << 20

	// MaxRetries is the hard limit for retry attempts
	MaxRetries = 100

	// MaxConcurrency is the hard limit for worker concurrency
	MaxConcurrency = 1000

	// MaxErrorMessageLength is the maximum length for stored error messages
	MaxErrorMessageLength = 4096

	// MaxQueueNameLength is the maximum length for queue names
	MaxQueueNameLength = 255

	// MaxUniqueKeyLength is the maximum length for unique keys
	MaxUniqueKeyLength = 255
)

// validJobTypeName matches alphanumeric, hyphens, underscores, and dots
var validJobTypeName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_\-\.]*$`)

// ValidateJobTypeName validates a job type name
func ValidateJobTypeName(name string) error {
	if name == "" {
		return core.ErrInvalidJobTypeName
	}
	if len(name) > MaxJobTypeNameLength {
		return core.ErrJobTypeNameTooLong
	}
	if !validJobTypeName.MatchString(name) {
		return core.ErrInvalidJobTypeName
	}
	return nil
}

// ValidateQueueName validates a queue name
func ValidateQueueName(name string) error {
	if name == "" {
		return core.ErrInvalidQueueName
	}
	if len(name) > MaxQueueNameLength {
		return core.ErrQueueNameTooLong
	}
	if !validJobTypeName.MatchString(name) {
		return core.ErrInvalidQueueName
	}
	return nil
}

// SanitizeErrorMessage truncates and sanitizes error messages for storage
func SanitizeErrorMessage(msg string) string {
	if msg == "" {
		return ""
	}

	// Remove any null bytes or control characters (except newlines)
	var sanitized strings.Builder
	sanitized.Grow(len(msg))

	for _, r := range msg {
		if r == '\n' || r == '\r' || r == '\t' || (r >= 32 && r != 127) {
			sanitized.WriteRune(r)
		}
	}

	result := sanitized.String()

	// Truncate if too long
	if utf8.RuneCountInString(result) > MaxErrorMessageLength {
		runes := []rune(result)
		result = string(runes[:MaxErrorMessageLength-3]) + "..."
	}

	return result
}

// ClampRetries ensures retry count is within limits
func ClampRetries(n int) int {
	if n < 0 {
		return 0
	}
	if n > MaxRetries {
		return MaxRetries
	}
	return n
}

// ClampConcurrency ensures concurrency is within limits
func ClampConcurrency(n int) int {
	if n < 1 {
		return 1
	}
	if n > MaxConcurrency {
		return MaxConcurrency
	}
	return n
}

// ValidateUniqueKey validates a unique key length
func ValidateUniqueKey(key string) error {
	if len(key) > MaxUniqueKeyLength {
		return core.ErrUniqueKeyTooLong
	}
	return nil
}
