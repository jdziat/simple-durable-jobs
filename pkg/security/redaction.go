package security

// RedactSecrets removes provider-key / token-shaped substrings (the same
// patterns as SanitizeErrorMessage) from s WITHOUT truncating it. Use it on
// read paths that surface potentially large user payloads (job args/results),
// where SanitizeErrorMessage's length cap would corrupt legitimate data.
// Only matched secret substrings are rewritten; all other bytes pass through.
func RedactSecrets(s string) string {
	return redactSecrets(s)
}
