package security

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// SanitizeErrorMessage is the last thing between a handler's error text and
// last_error / dead_letter_reason / the dashboard, and the auth pattern used to
// consume the SCHEME WORD instead of the credential:
//
//	"Authorization: Bearer <token>" -> "Authorization: [REDACTED] <token>"
//
// So the canonical HTTP spellings — the ones an HTTP client actually produces
// when it echoes a failing request — leaked verbatim, while only the schemeless
// forms were redacted. Any handler that wrapped an upstream 401 into its error
// wrote the caller's credential to the database in cleartext.
func TestSanitizeErrorMessage_RedactsCredentialNotJustTheScheme(t *testing.T) {
	const secret = "abc123SECRETTOKEN"
	for _, tc := range []struct{ name, in string }{
		{"canonical header", "Authorization: Bearer " + secret},
		{"lowercase header", "authorization: bearer " + secret},
		{"basic scheme", "Authorization: Basic " + secret},
		{"token scheme", "Authorization: Token " + secret},
		{"proxy header", "proxy-authorization: Bearer " + secret},
		{"no scheme", "Bearer " + secret},
		{"equals form", "Authorization=" + secret},
		{"embedded in a sentence", `upstream refused: {"Authorization": "Bearer ` + secret + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeErrorMessage(tc.in)
			require.NotContains(t, got, secret,
				"the credential survived sanitization (%q); it is written to last_error and dead_letter_reason and rendered on the dashboard", got)
			require.Contains(t, got, "[REDACTED]", "something must mark the removal: %q", got)
		})
	}
}

// The pattern must not swallow ordinary prose that merely mentions the word, or
// operators lose the error text they need. This is the over-redaction side of the
// same regex, asserted so a future widening cannot quietly eat diagnostics.
func TestSanitizeErrorMessage_LeavesUnrelatedTextIntact(t *testing.T) {
	for _, in := range []string{
		"connection refused by upstream",
		"invalid job payload: field author is required",
		"rate limited: retry after 30s",
	} {
		got := SanitizeErrorMessage(in)
		require.Equal(t, in, got,
			"unrelated error text must survive sanitization unchanged, or operators lose their diagnostics")
	}
}

// A credential must not survive merely by being long enough to look like prose.
func TestSanitizeErrorMessage_RedactsAcrossRealisticWrapping(t *testing.T) {
	in := `Post "https://api.example.com/v1/charge": 401 Unauthorized ` +
		`(sent Authorization: Bearer sk_live_0123456789abcdefghij)`
	got := SanitizeErrorMessage(in)
	require.NotContains(t, got, "sk_live_0123456789abcdefghij", "got: %q", got)
	require.True(t, strings.Contains(got, "[REDACTED]"), "got: %q", got)
}
