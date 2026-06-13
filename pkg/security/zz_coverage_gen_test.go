package security

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ──────────────────────────────────────────────────────────────────────────────
// isLikelyOpaqueToken — exercise every branch directly.
//
// The opaqueTokenRunPattern regex only feeds the function strings drawn from
// [A-Za-z0-9+/=] or hex, so to drive the `default: return false` arm (a rune
// outside every case) and the short-circuit length guard we call the helper
// directly. This is an internal test package, so the unexported symbol is in
// scope.
// ──────────────────────────────────────────────────────────────────────────────

func TestIsLikelyOpaqueToken_LengthGuard(t *testing.T) {
	// Shorter than 20 runes -> immediate false, never inspects characters.
	assert.False(t, isLikelyOpaqueToken(""))
	assert.False(t, isLikelyOpaqueToken("short"))
	assert.False(t, isLikelyOpaqueToken(strings.Repeat("a", 19)))
}

func TestIsLikelyOpaqueToken_HexOnlyWithDigit(t *testing.T) {
	// Pure hex (lower) with at least one digit -> hexOnly && hasDigit -> true.
	assert.True(t, isLikelyOpaqueToken("0123456789abcdef0123"))
	// Pure hex (upper A-F) with a digit -> still hexOnly because A-F keep hexOnly true.
	assert.True(t, isLikelyOpaqueToken("0123456789ABCDEF0123"))
	// Mixed-case hex digits with a numeric digit -> hexOnly true.
	assert.True(t, isLikelyOpaqueToken("0aAbBcCdDeEfF01234567"))
}

func TestIsLikelyOpaqueToken_AllHexLettersNoDigit(t *testing.T) {
	// Only hex letters, no digit: hexOnly stays true but hasDigit is false, so
	// the `hexOnly && hasDigit` arm is skipped. With no symbol and not all of
	// digit+upper+lower present, this is false.
	// "abcdef..." are hex lowercase letters only (a-f) -> hasLower true, hasDigit false.
	assert.False(t, isLikelyOpaqueToken(strings.Repeat("abcdef", 4))) // 24 chars, all a-f
	// All uppercase hex letters only (A-F) -> hasUpper true, hasDigit false, no lower.
	assert.False(t, isLikelyOpaqueToken(strings.Repeat("ABCDEF", 4)))
}

func TestIsLikelyOpaqueToken_TokenSymbol(t *testing.T) {
	// Presence of a base64 symbol (+, /, or =) makes hexOnly false and
	// hasTokenSymbol true -> final return is true regardless of case mix.
	assert.True(t, isLikelyOpaqueToken("aaaaaaaaaaaaaaaaaaaa+"))
	assert.True(t, isLikelyOpaqueToken("aaaaaaaaaaaaaaaaaaaa/"))
	assert.True(t, isLikelyOpaqueToken("aaaaaaaaaaaaaaaaaaaa="))
	// Symbol plus mixed content.
	assert.True(t, isLikelyOpaqueToken("AbCdEfGhIjKlMnOpQr12+/="))
}

func TestIsLikelyOpaqueToken_DigitUpperLowerMix(t *testing.T) {
	// No symbol, not pure hex (contains g-z / G-Z), but has digit AND upper AND
	// lower -> last clause (hasDigit && hasUpper && hasLower) -> true.
	assert.True(t, isLikelyOpaqueToken("Zz9gGhHiIjJkKlLmMnNoO")) // digit 9, upper, lower, non-hex letters
	assert.True(t, isLikelyOpaqueToken("aB3xY7zW9pQ2rS4tU6vWx"))
}

func TestIsLikelyOpaqueToken_NotEnoughVariety(t *testing.T) {
	// Non-hex letters only (g-z), no digit, no symbol, single case.
	// hexOnly is false (g-z), hasTokenSymbol false, and not (digit&&upper&&lower).
	assert.False(t, isLikelyOpaqueToken("zzzzzzzzzzzzzzzzzzzz")) // all lower non-hex
	assert.False(t, isLikelyOpaqueToken("ZZZZZZZZZZZZZZZZZZZZ")) // all upper non-hex
	assert.False(t, isLikelyOpaqueToken("gggggggggggggggggggg")) // lower g
	// Digit + lower non-hex but no upper -> fails the digit&&upper&&lower clause.
	assert.False(t, isLikelyOpaqueToken("g1g2g3g4g5g6g7g8g9g0"))
	// Digit + upper non-hex but no lower.
	assert.False(t, isLikelyOpaqueToken("G1G2G3G4G5G6G7G8G9G0"))
}

func TestIsLikelyOpaqueToken_DefaultBranchUnknownRune(t *testing.T) {
	// A rune outside every case (space, punctuation, unicode) triggers the
	// `default: return false` arm immediately.
	assert.False(t, isLikelyOpaqueToken("aaaaaaaaaa aaaaaaaaaa")) // space at index 10
	assert.False(t, isLikelyOpaqueToken("aaaaaaaaaa-aaaaaaaaaa")) // hyphen
	assert.False(t, isLikelyOpaqueToken("aaaaaaaaaa.aaaaaaaaaa")) // dot
	assert.False(t, isLikelyOpaqueToken("aaaaaaaaaaéaaaaaaaaa"))  // non-ASCII rune (é)
}

// ──────────────────────────────────────────────────────────────────────────────
// SanitizeErrorMessage / redactSecrets — feed varied tokens, DSNs, passwords to
// exercise the redaction wiring end to end (also keeps the opaque-token decision
// reachable through the real regex path).
// ──────────────────────────────────────────────────────────────────────────────

func TestSanitizeErrorMessage_AuthorizationHeader(t *testing.T) {
	// authTokenPattern matches `(authorization|bearer)\s*[:=]?\s*\S+`. The
	// `\S+` consumes the single token directly after the keyword. With a bare
	// `bearer <token>` the opaque token is consumed and redacted.
	out := SanitizeErrorMessage("failed: bearer abcdefghijklmnopqrstuvwxyz0123")
	assert.Contains(t, out, "[REDACTED]")
	assert.NotContains(t, out, "abcdefghijklmnopqrstuvwxyz0123")

	// `authorization=<value>` form is also redacted.
	out2 := SanitizeErrorMessage("hdr authorization=topsecretheadervalue")
	assert.Contains(t, out2, "[REDACTED]")
	assert.NotContains(t, out2, "topsecretheadervalue")
}

func TestSanitizeErrorMessage_PwdVariant(t *testing.T) {
	// `pwd=` is matched by passwordKVPattern alongside `password=`.
	out := SanitizeErrorMessage("login error pwd=topsecretvalue;next=ok")
	assert.Contains(t, out, "pwd=[REDACTED]")
	assert.NotContains(t, out, "topsecretvalue")
	// Ensure trailing content after the terminator is preserved.
	assert.Contains(t, out, "next=ok")
}

func TestSanitizeErrorMessage_DSNVariants(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		secret string
		keep   string
	}{
		{
			name:   "mysql dsn",
			in:     "dial mysql://root:rootpw@127.0.0.1:3306/db",
			secret: ":rootpw@",
			keep:   "mysql://root:[REDACTED]@",
		},
		{
			name:   "postgres dsn",
			in:     "open postgres://svc:p4ssw0rd@pg.internal/jobs",
			secret: ":p4ssw0rd@",
			keep:   "postgres://svc:[REDACTED]@",
		},
		{
			name:   "scheme with plus and dot",
			in:     "open jdbc.pg+ssl://user:hunter2pw@host/db",
			secret: ":hunter2pw@",
			keep:   "[REDACTED]@",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := SanitizeErrorMessage(tc.in)
			assert.NotContains(t, out, tc.secret)
			assert.Contains(t, out, tc.keep)
		})
	}
}

func TestSanitizeErrorMessage_OpaqueTokenNotRedactedWhenLowVariety(t *testing.T) {
	// A 20+ run of a single repeated non-hex letter matches the regex but
	// isLikelyOpaqueToken returns false, so it must be preserved verbatim.
	plain := strings.Repeat("z", 30)
	out := SanitizeErrorMessage("noise " + plain + " end")
	assert.Contains(t, out, plain)
	assert.NotContains(t, out, "[REDACTED]")
}

func TestSanitizeErrorMessage_OpaqueHexTokenRedacted(t *testing.T) {
	// Pure hex run with digits -> redacted via the opaque-token path.
	hexTok := "deadbeefdeadbeefdeadbeef0123"
	out := SanitizeErrorMessage("checksum " + hexTok)
	assert.Contains(t, out, "[REDACTED]")
	assert.NotContains(t, out, hexTok)
}

func TestSanitizeErrorMessage_Base64TokenRedacted(t *testing.T) {
	// Base64-ish run with + / = symbols -> redacted.
	b64 := "QWxhZGRpbjpvcGVuIHNlc2FtZQ+/=="
	out := SanitizeErrorMessage("payload " + b64)
	assert.Contains(t, out, "[REDACTED]")
}

func TestSanitizeErrorMessage_ControlCharsStripped(t *testing.T) {
	// DEL (127) and other control chars (except \n \r \t) are dropped; tab kept.
	in := "a\x07b\x1fc\x7fd\te"
	out := SanitizeErrorMessage(in)
	assert.Equal(t, "abcd\te", out)
}

func TestSanitizeErrorMessage_NoSecretsPassthrough(t *testing.T) {
	// Plain message with no secret-looking content is returned unchanged.
	in := "connection reset by peer at host 10.0.0.5"
	out := SanitizeErrorMessage(in)
	assert.Equal(t, in, out)
}
