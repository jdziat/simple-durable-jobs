package jobs_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These guards pin release-pipeline configuration that has no other test and
// whose breakage is SILENT. The govulncheck job is release-blocking (ci.yml's
// `release` lists it in `needs`), and a red gate manifests as a SKIPPED release
// rather than a failed one — so the pipeline can sit wedged for weeks with every
// merge looking green. The config below is what keeps the dependency floor
// current so that does not happen.
//
// They are deliberately TEXT-STRUCTURAL rather than YAML-unmarshalled: importing
// a YAML library here would promote gopkg.in/yaml.v3 from indirect to a direct
// requirement, adding a line to go.mod that this packet's acceptance criteria
// explicitly forbid. The block scan below is bounded to a single
// `- package-ecosystem:` stanza, so a key belonging to a different ecosystem
// cannot satisfy an assertion about this one.

// dependabotBlock returns the lines of the `- package-ecosystem: "<name>"`
// stanza, stopping at the next stanza. Returns false when no such stanza exists.
func dependabotBlock(t *testing.T, ecosystem string) ([]string, bool) {
	t.Helper()
	raw, err := os.ReadFile(".github/dependabot.yml")
	require.NoError(t, err, "dependabot.yml must exist")

	lines := strings.Split(string(raw), "\n")
	start := -1
	for i, ln := range lines {
		if strings.Contains(ln, "- package-ecosystem:") && strings.Contains(ln, ecosystem) {
			start = i
			break
		}
	}
	if start == -1 {
		return nil, false
	}
	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		if strings.Contains(lines[i], "- package-ecosystem:") {
			end = i
			break
		}
	}
	return lines[start:end], true
}

// TestDependabotCoversGoModules fails on the pre-remediation tree, where
// dependabot.yml carried only a github-actions stanza — which is why two
// reachable advisories (GO-2026-5970 in x/text, GO-2026-5506 in otel) were able
// to accumulate against an unchanged go.mod and wedge the release gate.
func TestDependabotCoversGoModules(t *testing.T) {
	block, ok := dependabotBlock(t, "gomod")
	require.True(t, ok, "dependabot.yml must declare a gomod ecosystem; without it the "+
		"release-blocking govulncheck gate is the only thing tracking dependency currency")

	joined := strings.Join(block, "\n")
	assert.Contains(t, joined, `directory: "/"`, "gomod stanza must target the module root")
	assert.Contains(t, joined, "interval:", "gomod stanza must declare a schedule interval")
	assert.Contains(t, joined, `prefix: "fix"`,
		`commit-message prefix must be "fix" so semantic-release cuts a PATCH for a dependency `+
			`bump — a bump that mints no tag never raises the floor for consumers who do not pin`)
}

// TestDependabotDoesNotBlockTheXNetSecurityFloor pins the correction to a rule
// that held x/net below v0.54 because h2c became deprecated there. Deprecation
// is not removal: h2c still builds, while GO-2026-5026 requires x/net v0.55.0.
// Dependabot must remain free to deliver security updates while the v5 h2c
// migration is pending.
func TestDependabotDoesNotBlockTheXNetSecurityFloor(t *testing.T) {
	handler, err := os.ReadFile("ui/handler.go")
	require.NoError(t, err)

	if !strings.Contains(string(handler), "golang.org/x/net/http2/h2c") {
		t.Skip("ui/handler.go no longer imports h2c")
	}

	block, ok := dependabotBlock(t, "gomod")
	require.True(t, ok)
	joined := strings.Join(block, "\n")

	assert.NotContains(t, joined, "golang.org/x/net",
		"deprecation must not pin x/net below a required security floor")
	assert.NotContains(t, joined, ">=0.54.0",
		"the obsolete h2c deprecation pin blocks GO-2026-5026's fixed versions")
}

// TestScheduledGovulncheckDiscriminatesExitCodes pins the three-way exit-code
// branch. govulncheck exits 3 on called vulnerabilities and 0 when clean; any
// other non-zero is a SCANNER failure (network, module resolution, toolchain).
// Collapsing "any non-3" into "clean" converts a broken scanner into a silent
// all-clear, which is the precise failure this workflow exists to remove.
func TestScheduledGovulncheckDiscriminatesExitCodes(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/govulncheck-scheduled.yml")
	require.NoError(t, err, "the scheduled govulncheck workflow must exist")
	wf := string(raw)

	assert.Contains(t, wf, "schedule:", "the scan must run on a schedule, not only on push")
	assert.Contains(t, wf, "status=clean")
	assert.Contains(t, wf, "status=findings")
	assert.Contains(t, wf, `exit "$code"`,
		"a scanner failure (exit code other than 0 or 3) must fail the job, not be reported as clean")
	assert.Contains(t, wf, "gh label create security-scan",
		"the label must be ensured before `gh issue create --label`, which errors on an unknown label")
}

// TestReleaseGatingGovulncheckIsUntouched asserts the scheduled scan did not
// weaken the release gate it complements. The strictness of that gate is an
// asset: it is what stops a tag from being minted while a reachable advisory is
// open.
func TestReleaseGatingGovulncheckIsUntouched(t *testing.T) {
	raw, err := os.ReadFile(".github/workflows/ci.yml")
	require.NoError(t, err)
	ci := string(raw)

	assert.Contains(t, ci, "needs: [build, govulncheck, api-compat, chaos-smoke, test-e2e]",
		"govulncheck must remain in the release job's needs")
	assert.NotContains(t, ci, "govulncheck@v1.3.0 ./... || true",
		"the release-gating scan must never be made non-blocking")
}

// TestSecurityAndVersioningDocsMatchModuleMajor caught two stale docs on the
// pre-remediation tree: SECURITY.md advertised a `1.x` support line and
// VERSIONING.md said `v3`, while go.mod declares /v4. A user pinning per those
// instructions would pin a major that receives no fixes.
func TestSecurityAndVersioningDocsMatchModuleMajor(t *testing.T) {
	gomod, err := os.ReadFile("go.mod")
	require.NoError(t, err)

	major := ""
	for ln := range strings.SplitSeq(string(gomod), "\n") {
		if strings.HasPrefix(ln, "module ") {
			if i := strings.LastIndex(ln, "/v"); i != -1 {
				major = strings.TrimSpace(ln[i+1:])
			}
			break
		}
	}
	require.NotEmpty(t, major, "module path must carry a /vN suffix")

	for _, f := range []string{"SECURITY.md", "VERSIONING.md"} {
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		body := string(raw)

		assert.Contains(t, body, major,
			"%s must reference the current module major (%s)", f, major)

		// The specific stale strings that shipped. Guarding the exact wrong
		// values keeps this from passing merely because the file mentions v4
		// somewhere incidental.
		assert.NotContains(t, body, "`1.x`", "%s still advertises the 1.x line", f)
		assert.NotContains(t, body, "(currently `v3`)", "%s still says the major is v3", f)
	}
}
