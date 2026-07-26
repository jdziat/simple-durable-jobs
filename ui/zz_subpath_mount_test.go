package ui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every doc site — the Handler godoc, the README, and six pages under docs/ —
// prescribes the same mount:
//
//	mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage)))
//
// index.html referenced its assets ROOT-ABSOLUTELY ("/assets/index-*.js"), so
// under that mount the browser requested /assets/... — outside the "/jobs/"
// pattern — and got a 404 from the surrounding mux. The shell loaded, no script
// ran, and the dashboard was a blank page.
//
// FALSE-GREEN TRAP: asserting that GET /jobs/ returns 200 passes with the bug
// fully present — index.html itself was always served fine; it was the assets it
// POINTS AT that 404'd. The whole existing handler suite has this shape ("200 or
// 404 both acceptable"), which is why it could not catch this. The discriminating
// assertion is to follow every asset reference in the returned HTML through the
// SAME mux the browser would use.
var htmlAssetRefRE = regexp.MustCompile(`(?:src|href)="([^"]+)"`)

// assetRefsIn returns the sub-resources a browser would fetch for this document.
func assetRefsIn(t *testing.T, html string) []string {
	t.Helper()
	var refs []string
	for _, m := range htmlAssetRefRE.FindAllStringSubmatch(html, -1) {
		ref := m[1]
		switch {
		case ref == "", strings.HasPrefix(ref, "#"),
			strings.HasPrefix(ref, "data:"), strings.HasPrefix(ref, "http"):
			continue
		}
		refs = append(refs, ref)
	}
	require.NotEmpty(t, refs, "index.html referenced no assets — the fixture is wrong, not the code")
	return refs
}

func TestHandler_BootsUnderTheDocumentedSubPathMount(t *testing.T) {
	const mount = "/jobs"

	mux := http.NewServeMux()
	mux.Handle(mount+"/", http.StripPrefix(mount, Handler(&mockStorage{})))

	shellURL, err := url.Parse(mount + "/")
	require.NoError(t, err)

	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, shellURL.String(), nil))
	require.Equal(t, http.StatusOK, rw.Code, "the SPA shell must be served at the documented mount")

	body, err := io.ReadAll(rw.Body)
	require.NoError(t, err)
	html := string(body)
	if strings.Contains(html, "Frontend Not Built") {
		t.Skip("ui/frontend/dist is not built in this tree; nothing to resolve")
	}

	for _, ref := range assetRefsIn(t, html) {
		t.Run(ref, func(t *testing.T) {
			refURL, err := url.Parse(ref)
			require.NoError(t, err)
			// Resolve exactly as the browser does: against the document's own URL.
			got := shellURL.ResolveReference(refURL)

			arw := httptest.NewRecorder()
			mux.ServeHTTP(arw, httptest.NewRequest(http.MethodGet, got.String(), nil))
			assert.Equal(t, http.StatusOK, arw.Code,
				"the browser resolves %q against %q to %q; that must be served by the SAME mux, "+
					"or the dashboard is a blank page under the mount every doc prescribes",
				ref, shellURL, got)
		})
	}
}

// TestHandler_StillBootsAtTheRootMount is the negative control: making the
// sub-path mount work must not break the root mount, which is what the e2e suite
// and the demo build both use.
func TestHandler_StillBootsAtTheRootMount(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/", Handler(&mockStorage{}))

	shellURL, err := url.Parse("/")
	require.NoError(t, err)

	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusOK, rw.Code)

	html := rw.Body.String()
	if strings.Contains(html, "Frontend Not Built") {
		t.Skip("ui/frontend/dist is not built in this tree")
	}

	for _, ref := range assetRefsIn(t, html) {
		refURL, err := url.Parse(ref)
		require.NoError(t, err)
		got := shellURL.ResolveReference(refURL)

		arw := httptest.NewRecorder()
		mux.ServeHTTP(arw, httptest.NewRequest(http.MethodGet, got.String(), nil))
		assert.Equal(t, http.StatusOK, arw.Code, "root mount must keep working: %q -> %q", ref, got)
	}
}

// TestHandler_RPCIsReachableUnderTheSubPathMount covers the other half of
// "the app boots". The compiled client derives its RPC base from the document's
// own directory, so under the documented mount it calls
// /jobs/jobs.v1.JobsService/... — which must route through StripPrefix to the
// Connect handler. It used to build that URL from window.location.origin, which
// pointed every call at a path the surrounding mux does not serve.
func TestHandler_RPCIsReachableUnderTheSubPathMount(t *testing.T) {
	const mount = "/jobs"

	mux := http.NewServeMux()
	mux.Handle(mount+"/", http.StripPrefix(mount, Handler(&mockStorage{})))

	// This is exactly what the browser computes: new URL('.', location.href)
	// at the mount root, trailing slash trimmed, plus the Connect path.
	req := httptest.NewRequest(http.MethodPost, mount+"/jobs.v1.JobsService/GetStats", nil)
	req.Header.Set("Content-Type", "application/connect+proto")
	rw := httptest.NewRecorder()
	mux.ServeHTTP(rw, req)

	assert.NotEqual(t, http.StatusNotFound, rw.Code,
		"the RPC endpoint must be routed under the documented mount, got %d", rw.Code)
}

// TestRelativeMountRoot pins the redirect target by DEPTH, not by text. The
// handler cannot know its own prefix — http.StripPrefix removed it before the
// request arrived — so an absolute Location would send the browser to the
// operator's site root, outside the mount entirely.
func TestRelativeMountRoot(t *testing.T) {
	for path, want := range map[string]string{
		"/queues":  "./",
		"/a/b":     "../",
		"/a/b/":    "../../", // a trailing slash is one more directory level
		"/a/b/c":   "../../",
		"/a/b/c/d": "../../../",
	} {
		assert.Equal(t, want, relativeMountRoot(path), "path %q", path)
	}
}

// TestHandler_UnknownPathRedirectsWithinTheMount is the invariant that matters:
// resolving the redirect the way a browser does must land back on the mount root,
// never above it.
//
// FALSE-GREEN TRAP: asserting the status is 302 passes with Location="/" — which
// is exactly the bug, since post-StripPrefix "/" means the OPERATOR'S root, not
// the mount's. The discriminating assertion resolves the Location against the
// request URL and compares it to the mount root.
func TestHandler_UnknownPathRedirectsWithinTheMount(t *testing.T) {
	const mount = "/jobs"

	mux := http.NewServeMux()
	mux.Handle(mount+"/", http.StripPrefix(mount, Handler(&mockStorage{})))

	probe := httptest.NewRecorder()
	mux.ServeHTTP(probe, httptest.NewRequest(http.MethodGet, mount+"/", nil))
	if strings.Contains(probe.Body.String(), "Frontend Not Built") {
		t.Skip("ui/frontend/dist is not built in this tree")
	}

	for _, stray := range []string{mount + "/queues", mount + "/a/b", mount + "/a/b/", mount + "/a/b/c"} {
		t.Run(stray, func(t *testing.T) {
			rw := httptest.NewRecorder()
			mux.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, stray, nil))
			require.Equal(t, http.StatusFound, rw.Code)

			loc, err := url.Parse(rw.Header().Get("Location"))
			require.NoError(t, err)
			from, err := url.Parse(stray)
			require.NoError(t, err)

			assert.Equal(t, mount+"/", from.ResolveReference(loc).Path,
				"a browser at %q resolving Location %q must land on the mount root, not above it",
				stray, rw.Header().Get("Location"))
		})
	}
}
