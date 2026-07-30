package ui

import (
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// h2c hijacks the connection on an HTTP/1.1 `Upgrade: h2c` and then serves every
// subsequent HTTP/2 stream itself. Middleware wrapped AROUND the h2c handler is
// therefore invoked exactly once — on the upgrade request — and never again, so
// an operator following SECURITY.md's instruction to authenticate via
// ui.WithMiddleware had a bypassable dashboard.
//
// Reproduced against the old wiring: one request to a middleware-permitted path
// upgraded, then stream 3 reached a protected RPC with middleware never
// consulted (middleware=1, inner=2).
//
// These tests drive a real TCP connection through a real upgrade because nothing
// short of that exercises the hijack. An httptest client request cannot: Go's
// client never sends the upgrade, so the handler is reached over HTTP/1.1 and
// the middleware is trivially in-path — a false green.

// h2cProbe holds the counters both tests assert on.
type h2cProbe struct {
	middlewareCalls atomic.Int64
	innerCalls      atomic.Int64
}

// newProbedHandler wires the REAL ui.Handler with a middleware that permits only
// "/" — the shape of every real deployment, which must serve the SPA shell
// unauthenticated so a login page can render.
//
// This deliberately exercises ui.Handler itself rather than re-creating its h2c
// wiring in the test. A reconstructed copy would keep passing if handler.go were
// later rewired back to middleware-outside-h2c, which is precisely the
// regression being guarded.
func newProbedHandler(t *testing.T, opts ...Option) (*h2cProbe, string) {
	t.Helper()
	p := &h2cProbe{}

	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p.middlewareCalls.Add(1)
			if r.URL.Path != "/" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			p.innerCalls.Add(1)
			next.ServeHTTP(w, r)
		})
	}

	opts = append([]Option{WithMiddleware(mw), WithInsecureAllowUnauthenticated()}, opts...)
	srv := httptest.NewServer(Handler(&mockStorage{}, opts...))
	t.Cleanup(srv.Close)
	return p, strings.TrimPrefix(srv.URL, "http://")
}

// h2cStream is one post-upgrade request. Streams are issued in ORDER and each is
// read to completion before the next is written: map iteration order is
// randomized, and a read loop that abandons the connection on the first error
// silently loses a later stream's status, which made an earlier version of this
// test flaky under load. A flaky security gate is worth nothing.
type h2cStream struct {
	id   uint32
	path string
}

// upgradeAndRequest performs a real h2c upgrade on "/" then issues each stream,
// returning the upgrade status line and each stream's :status.
func upgradeAndRequest(t *testing.T, addr string, streams []h2cStream) (statusLine string, got map[uint32]string) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(20*time.Second)))

	req := "GET / HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Connection: Upgrade, HTTP2-Settings\r\n" +
		"Upgrade: h2c\r\n" +
		"HTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n\r\n"
	_, err = conn.Write([]byte(req))
	require.NoError(t, err)

	br := bufio.NewReader(conn)
	statusLine, err = br.ReadString('\n')
	require.NoError(t, err)
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
	}
	got = map[uint32]string{}
	if !strings.Contains(statusLine, "101") {
		return statusLine, got
	}

	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	fr := http2.NewFramer(conn, br)
	require.NoError(t, fr.WriteSettings())
	dec := hpack.NewDecoder(4096, nil)

	for _, st := range streams {
		var hbuf bytes.Buffer
		enc := hpack.NewEncoder(&hbuf)
		for _, kv := range [][2]string{
			{":method", "POST"},
			{":path", st.path},
			{":scheme", "http"},
			{":authority", addr},
			{"content-type", "application/json"},
		} {
			require.NoError(t, enc.WriteField(hpack.HeaderField{Name: kv[0], Value: kv[1]}))
		}
		require.NoError(t, fr.WriteHeaders(http2.HeadersFrameParam{
			StreamID: st.id, BlockFragment: hbuf.Bytes(), EndStream: false, EndHeaders: true,
		}))
		require.NoError(t, fr.WriteData(st.id, true, []byte("{}")))

		// Drain until THIS stream's response headers arrive.
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			f, ferr := fr.ReadFrame()
			if ferr != nil {
				break
			}
			hf, ok := f.(*http2.HeadersFrame)
			if !ok {
				continue
			}
			fields, derr := dec.DecodeFull(hf.HeaderBlockFragment())
			if derr != nil {
				continue
			}
			for _, h := range fields {
				if h.Name == ":status" {
					got[hf.StreamID] = h.Value
				}
			}
			if _, done := got[st.id]; done {
				break
			}
		}
	}
	return statusLine, got
}

// TestH2CUpgrade_MiddlewareRunsOnEveryStream is the bypass guard. On the old
// wiring stream 3 and stream 5 reached the inner handler and middlewareCalls
// was 1.
//
// FALSE-GREEN TRAP: asserting middlewareCalls > 0 passes on the BROKEN wiring
// too, because the upgrade request itself always traverses the outer middleware.
// The count must be exact — one per stream plus the upgrade.
func TestH2CUpgrade_MiddlewareRunsOnEveryStream(t *testing.T) {
	p, addr := newProbedHandler(t)

	status, got := upgradeAndRequest(t, addr, []h2cStream{
		{3, "/not-a-real-asset.txt"},
		{5, "/jobs.v1.JobsService/ListJobs"},
	})
	require.Contains(t, status, "101", "the server must actually upgrade, or this test proves nothing")

	assert.Equal(t, "403", got[3], "a post-upgrade stream to a protected path must be refused by the middleware")
	assert.Equal(t, "403", got[5], "the RPC surface must be refused too")

	assert.Equal(t, int64(3), p.middlewareCalls.Load(),
		"middleware must run once for the upgrade request and once per post-upgrade stream; "+
			"a count of 1 is the bypass this test exists to catch")
	assert.Equal(t, int64(1), p.innerCalls.Load(),
		"only the permitted \"/\" request may reach the inner handler")
}

// TestH2CUpgrade_BodyIsCapped covers the window the fix itself widens: moving
// middleware inside h2c makes x/net's read-the-whole-body-to-replay-it strictly
// pre-authentication.
func TestH2CUpgrade_BodyIsCapped(t *testing.T) {
	p, addr := newProbedHandler(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	body := strings.Repeat("A", 256<<10) // 256 KiB, four times the cap
	req := "POST / HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Connection: Upgrade, HTTP2-Settings\r\n" +
		"Upgrade: h2c\r\n" +
		"HTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n" +
		"Content-Length: " + itoa(len(body)) + "\r\n\r\n" + body
	_, err = conn.Write([]byte(req))
	require.NoError(t, err)

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	require.NoError(t, err)

	assert.NotContains(t, statusLine, "101",
		"an oversized upgrade body must not be buffered and replayed as a stream")
	assert.Equal(t, int64(0), p.innerCalls.Load(),
		"an over-cap upgrade must never reach the inner handler")
}

// TestWithoutH2C_DisablesTheUpgrade proves the escape hatch: with h2c off there
// is no hijack to bypass at all.
func TestWithoutH2C_DisablesTheUpgrade(t *testing.T) {
	p, addr := newProbedHandler(t, WithoutH2C())

	status, _ := upgradeAndRequest(t, addr, []h2cStream{{3, "/jobs.v1.JobsService/ListJobs"}})

	assert.NotContains(t, status, "101", "WithoutH2C must refuse to upgrade")

	// The upgrade request is a plain GET "/" once the upgrade is declined, so it
	// is middleware-checked and permitted like any other shell request. What
	// matters is that no connection was hijacked, so the protected stream was
	// never reachable at all.
	assert.Equal(t, int64(1), p.middlewareCalls.Load(),
		"the single GET / must still be middleware-checked when h2c is off")
	assert.Equal(t, int64(1), p.innerCalls.Load(),
		"GET / is permitted by this middleware; the point is that no second stream exists")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// TestH2CUpgrade_BodyIsCappedForEveryConnectionSpelling closes an evasion of the
// cap above. That test sends `Connection: Upgrade, HTTP2-Settings`, which carries
// BOTH tokens, so it could not distinguish which one the cap keyed on.
//
// x/net's own isH2CUpgrade requires `Upgrade: h2c` plus the **HTTP2-Settings**
// token in Connection; the cap used to require the **upgrade** token. A request
// sending only HTTP2-Settings was therefore an upgrade to x/net — reaching its
// io.ReadAll(r.Body) — while escaping the cap. The read precedes the hijack, hence
// precedes cfg.middleware and the Connect auth interceptor, so an unauthenticated
// client could make the process buffer a body of any size.
//
// The cap now keys on `Upgrade: h2c` alone, so it cannot be narrower than x/net
// however Connection is spelled. Each subtest sends a body four times the cap and
// requires no 101 and no inner-handler call.
func TestH2CUpgrade_BodyIsCappedForEveryConnectionSpelling(t *testing.T) {
	// xnetUpgrades records whether x/net itself treats the shape as an upgrade,
	// i.e. whether it carries the HTTP2-Settings token in Connection. It decides
	// only the SECOND assertion: when x/net does not upgrade, the request is an
	// ordinary POST and correctly reaches the inner handler (with a capped body).
	// Requiring innerCalls == 0 for those would be asserting the wrong thing —
	// conflating "the body is capped" with "the request is rejected".
	for _, tc := range []struct {
		name         string
		connection   string
		xnetUpgrades bool
	}{
		// The evasion: exactly what x/net matches, and nothing more.
		{"HTTP2-Settings only", "Connection: HTTP2-Settings\r\n", true},
		{"both tokens", "Connection: Upgrade, HTTP2-Settings\r\n", true},
		{"odd casing and spacing", "Connection:  http2-SETTINGS , Upgrade \r\n", true},
		// x/net declines these, so they must merely never become a 101.
		{"upgrade only", "Connection: upgrade\r\n", false},
		{"no Connection header", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, addr := newProbedHandler(t)

			conn, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

			body := strings.Repeat("A", 256<<10) // four times the cap
			req := "POST / HTTP/1.1\r\n" +
				"Host: " + addr + "\r\n" +
				tc.connection +
				"Upgrade: h2c\r\n" +
				"HTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n" +
				"Content-Length: " + itoa(len(body)) + "\r\n\r\n" + body
			_, err = conn.Write([]byte(req))
			require.NoError(t, err)

			br := bufio.NewReader(conn)
			statusLine, err := br.ReadString('\n')
			require.NoError(t, err)

			assert.NotContains(t, statusLine, "101",
				"an oversized upgrade body was buffered and replayed as a stream; x/net keys on the HTTP2-Settings token, so a cap keyed on any narrower condition is a pre-auth memory-exhaustion window")
			if tc.xnetUpgrades {
				assert.Equal(t, int64(0), p.innerCalls.Load(),
					"an over-cap upgrade must never reach the inner handler")
			}
		})
	}
}

// The cap must not touch ordinary requests: a large body with no h2c upgrade
// header is a legitimate BulkDeleteJobs/BulkRetryJobs shape and must pass through
// intact. Without this, "cap everything" would look like a valid fix above.
func TestH2CUpgrade_CapDoesNotTouchOrdinaryLargeBodies(t *testing.T) {
	p, addr := newProbedHandler(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	body := strings.Repeat("B", 256<<10) // over the h2c cap, but this is not an upgrade
	req := "POST / HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Content-Length: " + itoa(len(body)) + "\r\n\r\n" + body
	_, err = conn.Write([]byte(req))
	require.NoError(t, err)

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	require.NoError(t, err)

	assert.NotContains(t, statusLine, "413",
		"a non-upgrade request must not be capped; the h2c cap exists only for the body x/net buffers before dispatch")
	assert.Equal(t, int64(1), p.innerCalls.Load(),
		"an ordinary large-bodied request must still reach the inner handler")
}
