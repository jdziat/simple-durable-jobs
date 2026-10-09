package ui

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1/jobsv1connect"
)

// A Connect RPC body is buffered, decompressed, and unmarshalled BEFORE the auth
// interceptor runs. Without connect.WithReadMaxBytes, gzip is on by default and
// both the raw and decompressed-body limiters are disabled, so an unauthenticated
// caller can post a small gzip bomb and drive the host process to OOM before the
// authorizer is ever consulted. This guards the cap at ui/handler.go.
//
// The payload is VALID JSON that unmarshals cleanly (the codec sets
// DiscardUnknown, so the oversized "pad" field is dropped) but decompresses well
// past the 4 MiB cap. That is what makes this test load-bearing: absent the cap,
// the body decodes to completion, the ListJobs interceptor runs, and the
// authorizer IS invoked (authorizerCalls==1). With the cap, connect aborts at
// decompression with resource_exhausted before the interceptor chain, so the
// authorizer never runs (authorizerCalls==0). A whitespace/invalid-JSON body would
// NOT distinguish the two: it fails to unmarshal pre-interceptor either way, so
// authorizerCalls==0 even without the cap — a false green. Keep the body valid.
func TestHandler_RPCBodyIsCappedBeforeAuth(t *testing.T) {
	var authorizerCalls atomic.Int64
	store := &mockUIStorage{}
	authz := authorizerFunc(func(context.Context, Action) error {
		authorizerCalls.Add(1)
		return nil
	})
	server := httptest.NewServer(Handler(store, WithAuthorizer(authz)))
	defer server.Close()

	// Valid JSON with an ~8 MiB unknown-field string. It unmarshals successfully
	// (DiscardUnknown drops "pad"), but its decompressed size is ~2x the cap, so
	// with WithReadMaxBytes in place connect rejects it at decompression.
	var body bytes.Buffer
	body.WriteString(`{"pad":"`)
	body.WriteString(strings.Repeat("a", 8<<20))
	body.WriteString(`"}`)
	require.Greater(t, body.Len(), maxRPCRequestBytes, "decompressed body must exceed the cap")

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(body.Bytes())
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.Less(t, buf.Len(), maxRPCRequestBytes, "compressed body should be small on the wire")

	req, err := http.NewRequest(http.MethodPost,
		server.URL+jobsv1connect.JobsServiceListJobsProcedure, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	// connect surfaces an over-limit body as resource_exhausted (HTTP 413 in the
	// Connect protocol). The exact code matters less than: it is an error, and the
	// authorizer never ran because the body was capped before the interceptor.
	assert.NotEqual(t, http.StatusOK, resp.StatusCode, "oversized body must be rejected")
	assert.Equal(t, int64(0), authorizerCalls.Load(), "body must be capped before the authorizer runs")
}
