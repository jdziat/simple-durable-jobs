package ui

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v4/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v4/ui/gen/jobs/v1/jobsv1connect"
)

var (
	statsCollectorMu      sync.Mutex
	statsCollectorsByDB   = map[*gorm.DB]bool{}
	startStatsCollectorFn = func(ctx context.Context, collector *StatsCollector) {
		go collector.Start(ctx)
	}
)

// Handler creates an http.Handler for the jobs UI dashboard.
// It serves both the Connect-RPC API and the static frontend assets.
//
// Usage:
//
//	mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage)))
func Handler(storage core.Storage, opts ...Option) http.Handler {
	cfg := &config{
		ctx:                      context.Background(),
		metadataRedaction:        true,
		scheduleOverdueThreshold: DefaultScheduleOverdueThreshold,
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}
	if cfg.authorizer == nil && cfg.insecureAllowUnauthenticated {
		slog.Default().Warn("jobs UI running WITHOUT authentication — all job payloads are exposed; use only on a local/trusted network")
	}

	// Set up stats storage if we have a GORM-backed storage
	var statsStorage StatsStorage
	if gs, ok := storage.(interface{ DB() *gorm.DB }); ok {
		statsStore := NewGormStatsStorage(gs.DB())
		if err := statsStore.MigrateStats(context.Background()); err != nil {
			slog.Default().Error("failed to migrate stats storage", "error", err)
		}
		statsStorage = statsStore

		if cfg.queue != nil && registerStatsCollector(gs.DB()) {
			var collectorOpts []StatsCollectorOption
			if cfg.statsRetention > 0 {
				collectorOpts = append(collectorOpts, WithStatsCollectorRetention(cfg.statsRetention))
			}
			collector := NewStatsCollector(cfg.queue, statsStorage, collectorOpts...)
			startStatsCollectorFn(cfg.ctx, collector)
			// Self-prune the package-global registry (the collector goroutine exits
			// via its own ctx) when the caller's context is cancelled, so a process
			// that mounts a Handler per distinct *gorm.DB over its lifetime (e.g.
			// per-tenant DBs) does not leak a map entry + goroutine per mount. Only
			// when the context is actually cancellable: the default is
			// context.Background() (Done()==nil), for which a cleanup goroutine would
			// block forever — a single long-lived mount intentionally keeps its one
			// collector for the process lifetime, so we skip it and add no
			// permanently-blocked goroutine.
			if done := cfg.ctx.Done(); done != nil {
				db := gs.DB()
				go func() {
					<-done
					unregisterStatsCollector(db)
				}()
			}
		}
	}

	mux := http.NewServeMux()

	// Create the jobs service
	svc := newJobsService(storage, cfg.queue, statsStorage)
	svc.metadataRedaction = cfg.metadataRedaction
	svc.scheduleOverdueThreshold = cfg.scheduleOverdueThreshold

	// Register Connect-RPC handler
	path, handler := jobsv1connect.NewJobsServiceHandler(
		svc,
		connect.WithInterceptors(authInterceptor(
			cfg.insecureAllowUnauthenticated,
			cfg.authorizer,
			cfg.allowedOrigins,
		)),
	)
	mux.Handle(path, handler)

	// Serve static frontend assets
	staticFS, err := fs.Sub(frontendFS, "frontend/dist")
	if err != nil {
		// If frontend isn't built yet, serve a placeholder
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" || r.URL.Path == "/index.html" {
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte(placeholderHTML))
				return
			}
			http.NotFound(w, r)
		})
	} else {
		fileServer := http.FileServer(http.FS(staticFS))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// For SPA routing, serve index.html for non-file requests
			if !strings.Contains(r.URL.Path, ".") && r.URL.Path != "/" {
				r.URL.Path = "/"
			}
			fileServer.ServeHTTP(w, r)
		})
	}

	withHostHeader := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Always overwrite the header-map Host from the server-authoritative
		// r.Host so the same-origin check cannot be fooled by a client-injected
		// Host header (browsers can't set Host; a non-browser client could).
		if r.Host != "" {
			r.Header.Set("Host", r.Host)
		}
		mux.ServeHTTP(w, r)
	})

	// The operator's middleware goes INSIDE h2c, not outside it.
	//
	// h2c.NewHandler hijacks the connection on an HTTP/1.1 `Upgrade: h2c` and
	// then serves every subsequent HTTP/2 stream on that connection itself. A
	// middleware wrapped AROUND the h2c handler is therefore invoked exactly once
	// — on the upgrade request — and never again. Demonstrated end to end: one
	// request to a middleware-permitted path (the SPA shell "/" in any real
	// deployment) carrying the upgrade headers returns 101, after which stream 3
	// on the same connection reaches a protected RPC with the middleware never
	// consulted (middleware=1, inner=2).
	//
	// That matters because SECURITY.md instructs operators to use
	// ui.WithMiddleware as THE authentication mechanism for the dashboard, so the
	// documented deployment shape was bypassable. Wrapping the inner handler puts
	// the middleware on the path of every stream (middleware=2, inner=1, with the
	// protected stream refused).
	//
	// Note the prior-knowledge h2c path was NOT affected: its preface arrives as
	// an HTTP/1.1-looking request that an outer middleware does see.
	inner := http.Handler(withHostHeader)
	if cfg.middleware != nil {
		inner = cfg.middleware(inner)
	}

	if cfg.disableH2C {
		// No h2c: the caller is terminating HTTP/2 themselves (TLS, or Go 1.24+
		// srv.Protocols.SetUnencryptedHTTP2). Nothing to hijack, so nothing to
		// bypass.
		return inner
	}

	h2cHandler := h2c.NewHandler(inner, &http2.Server{})

	// Cap the body of an UPGRADE request specifically. x/net's h2c handler reads
	// the whole request body into memory to replay it as the first stream, and
	// moving the middleware inside makes that read strictly PRE-authentication —
	// so this fix would otherwise widen an unauthenticated memory-exhaustion
	// window. The cap is applied only to requests that actually carry the upgrade
	// headers: a blanket http.MaxBytesHandler would silently truncate legitimate
	// BulkDeleteJobs/BulkRetryJobs bodies, changing behaviour on a path that has
	// no bug.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isH2CUpgrade(r) {
			r.Body = http.MaxBytesReader(w, r.Body, maxH2CUpgradeBody)
		}
		h2cHandler.ServeHTTP(w, r)
	})
}

// maxH2CUpgradeBody bounds the body x/net buffers while replaying an h2c upgrade
// as the connection's first stream. Dashboard RPCs that legitimately carry large
// bodies do not arrive as upgrade requests.
const maxH2CUpgradeBody = 64 << 10

// isH2CUpgrade reports whether r is an HTTP/1.1 cleartext-HTTP/2 upgrade — the
// only request shape whose body x/net reads into memory before dispatch. Header
// tokens are comma-separated and case-insensitive per RFC 9110.
func isH2CUpgrade(r *http.Request) bool {
	if r.ProtoMajor != 1 {
		return false
	}
	if !headerHasToken(r.Header, "Upgrade", "h2c") {
		return false
	}
	return headerHasToken(r.Header, "Connection", "upgrade")
}

func headerHasToken(h http.Header, key, token string) bool {
	for _, v := range h.Values(key) {
		for part := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func registerStatsCollector(db *gorm.DB) bool {
	statsCollectorMu.Lock()
	defer statsCollectorMu.Unlock()

	if statsCollectorsByDB[db] {
		return false
	}
	statsCollectorsByDB[db] = true
	return true
}

// unregisterStatsCollector removes a DB's registry entry so a later re-mount of a
// Handler for the same *gorm.DB starts a fresh collector. Called when a Handler's
// cancellable context is done (see Handler); a no-op if the entry is already gone.
func unregisterStatsCollector(db *gorm.DB) {
	statsCollectorMu.Lock()
	defer statsCollectorMu.Unlock()
	delete(statsCollectorsByDB, db)
}

var mutatingProcedures = map[string]Action{
	jobsv1connect.JobsServiceRetryJobProcedure:       ActionRetryJob,
	jobsv1connect.JobsServiceDeleteJobProcedure:      ActionDeleteJob,
	jobsv1connect.JobsServiceBulkRetryJobsProcedure:  ActionBulkRetryJobs,
	jobsv1connect.JobsServiceBulkDeleteJobsProcedure: ActionBulkDeleteJobs,
	jobsv1connect.JobsServicePauseJobProcedure:       ActionPauseJob,
	jobsv1connect.JobsServiceCancelJobProcedure:      ActionCancelJob,
	jobsv1connect.JobsServiceResumeJobProcedure:      ActionResumeJob,
	jobsv1connect.JobsServicePauseQueueProcedure:     ActionPauseQueue,
	jobsv1connect.JobsServiceResumeQueueProcedure:    ActionResumeQueue,
	jobsv1connect.JobsServicePurgeQueueProcedure:     ActionPurgeQueue,
}

var readProcedures = map[string]Action{
	jobsv1connect.JobsServiceGetStatsProcedure:          ActionViewStats,
	jobsv1connect.JobsServiceGetStatsHistoryProcedure:   ActionViewStats,
	jobsv1connect.JobsServiceListJobsProcedure:          ActionViewJobs,
	jobsv1connect.JobsServiceGetJobProcedure:            ActionViewJob,
	jobsv1connect.JobsServiceListQueuesProcedure:        ActionViewStats,
	jobsv1connect.JobsServiceListScheduledJobsProcedure: ActionViewJobs,
	jobsv1connect.JobsServiceGetWorkflowProcedure:       ActionViewJob,
	jobsv1connect.JobsServiceListWorkflowsProcedure:     ActionViewJobs,
	jobsv1connect.JobsServiceWatchEventsProcedure:       ActionWatchEvents,
}

const authRequiredMessage = "jobs UI requires an Authorizer (ui.WithAuthorizer) or an explicit ui.WithInsecureAllowUnauthenticated() for local/trusted-network use"

type dashboardAuthInterceptor struct {
	insecureAllowUnauthenticated bool
	authorizer                   Authorizer
	allowedOrigins               map[string]struct{}
}

func authInterceptor(insecureAllowUnauthenticated bool, authorizer Authorizer, allowedOrigins map[string]struct{}) connect.Interceptor {
	return dashboardAuthInterceptor{
		insecureAllowUnauthenticated: insecureAllowUnauthenticated,
		authorizer:                   authorizer,
		allowedOrigins:               allowedOrigins,
	}
}

func (i dashboardAuthInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return connect.UnaryFunc(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if _, mutates := mutatingProcedures[req.Spec().Procedure]; mutates {
			if err := i.authorizeOrigin(req); err != nil {
				return nil, err
			}
		}
		if err := i.authorize(ctx, req.Spec().Procedure); err != nil {
			return nil, err
		}
		return next(ctx, req)
	})
}

func (i dashboardAuthInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i dashboardAuthInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return connect.StreamingHandlerFunc(func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := i.authorize(ctx, conn.Spec().Procedure); err != nil {
			return err
		}
		return next(ctx, conn)
	})
}

func (i dashboardAuthInterceptor) authorize(ctx context.Context, procedure string) error {
	action, known := actionForProcedure(procedure)
	if !known {
		// Fail closed: an unmapped procedure is denied on EVERY path (before the
		// authorizer and before the insecure-allow branch), so a forgotten
		// classification can never grant access.
		return connect.NewError(connect.CodePermissionDenied, errors.New("unmapped RPC procedure: authorization denied"))
	}
	if i.authorizer != nil {
		if err := i.authorizer.Authorize(ctx, action); err != nil {
			var connectErr *connect.Error
			if errors.As(err, &connectErr) {
				return connectErr
			}
			return connect.NewError(connect.CodePermissionDenied, err)
		}
		return nil
	}
	if i.insecureAllowUnauthenticated {
		return nil
	}
	return connect.NewError(connect.CodePermissionDenied, errors.New(authRequiredMessage))
}

func (i dashboardAuthInterceptor) authorizeOrigin(req connect.AnyRequest) error {
	origin := req.Header().Get("Origin")
	if origin == "" {
		return nil
	}
	if _, ok := i.allowedOrigins[origin]; ok {
		return nil
	}
	// Same-origin check compares the browser Origin against the server's real
	// Host (copied into the Host header by withHostHeader). Client-supplied
	// X-Forwarded-Host is deliberately NOT trusted — it is forgeable by any
	// caller and would defeat the check. Cross-origin operators use
	// WithAllowedOrigins.
	originURL, err := url.Parse(origin)
	if err == nil && originURL.Host != "" {
		if host := req.Header().Get("Host"); host != "" && strings.EqualFold(originURL.Host, host) {
			return nil
		}
	}
	return connect.NewError(connect.CodePermissionDenied, errors.New("origin not allowed; configure ui.WithAllowedOrigins"))
}

// actionForProcedure returns the authorization Action for a procedure and whether
// it is KNOWN. Callers MUST treat known==false as DENIED — an unmapped procedure
// must never be authorized (a newly-added RPC that nobody classified must fail
// closed, not silently inherit read access). TestActionForProcedure_Exhaustive
// asserts every real JobsService procedure is classified.
func actionForProcedure(procedure string) (Action, bool) {
	if action, mutates := mutatingProcedures[procedure]; mutates {
		return action, true
	}
	if action, ok := readProcedures[procedure]; ok {
		return action, true
	}
	return "", false
}

const placeholderHTML = `<!DOCTYPE html>
<html>
<head>
    <title>Jobs UI</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
            background: #f5f5f5;
        }
        .container {
            text-align: center;
            padding: 40px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 { color: #333; margin-bottom: 16px; }
        p { color: #666; margin-bottom: 24px; }
        code {
            background: #f0f0f0;
            padding: 8px 16px;
            border-radius: 4px;
            display: block;
            margin-top: 16px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Jobs UI</h1>
        <p>The frontend hasn't been built yet.</p>
        <p>Run the following to build:</p>
        <code>cd ui/frontend && npm install && npm run build</code>
        <p style="margin-top: 24px; font-size: 14px;">
            API is available at <a href="/jobs.v1.JobsService/">/jobs.v1.JobsService/</a>
        </p>
    </div>
</body>
</html>`
