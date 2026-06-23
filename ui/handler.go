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
		ctx:               context.Background(),
		metadataRedaction: true,
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
		}
	}

	mux := http.NewServeMux()

	// Create the jobs service
	svc := newJobsService(storage, cfg.queue, statsStorage)
	svc.metadataRedaction = cfg.metadataRedaction

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

	// Wrap with H2C for HTTP/2 over cleartext (needed for Connect streaming)
	h2cHandler := h2c.NewHandler(withHostHeader, &http2.Server{})

	// Apply middleware if configured
	if cfg.middleware != nil {
		return cfg.middleware(h2cHandler)
	}

	return h2cHandler
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
	action := actionForProcedure(procedure)
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

func actionForProcedure(procedure string) Action {
	if action, mutates := mutatingProcedures[procedure]; mutates {
		return action
	}
	if action, ok := readProcedures[procedure]; ok {
		return action
	}
	// Any read procedure not explicitly mapped is still authorized (fail-closed),
	// just under the coarse ActionViewJobs — never treated as public.
	return ActionViewJobs
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
