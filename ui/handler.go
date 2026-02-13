package ui

import (
	"context"
	"io/fs"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/ui/gen/jobs/v1/jobsv1connect"
)

// Handler creates an http.Handler for the jobs UI dashboard.
// It serves both the Connect-RPC API and the static frontend assets.
//
// Usage:
//
//	mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage)))
func Handler(storage core.Storage, opts ...Option) http.Handler {
	cfg := &config{}
	for _, opt := range opts {
		opt.apply(cfg)
	}

	// Set up stats storage if we have a GORM-backed storage
	var statsStorage StatsStorage
	if gs, ok := storage.(interface{ DB() *gorm.DB }); ok {
		statsStore := NewGormStatsStorage(gs.DB())
		_ = statsStore.MigrateStats(context.Background())
		statsStorage = statsStore

		if cfg.queue != nil {
			var collectorOpts []StatsCollectorOption
			if cfg.statsRetention > 0 {
				collectorOpts = append(collectorOpts, WithStatsCollectorRetention(cfg.statsRetention))
			}
			collector := NewStatsCollector(cfg.queue, statsStorage, collectorOpts...)
			go collector.Start(context.Background())
		}
	}

	mux := http.NewServeMux()

	// Create the jobs service
	svc := newJobsService(storage, cfg.queue, statsStorage)

	// Register Connect-RPC handler
	path, handler := jobsv1connect.NewJobsServiceHandler(
		svc,
		connect.WithInterceptors(),
	)
	mux.Handle(path, handler)

	// Serve static frontend assets
	staticFS, err := fs.Sub(frontendFS, "frontend/dist")
	if err != nil {
		// If frontend isn't built yet, serve a placeholder
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" || r.URL.Path == "/index.html" {
				w.Header().Set("Content-Type", "text/html")
				w.Write([]byte(placeholderHTML))
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

	// Wrap with H2C for HTTP/2 over cleartext (needed for Connect streaming)
	h2cHandler := h2c.NewHandler(mux, &http2.Server{})

	// Apply middleware if configured
	if cfg.middleware != nil {
		return cfg.middleware(h2cHandler)
	}

	return h2cHandler
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
