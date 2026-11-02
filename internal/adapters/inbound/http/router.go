package http

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// RouterOption customises NewRouter / NewReportsRouter.
type RouterOption func(*routerConfig)

type routerConfig struct {
	// idempotencyPool, when non-nil, wires RequireIdempotencyKey onto
	// every POST (see idempotency.go and WithIdempotencyPool). Every cmd
	// binary passes its Postgres pool, so the check is on in every
	// deployment; a nil pool exists only for the unit tests that build a
	// router over in-memory test doubles with no transactional backing —
	// the middleware needs a real pgxpool.Pool to begin its own
	// transaction.
	idempotencyPool *pgxpool.Pool
}

// WithIdempotencyPool wires RequireIdempotencyKey onto EVERY POST route
// (see docs/docs/adr/0028-idempotency-key-middleware.md and ADR-0060).
// cmd/execution always passes its pool (Postgres is the only runtime); a nil
// pool (or omitting this option entirely) leaves the POSTs unprotected and
// is for unit tests over in-memory doubles only.
func WithIdempotencyPool(pool *pgxpool.Pool) RouterOption {
	return func(cfg *routerConfig) { cfg.idempotencyPool = pool }
}

// NewRouter builds the chi router for every Fulfillment Execution endpoint.
// A nil logger falls back to slog.Default() rather than panicking.
func NewRouter(h *Handlers, logger *slog.Logger, opts ...RouterOption) *chi.Mux {
	if logger == nil {
		logger = slog.Default()
	}
	var cfg routerConfig
	for _, o := range opts {
		o(&cfg)
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	// otelchi runs before the logger so every log line emitted downstream
	// sits inside the request span and picks up its trace_id/span_id.
	// WithChiRoutes makes the span name the route pattern
	// (POST /tasks/{id}/completions) rather than the raw path, keeping span
	// names low-cardinality.
	//
	// http.server.request.duration comes from the sanctioned
	// otelchimetric middleware (ADR-0019), directly after otelchi and
	// ahead of the request logger.
	serviceName := observability.ServiceName()
	r.Use(otelchi.Middleware(
		serviceName,
		otelchi.WithChiRoutes(r),
		otelchi.WithRequestMethodInSpanName(true),
	))
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware())
	// A NUL (U+0000) in a path, query or body is a client error: Postgres
	// text cannot hold it. Before any route-scoped idempotency middleware.
	r.Use(rejectNUL)

	r.Get("/healthz", h.GetHealthz)
	r.Get("/readyz", h.GetReadyz)

	// EVERY POST is route-scoped (r.With, not r.Use) behind
	// RequireIdempotencyKey: a client may retry any of them after a timeout
	// and the effect happens once (REST guideline section 2; ADR-0028,
	// ADR-0060). A nil pool (in-memory dev/test configuration, no
	// transactional Postgres backing) skips the middleware, mirroring every
	// other optional Postgres-backed capability's nil convention in this repo.
	post := func(pattern string, hf http.HandlerFunc) {
		if cfg.idempotencyPool != nil {
			r.With(RequireIdempotencyKey(cfg.idempotencyPool)).Post(pattern, hf)
			return
		}
		r.Post(pattern, hf)
	}

	post("/stations", h.PostRegisterStation)
	post("/tasks", h.PostTask)
	r.Get("/tasks", h.GetTasksHandler)
	post("/stations/{stationId}/task-claims", h.PostClaimNext)
	post("/stations/{stationId}/check-ins", h.PostCheckInStation)
	post("/stations/{stationId}/check-outs", h.PostCheckOutStation)
	post("/tasks/{id}/lease-renewals", h.PostRenewLease)
	post("/tasks/{id}/completions", h.PostCompleteTask)
	// A scan adds eaches to the task's pick count, so a retry after a lost
	// response (the floor client's outbox resends with the SAME key) must
	// replay the stored answer instead of counting the scan twice (ADR 0055).
	post("/tasks/{id}/scans", h.PostPickScan)
	post("/tasks/{id}/case-breaks", h.PostBreakCase)
	post("/tasks/{id}/package-seals", h.PostSealPackage)
	r.Get("/packages", h.GetPackagesHandler)
	r.Get("/packages/{id}", h.GetPackageHandler)
	post("/packages/{id}/slam-runs", h.PostRunSlam)
	r.Get("/queues/{taskType}/depth", h.GetQueueDepthHandler)
	r.Get("/capacity/{capability}", h.GetInstalledCapacityHandler)
	post("/task-lease-expirations", h.PostExpireLeases)
	post("/cpt-miss-sweeps", h.PostSweepCPTMisses)
	post("/rebin/arrivals", h.PostArriveAtRebin)
	// A2A delegation to Work Planning (ADR-0058): execution PULLS the next
	// unit for a path; the fact returns as WorkReleased on Kafka. Keyed: a
	// retry of one logical pull replays its answer, a new pull mints a key.
	post("/paths/{pathId}/work-requests", h.PostWorkRequest)

	return r
}

// RequestLogger returns chi middleware that logs one structured line per
// request via logger, including status, duration, response size and the
// chi request id. Responses with a 5xx status are logged at Error level;
// everything else logs at Info level.
//
// It logs through the *Context flavours of the slog API so that, when the
// logger is wrapped in observability.NewSlogHandler, each line also carries
// the trace_id/span_id of the request span otelchi started upstream.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r)

			attrs := []any{
				"method", r.Method,
				"path", sanitizeForLog(r.URL.Path),
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"bytes", ww.BytesWritten(),
				"request_id", middleware.GetReqID(r.Context()),
			}
			if ww.Status() >= 500 {
				logger.ErrorContext(r.Context(), "http request", attrs...)
			} else {
				logger.InfoContext(r.Context(), "http request", attrs...)
			}
		})
	}
}

// sanitizeForLog strips CR/LF from an attacker-controlled value (here,
// the raw request path) before it is written to a log line. Without this,
// a crafted path segment containing an encoded newline could forge a fake
// log entry that appears to be a separate, legitimate line (CWE-117 log
// injection) once the log record reaches a downstream viewer/aggregator
// that doesn't preserve slog's JSON string escaping.
func sanitizeForLog(s string) string {
	return strings.NewReplacer("\n", "", "\r", "").Replace(s)
}

// corsMiddleware allows the warehouse-console browser SPA (and this
// service's own future MFE remote dev origin) to call this API directly
// from the browser. CORS_ALLOWED_ORIGINS overrides the local-dev default
// (comma-separated) for staging/prod deployments.
func corsMiddleware() func(http.Handler) http.Handler {
	origins := []string{"http://localhost:5173", "http://localhost:5184"}
	if v := os.Getenv("CORS_ALLOWED_ORIGINS"); v != "" {
		origins = strings.Split(v, ",")
	}
	return cors.Handler(cors.Options{
		AllowedOrigins:   origins,
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete},
		AllowedHeaders:   []string{"Content-Type", "Authorization", "Idempotency-Key"},
		ExposedHeaders:   []string{"Location", IdempotencyReplayedHeader},
		AllowCredentials: false,
		MaxAge:           300,
	})
}
