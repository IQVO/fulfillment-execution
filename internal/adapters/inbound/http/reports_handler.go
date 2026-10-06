package http

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

	"github.com/claudioed/fulfillment-execution/internal/analytics/report"
	"github.com/claudioed/fulfillment-execution/internal/observability"
)

// ReportsHandlers is the inbound HTTP adapter for the fulfillment throughput
// data product's READER. It depends only on the read-model port
// (report.ReportStore); it never touches the OLTP use cases or the writer.
type ReportsHandlers struct {
	Store report.ReportStore
}

// throughputRowDTO is the wire shape of one report row. It is a dedicated
// DTO so the read-model struct (report.Row) never leaks onto the API.
type throughputRowDTO struct {
	TaskType                  string  `json:"taskType"`
	StationId                 string  `json:"stationId"`
	HourBucket                string  `json:"hourBucket"`
	Completions               int     `json:"completions"`
	AvgClaimToCompleteSeconds float64 `json:"avgClaimToCompleteSeconds"`
	LeaseExpiries             int     `json:"leaseExpiries"`
	WeighCheckDiverts         int     `json:"weighCheckDiverts"`
	// PackagesManifested/PackagesOnTimeToCPT/PackagesLateToCPT are the
	// on-time-to-CPT KPI's raw counts (ADR-0026, companion to
	// order-management ADR 0014 §6). The rate is derived by the caller
	// (PackagesOnTimeToCPT / PackagesManifested), matching this DTO's
	// existing convention of shipping raw counts rather than a precomputed
	// percentage.
	PackagesManifested  int `json:"packagesManifested"`
	PackagesOnTimeToCPT int `json:"packagesOnTimeToCPT"`
	PackagesLateToCPT   int `json:"packagesLateToCPT"`
}

// throughputReportDTO is the wire shape of a throughput report response.
type throughputReportDTO struct {
	Rows []throughputRowDTO `json:"rows"`
}

// freshnessDTO is the wire shape of the freshness-lag response.
type freshnessDTO struct {
	LagSeconds float64 `json:"lagSeconds"`
}

// GetThroughput serves GET /reports/throughput. from and to (RFC3339) are
// required; taskType, stationId and granularity are optional (granularity
// defaults to hour).
func (h *ReportsHandlers) GetThroughput(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	from, ok := parseRequiredTime(w, r, q.Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseRequiredTime(w, r, q.Get("to"), "to")
	if !ok {
		return
	}

	granularity := report.GranularityHour
	if g := q.Get("granularity"); g != "" {
		if g != string(report.GranularityHour) {
			writeReportBadRequest(w, r, "granularity must be 'hour'")
			return
		}
		granularity = report.Granularity(g)
	}

	rep, err := h.Store.Query(r.Context(), report.ReportQuery{
		From:        from,
		To:          to,
		TaskType:    q.Get("taskType"),
		StationId:   q.Get("stationId"),
		Granularity: granularity,
	})
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}

	dto := throughputReportDTO{Rows: make([]throughputRowDTO, 0, len(rep.Rows))}
	for _, row := range rep.Rows {
		dto.Rows = append(dto.Rows, throughputRowDTO{
			TaskType:                  row.Key.TaskType,
			StationId:                 row.Key.StationId,
			HourBucket:                row.Key.HourBucket.UTC().Format(time.RFC3339),
			Completions:               row.Completions,
			AvgClaimToCompleteSeconds: row.AvgClaimToCompleteSeconds,
			LeaseExpiries:             row.LeaseExpiries,
			WeighCheckDiverts:         row.WeighCheckDiverts,
			PackagesManifested:        row.PackagesManifested,
			PackagesOnTimeToCPT:       row.PackagesOnTimeToCPT,
			PackagesLateToCPT:         row.PackagesLateToCPT,
		})
	}
	writeJSON(w, http.StatusOK, dto)
}

// GetFreshness serves GET /reports/throughput/freshness.
func (h *ReportsHandlers) GetFreshness(w http.ResponseWriter, r *http.Request) {
	lag, err := h.Store.FreshnessLag(r.Context())
	if err != nil {
		writeReportInternal(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, freshnessDTO{LagSeconds: lag.Seconds()})
}

// GetHealthz serves GET /healthz for the reports service.
func (h *ReportsHandlers) GetHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// parseRequiredTime parses an RFC3339 timestamp, writing an RFC 7807 400 and
// returning ok=false when it is missing or malformed.
func parseRequiredTime(w http.ResponseWriter, r *http.Request, raw, name string) (time.Time, bool) {
	if raw == "" {
		writeReportBadRequest(w, r, "query parameter '"+name+"' is required (RFC3339)")
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeReportBadRequest(w, r, "query parameter '"+name+"' must be an RFC3339 timestamp")
		return time.Time{}, false
	}
	return t, true
}

// writeReportBadRequest writes the reports service's RFC 7807 400.
func writeReportBadRequest(w http.ResponseWriter, r *http.Request, detail string) {
	writeProblem(w, http.StatusBadRequest, "invalid-report-query",
		"The report query is malformed or missing a required parameter", detail, r.URL.Path)
}

// writeReportInternal writes the reports service's RFC 7807 500.
func writeReportInternal(w http.ResponseWriter, r *http.Request, err error) {
	writeProblem(w, http.StatusInternalServerError, "report-store-error",
		"The report could not be served", err.Error(), r.URL.Path)
}

// NewReportsRouter builds the chi router for the fulfillment-reports reader
// service. A nil logger falls back to slog.Default().
func NewReportsRouter(h *ReportsHandlers, logger *slog.Logger, opts ...RouterOption) *chi.Mux {
	if logger == nil {
		logger = slog.Default()
	}
	var cfg routerConfig
	for _, o := range opts {
		o(&cfg)
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	serviceName := observability.ServiceName()
	r.Use(otelchi.Middleware(
		serviceName,
		otelchi.WithChiRoutes(r),
		otelchi.WithRequestMethodInSpanName(true),
	))
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(serviceName)))
	r.Use(RequestLogger(logger))
	r.Use(middleware.Recoverer)

	r.Get("/healthz", h.GetHealthz)
	r.Get("/reports/throughput", h.GetThroughput)
	r.Get("/reports/throughput/freshness", h.GetFreshness)

	return r
}
