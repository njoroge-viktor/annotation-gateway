// Package api wires the queue to HTTP.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/njoroge-viktor/annotation-gateway/internal/httpx"
	"github.com/njoroge-viktor/annotation-gateway/internal/metrics"
	"github.com/njoroge-viktor/annotation-gateway/internal/queue"
	"github.com/njoroge-viktor/annotation-gateway/internal/ratelimit"
)

// maxBodyBytes caps request bodies. Without it, a single client can exhaust
// server memory by streaming an endless payload.
const maxBodyBytes = 1 << 20 // 1 MiB

// Server holds the gateway's dependencies.
type Server struct {
	queue   *queue.Queue
	limiter *ratelimit.Limiter
	metrics *metrics.Registry
	logger  *slog.Logger

	claimed   *metrics.Counter
	submitted *metrics.Counter
	rejected  *metrics.Counter
	requests  *metrics.Counter
	latency   *metrics.Histogram
	depth     *metrics.Gauge
}

// New builds a Server and registers its metrics.
func New(q *queue.Queue, l *ratelimit.Limiter, reg *metrics.Registry, logger *slog.Logger) *Server {
	return &Server{
		queue:   q,
		limiter: l,
		metrics: reg,
		logger:  logger,

		claimed:   reg.Counter("gateway_tasks_claimed_total", "Tasks leased to annotators."),
		submitted: reg.Counter("gateway_tasks_submitted_total", "Tasks completed by annotators."),
		rejected:  reg.Counter("gateway_requests_rate_limited_total", "Requests rejected by the rate limiter."),
		requests:  reg.Counter("gateway_requests_total", "Requests handled."),
		latency:   reg.Histogram("gateway_request_duration_seconds", "Request latency.", nil),
		depth:     reg.Gauge("gateway_queue_pending", "Tasks currently pending."),
	}
}

// Handler returns the fully wired HTTP handler.
//
// Ordering matters: request ids are attached first so every later log line
// carries one; recovery wraps everything downstream of it; auth runs before
// the rate limiter so throttling is per annotator rather than per connection.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /stats", s.handleStats)

	authed := http.NewServeMux()
	authed.HandleFunc("POST /v1/tasks", s.handleAddTask)
	authed.HandleFunc("POST /v1/tasks/claim", s.handleClaim)
	authed.HandleFunc("POST /v1/tasks/{id}/submit", s.handleSubmit)
	authed.HandleFunc("POST /v1/tasks/{id}/extend", s.handleExtend)
	authed.HandleFunc("POST /v1/tasks/{id}/release", s.handleRelease)
	authed.HandleFunc("GET /v1/tasks/{id}/result", s.handleResult)

	protected := httpx.Chain(
		httpx.Auth(),
		httpx.RateLimit(s.limiter, s.rejected),
	)(authed)

	mux.Handle("/v1/", protected)

	return httpx.Chain(
		httpx.RequestID(),
		httpx.Recover(s.logger),
		httpx.Logging(s.logger, s.latency, s.requests),
	)(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	stats := s.queue.Stats()
	s.depth.Set(float64(stats.Pending))

	var sb strings.Builder
	s.metrics.Write(&sb)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(sb.String()))
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	httpx.JSON(w, http.StatusOK, s.queue.Stats())
}

type addTaskRequest struct {
	ID       string         `json:"id"`
	Payload  map[string]any `json:"payload"`
	Priority int            `json:"priority"`
}

func (s *Server) handleAddTask(w http.ResponseWriter, r *http.Request) {
	var req addTaskRequest
	if !decode(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.ID) == "" {
		httpx.Error(w, http.StatusBadRequest, "invalid_request", "id is required")
		return
	}

	if !s.queue.Add(queue.Task{ID: req.ID, Payload: req.Payload, Priority: req.Priority}) {
		httpx.Error(w, http.StatusConflict, "already_exists",
			"a task with this id is already leased or complete")
		return
	}

	httpx.JSON(w, http.StatusCreated, map[string]string{"id": req.ID, "state": string(queue.StatePending)})
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	lease, err := s.queue.Claim(httpx.ActorFrom(r.Context()))
	if errors.Is(err, queue.ErrEmpty) {
		httpx.Error(w, http.StatusNoContent, "empty", "no tasks available")
		return
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	s.claimed.Inc()
	httpx.JSON(w, http.StatusOK, lease)
}

type submitRequest struct {
	Result map[string]any `json:"result"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if !decode(w, r, &req) {
		return
	}

	err := s.queue.Submit(r.PathValue("id"), httpx.ActorFrom(r.Context()), req.Result)
	if err != nil {
		writeQueueError(w, err)
		return
	}

	s.submitted.Inc()
	httpx.JSON(w, http.StatusOK, map[string]string{"state": string(queue.StateComplete)})
}

func (s *Server) handleExtend(w http.ResponseWriter, r *http.Request) {
	expiry, err := s.queue.Extend(r.PathValue("id"), httpx.ActorFrom(r.Context()))
	if err != nil {
		writeQueueError(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"expires_at": expiry})
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	if err := s.queue.Release(r.PathValue("id"), httpx.ActorFrom(r.Context())); err != nil {
		writeQueueError(w, err)
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]string{"state": string(queue.StatePending)})
}

func (s *Server) handleResult(w http.ResponseWriter, r *http.Request) {
	result, ok := s.queue.Result(r.PathValue("id"))
	if !ok {
		httpx.Error(w, http.StatusNotFound, "not_found", "no completed task with that id")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"result": result})
}

// decode reads a JSON body, writing an error response and reporting false on
// failure. Unknown fields are rejected so a typo'd key fails loudly instead of
// being silently dropped.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			httpx.Error(w, http.StatusRequestEntityTooLarge, "payload_too_large",
				"request body exceeds 1 MiB")
			return false
		}
		httpx.Error(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	return true
}

func writeQueueError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		httpx.Error(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, queue.ErrNotLeaseHolder):
		httpx.Error(w, http.StatusForbidden, "not_lease_holder", err.Error())
	case errors.Is(err, queue.ErrLeaseExpired):
		httpx.Error(w, http.StatusConflict, "lease_expired", err.Error())
	default:
		httpx.Error(w, http.StatusInternalServerError, "internal_error", err.Error())
	}
}
