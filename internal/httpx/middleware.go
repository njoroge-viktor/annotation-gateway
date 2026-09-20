// Package httpx provides HTTP middleware and response helpers.
package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/njoroge-viktor/annotation-gateway/internal/metrics"
	"github.com/njoroge-viktor/annotation-gateway/internal/ratelimit"
)

type contextKey string

const (
	requestIDKey contextKey = "request_id"
	actorKey     contextKey = "actor"
)

// Middleware wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain composes middleware so that Chain(a, b)(h) runs a, then b, then h.
func Chain(mw ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		for i := len(mw) - 1; i >= 0; i-- {
			final = mw[i](final)
		}
		return final
	}
}

// recorder captures status and size for logging and metrics.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Status reports the status code written, defaulting to 200.
func (r *recorder) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// RequestID attaches a request id to the context and echoes it in the response.
// An inbound X-Request-ID is honoured so ids survive across service hops.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Request-ID")
			if id == "" {
				id = newID()
			}
			w.Header().Set("X-Request-ID", id)
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
		})
	}
}

// RequestIDFrom returns the request id stored in ctx, or "" if absent.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// Recover turns a panicking handler into a 500 instead of a dropped connection,
// logging the stack so the panic is not lost.
func Recover(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					logger.Error("panic recovered",
						"error", err,
						"path", r.URL.Path,
						"request_id", RequestIDFrom(r.Context()),
						"stack", string(debug.Stack()),
					)
					Error(w, http.StatusInternalServerError, "internal_error", "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Logging emits one structured line per request and records latency.
func Logging(logger *slog.Logger, hist *metrics.Histogram, total *metrics.Counter) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &recorder{ResponseWriter: w}

			next.ServeHTTP(rec, r)

			elapsed := time.Since(start)
			if hist != nil {
				hist.Observe(elapsed.Seconds())
			}
			if total != nil {
				total.Inc()
			}

			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.Status(),
				"bytes", rec.bytes,
				"duration_ms", float64(elapsed.Microseconds())/1000,
				"request_id", RequestIDFrom(r.Context()),
			)
		})
	}
}

// Auth extracts the caller identity from X-Annotator-ID.
//
// Deliberately a placeholder for a real authenticator: the gateway's job here
// is to demonstrate the middleware seam, and pretending a header is
// authentication would be worse than saying so.
func Auth() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			actor := r.Header.Get("X-Annotator-ID")
			if actor == "" {
				Error(w, http.StatusUnauthorized, "unauthenticated", "X-Annotator-ID header is required")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), actorKey, actor)))
		})
	}
}

// ActorFrom returns the authenticated caller, or "" if unauthenticated.
func ActorFrom(ctx context.Context) string {
	actor, _ := ctx.Value(actorKey).(string)
	return actor
}

// RateLimit throttles per authenticated actor, falling back to remote address.
func RateLimit(limiter *ratelimit.Limiter, rejected *metrics.Counter) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := ActorFrom(r.Context())
			if key == "" {
				key = r.RemoteAddr
			}

			if !limiter.Allow(key) {
				if rejected != nil {
					rejected.Inc()
				}
				retry := limiter.RetryAfter(key)
				w.Header().Set("Retry-After", strconv.Itoa(int(retry.Seconds())+1))
				w.Header().Set("X-RateLimit-Remaining", "0")
				Error(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded")
				return
			}

			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(limiter.Remaining(key)))
			next.ServeHTTP(w, r)
		})
	}
}

// ErrorBody is the single error shape every endpoint returns.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// JSON writes v as JSON with the given status.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	// The header is already sent, so an encode failure cannot change the
	// status; log-free best effort is the only option left.
	_ = json.NewEncoder(w).Encode(v)
}

// Error writes a structured error response.
func Error(w http.ResponseWriter, status int, code, message string) {
	JSON(w, status, ErrorBody{Code: code, Message: message})
}

func newID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the system is in serious trouble; a
		// timestamp keeps requests traceable rather than crashing the server.
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}
