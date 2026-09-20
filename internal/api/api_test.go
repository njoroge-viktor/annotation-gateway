package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/njoroge-viktor/annotation-gateway/internal/api"
	"github.com/njoroge-viktor/annotation-gateway/internal/metrics"
	"github.com/njoroge-viktor/annotation-gateway/internal/queue"
	"github.com/njoroge-viktor/annotation-gateway/internal/ratelimit"
)

func newServer(t *testing.T, burst int) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return api.New(
		queue.New(time.Minute),
		ratelimit.New(burst, 1000),
		metrics.NewRegistry(),
		logger,
	).Handler()
}

func do(t *testing.T, h http.Handler, method, path, actor string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(encoded)
	}

	req := httptest.NewRequest(method, path, reader)
	if actor != "" {
		req.Header.Set("X-Annotator-ID", actor)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthzIsUnauthenticated(t *testing.T) {
	rec := do(t, newServer(t, 100), http.MethodGet, "/healthz", "", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestProtectedRoutesRequireAnnotatorHeader(t *testing.T) {
	rec := do(t, newServer(t, 100), http.MethodPost, "/v1/tasks/claim", "", nil)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestFullTaskLifecycle(t *testing.T) {
	h := newServer(t, 100)

	if rec := do(t, h, http.MethodPost, "/v1/tasks", "admin",
		map[string]any{"id": "t1", "payload": map[string]any{"text": "review this"}, "priority": 5},
	); rec.Code != http.StatusCreated {
		t.Fatalf("create: status = %d, want 201 (%s)", rec.Code, rec.Body)
	}

	claim := do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)
	if claim.Code != http.StatusOK {
		t.Fatalf("claim: status = %d, want 200 (%s)", claim.Code, claim.Body)
	}

	var lease queue.Lease
	if err := json.Unmarshal(claim.Body.Bytes(), &lease); err != nil {
		t.Fatalf("decode lease: %v", err)
	}
	if lease.Task.ID != "t1" {
		t.Errorf("leased %q, want t1", lease.Task.ID)
	}

	if rec := do(t, h, http.MethodPost, "/v1/tasks/t1/submit", "ann-a",
		map[string]any{"result": map[string]any{"label": "safe"}},
	); rec.Code != http.StatusOK {
		t.Fatalf("submit: status = %d, want 200 (%s)", rec.Code, rec.Body)
	}

	result := do(t, h, http.MethodGet, "/v1/tasks/t1/result", "admin", nil)
	if result.Code != http.StatusOK {
		t.Fatalf("result: status = %d, want 200", result.Code)
	}
	if !strings.Contains(result.Body.String(), "safe") {
		t.Errorf("result body = %s, want it to contain the submitted label", result.Body)
	}
}

func TestClaimOnEmptyQueue(t *testing.T) {
	rec := do(t, newServer(t, 100), http.MethodPost, "/v1/tasks/claim", "ann-a", nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
}

func TestSubmitByNonHolderIsForbidden(t *testing.T) {
	h := newServer(t, 100)
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})
	do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)

	rec := do(t, h, http.MethodPost, "/v1/tasks/t1/submit", "ann-b", map[string]any{"result": nil})
	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", rec.Code)
	}

	var body struct{ Code string }
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Code != "not_lease_holder" {
		t.Errorf("code = %q, want not_lease_holder", body.Code)
	}
}

func TestReleaseReturnsTaskToPool(t *testing.T) {
	h := newServer(t, 100)
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})
	do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)

	if rec := do(t, h, http.MethodPost, "/v1/tasks/t1/release", "ann-a", nil); rec.Code != http.StatusOK {
		t.Fatalf("release: status = %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-b", nil); rec.Code != http.StatusOK {
		t.Errorf("reclaim: status = %d, want 200", rec.Code)
	}
}

func TestExtendRenewsLease(t *testing.T) {
	h := newServer(t, 100)
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})
	do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)

	rec := do(t, h, http.MethodPost, "/v1/tasks/t1/extend", "ann-a", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("extend: status = %d (%s)", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "expires_at") {
		t.Error("extend response did not carry the new expiry")
	}
}

func TestDuplicateTaskIDConflicts(t *testing.T) {
	h := newServer(t, 100)
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})
	do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)

	rec := do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})
	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rec.Code)
	}
}

func TestMissingTaskIDIsRejected(t *testing.T) {
	rec := do(t, newServer(t, 100), http.MethodPost, "/v1/tasks", "admin", map[string]any{"priority": 1})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestUnknownJSONFieldIsRejected(t *testing.T) {
	// A typo'd key must fail loudly rather than being silently dropped.
	h := newServer(t, 100)
	req := httptest.NewRequest(http.MethodPost, "/v1/tasks",
		strings.NewReader(`{"id":"t1","prioritee":5}`))
	req.Header.Set("X-Annotator-ID", "admin")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	h := newServer(t, 100)
	huge := `{"id":"t1","payload":{"text":"` + strings.Repeat("x", 2<<20) + `"}}`

	req := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(huge))
	req.Header.Set("X-Annotator-ID", "admin")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", rec.Code)
	}
}

func TestRateLimitReturns429WithRetryAfter(t *testing.T) {
	h := newServer(t, 2) // burst of 2

	for i := 0; i < 2; i++ {
		if rec := do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("throttled at request %d, within the burst", i+1)
		}
	}

	rec := do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 response carried no Retry-After header")
	}
}

func TestRateLimitIsPerAnnotator(t *testing.T) {
	h := newServer(t, 1)

	do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)
	if rec := do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("ann-a not throttled: %d", rec.Code)
	}
	// One annotator burning their quota must not throttle anybody else.
	if rec := do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-b", nil); rec.Code == http.StatusTooManyRequests {
		t.Error("ann-b throttled by ann-a's usage")
	}
}

func TestRequestIDIsEchoed(t *testing.T) {
	rec := do(t, newServer(t, 100), http.MethodGet, "/healthz", "", nil)
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("no X-Request-ID on the response")
	}
}

func TestInboundRequestIDIsHonoured(t *testing.T) {
	h := newServer(t, 100)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "trace-me-across-services")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("X-Request-ID"); got != "trace-me-across-services" {
		t.Errorf("X-Request-ID = %q, want the inbound value preserved", got)
	}
}

func TestMetricsEndpointExposesPrometheusText(t *testing.T) {
	h := newServer(t, 100)
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})
	do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)

	rec := do(t, h, http.MethodGet, "/metrics", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE gateway_tasks_claimed_total counter",
		"gateway_tasks_claimed_total 1",
		"# TYPE gateway_request_duration_seconds histogram",
		"gateway_request_duration_seconds_count",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestStatsReflectsQueueState(t *testing.T) {
	h := newServer(t, 100)
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t2"})
	do(t, h, http.MethodPost, "/v1/tasks/claim", "ann-a", nil)

	rec := do(t, h, http.MethodGet, "/stats", "", nil)

	var stats queue.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Pending != 1 || stats.Leased != 1 {
		t.Errorf("stats = %+v, want 1 pending and 1 leased", stats)
	}
}

func TestResultMissingForIncompleteTask(t *testing.T) {
	h := newServer(t, 100)
	do(t, h, http.MethodPost, "/v1/tasks", "admin", map[string]any{"id": "t1"})

	rec := do(t, h, http.MethodGet, "/v1/tasks/t1/result", "admin", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestErrorsShareOneShape(t *testing.T) {
	h := newServer(t, 100)

	for _, tc := range []struct{ name, method, path string }{
		{"unauthorised", http.MethodPost, "/v1/tasks/claim"},
		{"not found", http.MethodGet, "/v1/tasks/ghost/result"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actor := ""
			if tc.name == "not found" {
				actor = "admin"
			}
			rec := do(t, h, tc.method, tc.path, actor, nil)

			var body struct{ Code, Message string }
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if body.Code == "" || body.Message == "" {
				t.Errorf("error body = %+v, want both code and message populated", body)
			}
		})
	}
}
