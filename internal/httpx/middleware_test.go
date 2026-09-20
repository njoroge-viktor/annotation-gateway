package httpx_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/njoroge-viktor/annotation-gateway/internal/httpx"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestChainRunsInOrder(t *testing.T) {
	var order []string

	mark := func(name string) httpx.Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}

	handler := httpx.Chain(mark("first"), mark("second"))(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			order = append(order, "handler")
		}),
	)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	if strings.Join(order, ",") != "first,second,handler" {
		t.Errorf("order = %v, want first,second,handler", order)
	}
}

func TestRecoverTurnsPanicInto500(t *testing.T) {
	handler := httpx.Recover(discardLogger())(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("boom")
		}),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal_error") {
		t.Errorf("body = %s, want a structured error", rec.Body)
	}
}

func TestRequestIDIsGeneratedAndUnique(t *testing.T) {
	handler := httpx.RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if httpx.RequestIDFrom(r.Context()) == "" {
			t.Error("no request id in context")
		}
	}))

	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		id := rec.Header().Get("X-Request-ID")
		if seen[id] {
			t.Fatalf("duplicate request id %q", id)
		}
		seen[id] = true
	}
}

func TestActorFromEmptyContext(t *testing.T) {
	if got := httpx.ActorFrom(httptest.NewRequest(http.MethodGet, "/", nil).Context()); got != "" {
		t.Errorf("actor = %q, want empty", got)
	}
}

func TestJSONSetsContentType(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.JSON(rec, http.StatusCreated, map[string]string{"ok": "yes"})

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q", ct)
	}
}

func TestJSONWithNilBodyWritesNothing(t *testing.T) {
	rec := httptest.NewRecorder()
	httpx.JSON(rec, http.StatusNoContent, nil)

	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body)
	}
}

func TestFirstWriteHeaderWins(t *testing.T) {
	// A handler that writes a status then panics must not have its status
	// rewritten by the recovery middleware — the client already has it.
	handler := httpx.Recover(discardLogger())(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTeapot)
			panic("after the header went out")
		}),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want 418 preserved", rec.Code)
	}
}
