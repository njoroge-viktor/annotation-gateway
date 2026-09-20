package metrics_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/njoroge-viktor/annotation-gateway/internal/metrics"
)

func TestCounter(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.Counter("requests_total", "Requests.")

	c.Inc()
	c.Add(4)

	if got := c.Value(); got != 5 {
		t.Errorf("value = %d, want 5", got)
	}
}

func TestCounterRejectsDecrease(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Add(-1) did not panic; counters must be monotonic")
		}
	}()
	metrics.NewRegistry().Counter("c", "").Add(-1)
}

func TestRegistryReturnsSameInstance(t *testing.T) {
	r := metrics.NewRegistry()
	r.Counter("shared", "").Inc()

	// Fetching by name again must not silently reset the value.
	if got := r.Counter("shared", "").Value(); got != 1 {
		t.Errorf("value = %d, want 1", got)
	}
}

func TestGauge(t *testing.T) {
	g := metrics.NewRegistry().Gauge("depth", "Queue depth.")

	g.Set(42)
	if got := g.Value(); got != 42 {
		t.Errorf("value = %g, want 42", got)
	}
	g.Set(7) // gauges may decrease
	if got := g.Value(); got != 7 {
		t.Errorf("value = %g, want 7", got)
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	r := metrics.NewRegistry()
	h := r.Histogram("latency", "Latency.", []float64{1, 10, 100})

	h.Observe(0.5)
	h.Observe(5)
	h.Observe(50)

	if got := h.Count(); got != 3 {
		t.Errorf("count = %d, want 3", got)
	}
	if got := h.Sum(); got != 55.5 {
		t.Errorf("sum = %g, want 55.5", got)
	}

	var sb strings.Builder
	r.Write(&sb)
	out := sb.String()

	// Cumulative: le=1 has 1, le=10 has 2, le=100 has 3.
	for _, want := range []string{
		`latency_bucket{le="1"} 1`,
		`latency_bucket{le="10"} 2`,
		`latency_bucket{le="100"} 3`,
		`latency_bucket{le="+Inf"} 3`,
		"latency_count 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\ngot:\n%s", want, out)
		}
	}
}

func TestHistogramSortsSuppliedBuckets(t *testing.T) {
	r := metrics.NewRegistry()
	h := r.Histogram("h", "", []float64{100, 1, 10})
	h.Observe(5)

	var sb strings.Builder
	r.Write(&sb)

	// Out-of-order input must still produce a monotonic cumulative series.
	out := sb.String()
	first := strings.Index(out, `le="1"`)
	second := strings.Index(out, `le="10"`)
	if first == -1 || second == -1 || first > second {
		t.Errorf("buckets not emitted in ascending order:\n%s", out)
	}
}

func TestHistogramDefaultsWhenNoBuckets(t *testing.T) {
	h := metrics.NewRegistry().Histogram("h", "", nil)
	h.Observe(0.002)
	if h.Count() != 1 {
		t.Error("observation lost with default buckets")
	}
}

func TestWriteIsStable(t *testing.T) {
	r := metrics.NewRegistry()
	r.Counter("z_last", "").Inc()
	r.Counter("a_first", "").Inc()

	var first, second strings.Builder
	r.Write(&first)
	r.Write(&second)

	if first.String() != second.String() {
		t.Error("Write is not byte-stable across calls")
	}
	// Sorted output keeps diffs readable in tests.
	if strings.Index(first.String(), "a_first") > strings.Index(first.String(), "z_last") {
		t.Error("metric families are not emitted in sorted order")
	}
}

func TestExpositionFormat(t *testing.T) {
	r := metrics.NewRegistry()
	r.Counter("my_total", "A help string.").Add(3)

	var sb strings.Builder
	r.Write(&sb)

	for _, want := range []string{
		"# HELP my_total A help string.",
		"# TYPE my_total counter",
		"my_total 3",
	} {
		if !strings.Contains(sb.String(), want) {
			t.Errorf("missing %q", want)
		}
	}
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	r := metrics.NewRegistry()
	var wg sync.WaitGroup

	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Counter("c", "").Inc()
				r.Gauge("g", "").Set(float64(j))
				r.Histogram("h", "", nil).Observe(0.01)
			}
		}()
	}
	wg.Wait()

	if got := r.Counter("c", "").Value(); got != 3200 {
		t.Errorf("counter = %d, want 3200 — increments were lost", got)
	}
}
