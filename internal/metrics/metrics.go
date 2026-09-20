// Package metrics provides minimal Prometheus-compatible instrumentation.
//
// Hand-rolled rather than pulling in the client library: the gateway needs
// counters, gauges and a latency histogram, and that is a small enough surface
// that a dependency-free implementation is cheaper to audit than to import.
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultBuckets covers sub-millisecond to multi-second request latencies.
var DefaultBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Registry holds every metric the process exposes.
type Registry struct {
	mu         sync.RWMutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
}

// Counter is a monotonically increasing value.
type Counter struct {
	name, help string
	value      atomic.Int64
}

// Inc adds one.
func (c *Counter) Inc() { c.value.Add(1) }

// Add adds n, which must be non-negative.
func (c *Counter) Add(n int64) {
	if n < 0 {
		panic("metrics: counters cannot decrease")
	}
	c.value.Add(n)
}

// Value returns the current count.
func (c *Counter) Value() int64 { return c.value.Load() }

// Gauge is a value that can go up or down.
type Gauge struct {
	name, help string
	mu         sync.RWMutex
	value      float64
}

// Set replaces the gauge's value.
func (g *Gauge) Set(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.value = v
}

// Value returns the current value.
func (g *Gauge) Value() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.value
}

// Histogram accumulates observations into cumulative buckets.
type Histogram struct {
	name, help string
	buckets    []float64

	mu     sync.Mutex
	counts []uint64
	sum    float64
	total  uint64
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sum += v
	h.total++
	for i, upper := range h.buckets {
		if v <= upper {
			h.counts[i]++
		}
	}
}

// Count returns how many observations were recorded.
func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.total
}

// Sum returns the total of all observations.
func (h *Histogram) Sum() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sum
}

// Counter returns the named counter, creating it on first use.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()

	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{name: name, help: help}
	r.counters[name] = c
	return c
}

// Gauge returns the named gauge, creating it on first use.
func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()

	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := &Gauge{name: name, help: help}
	r.gauges[name] = g
	return g
}

// Histogram returns the named histogram, creating it on first use.
func (r *Registry) Histogram(name, help string, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()

	if h, ok := r.histograms[name]; ok {
		return h
	}
	if len(buckets) == 0 {
		buckets = DefaultBuckets
	}
	sorted := append([]float64(nil), buckets...)
	sort.Float64s(sorted)

	h := &Histogram{
		name:    name,
		help:    help,
		buckets: sorted,
		counts:  make([]uint64, len(sorted)),
	}
	r.histograms[name] = h
	return h
}

// Write renders the registry in Prometheus text exposition format.
//
// Metric families are emitted in sorted order so the output is byte-stable,
// which makes it diffable in tests.
func (r *Registry) Write(w *strings.Builder) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, name := range sortedKeys(r.counters) {
		c := r.counters[name]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, c.help, name, name, c.Value())
	}

	for _, name := range sortedKeys(r.gauges) {
		g := r.gauges[name]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %g\n", name, g.help, name, name, g.Value())
	}

	for _, name := range sortedKeys(r.histograms) {
		h := r.histograms[name]
		h.mu.Lock()
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, h.help, name)
		for i, upper := range h.buckets {
			fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, upper, h.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n",
			name, h.total, name, h.sum, name, h.total)
		h.mu.Unlock()
	}
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
