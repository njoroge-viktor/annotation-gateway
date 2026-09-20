package ratelimit_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/njoroge-viktor/annotation-gateway/internal/ratelimit"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func TestBurstIsAllowedThenExhausted(t *testing.T) {
	l := ratelimit.New(5, 1)

	for i := 0; i < 5; i++ {
		if !l.Allow("ann-a") {
			t.Fatalf("request %d denied within burst of 5", i+1)
		}
	}
	if l.Allow("ann-a") {
		t.Error("6th request allowed with a burst of 5")
	}
}

func TestBucketRefillsOverTime(t *testing.T) {
	c := newClock()
	l := ratelimit.New(2, 1, ratelimit.WithClock(c.Now)) // 1 token/sec

	l.Allow("ann-a")
	l.Allow("ann-a")
	if l.Allow("ann-a") {
		t.Fatal("allowed beyond burst")
	}

	c.Advance(time.Second)
	if !l.Allow("ann-a") {
		t.Error("denied after a full second of refill")
	}
}

func TestRefillIsCappedAtBurst(t *testing.T) {
	c := newClock()
	l := ratelimit.New(3, 10, ratelimit.WithClock(c.Now))

	c.Advance(time.Hour) // would be 36,000 tokens if uncapped

	for i := 0; i < 3; i++ {
		if !l.Allow("ann-a") {
			t.Fatalf("request %d denied", i+1)
		}
	}
	if l.Allow("ann-a") {
		t.Error("bucket accumulated beyond its capacity")
	}
}

// TestNoBoundaryBurst is the reason for a token bucket over a fixed window:
// a fixed window lets a client send 2x the limit across the boundary.
func TestNoBoundaryBurst(t *testing.T) {
	c := newClock()
	l := ratelimit.New(10, 10, ratelimit.WithClock(c.Now))

	for i := 0; i < 10; i++ {
		l.Allow("ann-a")
	}
	c.Advance(100 * time.Millisecond) // refills exactly 1 token

	allowed := 0
	for i := 0; i < 10; i++ {
		if l.Allow("ann-a") {
			allowed++
		}
	}
	if allowed != 1 {
		t.Errorf("allowed %d immediately after the window edge, want 1", allowed)
	}
}

func TestKeysAreIsolated(t *testing.T) {
	l := ratelimit.New(1, 1)

	if !l.Allow("ann-a") {
		t.Fatal("first key denied")
	}
	if !l.Allow("ann-b") {
		t.Error("second key denied — one annotator exhausting their bucket must not throttle another")
	}
}

func TestRemaining(t *testing.T) {
	l := ratelimit.New(5, 1)

	if got := l.Remaining("new-key"); got != 5 {
		t.Errorf("Remaining for an unseen key = %d, want 5", got)
	}
	l.Allow("ann-a")
	l.Allow("ann-a")
	if got := l.Remaining("ann-a"); got != 3 {
		t.Errorf("Remaining = %d, want 3", got)
	}
}

func TestRetryAfter(t *testing.T) {
	c := newClock()
	l := ratelimit.New(1, 2, ratelimit.WithClock(c.Now)) // 2 tokens/sec

	if got := l.RetryAfter("fresh"); got != 0 {
		t.Errorf("RetryAfter with tokens available = %v, want 0", got)
	}

	l.Allow("ann-a")
	// Bucket is empty; one token at 2/sec takes 500ms.
	if got := l.RetryAfter("ann-a"); got < 400*time.Millisecond || got > 600*time.Millisecond {
		t.Errorf("RetryAfter = %v, want ~500ms", got)
	}
}

func TestAllowNConsumesMultipleTokens(t *testing.T) {
	l := ratelimit.New(10, 1)

	if !l.AllowN("ann-a", 7) {
		t.Fatal("AllowN(7) denied with a burst of 10")
	}
	if l.AllowN("ann-a", 5) {
		t.Error("AllowN(5) allowed with only 3 tokens left")
	}
}

func TestEvictKeepsStillThrottledBuckets(t *testing.T) {
	c := newClock()
	// Refill deliberately slower than the idle TTL, so a drained bucket is
	// still short of capacity by the time it looks idle.
	l := ratelimit.New(5, 0.001, ratelimit.WithClock(c.Now), ratelimit.WithIdleTTL(time.Minute))

	for i := 0; i < 5; i++ {
		l.Allow("throttled") // drain to zero
	}

	c.Advance(2 * time.Minute) // idle, but only ~0.12 tokens refilled
	if removed := l.Evict(); removed != 0 {
		t.Errorf("evicted %d still-throttled buckets; going quiet must not reset a limit", removed)
	}

	// Once it has genuinely refilled, forgetting it is indistinguishable from
	// keeping it, so eviction is safe.
	c.Advance(10 * time.Hour)
	if removed := l.Evict(); removed != 1 {
		t.Errorf("evicted %d fully refilled buckets, want 1", removed)
	}
}

func TestEvictKeepsActiveBuckets(t *testing.T) {
	c := newClock()
	l := ratelimit.New(5, 1, ratelimit.WithClock(c.Now), ratelimit.WithIdleTTL(time.Minute))

	l.Allow("active")
	c.Advance(30 * time.Second) // still within the idle TTL

	if removed := l.Evict(); removed != 0 {
		t.Errorf("evicted %d recently active buckets, want 0", removed)
	}
}

func TestEvictBoundsMemory(t *testing.T) {
	c := newClock()
	l := ratelimit.New(1, 1, ratelimit.WithClock(c.Now), ratelimit.WithIdleTTL(time.Minute))

	for i := 0; i < 500; i++ {
		l.Allow(fmt.Sprintf("client-%d", i))
	}
	if l.Len() != 500 {
		t.Fatalf("tracked %d buckets, want 500", l.Len())
	}

	c.Advance(time.Hour) // everything refills and goes idle
	removed := l.Evict()

	if removed != 500 || l.Len() != 0 {
		t.Errorf("evicted %d leaving %d, want 500 leaving 0", removed, l.Len())
	}
}

func TestJanitorRunsUntilStopped(t *testing.T) {
	c := newClock()
	l := ratelimit.New(1, 1000, ratelimit.WithClock(c.Now), ratelimit.WithIdleTTL(time.Nanosecond))
	l.Allow("ann-a")

	stop := make(chan struct{})
	l.StartJanitor(5*time.Millisecond, stop)
	c.Advance(time.Hour)

	deadline := time.After(2 * time.Second)
	for l.Len() > 0 {
		select {
		case <-deadline:
			close(stop)
			t.Fatal("janitor did not evict within 2s")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	close(stop)
}

func TestConcurrentAllowIsRaceFree(t *testing.T) {
	l := ratelimit.New(1000, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("ann-%d", n%4)
			for j := 0; j < 100; j++ {
				l.Allow(key)
				l.Remaining(key)
			}
		}(i)
	}
	wg.Wait()
}
