// Package ratelimit provides a per-key token bucket limiter.
//
// Token buckets are used rather than fixed windows because fixed windows allow
// a client to send 2x the limit across a window boundary. A bucket refills
// continuously, so the limit holds over every interval, not just the aligned
// ones.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter enforces a token bucket per key, safe for concurrent use.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket

	capacity   float64
	refillRate float64 // tokens per second
	idleTTL    time.Duration

	now func() time.Time
}

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

// tokensAt reports how many tokens the bucket holds at time now, accounting
// for refill since it was last touched. Refill is lazy — nothing ticks in the
// background — so every reader must apply it rather than trusting b.tokens.
func (l *Limiter) tokensAt(b *bucket, now time.Time) float64 {
	elapsed := now.Sub(b.lastSeen).Seconds()
	if elapsed <= 0 {
		return b.tokens
	}
	return min(l.capacity, b.tokens+elapsed*l.refillRate)
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithClock replaces the time source. Intended for tests.
func WithClock(now func() time.Time) Option {
	return func(l *Limiter) { l.now = now }
}

// WithIdleTTL sets how long an untouched bucket is kept before eviction.
// Without eviction, a limiter keyed by client id is an unbounded map and
// therefore a memory leak.
func WithIdleTTL(d time.Duration) Option {
	return func(l *Limiter) { l.idleTTL = d }
}

// New returns a Limiter allowing burst tokens, refilling at perSecond.
func New(burst int, perSecond float64, opts ...Option) *Limiter {
	l := &Limiter{
		buckets:    make(map[string]*bucket),
		capacity:   float64(burst),
		refillRate: perSecond,
		idleTTL:    10 * time.Minute,
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Allow consumes one token for key, reporting whether the request may proceed.
func (l *Limiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN consumes n tokens for key.
func (l *Limiter) AllowN(key string, n float64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.capacity, lastSeen: now}
		l.buckets[key] = b
	} else {
		b.tokens = l.tokensAt(b, now)
		b.lastSeen = now
	}

	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// Remaining reports whole tokens currently available for key.
func (l *Limiter) Remaining(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		return int(l.capacity)
	}
	return int(l.tokensAt(b, l.now()))
}

// RetryAfter reports how long until one token is available for key.
func (l *Limiter) RetryAfter(key string) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		return 0
	}
	tokens := l.tokensAt(b, l.now())
	if tokens >= 1 {
		return 0
	}
	if l.refillRate <= 0 {
		return time.Duration(1<<62 - 1)
	}

	deficit := 1 - tokens
	return time.Duration(deficit / l.refillRate * float64(time.Second))
}

// Evict drops buckets untouched for longer than the idle TTL and reports how
// many were removed. Call it periodically; see StartJanitor.
func (l *Limiter) Evict() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	cutoff := now.Add(-l.idleTTL)
	removed := 0
	for key, b := range l.buckets {
		// Only evict a bucket that has refilled to capacity, so forgetting it
		// is indistinguishable from keeping it. Crucially this uses the
		// refilled count, not b.tokens: refill is lazy, so a stale b.tokens
		// would keep every idle bucket alive forever and leak memory.
		if b.lastSeen.Before(cutoff) && l.tokensAt(b, now) >= l.capacity {
			delete(l.buckets, key)
			removed++
		}
	}
	return removed
}

// Len reports how many buckets are currently tracked.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// StartJanitor runs Evict on a ticker until stop is closed.
func (l *Limiter) StartJanitor(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				l.Evict()
			case <-stop:
				return
			}
		}
	}()
}
