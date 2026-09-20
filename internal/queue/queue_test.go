package queue_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/njoroge-viktor/annotation-gateway/internal/queue"
)

// clock is a manually advanced time source, so lease-expiry tests are
// deterministic and instant rather than sleeping through real timeouts.
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

func task(id string, priority int) queue.Task {
	return queue.Task{ID: id, Priority: priority, Payload: map[string]any{"text": id}}
}

func TestAddAndClaim(t *testing.T) {
	q := queue.New(time.Minute)

	if !q.Add(task("t1", 0)) {
		t.Fatal("Add returned false for a new task")
	}

	lease, err := q.Claim("ann-a")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if lease.Task.ID != "t1" {
		t.Errorf("claimed %q, want t1", lease.Task.ID)
	}
	if lease.Holder != "ann-a" {
		t.Errorf("holder %q, want ann-a", lease.Holder)
	}
	if lease.Task.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", lease.Task.Attempts)
	}
}

func TestClaimEmptyQueue(t *testing.T) {
	q := queue.New(time.Minute)
	if _, err := q.Claim("ann-a"); err != queue.ErrEmpty {
		t.Errorf("err = %v, want ErrEmpty", err)
	}
}

func TestClaimIsExclusive(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("t1", 0))

	if _, err := q.Claim("ann-a"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	// The single task is leased, so a second annotator must get nothing
	// rather than the same work.
	if _, err := q.Claim("ann-b"); err != queue.ErrEmpty {
		t.Errorf("second claim err = %v, want ErrEmpty", err)
	}
}

func TestPriorityOrdering(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("low", 1))
	q.Add(task("urgent", 10))
	q.Add(task("mid", 5))

	for _, want := range []string{"urgent", "mid", "low"} {
		lease, err := q.Claim("ann-a")
		if err != nil {
			t.Fatalf("claim %s: %v", want, err)
		}
		if lease.Task.ID != want {
			t.Errorf("claimed %q, want %q", lease.Task.ID, want)
		}
	}
}

func TestEqualPriorityIsFIFO(t *testing.T) {
	q := queue.New(time.Minute)
	for _, id := range []string{"first", "second", "third"} {
		q.Add(task(id, 5))
	}

	for _, want := range []string{"first", "second", "third"} {
		lease, _ := q.Claim("ann-a")
		if lease.Task.ID != want {
			t.Errorf("claimed %q, want %q — equal priorities must stay FIFO", lease.Task.ID, want)
		}
	}
}

func TestSubmitCompletesTask(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("t1", 0))
	q.Claim("ann-a")

	if err := q.Submit("t1", "ann-a", map[string]any{"label": "safe"}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	result, ok := q.Result("t1")
	if !ok {
		t.Fatal("Result not available after submit")
	}
	if result["label"] != "safe" {
		t.Errorf("result = %v, want label=safe", result)
	}
	if got := q.Stats().Complete; got != 1 {
		t.Errorf("complete = %d, want 1", got)
	}
}

func TestSubmitRejectsNonHolder(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("t1", 0))
	q.Claim("ann-a")

	// Accepting this would silently overwrite the real holder's work.
	if err := q.Submit("t1", "ann-b", nil); err != queue.ErrNotLeaseHolder {
		t.Errorf("err = %v, want ErrNotLeaseHolder", err)
	}
}

func TestSubmitUnknownTask(t *testing.T) {
	q := queue.New(time.Minute)
	if err := q.Submit("nope", "ann-a", nil); err != queue.ErrNotFound {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestExpiredLeaseReturnsToPool(t *testing.T) {
	c := newClock()
	q := queue.New(time.Minute, queue.WithClock(c.Now))
	q.Add(task("t1", 0))

	q.Claim("ann-a")
	c.Advance(2 * time.Minute) // annotator walked away

	lease, err := q.Claim("ann-b")
	if err != nil {
		t.Fatalf("reclaim after expiry: %v", err)
	}
	if lease.Task.ID != "t1" {
		t.Errorf("reclaimed %q, want t1", lease.Task.ID)
	}
	if lease.Task.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", lease.Task.Attempts)
	}
}

func TestSubmitAfterExpiryIsRejected(t *testing.T) {
	c := newClock()
	q := queue.New(time.Minute, queue.WithClock(c.Now))
	q.Add(task("t1", 0))
	q.Claim("ann-a")

	c.Advance(2 * time.Minute)

	if err := q.Submit("t1", "ann-a", map[string]any{"late": true}); err != queue.ErrLeaseExpired {
		t.Errorf("err = %v, want ErrLeaseExpired", err)
	}
	// And the task must be back in the pool, not stranded.
	if got := q.Stats().Pending; got != 1 {
		t.Errorf("pending = %d, want 1", got)
	}
}

func TestExtendKeepsLeaseAlive(t *testing.T) {
	c := newClock()
	q := queue.New(time.Minute, queue.WithClock(c.Now))
	q.Add(task("t1", 0))
	q.Claim("ann-a")

	c.Advance(50 * time.Second)
	if _, err := q.Extend("t1", "ann-a"); err != nil {
		t.Fatalf("Extend: %v", err)
	}

	c.Advance(50 * time.Second) // 100s total, but the lease was renewed at 50s
	if err := q.Submit("t1", "ann-a", nil); err != nil {
		t.Errorf("Submit after extend: %v, want success", err)
	}
}

func TestExtendRejectsNonHolder(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("t1", 0))
	q.Claim("ann-a")

	if _, err := q.Extend("t1", "ann-b"); err != queue.ErrNotLeaseHolder {
		t.Errorf("err = %v, want ErrNotLeaseHolder", err)
	}
}

func TestReleaseReturnsTaskImmediately(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("t1", 0))
	q.Claim("ann-a")

	if err := q.Release("t1", "ann-a"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	lease, err := q.Claim("ann-b")
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if lease.Task.ID != "t1" {
		t.Errorf("claimed %q, want t1", lease.Task.ID)
	}
}

func TestMaxAttemptsDropsPoisonTask(t *testing.T) {
	c := newClock()
	q := queue.New(time.Minute, queue.WithClock(c.Now), queue.WithMaxAttempts(2))
	q.Add(task("poison", 0))

	// Two annotators claim it and both abandon it.
	for i := 0; i < 2; i++ {
		if _, err := q.Claim(fmt.Sprintf("ann-%d", i)); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		c.Advance(2 * time.Minute)
	}

	if _, err := q.Claim("ann-x"); err != queue.ErrEmpty {
		t.Errorf("err = %v, want ErrEmpty — a task that defeated 2 annotators should be dropped", err)
	}
	if got := q.Stats().Pending; got != 0 {
		t.Errorf("pending = %d, want 0", got)
	}
}

func TestAddRejectsDuplicateOfLeasedTask(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("t1", 0))
	q.Claim("ann-a")

	if q.Add(task("t1", 0)) {
		t.Error("Add returned true for an id that is currently leased")
	}
}

func TestResultUnavailableUntilComplete(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("t1", 0))

	if _, ok := q.Result("t1"); ok {
		t.Error("Result available for a pending task")
	}
	q.Claim("ann-a")
	if _, ok := q.Result("t1"); ok {
		t.Error("Result available for a leased task")
	}
}

func TestStatsCountsEachState(t *testing.T) {
	q := queue.New(time.Minute)
	q.Add(task("pending", 0))
	q.Add(task("leased", 0))
	q.Add(task("done", 0))

	q.Claim("ann-a") // takes one
	l2, _ := q.Claim("ann-b")
	q.Submit(l2.Task.ID, "ann-b", nil)

	s := q.Stats()
	if s.Pending != 1 || s.Leased != 1 || s.Complete != 1 {
		t.Errorf("stats = %+v, want 1 of each", s)
	}
}

// TestConcurrentClaimsAreExclusive is the property that matters most: under
// -race, no two annotators may ever receive the same task.
func TestConcurrentClaimsAreExclusive(t *testing.T) {
	const tasks = 200
	q := queue.New(time.Minute)
	for i := 0; i < tasks; i++ {
		q.Add(task(fmt.Sprintf("t%03d", i), i%5))
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = make(map[string]string)
	)

	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			holder := fmt.Sprintf("ann-%02d", worker)
			for {
				lease, err := q.Claim(holder)
				if err != nil {
					return
				}
				mu.Lock()
				if prev, dup := claimed[lease.Task.ID]; dup {
					t.Errorf("task %s claimed twice: %s and %s", lease.Task.ID, prev, holder)
				}
				claimed[lease.Task.ID] = holder
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(claimed) != tasks {
		t.Errorf("claimed %d distinct tasks, want %d", len(claimed), tasks)
	}
}

func TestConcurrentSubmitsAreSafe(t *testing.T) {
	q := queue.New(time.Minute)
	for i := 0; i < 100; i++ {
		q.Add(task(fmt.Sprintf("t%03d", i), 0))
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			holder := fmt.Sprintf("ann-%d", worker)
			for {
				lease, err := q.Claim(holder)
				if err != nil {
					return
				}
				_ = q.Submit(lease.Task.ID, holder, map[string]any{"by": holder})
			}
		}(w)
	}
	wg.Wait()

	if got := q.Stats().Complete; got != 100 {
		t.Errorf("complete = %d, want 100", got)
	}
}
