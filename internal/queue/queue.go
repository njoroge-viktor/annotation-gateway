// Package queue implements a lease-based work queue for annotation tasks.
//
// The problem it solves: an annotator claims a task and then closes the tab.
// Without leases that task is lost forever. Here a claim grants a time-limited
// lease, and a task whose lease expires returns to the pool automatically.
//
// The queue is safe for concurrent use.
package queue

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrEmpty is returned when no task is available to claim.
	ErrEmpty = errors.New("queue: no tasks available")
	// ErrNotFound is returned when a task id is unknown.
	ErrNotFound = errors.New("queue: task not found")
	// ErrNotLeaseHolder is returned when a worker submits a task it does not hold.
	ErrNotLeaseHolder = errors.New("queue: caller does not hold the lease")
	// ErrLeaseExpired is returned when a submission arrives after its lease ended.
	ErrLeaseExpired = errors.New("queue: lease has expired")
)

// State is where a task sits in its lifecycle.
type State string

const (
	StatePending  State = "pending"
	StateLeased   State = "leased"
	StateComplete State = "complete"
)

// Task is a unit of annotation work.
type Task struct {
	ID       string         `json:"id"`
	Payload  map[string]any `json:"payload"`
	Priority int            `json:"priority"`
	State    State          `json:"state"`
	Attempts int            `json:"attempts"`

	leaseHolder string
	leaseExpiry time.Time
	result      map[string]any
}

// Lease is a time-limited grant of exclusive access to a task.
type Lease struct {
	Task      Task      `json:"task"`
	Holder    string    `json:"holder"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Stats is a point-in-time snapshot of queue depth.
type Stats struct {
	Pending  int `json:"pending"`
	Leased   int `json:"leased"`
	Complete int `json:"complete"`
	Expired  int `json:"expired_total"`
}

// Queue is a priority work queue with lease semantics.
//
// The zero value is not usable; call New.
type Queue struct {
	mu sync.Mutex

	tasks map[string]*Task
	// order holds pending ids, kept sorted by (priority desc, insertion asc).
	order []string

	leaseTTL     time.Duration
	maxAttempts  int
	expiredTotal int

	// now is swappable so tests can control time instead of sleeping.
	now func() time.Time
}

// Option configures a Queue.
type Option func(*Queue)

// WithClock replaces the queue's time source. Intended for tests.
func WithClock(now func() time.Time) Option {
	return func(q *Queue) { q.now = now }
}

// WithMaxAttempts caps how many times a task may be re-leased after expiry.
// Once exceeded the task is dropped rather than cycling forever. Zero means
// unlimited.
func WithMaxAttempts(n int) Option {
	return func(q *Queue) { q.maxAttempts = n }
}

// New returns a Queue granting leases of the given duration.
func New(leaseTTL time.Duration, opts ...Option) *Queue {
	q := &Queue{
		tasks:    make(map[string]*Task),
		leaseTTL: leaseTTL,
		now:      time.Now,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

// Add enqueues a task. Adding an id that already exists replaces it only if
// the existing task is still pending; otherwise Add reports false.
func (q *Queue) Add(t Task) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if existing, ok := q.tasks[t.ID]; ok && existing.State != StatePending {
		return false
	}

	t.State = StatePending
	if _, existed := q.tasks[t.ID]; !existed {
		q.order = append(q.order, t.ID)
	}
	q.tasks[t.ID] = &t
	q.sortPending()
	return true
}

// Claim grants holder a lease on the highest-priority pending task.
func (q *Queue) Claim(holder string) (Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reapLocked()

	for i, id := range q.order {
		task := q.tasks[id]
		if task == nil || task.State != StatePending {
			continue
		}

		q.order = append(q.order[:i], q.order[i+1:]...)

		now := q.now()
		task.State = StateLeased
		task.leaseHolder = holder
		task.leaseExpiry = now.Add(q.leaseTTL)
		task.Attempts++

		return Lease{Task: *task, Holder: holder, ExpiresAt: task.leaseExpiry}, nil
	}

	return Lease{}, ErrEmpty
}

// Submit completes a leased task. Only the lease holder may submit, and only
// before the lease expires — otherwise the task may already have been handed
// to somebody else, and accepting the result would silently overwrite theirs.
func (q *Queue) Submit(id, holder string, result map[string]any) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[id]
	if !ok {
		return ErrNotFound
	}
	if task.State != StateLeased || task.leaseHolder != holder {
		return ErrNotLeaseHolder
	}
	if q.now().After(task.leaseExpiry) {
		q.expireLocked(task)
		return ErrLeaseExpired
	}

	task.State = StateComplete
	task.result = result
	task.leaseHolder = ""
	return nil
}

// Extend pushes a lease further out, for a worker still making progress.
func (q *Queue) Extend(id, holder string) (time.Time, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[id]
	if !ok {
		return time.Time{}, ErrNotFound
	}
	if task.State != StateLeased || task.leaseHolder != holder {
		return time.Time{}, ErrNotLeaseHolder
	}
	if q.now().After(task.leaseExpiry) {
		q.expireLocked(task)
		return time.Time{}, ErrLeaseExpired
	}

	task.leaseExpiry = q.now().Add(q.leaseTTL)
	return task.leaseExpiry, nil
}

// Release returns a leased task to the pool without completing it.
func (q *Queue) Release(id, holder string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[id]
	if !ok {
		return ErrNotFound
	}
	if task.State != StateLeased || task.leaseHolder != holder {
		return ErrNotLeaseHolder
	}

	q.requeueLocked(task)
	return nil
}

// Result returns a completed task's submitted result.
func (q *Queue) Result(id string) (map[string]any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	task, ok := q.tasks[id]
	if !ok || task.State != StateComplete {
		return nil, false
	}
	return task.result, true
}

// Stats reports queue depth, reaping expired leases first so the numbers
// reflect reality rather than stale state.
func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.reapLocked()

	var s Stats
	s.Expired = q.expiredTotal
	for _, task := range q.tasks {
		switch task.State {
		case StatePending:
			s.Pending++
		case StateLeased:
			s.Leased++
		case StateComplete:
			s.Complete++
		}
	}
	return s
}

// reapLocked returns every expired lease to the pending pool.
func (q *Queue) reapLocked() {
	now := q.now()
	for _, task := range q.tasks {
		if task.State == StateLeased && now.After(task.leaseExpiry) {
			q.expireLocked(task)
		}
	}
}

func (q *Queue) expireLocked(task *Task) {
	q.expiredTotal++
	if q.maxAttempts > 0 && task.Attempts >= q.maxAttempts {
		// Exhausted: drop it rather than cycling forever. A task that has
		// defeated N annotators is a data problem, not a scheduling one.
		delete(q.tasks, task.ID)
		q.removeFromOrderLocked(task.ID)
		return
	}
	q.requeueLocked(task)
}

func (q *Queue) requeueLocked(task *Task) {
	task.State = StatePending
	task.leaseHolder = ""
	task.leaseExpiry = time.Time{}

	for _, id := range q.order {
		if id == task.ID {
			return
		}
	}
	q.order = append(q.order, task.ID)
	q.sortPending()
}

func (q *Queue) removeFromOrderLocked(id string) {
	for i, existing := range q.order {
		if existing == id {
			q.order = append(q.order[:i], q.order[i+1:]...)
			return
		}
	}
}

// sortPending orders by priority descending, preserving insertion order among
// equal priorities so the queue stays fair (insertion sort: order is nearly
// sorted already, and stability is what keeps FIFO within a priority).
func (q *Queue) sortPending() {
	for i := 1; i < len(q.order); i++ {
		for j := i; j > 0; j-- {
			prev, cur := q.tasks[q.order[j-1]], q.tasks[q.order[j]]
			if prev == nil || cur == nil || prev.Priority >= cur.Priority {
				break
			}
			q.order[j-1], q.order[j] = q.order[j], q.order[j-1]
		}
	}
}
