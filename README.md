# annotation-gateway

[![CI](https://github.com/njoroge-viktor/annotation-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/njoroge-viktor/annotation-gateway/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.22%20%7C%201.23-00ADD8)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A task-distribution gateway for annotation workforces. **Lease-based** work
queue, per-annotator rate limiting, structured logging and Prometheus metrics.

Standard library only — no third-party dependencies, which means a ~10 MB
static binary, a `scratch` container, and nothing to audit but this repo.

## The problem it solves

An annotator claims a task and then closes the tab. Without leases that task is
lost forever, and at scale a meaningful slice of the queue quietly evaporates.

Here a claim grants a **time-limited lease**. A lease that expires returns its
task to the pool automatically, with the attempt count incremented:

```
POST /v1/tasks/claim          -> lease on task-1, expires in 5m, attempts=1
   (annotator disappears)
POST /v1/tasks/claim          -> same task, new holder,        attempts=2
```

Three consequences worth stating, because each is a decision rather than an
accident:

- **Only the lease holder may submit**, and only before expiry. Accepting a
  late submission would silently overwrite work the queue already handed to
  somebody else.
- **A long task can `extend`** its lease rather than losing it mid-review.
- **A task that defeats `-max-attempts` annotators is dropped**, not recycled
  forever. A task nobody can complete is a data problem, and cycling it
  indefinitely just hides it.

## Running

```bash
go run ./cmd/gateway -addr :8080 -lease-ttl 5m -max-attempts 3

# or
docker build -t annotation-gateway . && docker run -p 8080:8080 annotation-gateway
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `-addr` | `:8080` | Listen address |
| `-lease-ttl` | `5m` | How long a claimed task stays leased |
| `-max-attempts` | `3` | Re-lease attempts before a task is dropped (0 = unlimited) |
| `-rate-burst` | `20` | Burst allowance per annotator |
| `-rate-per-second` | `5` | Refill rate per annotator |
| `-shutdown-timeout` | `15s` | Grace period for in-flight requests |
| `-log-level` | `info` | `debug`, `info`, `warn` or `error` |

## API

Every `/v1/` route requires an `X-Annotator-ID` header.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/tasks` | Enqueue a task |
| `POST` | `/v1/tasks/claim` | Lease the highest-priority pending task |
| `POST` | `/v1/tasks/{id}/submit` | Complete a leased task |
| `POST` | `/v1/tasks/{id}/extend` | Renew a lease still in progress |
| `POST` | `/v1/tasks/{id}/release` | Hand a task back without completing it |
| `GET` | `/v1/tasks/{id}/result` | Fetch a completed result |
| `GET` | `/healthz` | Liveness |
| `GET` | `/stats` | Queue depth as JSON |
| `GET` | `/metrics` | Prometheus exposition format |

```bash
curl -X POST localhost:8080/v1/tasks -H 'X-Annotator-ID: admin' \
     -d '{"id":"task-1","payload":{"text":"Is this code safe?"},"priority":9}'

curl -X POST localhost:8080/v1/tasks/claim -H 'X-Annotator-ID: ann-a'
# {"task":{"id":"task-1",...,"attempts":1},"holder":"ann-a","expires_at":"..."}

curl -X POST localhost:8080/v1/tasks/task-1/submit -H 'X-Annotator-ID: ann-a' \
     -d '{"result":{"label":"unsafe","severity":3}}'
```

Errors share one shape, so clients need one code path:

```json
{ "code": "lease_expired", "message": "queue: lease has expired" }
```

| Status | Code | Cause |
| --- | --- | --- |
| `401` | `unauthenticated` | Missing `X-Annotator-ID` |
| `403` | `not_lease_holder` | Submitting a task you do not hold |
| `409` | `lease_expired` / `already_exists` | Lease lapsed, or duplicate task id |
| `413` | `payload_too_large` | Body over 1 MiB |
| `429` | `rate_limited` | Bucket exhausted; carries `Retry-After` |

## Rate limiting

A **token bucket** per annotator, not a fixed window. Fixed windows let a
client send twice the limit across a window boundary — 10 requests at the end
of one window and 10 at the start of the next. A bucket refills continuously,
so the limit holds over *every* interval:

```go
// After exhausting a burst of 10, only one token has refilled 100ms later.
allowed := 0
for i := 0; i < 10; i++ { if limiter.Allow("ann-a") { allowed++ } }
// allowed == 1
```

Refill is **lazy** — nothing ticks in the background — so every reader applies
elapsed time rather than trusting the stored count. Buckets are evicted once
idle *and* refilled to capacity; an unbounded map keyed by client id is
otherwise a memory leak. A still-throttled bucket is never evicted, because
going quiet must not reset a limit.

## Middleware

```go
httpx.Chain(
    httpx.RequestID(),   // first, so every later log line carries one
    httpx.Recover(log),  // wraps everything downstream
    httpx.Logging(...),
)(mux)
```

Ordering is deliberate. Auth runs *before* the rate limiter so throttling is
per annotator rather than per connection — otherwise one annotator behind a
shared NAT could throttle a whole office.

`X-Request-ID` is honoured when inbound and generated when not, so a trace id
survives across service hops.

> The `Auth` middleware reads an `X-Annotator-ID` header and is a **placeholder
> for a real authenticator**. It is a seam to plug JWT or mTLS into, not an
> authentication scheme. Saying so is better than implying otherwise.

## Testing

```bash
make race    # every test under the race detector
make cover   # cross-package coverage
```

95.6% statement coverage across `internal/`, all of it under `-race`.

Time is injected rather than slept on, so lease-expiry and token-refill tests
are deterministic and finish instantly:

```go
c := newClock()
q := queue.New(time.Minute, queue.WithClock(c.Now))
q.Claim("ann-a")
c.Advance(2 * time.Minute)   // no sleep
```

The test that matters most is `TestConcurrentClaimsAreExclusive`: sixteen
goroutines drain a 200-task queue and assert no task was ever handed to two
annotators.

## License

MIT — see [LICENSE](LICENSE).
