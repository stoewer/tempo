# Implementation plan: PR 1a — backend transport role label + size histogram (Proposal 2.D)

| |                                                                                                    |
| :--- |:---------------------------------------------------------------------------------------------------|
| **Issue** | grafana/tempo-squad#1274                                                                           |
| **Epic** | grafana/tempo-squad#1263 (Project Sleipnir: Observability Improvements)                            |
| **Author** | Adrian Stoewer                                                                                     |
| **Created** | 2026-07-01                                                                                         |
| **Status** | Draft                                                                                              |
| **Source design doc** | `design-doc-info/Design Doc: Observability for Tempo Read-Path Performance.md` (Proposal 2.D)      |

Small, focused PR that adds the `role` label and a size histogram to the
tempodb backend transport. Independent of every other proposal in this
work stream.

## Scope

- **Proposal 2.D** — backend transport role label + size histogram.

Companion PRs:
- `implementation-plan-proposal1-gaps.md` (PR 1b) — Proposal 1 producer
  gaps (per-role FetchSpansStats population + populators for
  TraceByIDMetrics/MetadataMetrics).
- `implementation-plan-2EF.md` (PR 2) — iterator counters + bloom
  hit/miss.

## Relationship to other PRs

- **Fully independent** of PR 1b and PR 2. Ships or blocks on its own.
- Touches `tempodb/backend/cache/cache.go` in the same function as PR 1b
  will (`Read`/`ReadRange`/`Write`), but at a different site (role
  wrap vs. counter bump). Textual conflicts unlikely; mechanical rebase
  if they happen.

## Pre-conditions

- `cache.Role` exists in `pkg/cache` and is already set on
  `backend.CacheInfo.Role` at every call site. This PR consumes those
  values.
- `instrumentation.NewTransport` wraps s3/gcs/azure HTTP transports
  (`tempodb/backend/{s3,gcs,azure}/*.go`). Local / in-memory backends
  bypass this — 2.D signals will be silent for local-backend deploys.

## Phase split

Two phases: implementation and verification. Single commit for the
implementation phase.

---

## Phase 1 — Change

### Files

- `tempodb/backend/instrumentation/backend_transports.go`
- `tempodb/backend/role_ctx.go` (new) — unexported context key +
  helpers for the role label.
- `tempodb/backend/cache/cache.go` — wrap `ctx` in
  `Read`/`ReadRange`/`Write` so the request carries the role.
- `tempodb/backend/instrumentation/backend_transports_test.go` (new).

### Change shape

1. **Role context key.** In a new `tempodb/backend/role_ctx.go`:
   ```go
   package backend

   type roleCtxKey struct{}

   // ContextWithRole and RoleFromContext attach a cache.Role to ctx so
   // downstream HTTP transports can label backend metrics by role.
   // Exported because the writer lives in tempodb/backend/cache and the
   // reader in tempodb/backend/instrumentation — both sub-packages,
   // which can't reach unexported symbols in tempodb/backend.
   //
   // RoleFromContext returns the empty cache.Role when no role is
   // attached; callers observe the metric under an empty `role=""`
   // label without special handling.
   func ContextWithRole(ctx context.Context, role cache.Role) context.Context { ... }
   func RoleFromContext(ctx context.Context) cache.Role { ... }
   ```

2. **Cache layer sets the role.** In `cache.go` `Read`/`ReadRange`/`Write`,
   wrap `ctx` with `backend.ContextWithRole(ctx, cacheInfo.Role)`
   whenever `cacheInfo != nil`. Do this **before** the cache-lookup
   branch — cache-bypassed reads (where the cache is nil for the given
   role, e.g. bloom filters at high compaction levels) still carry the
   role label into the transport metrics. Reads that reach the cache
   layer with `cacheInfo == nil` remain unlabelled and emit `role=""`
   (documented as a bucket).

3. **Histogram + label + size capture (reads and writes).**
   ```go
   var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
       Namespace:                       "tempodb",
       Name:                            "backend_request_duration_seconds",
       Help:                            "Time spent doing backend storage requests.",
       Buckets:                         prometheus.ExponentialBuckets(0.005, 4, 6),
       NativeHistogramBucketFactor:     1.1,
       NativeHistogramMaxBucketNumber:  100,
       NativeHistogramMinResetDuration: 1 * time.Hour,
   }, []string{"operation", "role", "status_code"})

   var requestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
       Namespace: "tempodb",
       Name:      "backend_request_size_bytes",
       Help:      "Size of backend storage requests by role.",
       Buckets:   prometheus.ExponentialBuckets(64, 2, 24),
   }, []string{"operation", "role", "status_code"})
   ```
   In `RoundTrip`:
   - Read `role := string(backend.RoleFromContext(req.Context()))`.
   - Reads (GET): prefer `resp.ContentLength` when ≥ 0; skip when < 0.
   - Writes (PUT/POST): wrap `req.Body` with a counting reader so
     chunked uploads (`ContentLength == -1`) still produce a
     measurement:
     ```go
     type countingReadCloser struct {
         io.ReadCloser
         n int64
     }
     func (c *countingReadCloser) Read(p []byte) (int, error) {
         n, err := c.ReadCloser.Read(p)
         c.n += int64(n)
         return n, err
     }
     ```
   - Retry caveat: Go's `http.Transport` retries via `req.GetBody()`,
     so each attempt enters `RoundTrip` afresh and the wrapper resets.
     SDK-layer retries produce multiple `RoundTrip` invocations, each a
     separate observation. Document in a comment on the wrapper.

4. **Tests.** Transport-level test wrapping a fake `RoundTripper`:
   - Duration observed for GET + PUT with the right role label.
   - Size observed for GET-with-known-length, known-length PUT, and
     chunked PUT (via counting reader).
   - Retry: a PUT retried once observes twice.

### Risks

- **Local / in-memory backends** don't go through
  `instrumentation.NewTransport`. Size histogram will be empty for
  local-backend deploys (dev, CI). Acceptable; production uses cloud
  backends. Document in PR description.
- **`role=""` bucket.** Reads reaching the cache layer with
  `cacheInfo == nil` (paths that skip caching entirely, and the
  `nextReader.Read` calls where cache.go passes `nil` forward) emit
  transport metrics under `role=""`. A pre-existing "reevaluate. should
  we pass the cacheInfo forward?" comment in `cache.go:129,165` covers
  the same gap; left for a follow-up PR.
- **Alert rules pinned to `{operation, status_code}` exact-match** may
  need to switch to `sum by(...)`. Call out in chloggen.
- **`operation` label is misleading** (it's the HTTP method). Design
  doc explicitly defers the rename.
- **SDK-layer retries** produce over-count on `_sum`. Documented in a
  code comment on `countingReadCloser`.

---

## Phase 2 — Verification via distributed docker-compose

Prometheus at `:9090` scrapes querier/live-store. Tempo HTTP API at
`:3202` (query-frontend).

### Setup

```
docker compose up -d --build
```
Wait for k6-tracing to produce ingest and for a block flush +
compaction (2–3 min).

### Exercise 2.D signals

```
curl -s 'http://localhost:3202/api/search?q=%7B%7D&limit=20'

curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum by (role) (tempodb_backend_request_duration_seconds_count)'
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum by (role, operation) (tempodb_backend_request_size_bytes_count)'
```
Expected shape:
- Read roles (`parquet-footer`, `parquet-page`, `parquet-column-idx`,
  `parquet-offset-idx`, `bloom`, `trace-id-idx`) under `operation="GET"`.
- Write roles under `operation="PUT"` (and possibly `POST`).
- `role=""` rows indicate un-roled paths; not an error, but note the
  ratio.

### Log scan

```
docker compose logs querier query-frontend backend-worker-0 \
    live-store-zone-a-0 live-store-zone-a-1 \
    live-store-zone-b-0 live-store-zone-b-1 \
    | grep -E 'level=(error|panic)'
```
Only #1273 baseline noise acceptable.

### Tear down

```
docker compose down -v
```

---

## Out of scope

- **Proposal 1 producer gaps** (per-role FetchSpansStats population +
  `TraceByIDMetrics` / `MetadataMetrics` populators). See
  `implementation-plan-proposal1-gaps.md`.
- **Proposal 2.E, 2.F** — see `implementation-plan-2EF.md`.
- **Proposal 2.A-C** — colleague's PR.
- **Proposal 2.G** — deferred.
- **`operation` label rename** on
  `tempodb_backend_request_duration_seconds` — design doc explicitly
  defers.
- **Local / in-memory backend instrumentation** — documented gap.

## Chloggen entry

`enhancement` / `tempodb`:
> Add `role` label to `tempodb_backend_request_duration_seconds` and a
> new `tempodb_backend_request_size_bytes` histogram for per-role
> backend transport observability.
