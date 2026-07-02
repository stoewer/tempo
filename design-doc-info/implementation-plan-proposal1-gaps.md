# Implementation plan: PR 1b — Proposal 1 producer gaps

| |                                                                                                                                       |
| :--- |:--------------------------------------------------------------------------------------------------------------------------------------|
| **Issue** | grafana/tempo-squad#1274                                                                                                              |
| **Epic** | grafana/tempo-squad#1263 (Project Sleipnir: Observability Improvements)                                                               |
| **Author** | Adrian Stoewer                                                                                                                        |
| **Created** | 2026-07-01                                                                                                                            |
| **Status** | Draft                                                                                                                                 |
| **Source design doc** | `design-doc-info/Design Doc: Observability for Tempo Read-Path Performance.md` (Proposal 1 producer wiring, undecomposed gap)         |

Closes the two Proposal 1 producer gaps left over after #7504:
per-role `FetchSpansStats` population and populator helpers for
`TraceByIDMetrics` and `MetadataMetrics` (plus the call sites that
invoke them).

## Scope

- **Gap 1** — per-fetch cache/backend counter surfaced on
  `FetchSpansStats` (per-role maps: `CacheHitsByRole`,
  `CacheMissesByRole`, `CacheBytesByRole`, `BackendReadsByRole`,
  `BackendBytesByRole`).
- **Gap 2a** — populator helpers for `TraceByIDMetrics` and
  `MetadataMetrics`, analogous to the existing
  `populateMetricsFromFetchStats` for `SearchMetrics`.
- **Gap 2b** — call sites: block-layer `FindTraceByID` populates its
  own `TraceByIDMetrics`; querier/livestore tag-search paths populate
  `MetadataMetrics`.

Companion PRs:
- `implementation-plan-2D.md` (PR 1a) — Proposal 2.D backend transport
  role label + size histogram.
- `implementation-plan-2EF.md` (PR 2) — iterator counters + bloom
  hit/miss.

## Relationship to other PRs

- **Fully independent** of PR 1a (2.D). Different context key
  (`fetchCountersCtxKey` vs `roleCtxKey`), different lifetime (per-fetch
  vs per-Read), different producer/consumer.
- **Fully independent** of PR 2. Both extend `FetchSpansStats`;
  mechanical rebase on the struct if both are in flight.
- Touches `tempodb/backend/cache/cache.go` in the same functions as PR
  1a (`Read`/`ReadRange`/`Write`) but at different sites (counter bump
  vs role wrap). Textual conflicts unlikely.

## Coordination with colleague's 2.A-C PR

Gap 2b touches the same querier/livestore functions the colleague edits
for span coverage: `Querier.SearchTags*`, `LiveStore.SearchTags*`, and
the corresponding `instance.*` methods (V2 variants included). Their
edits add `tracer.Start` at the top of each function; ours add
`populate...(metrics, ...)` near the bottom. Textual conflicts unlikely;
mechanical resolution if they happen. No semantic dependency. One
Slack message before both PRs go into review.

Trace-by-id is fully contained in the block layer for this PR — no
querier/livestore edits for that path. See Phase 1 step 4.

## Pre-conditions from #7504

- `traceql.FetchSpansStats` exists in `pkg/traceql/storage.go` with
  per-role maps as fields. **Maps are empty today** — this PR fills
  them.
- `populateMetricsFromFetchStats` writes row-group / page counts into
  `SearchMetrics.AdditionalMetrics` via `addIfNonZero`, and sums the
  per-role maps into scalar `BackendReads` / `BackendBytes` on
  `SearchMetrics`. Once this PR fills the maps, those scalars become
  non-zero automatically.
- `TraceByIDMetrics` and `MetadataMetrics` carry `BackendReads`,
  `BackendBytes`, and `AdditionalMetrics` fields. **No populator
  exists** — this PR adds them.
- Combiners in `modules/frontend/combiner/response_metrics.go` already
  sum the new fields on all three metrics structs (added in #7504). No
  combiner changes needed.
- Cell-wide Prometheus counters `tempodb_cache_requests_total{role,outcome}`
  and `tempodb_cache_request_bytes_total{role,outcome}` are wired at the
  cache layer. This PR does not modify them.
- TODO markers `TODO(issue/1274): populate per-iterator and per-role
  counters` sit in the vp4/vp5 fetch paths (`block_traceql.go`,
  `block_traceql_fetch.go`, `wal_block.go`). This PR resolves the
  **per-role** half.

## Phase split

Two implementation phases plus verification.

| Phase | Scope | Risk |
|---|---|---|
| 1 | Gap 1 — per-fetch counter propagation via context; cache-layer wiring; block-layer wiring for search, tag search, and trace-by-id paths | New context object; touches every fetch entry point in vp4/vp5. |
| 2 | Gap 2a + Gap 2b — populator helpers + call sites in querier/livestore tag-search paths | Colleague conflict surface. |
| 3 | Distributed docker-compose verification | n/a |

---

## Phase 1 — Gap 1: per-fetch cache/backend counter propagation

### Files

- `tempodb/backend/fetch_counters.go` (new) — context-carried counter
  struct + context helpers.
- `tempodb/backend/cache/cache.go` — bump counters on
  hit/miss/backend-read (adjacent to the existing Prometheus counter
  increments).
- `pkg/traceql/storage.go` — `FetchSpansStats` per-role maps populated
  from the context counters at `Stats()` time.
- Fetch callers:
  - `tempodb/encoding/vparquet5/block_traceql_fetch.go` — `FetchSpans`
  - `tempodb/encoding/vparquet5/block_traceql.go` — `Fetch`
  - `tempodb/encoding/vparquet5/block_autocomplete.go` —
    `FetchTagNames` and `FetchTagValues`
  - `tempodb/encoding/vparquet5/wal_block.go` — walblock equivalents
  - `tempodb/encoding/vparquet5/block_findtracebyid.go` —
    `FindTraceByID` populates `TraceByIDMetrics` directly (see step 4).
  - Mirror in `tempodb/encoding/vparquet4/*` (backport).
- Benchmark harness updates (see step 7):
  - `tempodb/encoding/vparquet5/block_traceql_test.go` —
    `blockForBenchmarks` + `BenchmarkBackendBlockTraceQL` /
    `BenchmarkBackendBlockQueryRange` metric reporting.
  - Mirror in `tempodb/encoding/vparquet{3,4}/block_traceql_test.go`.

### Change shape

1. **Counter struct on context.** In
   `tempodb/backend/fetch_counters.go`:
   ```go
   package backend

   import "github.com/grafana/tempo/pkg/cache"

   // FetchCounters accumulates per-role cache/backend work across a
   // single fetch or trace-by-id call. Not safe for concurrent use —
   // one instance per fetch, carried on context.
   type FetchCounters struct {
       CacheHits    map[cache.Role]uint64
       CacheMisses  map[cache.Role]uint64
       CacheBytes   map[cache.Role]uint64
       BackendReads map[cache.Role]uint64
       BackendBytes map[cache.Role]uint64
   }

   type fetchCountersCtxKey struct{}

   // ContextWithFetchCounters attaches counters to ctx.
   // FetchCountersFromContext returns them, or nil when none attached.
   // Both exported: the writer lives in fetch-caller packages
   // (vparquet4/5), the reader lives in tempodb/backend/cache — both
   // outside tempodb/backend and can't reach unexported symbols.
   func ContextWithFetchCounters(ctx context.Context, c *FetchCounters) context.Context { ... }
   func FetchCountersFromContext(ctx context.Context) *FetchCounters { ... }
   ```
   Add `(*FetchCounters).AddHit(role, bytes)`, `.AddMiss(role, bytes)`,
   `.AddBackendRead(role, bytes)` methods. Lazy-init the maps in the
   methods.

2. **Cache layer bumps counters.** In `cache.go` `Read`/`ReadRange`,
   split the bumps into two loci so backend reads without a cache still
   count.

   **Cache-related counters — inside `if cache != nil`.** Match the
   existing Prometheus cache-metric semantics (only fire when cache is
   configured for this role). Sit next to the existing
   `cacheRequests`/`cacheRequestBytes` increments:
   ```go
   if cache != nil {
       // ... existing cache.FetchKey ...
       if found {
           cacheRequests.WithLabelValues(role, cacheOutcomeHit).Inc()
           cacheRequestBytes.WithLabelValues(role, cacheOutcomeHit).Add(float64(len(b)))
           if fc := backend.FetchCountersFromContext(ctx); fc != nil {
               fc.AddHit(cacheInfo.Role, uint64(len(b)))
           }
           return ...
       }
       cacheRequests.WithLabelValues(role, cacheOutcomeMiss).Inc()
       if fc := backend.FetchCountersFromContext(ctx); fc != nil {
           fc.AddMiss(cacheInfo.Role)  // byte count comes with the eventual backend read
       }
   }
   ```
   `CacheBytes` (miss-path bytes) is bumped alongside the existing
   `cacheRequestBytes(miss)` increment, which fires only after the
   backend read succeeds and only when `cache != nil`. Same semantics
   as today.

   **Backend-read counters — unconditional on the fall-through path.**
   Every `nextReader.Read` that succeeds counts, regardless of whether
   a cache was configured or whether the caller passed `cacheInfo`:
   ```go
   // ... after nextReader.Read succeeds and ReadAllWithEstimate returns ...
   if fc := backend.FetchCountersFromContext(ctx); fc != nil {
       var r cache.Role
       if cacheInfo != nil {
           r = cacheInfo.Role
       }
       fc.AddBackendRead(r, uint64(len(b)))
   }
   ```
   Role is `cacheInfo.Role` when known, else empty. The empty-role
   bucket surfaces the pre-existing "reevaluate. should we pass the
   cacheInfo forward?" gap (`cache.go:129,165`) without hiding those
   reads. Rationale: `BackendReads` and `BackendBytes` should reflect
   *actual* backend work — a local-backend dev deployment without any
   cache configured still gets meaningful metrics; a bloom-suppressed
   read at high compaction level still shows up.

   This makes the scalar `SearchMetrics.BackendReads` /
   `TraceByIDMetrics.BackendReads` / `MetadataMetrics.BackendReads`
   (summed from the per-role map by the populator helpers) reflect
   *every* backend read on the query's path, not just the
   cache-mediated ones.

3. **Fetch callers attach counters and read them back.** Pattern for
   `FetchSpans` / `Fetch` / `FetchTagNames` / `FetchTagValues` in
   vparquet4/5 (and walblock equivalents):
   ```go
   counters := &backend.FetchCounters{}
   ctx = backend.ContextWithFetchCounters(ctx, counters)
   // ... existing body, which calls b.r.Read(ctx, ...) ...

   return traceql.FetchSpansOnlyResponse{
       Results: iter,
       Stats: func() traceql.FetchSpansStats {
           return traceql.FetchSpansStats{
               Bytes:              rr.BytesRead(),
               CacheHitsByRole:    counters.CacheHits,
               CacheMissesByRole:  counters.CacheMisses,
               CacheBytesByRole:   counters.CacheBytes,
               BackendReadsByRole: counters.BackendReads,
               BackendBytesByRole: counters.BackendBytes,
           }
       },
   }, nil
   ```
   For `FetchTagNames` / `FetchTagValues`, the block methods still
   return only `error` — the counters flow through Phase 2's call sites
   (querier/livestore holds the pointer and populates
   `MetadataMetrics`). See Phase 2.

   The existing `TODO(issue/1274): populate per-iterator and per-role
   counters` markers combine both halves into one line. This PR
   **rewrites** each marker to drop the per-role half:
   `TODO(issue/1274): populate per-iterator counters`. PR 2 then removes
   the marker entirely.

4. **Trace-by-ID path — populate inside the block layer.**
   `backendBlock.FindTraceByID` in `block_findtracebyid.go` already
   owns its response and constructs `TraceByIDMetrics{}` locally. Attach
   a fresh `*FetchCounters` to ctx at the top of `FindTraceByID`, let
   cache/backend layer bump it via ctx, then populate the response's
   `TraceByIDMetrics` from the counters before return. No cross-layer
   context extraction; the querier receives an already-populated struct.
   ```go
   func (b *backendBlock) FindTraceByID(ctx context.Context, id common.ID) (*tempopb.TraceByIDResponse, error) {
       counters := &backend.FetchCounters{}
       ctx = backend.ContextWithFetchCounters(ctx, counters)
       // ... existing body (checkBloom + checkIndex + findTraceByID) ...
       response := &tempopb.TraceByIDResponse{Trace: t, Metrics: &tempopb.TraceByIDMetrics{}}
       populateTraceByIDMetricsFromFetchCounters(response.Metrics, counters)
       return response, nil
   }
   ```
   The `populateTraceByIDMetricsFromFetchCounters` helper is defined in
   Phase 2.

5. **Tag-search path — caller holds the pointer.** `FetchTagNames` /
   `FetchTagValues` return only `error` and use a
   `MetricsCallback func(bytesRead uint64)` on the request. Rather than
   widen the callback signature (breaking change across common
   interface + vp3/4/5 + LocalBlock + all callers), extend the same
   context-carried mechanism to tag search. The caller (querier /
   livestore in Phase 2) attaches counters to ctx, calls
   `FetchTagNames` / `FetchTagValues`, and reads counters back from its
   local pointer to populate `MetadataMetrics`.
   - No changes to `MetricsCallback` signature or to
     `common.Reader.FetchTagNames`/`FetchTagValues` signatures.
   - Consistent mental model with search + trace-by-id: caller holds
     the `*FetchCounters` pointer, attaches to ctx (so cache/backend
     can find it), populates its metrics struct from the local pointer.

6. **Tests.** Extend the existing vparquet5 fetch tests to cover:
   - **With cache configured:** assert `Stats().CacheHitsByRole` and
     friends are non-zero after a fetch that exercises cache reads. Use
     a real cache implementation (in-memory cache), not a mock. One
     test per fetch entry point. For trace-by-id, assert the returned
     `TraceByIDMetrics` carries non-zero per-role counts.
   - **Without any cache configured:** verify the split-locus behavior.
     `Cache*ByRole` maps stay empty. `BackendReadsByRole` /
     `BackendBytesByRole` are non-zero — bucketed by role when
     `cacheInfo.Role` is set, or under empty role when
     `cacheInfo == nil`. This is the local-backend / no-cache
     deployment case; without a specific test, that path silently
     regresses.

7. **Benchmark harness — wire the cache wrapper.**
   `blockForBenchmarks` (in
   `tempodb/encoding/vparquet{3,4,5}/block_traceql_test.go`) currently
   constructs the block via `backend.NewReader(local.New(...))` — the
   raw reader is passed directly, **skipping the `cache.readerWriter`
   wrapper**. Under this PR's design, that means `BackendReadsByRole` /
   `BackendBytesByRole` would stay empty in `BenchmarkBackendBlockTraceQL`
   and `BenchmarkBackendBlockQueryRange` even though the benchmark does
   perform real backend reads.

   Fix: wrap the raw reader with `cache.NewCache(nil, r, w,
   noopCacheProvider{}, log.NewNopLogger())` in `blockForBenchmarks`
   before handing it to `backend.NewReader`. `cacheFor` returns nil for
   every role (no caches configured), but the wrapper is still in the
   chain so the fall-through `BackendReads` bump fires. Matches
   production shape more closely.

   Report the new counters in the benchmark output alongside
   `MB_io/op`:
   ```go
   b.ReportMetric(float64(backendReads)/float64(b.N), "backendReads/op")
   b.ReportMetric(float64(backendBytes)/float64(b.N)/1000.0/1000.0, "MB_backend/op")
   ```
   (Pull the values off `resp.Metrics.BackendReads` / `BackendBytes`
   or `resp.Metrics.AdditionalMetrics` depending on what's easier.)

   Do the same in the vp4 and vp3 benchmark harnesses for consistency,
   even though this PR only ships wiring for vp4/vp5.

### Risks

- **Context vs. explicit param.** Passing counters via context is
  implicit; callers may forget to attach and get silent zeros. Guard by
  making `nil` counters a no-op (the cache layer's `if fc != nil` check
  handles this) and by asserting non-zero maps in the integration
  tests.
- **Concurrency.** `FetchCounters` is documented not safe for
  concurrent use — one instance per fetch, single-goroutine. If a
  fetch caller spawns goroutines that all hit the cache (unusual
  today), atomics would be needed. Not blocking; document and revisit
  if needed.
- **BackendReads accounting** fires on the fall-through path after
  `nextReader.Read` returns successfully. Failed backend reads are not
  counted — matches existing Prometheus counter semantics.
- **`role=""` in `BackendReadsByRole` / `BackendBytesByRole`.** Reads
  reaching the cache layer with `cacheInfo == nil` bucket under empty
  role. Visible in per-fetch responses (search / trace-by-id / tag
  search) as an unlabelled entry in the AdditionalMetrics scalars.
  Pre-existing "reevaluate. should we pass the cacheInfo forward?"
  comment (`cache.go:129,165`) covers the same gap; a follow-up PR
  addresses it. The plan does **not** hide these reads.
- **CacheBytes miss-bytes timing.** `CacheBytes[role]` (miss-path
  bytes) is bumped alongside the existing `cacheRequestBytes(miss)`
  increment — only when `cache != nil` and the backend read succeeded.
  Matches existing metric semantics.

---

## Phase 2 — Gap 2a + Gap 2b: populators and tag-search call sites

Trace-by-id is already fully populated by Phase 1 (block layer). Phase
2 covers only the populator helpers and the tag-search call sites.

### Files

- `pkg/traceql/storage.go` — new populator helpers for
  `TraceByIDMetrics` and `MetadataMetrics`.
- `modules/querier/querier.go` (and wherever tag-search response is
  built in the querier) — call the `MetadataMetrics` populator.
- `modules/livestore/instance_search.go` — call the populator from
  `SearchTags` / `SearchTagValues` and V2 variants.

No combiner changes: `modules/frontend/combiner/response_metrics.go`
already sums the new fields for both `TraceByIDMetrics` and
`MetadataMetrics` (added in #7504). Existing tests should continue to
pass.

### Change shape

1. **Populator helpers** in `pkg/traceql/storage.go`. Two thin
   functions mirroring the existing `populateMetricsFromFetchStats`
   pattern:
   ```go
   // populateTraceByIDMetricsFromFetchCounters writes per-role and
   // scalar counters into a TraceByIDMetrics. Used by the block-layer
   // FindTraceByID before returning.
   func populateTraceByIDMetricsFromFetchCounters(m *tempopb.TraceByIDMetrics, c *backend.FetchCounters) { ... }

   // populateMetadataMetricsFromFetchCounters writes per-role and
   // scalar counters into a MetadataMetrics. Used by the querier /
   // livestore tag-search paths.
   func populateMetadataMetricsFromFetchCounters(m *tempopb.MetadataMetrics, c *backend.FetchCounters) { ... }
   ```
   Semantics mirror `populateMetricsFromFetchStats`:
   `BackendReads`/`BackendBytes` summed from per-role maps;
   `AdditionalMetrics["cacheHits" | "cacheMisses" | "cacheBytes"]`
   populated via `addIfNonZero`.

2. **Tag-search call sites.** Both querier and livestore build
   `MetadataMetrics` on the response.
   - **Querier**: `Querier.SearchTags`, `Querier.SearchTagsV2`,
     `Querier.SearchTagValues`, `Querier.SearchTagValuesV2`. Attach
     `*FetchCounters` to ctx before iterating blocks; call the
     `FetchTagNames` / `FetchTagValues` block methods; populate
     `MetadataMetrics` on the response from the local counters pointer.
   - **Livestore**: `LiveStore.SearchTags`, `LiveStore.SearchTagValues`,
     and V2 variants (via `instance.SearchTags` / `SearchTagValues`).
     Same pattern.

3. **Tests.**
   - Unit tests for the two new populators (mirror the existing
     `populateMetricsFromFetchStats` tests).
   - Integration-style test at the querier level that exercises
     `SearchTags` and asserts the response's `Metrics` carries non-zero
     `BackendReads`, `AdditionalMetrics["cacheHits"]`, etc.
   - Confirm existing combiner tests still pass; add cases for the
     per-role paths if they aren't already covered.

### Risks

- **Colleague merge conflicts** on `querier.go` / `instance_search.go`.
  Their edits (`tracer.Start` at top) don't overlap with ours
  (`populate...(metrics, ...)` near the bottom). Mechanical resolution
  if a textual conflict happens. Communicate before both PRs go into
  review.
- **Coverage of every tag-search entry point.** Missing a call site
  means silent zero. Grep at end of Phase 2:
  `grep -rn 'MetadataMetrics{' --include='*.go' modules/`
  Every construction site in querier/livestore should be paired with a
  populate call. `TraceByIDMetrics{}` construction sites in
  querier/livestore don't need populate calls — the field is already
  filled by the block layer.

---

## Phase 3 — Verification via distributed docker-compose

Prometheus at `:9090`, Tempo at `:3202`.

### Setup

```
docker compose up -d --build
```
Wait 2–3 min for ingest + block flush + compaction.

### Gap 1 — per-role `FetchSpansStats` maps → `SearchMetrics`

```
curl -s 'http://localhost:3202/api/search?q=%7B%7D&limit=20' | jq '.metrics'
```
Expect `additionalMetrics` to include (as string-encoded int64s):
`cacheHits`, `cacheMisses`, `cacheBytes`.
Also `backendReads`, `backendBytes` at the top level (scalar sums).

### Gap 2b — `TraceByIDMetrics` (block-layer populated in Phase 1)

```
curl -s 'http://localhost:3202/api/traces/<real-id>' | jq '.metrics'
```
Expect non-zero `backendReads`, `backendBytes`, and
`additionalMetrics.cacheHits` / `cacheMisses` / `cacheBytes`.

### Gap 2b — `MetadataMetrics` (querier/livestore populated in Phase 2)

```
curl -s 'http://localhost:3202/api/search/tags' | jq '.metrics'
curl -s 'http://localhost:3202/api/search/tag/service.name/values' | jq '.metrics'
```
Same shape as trace-by-id.

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

- **Proposal 2.D** — see `implementation-plan-2D.md`.
- **Proposal 2.E, 2.F** — see `implementation-plan-2EF.md`.
- **Proposal 2.A-C** — colleague's PR.
- **Proposal 2.G** — deferred. This PR's producer work makes 2.G
  possible; the log-emission side is queued separately.

## Chloggen entry

`enhancement` / `tempodb`:
> Populate per-role cache/backend counters on `FetchSpansStats`.
> `BackendReads` / `BackendBytes` count every backend read, including
> cache-suppressed and no-cache paths (visible even on local-backend
> deployments without any cache configured). `TraceByIDMetrics` and
> `MetadataMetrics` gain populators so trace-by-id and tag-search
> responses expose the same per-role work as search responses.
