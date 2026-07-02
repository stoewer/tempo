# Implementation Plan: #1273 Read-path observability: baseline signals

| |                                                                                             |
| :--- |:--------------------------------------------------------------------------------------------|
| **Issue** | grafana/tempo-squad#1273                                                                    |
| **Epic** | grafana/tempo-squad#1263 (Project Sleipnir: Observability Improvements)                     |
| **Author** | Adrian Stoewer                                                                              |
| **Created** | 2026-06-10                                                                                  |
| **Status** | Completed (PR https://github.com/grafana/tempo/pull/7504)                                   |
| **Source design doc** | `design-doc-info/Design Doc: Observability for Tempo Read-Path Performance.md` (Proposal 1) |

## 1. Scope and overview

Issue #1273 covers Proposal 1 (A–E) of the read-path observability design doc.

* **Task A.** Widen `traceql.FetchSpansResponse` / `FetchSpansOnlyResponse` from a `Bytes func() uint64` callback to a typed `Stats func() FetchSpansStats` callback.
* **Task B.** Extend `tempopb.SearchMetrics`, `tempopb.TraceByIDMetrics`, and `tempopb.MetadataMetrics` proto messages with `backendReads`, `backendBytes`, and a generic `additionalMetrics` map. Extend the four `*MetricsCombiner` aggregation paths and the MCP marshaler.
* **Task C.** Add per-role cache hit/miss/byte counters at `tempodb/backend/cache/cache.go`.
* **Task D.** Surface query shape on the six per-query response log lines and on the sharder spans, using values already computed by the async-weight middleware.
* **Task E.** Add a new OTel span `parquet.backendBlock.Fetch` at `vparquet5/block_traceql.go:1106` (and counterparts on `FetchSpans`, autocomplete, and WAL blocks).

### Out of scope for this issue

* Incrementing the per-iterator counters (`RowGroupsInspected/Skipped`, `PagesInspected/Skipped`, `ValuesMatched`, dictionary short-circuits) inside `pkg/parquetquery/iters.go`. Those land in issue #1274 (Proposal 2E). For #1273 the `FetchSpansStats` callback exists with the full shape, but vparquet5 returns zeroed counters and only `Bytes` is populated. A TODO comment marks the deferred wiring in each `Stats:` lambda.
* Per-role cache hit/miss accounting inside `BackendReaderAt.ReadAtWithCache` (`vparquet5/readers.go:41`). Also deferred to #1274; the `cache.Role`-keyed maps in `FetchSpansStats` exist but return `nil` for now.
* Backend transport `role` label on `tempodb_backend_request_duration_seconds`, request-size histogram, and the `cache_store_error_bytes_total` counter from PR #7137. Those are #1274 (Proposal 2D) territory. #1273 does **not** introduce `cache_store_error_bytes_total` even as a placeholder; if PR #7137 lands before #1273, commit C rebases onto it but does not redefine the variable.

## 2. Dependency graph

```
                      ┌──────────────────────────┐
                      │ A. FetchSpansResponse     │
                      │    widening               │
                      └──────────┬────────────────┘
                                 │ provides Stats() shape
              ┌──────────────────┼──────────────────┐
              ▼                                     ▼
   ┌────────────────────┐                ┌─────────────────────┐
   │ B. Proto extension │                │ E. block.Fetch span │
   │  + combiners       │                │  + autocomplete +   │
   │  + producer wiring │                │  WAL counterparts   │
   └────────────────────┘                └─────────────────────┘

   ┌─────────────────────────────────┐   ┌──────────────────────────┐
   │ C. Per-role cache hit/miss      │   │ D. Query shape on logs   │
   │    counters (cache.go)          │   │    and spans             │
   └─────────────────────────────────┘   └──────────────────────────┘
   (independent of A/B/E; can land first)  (independent; can land first)
```

Tasks C and D are independent of everything else. Task A is a prerequisite for B and E.

## 3. Commit strategy

Ship as a single PR with five commits, one per task, in the order **C → D → A → B → E**. Each commit must compile and pass the full test suite individually so `git bisect` works cleanly across the whole range.

| # | Commit | Files touched |
| :--- | :--- | :--- |
| 1 | **C** — per-role cache hit/miss counters | `tempodb/backend/cache/cache.go` + `tempodb/backend/cache/cache_test.go` |
| 2 | **D** — query shape on logs and sharder spans | `modules/frontend/pipeline/{async_weight_middleware.go, pipeline.go}` + `modules/frontend/util.go` + 4 sharders + 6 handlers + `async_weight_middleware_test.go` |
| 3 | **A** — widen `FetchSpansResponse` to a typed `Stats()` callback | `pkg/traceql/{storage.go, engine.go, engine_metrics.go, engine_test.go}` + all 13 construction sites across `tempodb/encoding/{unsupported, vparquet3, vparquet4, vparquet5}/`, `tempodb/tempodb.go`, `modules/frontend/search_sharder_test.go` |
| 4 | **B** — extend `SearchMetrics`/`TraceByIDMetrics`/`MetadataMetrics`, combiners, producer wiring | `pkg/tempopb/{tempo.proto, tempo.pb.go, additional_metrics_keys.go}` + `modules/frontend/combiner/{response_metrics.go, llm_marshaler.go}` + producer wiring in `pkg/traceql/{engine.go, engine_metrics.go}` + 2 production `eval.Metrics()` callers (`modules/querier/querier_query_range.go`, `modules/livestore/instance_search.go`) + 5 test callers + combiner tests |
| 5 | **E** — `parquet.backendBlock.Fetch` span and counterparts | `tempodb/encoding/vparquet5/{block_traceql.go, block_traceql_fetch.go, block_autocomplete.go, wal_block.go}` |

**Ordering rationale.** Two hard dependencies force the (A, B, E) cluster. Commit B's producer wiring (§5.5) reads from `Stats()`, and commit E's end-attributes set fields from the same callback. Both must come after A. C and D have no dependencies on any other task; placing them first keeps the simpler changes at the top of the review and gets the easy wins out of the way.

**Atomic-per-commit guarantee.** Each commit lands every file the task touches; no commit leaves the tree in a partial state. After each of the five commits, `go build ./...` succeeds and `go test ./...` is green.

**Value delivery.** Commits C and D deliver immediate user-visible value: C unblocks cache evaluation work (memcached tuning, Redis migration); D unblocks production query classification in Loki. A is internal plumbing with no user-visible effect alone. B and E together complete the data flow from the fetch layer to the response and to traces.

## 4. Task A — widen `FetchSpansResponse`

### 4.1 Interface change

File `pkg/traceql/storage.go`. Add a new type before line 252 and modify the two existing response structs.

```go
// FetchSpansStats carries cumulative read-side counters reported by a storage
// Fetch / FetchSpans call.
type FetchSpansStats struct {
    Bytes              uint64
    RowGroupsInspected uint32
    RowGroupsSkipped   uint32
    PagesInspected     uint32
    PagesSkipped       uint32
    ValuesMatched      uint64
    CacheHitsByRole    map[cache.Role]uint64
    CacheMissesByRole  map[cache.Role]uint64
    CacheBytesByRole   map[cache.Role]uint64
    BackendReadsByRole map[cache.Role]uint64
    BackendBytesByRole map[cache.Role]uint64
}

type FetchSpansResponse struct {
    Results SpansetIterator
    Stats   func() FetchSpansStats // replaces the existing Bytes func() uint64
}

type FetchSpansOnlyResponse struct {
    Results SpanIterator
    Stats   func() FetchSpansStats // replaces the existing Bytes func() uint64
}
```

`storage.go` gains an import of `github.com/grafana/tempo/pkg/cache`. The `SpansetFetcher` interface (lines 268–271) is unchanged — only the response shape changes.

### 4.2 Construction sites

Walk every site that builds a `FetchSpansResponse{}` or `FetchSpansOnlyResponse{}` and swap `Bytes:` for `Stats:`. For vparquet3 and vparquet4 the lambda returns a `FetchSpansStats{Bytes: rr.BytesRead()}` with all other counters zero. For vparquet5 the same applies for this issue, with a TODO comment marking the counters that #1274 will plumb.

| File | Site | Change |
| :--- | :--- | :--- |
| `tempodb/encoding/unsupported/block.go:44, :48` | Empty `ErrUnsupported` returns | No change (empty struct still compiles) |
| `tempodb/encoding/vparquet3/block.go:45` | `FetchSpans → ErrUnsupported` | No change |
| `tempodb/encoding/vparquet3/block_traceql.go:904-907` | `(*backendBlock).Fetch` | `Bytes: → Stats:` (bytes-only) |
| `tempodb/encoding/vparquet3/wal_block.go:693-705` | `walBlock.Fetch` (multiple readers) | `Stats: func() { sum r.BytesRead() }` |
| `tempodb/encoding/vparquet3/wal_block.go:708-710` | `walBlock.FetchSpans → ErrUnsupported` | No change |
| `tempodb/encoding/vparquet4/block.go:81-83` | `FetchSpans → ErrUnsupported` | No change |
| `tempodb/encoding/vparquet4/block_traceql.go:1060-1063` | `(*backendBlock).Fetch` | `Bytes: → Stats:` (bytes-only) |
| `tempodb/encoding/vparquet4/wal_block.go:729-742` | `walBlock.Fetch` | `Stats:` summing multi-reader |
| `tempodb/encoding/vparquet4/wal_block.go:745-747` | `walBlock.FetchSpans → ErrUnsupported` | No change |
| `tempodb/encoding/vparquet5/block_traceql.go:1126-1129` | `(*backendBlock).Fetch` | `Bytes: → Stats:` (full struct, counters zero, `Bytes: rr.BytesRead()`). TODO marks #1274 wiring. |
| `tempodb/encoding/vparquet5/block_traceql_fetch.go:26-29` | `(*backendBlock).FetchSpans` | same as above |
| `tempodb/encoding/vparquet5/wal_block.go:744-757` | `walBlock.Fetch` | `Stats:` summing multi-reader |
| `tempodb/encoding/vparquet5/wal_block.go:786-795` | `walBlock.FetchSpans` | `Stats:` summing multi-reader |
| `pkg/traceql/engine_test.go:416-421` | `MockSpanSetFetcher.Fetch` | `Bytes: → Stats:` |
| `pkg/traceql/storage.go:353` | `SpansetFetcherWrapper.FetchSpans → ErrUnsupported` | No change (empty struct) |
| `tempodb/tempodb.go:538` | `readerWriter.Fetch` early-error return | No change (empty struct) |
| `tempodb/tempodb.go:548` | `readerWriter.FetchSpans` early-error return | No change (empty struct) |
| `modules/frontend/search_sharder_test.go:85,89` | `mockReader.Fetch/FetchSpans` empty returns | No change (empty struct) |

### 4.3 Consumers

Two files, three call sites.

**Nil-safety decision.** The current `Bytes` callback is nil-checked at `pkg/traceql/engine.go:172` but **not** at `engine_metrics.go:1380` or `:1476`. The new `Stats` callback adopts the engine.go pattern: every consumer call site checks `if fetch.Stats != nil` before invoking. Producers may still return `Stats: nil` from `ErrUnsupported` paths.

* `pkg/traceql/engine.go:172-177` (`ExecuteSearch`). Replace the `Bytes()` callback read with `if fetchSpansResponse.Stats != nil { stats := fetchSpansResponse.Stats(); ... }`. The existing `InspectedBytes` assignment and span attribute continue to work using `stats.Bytes`. Task B then layers in the new SearchMetrics fields.
* `pkg/traceql/engine_metrics.go:1380` and `:1476` (`MetricsEvaluator.Do` and `DoSpansOnly`). Add a nil check that doesn't exist today. Replace `e.bytes += fetch.Bytes()` with `if fetch.Stats != nil { s := fetch.Stats(); e.bytes += s.Bytes }`. For #1273 keep `MetricsEvaluator`'s existing `bytes/spansTotal/spansDeduped` fields; Task B's PR adds the rest of the accumulators and widens `Metrics()` (see §5.5).

### 4.4 Tests

Limited additions: a small assertion in `tempodb/encoding/vparquet5/block_traceql_test.go` that `resp.Stats().Bytes > 0` after iteration, replacing any current `resp.Bytes()` assertions. Update `MockSpanSetFetcher` (`pkg/traceql/engine_test.go:416-421`) to populate `Stats:`. No new span-attribute test scaffolding — the codebase has no precedent for `tracetest.SpanRecorder`, so Task E's span coverage is validated by integration / manual trace inspection.

## 5. Task B — extend proto, combiners, producer wiring

### 5.1 Proto changes

File `pkg/tempopb/tempo.proto`. Lowest free field numbers used (8/9/10 on SearchMetrics, 2/3/4 on TraceByIDMetrics, 6/7/8 on MetadataMetrics).

```protobuf
message TraceByIDMetrics {
  uint64 inspectedBytes = 1;
  // Number of backend (object-storage) read operations issued while serving this trace lookup.
  uint64 backendReads = 2;
  // Number of bytes read from the backend. Excludes cache hits.
  uint64 backendBytes = 3;
  // Open-ended counters keyed by stable string constants in
  // pkg/tempopb/additional_metrics_keys.go (rowGroupsInspected, rowGroupsSkipped,
  // pagesInspected, pagesSkipped, cacheHits, cacheMisses, cacheBytes).
  map<string, int64> additionalMetrics = 4;
}

message SearchMetrics {
  uint32 inspectedTraces = 1;
  uint64 inspectedBytes  = 2;
  uint32 totalBlocks     = 3;
  uint32 completedJobs   = 4;
  uint32 totalJobs       = 5;
  uint64 totalBlockBytes = 6;
  uint64 inspectedSpans  = 7;
  uint64 backendReads    = 8;
  uint64 backendBytes    = 9;
  map<string, int64> additionalMetrics = 10;
}

message MetadataMetrics {
  uint64 inspectedBytes  = 1;
  uint32 totalJobs       = 2;
  uint32 completedJobs   = 3;
  uint32 totalBlocks     = 4;
  uint64 totalBlockBytes = 5;
  uint64 backendReads    = 6;
  uint64 backendBytes    = 7;
  map<string, int64> additionalMetrics = 8;
}
```

Regeneration: `make gen-proto` (Makefile target at lines 290 / 331–332, calls `buf generate --config buf/buf.gen-config.yaml --template buf/buf.gen.tempopb.yaml --path pkg/tempopb/tempo.proto`). Both `tempo.proto` and the regenerated `tempo.pb.go` are committed together.

### 5.2 Constants file

New file `pkg/tempopb/additional_metrics_keys.go`. Other hand-written files in the package (`pool.go`, `utils.go`, `prealloc.go`) confirm non-generated `.go` files are accepted.

```go
package tempopb

// AdditionalMetric* are the stable string keys used in *.AdditionalMetrics maps
// on SearchMetrics, TraceByIDMetrics, and MetadataMetrics. They are part of the
// wire contract: rename only with a deprecation cycle.
const (
    AdditionalMetricRowGroupsInspected = "rowGroupsInspected"
    AdditionalMetricRowGroupsSkipped   = "rowGroupsSkipped"
    AdditionalMetricPagesInspected     = "pagesInspected"
    AdditionalMetricPagesSkipped       = "pagesSkipped"
    AdditionalMetricCacheHits          = "cacheHits"
    AdditionalMetricCacheMisses        = "cacheMisses"
    AdditionalMetricCacheBytes         = "cacheBytes"
)
```

### 5.3 Combiner aggregation

File `modules/frontend/combiner/response_metrics.go`. Add a private helper near the top of the file, then extend the four `Combine` functions.

```go
// mergeAdditionalMetrics performs an in-place key-by-key sum of src into dst,
// allocating dst lazily. Both maps may be nil.
func mergeAdditionalMetrics(dst, src map[string]int64) map[string]int64 {
    if len(src) == 0 {
        return dst
    }
    if dst == nil {
        dst = make(map[string]int64, len(src))
    }
    for k, v := range src {
        dst[k] += v
    }
    return dst
}
```

Extend each `Combine` function (inside the existing cache-hit suppression):

```go
// SearchMetricsCombiner.Combine (lines 19-27)
mc.Metrics.BackendReads += newMetrics.BackendReads
mc.Metrics.BackendBytes += newMetrics.BackendBytes
mc.Metrics.AdditionalMetrics = mergeAdditionalMetrics(mc.Metrics.AdditionalMetrics, newMetrics.AdditionalMetrics)
```

Same three lines (with the per-type cache-hit suppression preserved) in:

* `TraceByIDMetricsCombiner.Combine` (lines 50–54).
* `MetadataMetricsCombiner.Combine` (lines 66–70).
* `QueryRangeMetricsCombiner.Combine` (lines 82–100).

`SearchMetricsCombiner.CombineMetadata` (lines 29–38) needs no change — that path only carries sharder-emitted totals.

### 5.4 MCP marshaler

File `modules/frontend/combiner/llm_marshaler.go`. Extend `LLMMetrics` (lines 72–78) with `BackendReads`, `BackendBytes`, `AdditionalMetrics` fields, then update both populate sites:

* `traceByIDResponseToSimplifiedJSON` (lines 163–167) — extend the `t.Metrics.InspectedBytes > 0` guard to OR in the new fields.
* `searchTagValuesV2ResponseToSimplifiedJSON` (lines 326–340) — extend `hasMetrics` and the populated struct.

### 5.5 Producer wiring

Updates `pkg/traceql/engine.go` and `engine_metrics.go` so the values flowing through Task A's `Stats()` callback land on the new proto fields.

`engine.go:172-177`:

```go
if fetchSpansResponse.Stats != nil {
    s := fetchSpansResponse.Stats()
    res.Metrics.InspectedBytes = s.Bytes
    span.SetAttributes(attribute.Int64("inspectedBytes", int64(s.Bytes)))

    res.Metrics.BackendReads = sumByRole(s.BackendReadsByRole)
    res.Metrics.BackendBytes = sumByRole(s.BackendBytesByRole)
    res.Metrics.AdditionalMetrics = map[string]int64{
        tempopb.AdditionalMetricRowGroupsInspected: int64(s.RowGroupsInspected),
        tempopb.AdditionalMetricRowGroupsSkipped:   int64(s.RowGroupsSkipped),
        tempopb.AdditionalMetricPagesInspected:     int64(s.PagesInspected),
        tempopb.AdditionalMetricPagesSkipped:       int64(s.PagesSkipped),
        tempopb.AdditionalMetricCacheHits:          int64(sumByRole(s.CacheHitsByRole)),
        tempopb.AdditionalMetricCacheMisses:        int64(sumByRole(s.CacheMissesByRole)),
        tempopb.AdditionalMetricCacheBytes:         int64(sumByRole(s.CacheBytesByRole)),
    }
}
```

`engine_metrics.go:1380` and `:1476` get equivalent accumulators on `MetricsEvaluator` (new private fields next to `bytes/spansTotal/spansDeduped` at line 1244). The `Metrics()` getter at line 1485 is widened to return a struct rather than the current `(uint64, uint64, uint64)` tuple, so future additions don't keep breaking callers:

```go
type EvaluatorMetrics struct {
    Bytes             uint64
    SpansTotal        uint64
    SpansDeduped      uint64
    BackendReads      uint64
    BackendBytes      uint64
    AdditionalMetrics map[string]int64
}

func (e *MetricsEvaluator) Metrics() EvaluatorMetrics { ... }
```

All `eval.Metrics()` callers must be updated in the same PR. There are seven sites:

* Production:
  * `modules/querier/querier_query_range.go:131` — `inspectedBytes, spansTotal, _ := eval.Metrics()`. Update to read from the struct and populate the new `SearchMetrics.BackendReads/BackendBytes/AdditionalMetrics` fields on the response.
  * `modules/livestore/instance_search.go:900` — `inspectedBytes, _, _ := eval.Metrics()`. Same treatment; this is the live-store metrics-query response path.
* Tests:
  * `tempodb/encoding/vparquet3/block_traceql_test.go` — one site reading the tuple form.
  * `tempodb/encoding/vparquet4/block_traceql_test.go` — same.
  * `tempodb/encoding/vparquet5/block_traceql_test.go` — same.

The struct return is intentional even though only two production callers exist today — issue #1274 will add per-iterator and per-cache-role counters that the struct accommodates cleanly. A tuple would need to keep growing.

### 5.6 Tests

* New file `modules/frontend/combiner/response_metrics_test.go` (doesn't exist today). Table-driven tests for all four combiners: BackendReads/BackendBytes summing, AdditionalMetrics key-by-key merge, cache-hit suppression preserved, nil input no-op, `CombineMetadata` regression guard.
* Extend `modules/frontend/combiner/search_test.go:325-418` ("respects total blocks message", "200+200") to set new fields and assert the merged totals.
* Extend `modules/frontend/combiner/trace_by_id_test.go:67,81` and `search_tags_test.go` table cases.
* Extend `modules/frontend/combiner/llm_marshaler_test.go:76-81,148-151,165-171,189-196`.

## 6. Task C — per-role cache hit/miss counters

### 6.1 Counter declarations

File `tempodb/backend/cache/cache.go`. Add near `cacheStoreSizeBytes` (line 24):

```go
var cacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: "tempodb",
    Name:      "cache_requests_total",
    Help:      "Cache lookup outcome by role.",
}, []string{"role", "outcome"})

var cacheRequestBytes = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: "tempodb",
    Name:      "cache_request_bytes_total",
    Help:      "Bytes served by cache (hit) or fetched on miss, by role.",
}, []string{"role", "outcome"})
```

### 6.2 Instrumentation in `Read` (lines 92–117)

After `cache.FetchKey(ctx, k)` on line 97:

* On `found == true`: `cacheRequests.WithLabelValues(role, "hit").Inc()` and `cacheRequestBytes.WithLabelValues(role, "hit").Add(float64(len(b)))`.
* On `found == false`: `cacheRequests.WithLabelValues(role, "miss").Inc()`.
* After the successful `tempo_io.ReadAllWithEstimate` populate at line 111 (i.e. inside `if err == nil && cache != nil`): `cacheRequestBytes.WithLabelValues(role, "miss").Add(float64(len(b)))`.

`len(b)` after `ReadAllWithEstimate` is the cleanest "bytes on miss" source — exact, includes only successful reads, matches the policy already used to gate `store()`.

### 6.3 Instrumentation in `ReadRange` (lines 120–141)

Same pattern. Bytes-on-miss uses `len(buffer)` (caller-allocated, exact).

### 6.4 Bypass and scope

When `cache == nil` (role not configured, or bloom-config suppression), neither counter increments — by design those reads aren't "cache lookups". The frontend search cache (`RoleFrontendSearch`, used in `modules/frontend/pipeline/sync_handler_cache.go`) is **not** covered by these counters; it's a different layer with its own existing `TempoCacheHeader` hit/miss signal. Out of scope for this issue.

### 6.5 PR #7137 / `cache_store_error_bytes_total`

Not yet present in the branch (`grep` returns no hits for `cache_store_error_bytes_total` or `pageCacheError`). **Decision**: commit C does not introduce `cache_store_error_bytes_total` in any form. If PR #7137 lands before #1273, rebase onto it (no conflict expected — different code paths). If #7137 lands after, it does its own variable declaration and instrumentation. #1273 stays focused on hit/miss/bytes.

### 6.6 Cardinality

6 wirable roles (`bloom`, `parquet-footer`, `parquet-column-idx`, `parquet-offset-idx`, `parquet-page`, `trace-id-index`) × 2 outcomes × 2 counters = 24 new series per process. Negligible. No tenant label, matching the out-of-scope statement in the design doc.

### 6.7 Tests

File `tempodb/backend/cache/cache_test.go`. New cases:

* Hit increments both counters; miss increments both.
* Per-role isolation (footer hit + bloom miss in one test).
* Both `Read` and `ReadRange` exercised.
* Bypass (`cache == nil`) does not increment.
* Error path on miss does not bump miss-bytes (the hit/miss counter increment did happen at FetchKey time — that's intentional).

Tests use `testutil.ToFloat64` with before/after deltas because counters are package-level globals.

## 7. Task D — query shape on logs and spans

### 7.1 Carrier design

Hybrid: extend the `pipeline.Request` interface (parallel to the existing `SetWeight`/`Weight` precedent) and additionally stamp the shape onto `req.Context()` so handler-side response loggers can read it back.

In `modules/frontend/pipeline/async_weight_middleware.go`:

```go
type QueryShape struct {
    Type            string // "traces" | "search" | "metrics" | "metadata"
    Weight          int
    Conditions      int
    RegexConditions int
    HasOr           bool   // !AllConditions
    NeedsFullTrace  bool
    SelectAll       bool   // SecondPassSelectAll
}

type queryShapeCtxKey struct{}

func QueryShapeFromContext(ctx context.Context) (QueryShape, bool) {
    v, ok := ctx.Value(queryShapeCtxKey{}).(QueryShape)
    return v, ok
}
```

In `modules/frontend/pipeline/pipeline.go`: add `SetQueryShape(QueryShape)` and `QueryShape() QueryShape` to the `Request` interface (line 13–28). Implement on `HTTPRequest` (line 30–36) and propagate through `CloneFromHTTPRequest` (line 82) — mirrors how `weight` is already carried.

`setWeight` and `setTraceQLWeight` (`async_weight_middleware.go:74-148`) build a `QueryShape` value from the already-computed locals and call `req.SetQueryShape(shape)` plus `req.SetContext(context.WithValue(ctx, queryShapeCtxKey{}, shape))`.

### 7.2 RequestType → query_type mapping

| `RequestType` enum (line 30–35) | Wiring | `query_type` value |
| :--- | :--- | :--- |
| `TraceByID` | `frontend.go:146` | `traces` |
| `TraceQLSearch` | `frontend.go:160` | `search` |
| `TraceQLMetrics` | `frontend.go:216,230` | `metrics` |
| `Default` | `frontend.go:173,186,199` (tag/tag-value) | `metadata` |

The default branch (line 84 in middleware) emits `QueryShape{Type: "metadata", Weight: defaultWeight}` with all other fields zero. Tag endpoints don't compile TraceQL, so they only carry `query_type` plus the endpoint-specific fields they already log.

### 7.3 Sharder span attributes

Helper in `modules/frontend/util.go`:

```go
func setQueryShapeSpanAttrs(span trace.Span, qs pipeline.QueryShape) {
    span.SetAttributes(
        attribute.String("query_type", qs.Type),
        attribute.Int("query_weight", qs.Weight),
        attribute.Int("query_conditions", qs.Conditions),
        attribute.Int("query_regex_conditions", qs.RegexConditions),
        attribute.Bool("query_has_or", qs.HasOr),
        attribute.Bool("query_needs_full_trace", qs.NeedsFullTrace),
        attribute.Bool("query_select_all", qs.SelectAll),
    )
}
```

Call sites: after `tracer.Start` returns the span, retrieve the `QueryShape` from the request via `pipelineRequest.QueryShape()` (the interface method, not the context helper — sharders hold the `pipeline.Request` directly), then call `setQueryShapeSpanAttrs(span, qs)`. Apply to:

* `modules/frontend/search_sharder.go:99` (`frontend.ShardSearch`)
* `modules/frontend/traceid_sharder.go:68` (`frontend.ShardQuery`)
* `modules/frontend/tag_sharder.go:213` (`frontend.ShardSearchTags`)
* `modules/frontend/metrics_query_range_sharder.go:81` (`frontend.QueryRangeSharder.range/instant`)

### 7.4 Response log fields

Helper in `modules/frontend/util.go`:

```go
func queryShapeLogFields(ctx context.Context) []any {
    qs, ok := pipeline.QueryShapeFromContext(ctx)
    if !ok {
        return nil
    }
    return []any{
        "query_type", qs.Type,
        "query_weight", qs.Weight,
        "query_conditions", qs.Conditions,
        "query_regex_conditions", qs.RegexConditions,
        "query_has_or", qs.HasOr,
        "query_needs_full_trace", qs.NeedsFullTrace,
        "query_select_all", qs.SelectAll,
    }
}
```

Append to the existing log-field slice in six handlers:

| File | Function | `msg` |
| :--- | :--- | :--- |
| `modules/frontend/search_handlers.go:212` | `logResult` | `search response` |
| `modules/frontend/metrics_query_handler.go:233` | `logQueryInstantResult` | `query instant results` |
| `modules/frontend/metrics_query_range_handler.go:235` | `logQueryRangeResult` | `query range response` |
| `modules/frontend/traceid_handlers.go:73,145` | V1 / V2 handlers | `trace id response` |
| `modules/frontend/tag_handlers.go:560` | `logTagsResult` | `search tag response` |
| `modules/frontend/tag_handlers.go:585` | `logTagValuesResult` | `search tag values response` |

Also append in the early-return branches of each `logResult`-family function (`"… - no resp"`, `"… - no metrics"`). Concretely: append the full `queryShapeLogFields(ctx)` slice on every branch. The cost of including all seven fields uniformly is small and avoids per-branch ad-hoc decisions about which subset to log.

**Streaming handlers caveat.** The streaming gRPC variants in `tag_handlers.go:89,159,207,261` (`SearchTagsStreaming`, `SearchTagsV2Streaming`, `SearchTagValuesStreaming`, `SearchTagValuesV2Streaming`) and the streaming search/query-range handlers go through their own pipeline configuration. Verify during implementation that the async-weight middleware is wired into the streaming pipeline as well — if not, streaming response logs will see `query_type=""` and the helper's `ok=false` branch will return `nil` (degrading gracefully but losing signal). If the streaming pipeline doesn't run the middleware, add it as a small follow-up scoped change in the same PR.

### 7.5 Safety

* New log fields are additive; no existing dashboard or LogQL parser is affected.
* New span attributes are additive; only the QueryRange sharder already calls `SetAttributes`, and the new keys don't collide.
* Stamping the shape at the entry of `setTraceQLWeight` (before the early-return branches at lines 102 and 111) ensures even malformed queries get a shape with `Type` set and other fields zero.
* No new field contains the query string. Only structural counts and booleans.

### 7.6 Tests

* `modules/frontend/pipeline/async_weight_middleware_test.go:60-183` (`TestWeightMiddlewareForTraceQLRequest`): each existing case is augmented with expected `QueryShape` fields. The existing arithmetic in the comments (`+1 for regex`, `+1 for AllConditions is false`, `+1 for NeedsFullTrace`) becomes per-field assertions on `request.QueryShape()`. Add separate cases for `Default` and `TraceByID` types.
* Skip handler log-output tests (no existing precedent in the codebase).
* Skip sharder span-attribute tests (no `tracetest.SpanRecorder` precedent).

## 8. Task E — new `parquet.backendBlock.Fetch` span

### 8.1 vparquet5 backendBlock spans

Tracer: `var tracer = otel.Tracer("tempodb/encoding/vparquet5")` at `vparquet5/block.go:20`. Pattern follows `backendBlock.Search` at `vparquet5/block_search.go:92-105`.

For each function below, insert at the top:

```go
ctx, span := tracer.Start(ctx, "parquet.backendBlock.Fetch",
    trace.WithAttributes(
        attribute.String("blockID", b.meta.BlockID.String()),
        attribute.String("tenantID", b.meta.TenantID),
        attribute.Int64("blockSize", int64(b.meta.Size_)),
        attribute.Int("numConditions", len(req.Conditions)),
        attribute.Bool("allConditions", req.AllConditions),
        attribute.Bool("secondPassSelectAll", req.SecondPassSelectAll),
    ))
defer span.End()

defer func() {
    s := traceql.FetchSpansStats{Bytes: rr.BytesRead()}
    span.SetAttributes(
        attribute.Int64("inspectedBytes", int64(s.Bytes)),
        attribute.Int64("rowGroupsInspected", int64(s.RowGroupsInspected)),
        attribute.Int64("rowGroupsSkipped", int64(s.RowGroupsSkipped)),
        attribute.Int64("pagesInspected", int64(s.PagesInspected)),
        attribute.Int64("pagesSkipped", int64(s.PagesSkipped)),
    )
}()
```

Note: the OTel span covers planning and iterator creation, not iteration consumption (the iterator is consumed lazily after `Fetch` returns, and the deferred `span.End()` fires earlier). This matches the existing behavior of `backendBlock.Search`. The trade-off is documented in the span-attribute description.

Sites:

* `vparquet5/block_traceql.go:1106` (`backendBlock.Fetch`).
* `vparquet5/block_traceql_fetch.go:17` (`backendBlock.FetchSpans`) — same shape.
* `vparquet5/block_autocomplete.go:52` (`FetchTagNames`) and `:202` (`FetchTagValues`) — same attributes except `secondPassSelectAll` (not on the request struct); use `numConditionGroups` instead.

Imports added to `block_traceql.go`, `block_traceql_fetch.go`, `block_autocomplete.go`: `go.opentelemetry.io/otel/attribute` and `go.opentelemetry.io/otel/trace`.

### 8.2 vparquet5 walBlock spans

`wal_block.go` already calls `tracer.Start` at lines 714, 761, 799, 891 — but with no attributes. Replace each call with the attribute-bearing variant (using `walBlock.*` span names, not `parquet.backendBlock.*`, to preserve the existing distinction). Add the deferred end-attribute setter using the multi-reader sum for `Bytes`.

### 8.3 vparquet3 / vparquet4

No new spans for older block formats in this issue. The design doc Proposal 1E targets vparquet5 (current default). vparquet3/4 retain their existing observability.

## 9. Verification

### 9.1 Local

* `go build ./...` and `go test ./...` green after each PR.
* `make gen-proto` produces no untracked diff after commit B.
* `tempodb/backend/cache/cache_test.go` covers the new hit/miss counters.

### 9.2 Trace inspection

* Manually run a TraceQL search through a local Tempo with a few blocks.
* Verify a trace appears with `parquet.backendBlock.Fetch` spans containing `blockID`, `inspectedBytes`, and the (zeroed) counter attributes.
* Verify the frontend sharder spans contain `query_type`, `query_weight`, etc.

### 9.3 Loki inspection

* Run the same search and confirm the per-query response log line carries the new `query_*` fields.
* Confirm OR-heavy and regex-heavy queries surface distinct `query_has_or=true` and `query_regex_conditions>0` values.

### 9.4 gcx production check (post-deploy)

* `gcx metrics series --datasource <ds> --match '{__name__="tempodb_cache_requests_total"}'` confirms the new metric appears and carries the expected `role` and `outcome` labels.
* Same for `tempodb_cache_request_bytes_total`.
* `gcx logs query` on the frontend container for `{component="query-frontend"} | json | query_has_or="true"` confirms the new log fields land in Loki.

## 10. Backward compatibility and rollout

* `traceql.FetchSpansResponse` is an internal Go type. No external module compatibility concern. Single-PR transition; no `Bytes` alias kept.
* Proto changes are additive. Mixed-version deployments (new frontend, old querier or live-store) see zero values for the new fields, which the combiner handles naturally (`+= 0` and merging an empty/nil map are no-ops).
* `SearchMetrics.AdditionalMetrics` (and equivalents on `TraceByIDMetrics` / `MetadataMetrics`) carry seven `string → int64` entries on every search/trace-by-ID/tag response. Protobuf-encoded that's roughly 120–180 bytes per response. At the cell-wide search rate this is a small but non-zero ingress increase on the frontend→client wire. Acceptable per the design doc's cost analysis.
* New Prometheus metrics are additive. Total new series across commit C: ~24 per process.
* New log fields are additive; existing LogQL queries unaffected.
* New span attributes are additive; existing trace consumers unaffected.

No feature flag is required. The new signals fire as soon as the code is deployed.

## 11. Decisions made and open questions

### Decisions

* **`Stats()` nil-safety.** Consumers check `if fetch.Stats != nil` before invoking. Matches the existing `engine.go:172` pattern; covers the `ErrUnsupported` paths that return `FetchSpansResponse{}` with `Stats: nil`. See §4.3.
* **`MetricsEvaluator.Metrics()` getter signature.** Widen to return a struct (`EvaluatorMetrics`) in this issue, not later. The 5 test sites and 2 production sites are listed and updated in commit B. See §5.5.
* **PR #7137 / `cache_store_error_bytes_total`.** Not introduced by #1273 in any form. See §6.5.
* **Early-return log branches.** Append the full `queryShapeLogFields(ctx)` slice uniformly on every branch (no per-branch subset decisions). See §7.4.

### Open

* **Order of `BackendReads` aggregation in `engine.go`.** Task A's `FetchSpansStats` carries per-role maps; `SearchMetrics.BackendReads` is a scalar. The plan goes with summing `BackendReadsByRole.values()` into the scalar and leaving per-role detail on spans only. Reconfirm during commit B review whether per-role bytes/reads should also appear in `AdditionalMetrics` (would add ~12 more keys to the map).
* **Streaming pipeline middleware.** Verify during commit D implementation that the async-weight middleware is wired into the streaming gRPC pipeline. If not, decide whether to add it within #1273 or defer.

## 12. File:line index (consolidated)

### Task A
* `pkg/traceql/storage.go:252,262,268-271,340-356,353`
* `pkg/traceql/engine.go:172-177`
* `pkg/traceql/engine_metrics.go:1244,1380,1476,1485,1489`
* `pkg/traceql/engine_test.go:413-426`
* `tempodb/tempodb.go:538,548`
* `tempodb/encoding/unsupported/block.go:43-49`
* `tempodb/encoding/vparquet3/block.go:44-46`
* `tempodb/encoding/vparquet3/block_traceql.go:884-908`
* `tempodb/encoding/vparquet3/wal_block.go:663-706,708-710`
* `tempodb/encoding/vparquet4/block.go:81-83`
* `tempodb/encoding/vparquet4/block_traceql.go:1040-1064`
* `tempodb/encoding/vparquet4/wal_block.go:696-743,745-747`
* `tempodb/encoding/vparquet5/block_traceql.go:1106-1130`
* `tempodb/encoding/vparquet5/block_traceql_fetch.go:17-30`
* `tempodb/encoding/vparquet5/wal_block.go:713-758,760-796`
* `modules/frontend/search_sharder_test.go:85,89`

### Task B
* `pkg/tempopb/tempo.proto:63,164,269`
* `pkg/tempopb/tempo.pb.go` (regenerated)
* `pkg/tempopb/additional_metrics_keys.go` (new)
* `modules/frontend/combiner/response_metrics.go:19,29,50,66,82`
* `modules/frontend/combiner/llm_marshaler.go:72,163,326`
* `modules/frontend/combiner/response_metrics_test.go` (new)
* `modules/frontend/combiner/search_test.go:325-418`
* `modules/frontend/combiner/trace_by_id_test.go:67,81`
* `modules/frontend/combiner/search_tags_test.go:165-331`
* `modules/frontend/combiner/llm_marshaler_test.go:76,148,165,189`
* `modules/querier/querier_query_range.go:131` (`eval.Metrics()` production caller)
* `modules/livestore/instance_search.go:900` (`eval.Metrics()` production caller)
* `tempodb/encoding/vparquet3/block_traceql_test.go` (`eval.Metrics()` test caller)
* `tempodb/encoding/vparquet4/block_traceql_test.go` (`eval.Metrics()` test caller)
* `tempodb/encoding/vparquet5/block_traceql_test.go` (`eval.Metrics()` test caller)
* `Makefile:290,331-332` (`make gen-proto`)

### Task C
* `tempodb/backend/cache/cache.go:24,92,97,111-112,120,125,136,187-228`
* `tempodb/backend/cache/cache_test.go:167-229,272-307`
* `pkg/cache/cache.go:13-23` (role enum)

### Task D
* `modules/frontend/pipeline/async_weight_middleware.go:30-35,38,74-148`
* `modules/frontend/pipeline/pipeline.go:13-28,30-89`
* `modules/frontend/util.go` (new helpers)
* `modules/frontend/frontend.go:146,160,173,186,199,216,230`
* `modules/frontend/search_sharder.go:99`
* `modules/frontend/traceid_sharder.go:68`
* `modules/frontend/tag_sharder.go:213`
* `modules/frontend/metrics_query_range_sharder.go:75-81,152-154`
* `modules/frontend/search_handlers.go:177-228`
* `modules/frontend/metrics_query_handler.go:206-251`
* `modules/frontend/metrics_query_range_handler.go:208-254`
* `modules/frontend/traceid_handlers.go:73,145`
* `modules/frontend/tag_handlers.go:557-570,582-596`
* `modules/frontend/pipeline/async_weight_middleware_test.go:60-183`

### Task E
* `tempodb/encoding/vparquet5/block.go:20` (tracer)
* `tempodb/encoding/vparquet5/block_search.go:92-105` (pattern reference)
* `tempodb/encoding/vparquet5/block_traceql.go:1106`
* `tempodb/encoding/vparquet5/block_traceql_fetch.go:17`
* `tempodb/encoding/vparquet5/block_autocomplete.go:52,202`
* `tempodb/encoding/vparquet5/wal_block.go:714,761,799,891`
