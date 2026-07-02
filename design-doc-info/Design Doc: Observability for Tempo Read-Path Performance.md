# Design Doc: Observability for Tempo Read-Path Performance

| Author(s) | [Adrian Stoewer](mailto:adrian.stoewer@grafana.com) |  |
| :---- | :---- | ----- |
| **Created** | 2026-05-22 |  |
| **Status** | **Draft** |  |
| **Reviewer(s)** | In Discussion [Javi Molina](mailto:javi.molina@grafana.com)In Discussion [Oleg Kozliuk](mailto:oleg.kozliuk@grafana.com)In Discussion [Marty Disibio](mailto:martin.disibio@grafana.com) |  |
| **Informed** | Tempo team |  |
| **Product DNA(s)** | N/A |  |
| **Delivery Plan(s)** | N/A |  |
| **Readiness Review** | N/A |  |

# Background

This design doc is part of Project Sleipnir, a workstream to improve Tempo's read-path performance. One of Sleipnir's Q2 Key Results explicitly include "Improve observability of Tempo query execution". This document is this deliverable. Sleipnir contributes to the Tempo North Star 2026 objective *"Achieve operational excellence at scale"*. Specifically, it advances the *"Improve Performance"* theme of finding problematic queries and driving targeted improvements.

Tempo's read path serves four families of queries (Trace-by-ID, TraceQL search, TraceQL metrics, and metadata) through four cooperating layers. The query-frontend shards queues, retries, and combines. The querier dispatches jobs to live-stores and to tempodb. The livestore holds in-memory recent data, the WAL, and local complete blocks. The tempodb fetch layer reads parquet blocks from object storage through a multi-role cache. The team currently has insufficient data about how production queries perform on this read path. Sleipnir's measurement-first approach requires that read-path changes are evaluable from production telemetry, both to choose the right optimization and to prove its effect on rollout. This document defines the performance signals that the execution loop needs in order to start.

A static and runtime audit of the read path (see the existing-signals appendix below) confirmed the intuition that large parts of the work the read path performs is invisible to telemetry today.

# Problem

In production today the team cannot answer fundamental questions about Tempo's read path from telemetry alone. We cannot classify a week of real-world queries by shape and correlate to performance. Query shape (regex use, OR vs AND, structural operators, weight class) is computed by the weight middleware but never exposed. We cannot attribute a slow query to a layer, because per-layer durations and per-layer work counters are largely missing. For a given query we cannot easily find out whether it spent its time in the frontend queue, in cache lookups, in S3 reads, or in TraceQL evaluation. We cannot quantify the effect of an optimization, because most measurement is load-dependent (raw P95 latency, for example, moves with cell traffic). The load-independent quantities we would prefer (bytes-per-query, row-groups-skipped-per-query, cache-hit-ratio) are either not tracked or not surfaced.

Concrete consequences are visible in production today. Sleipnir documents four recurring pathological cases: slow metrics queries, tag-value enumeration bursts, unrestricted TraceByID in large cells, and long-range TraceQL search. For each, we can describe the symptom but cannot attribute the cost.

Two active workstreams (memcached tuning, Redis migration) have independently surfaced the same observability gaps as concrete blockers. Per-role cache hit/miss visibility is a prerequisite for evaluating either, and is one of the signals proposed here.

# Goals

For any read-path query, the team should be able to answer the following from production telemetry, without attaching a debugger. The list below names what we currently cannot see and need to.

* **Query shape:** Per query: weight, condition count, regex use, OR vs AND, structural-operator use, select-all flag, query type, range, and limits.  
* **Layer attribution**: For each slow query: time spent per layer (frontend queue, querier RPC, S3, cache, fetch, TraceQL evaluation), with per-block attribution in the fetch layer.  
* **Cache efficacy by role:** Per cache role: hit count, miss count, bytes served on hit, bytes fetched on miss; per query: aggregate cache-hit ratio.  
* **Block selectivity**: Per query: row groups inspected vs skipped, pages inspected vs skipped, dictionary short-circuits; bloom-filter hit/miss recorded per trace-by-ID.  
* **Load-independent measures:** bytes-per-query (-per-job), row-groups-skipped-per-query (-per-job), cache-hit-ratio-per-query (-per-job). Per query measures normalized against query count by op.

Out of scope:

* Building dashboards or alerts on top of the new signals. That is a parallel workstream.  
* Designing a deterministic benchmark tool.  
* Restructuring the read path itself. This design is the prerequisite for evaluating such changes, not a replacement.  
* Per-tenant labels on high-cardinality fetch-layer counters (row groups, pages, dictionary short-circuits). These stay cell-wide or labeled only by outcome; per-tenant correlation comes through response-stat propagation in the per-query log line, not through Prometheus labels.

# Proposals

The proposals below are organized by priority, from must-have (Proposal 1\) to hygiene (Proposal 3). Each proposal stands on its own and can be deferred or skipped, though later proposals build on data exposed by earlier ones. Within each proposal, tasks are grouped by theme.

## Proposal 0: Do nothing

If no new signals are added, slow-query investigations remain less targeted and depend on engineer intuition rather than telemetry. Cache, predicate-pushdown, and bloom-filter optimizations cannot be evaluated against a baseline. The longer the read path evolves uninstrumented, the harder retroactive instrumentation becomes.

## Proposal 1: Baseline read-path performance signals

Establish the minimum signals needed to answer three questions about any production query: what was its shape, how much work did the fetch layer do, and how much was served from cache. The first part widens the per-query stats information (`FetchSpansResponse` interface and `SearchMetrics` proto). The second part adds the highest-value direct instrumentation: per-role cache counters, query shape on logs and spans, and missing fetch-layer spans.

### API widening

**A)** **Widen `FetchSpansResponse` with a typed stats struct:**   
The current `FetchSpansResponse` carries only `Bytes func() uint64`, so any per-block work counter the fetch layer measures is dropped at this boundary. Replace the single callback with a `Stats()` callback returning a typed struct carrying row-group counts, page counts, per-role cache breakdowns, and bytes. The `FetchSpansOnlyResponse` (used by the metrics-query span-only path) gets the same treatment. This is purely an internal interface change; no public Go API is affected.

**B) Extend `SearchMetrics` proto:**   
The proto vocabulary that travels from the fetch layer back to the user currently has seven counters, of which only `InspectedBytes` is consistently populated. Add fields for row groups inspected/skipped, pages inspected/skipped, cache hits/misses/bytes, and backend reads/bytes. Mirror equivalent fields on `TraceByIDMetrics` and `MetadataMetrics` so the same vocabulary works across search, trace-by-ID, and tag responses. The four combiner functions in `combiner/response_metrics.go` are extended to sum the new fields, using the same cache-hit suppression they already apply to `InspectedBytes`.

### Direct instrumentation

**C) Per-role cache hit/miss/byte counters:**   
The role-aware cache router at `cache/cache.go:187` knows the role of every read. Role label values emitted are `bloom`, `parquet-footer`, `parquet-column-idx`, `parquet-offset-idx`, `parquet-page`, and `trace-id-index`. It emits no metric for hit/miss or bytes per role today. Add Prometheus counters labeled by role and outcome. This is the only way today to validate cache-policy changes (LFU for dictionary pages, separate cache for bloom, etc.) without writing custom logging.

**D) Surface query shape on logs and spans:**  
The async-weight middleware at `pipeline/async_weight_middleware.go` already computes per-query weight, condition counts, and regex-condition counts. It also computes the `AllConditions`, `NeedsFullTrace`, and `SecondPassSelectAll` flags. None of these reach logs or spans today. Stash the computed values on context, then add them to the per-request response log line and to the sharder span attributes. This is the single highest-value individual change for production-query classification. With these fields in Loki, "find me all OR-heavy queries above 5 seconds" becomes a single LogQL query.

**E) Span on `block_traceql.go:Fetch` and `block_traceql_fetch.go:FetchSpans`:** The two fetch paths have no separate span today. Add `tracer.Start` calls at the entry of each, with block-identifying attributes plus request shape on open. Set the per-block outcome attributes at span end once the stats struct from (A) is available. Include the autocomplete entry points in `block_autocomplete.go` and the WAL counterparts in `wal_block.go` so trace coverage is uniform across block types.

### Central code changes

```go
// pkg/traceql/storage.go

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
    // existing fields
    Results SpansetIterator
    // proposal: replaced the existing `Bytes func() uint64` callback
    Stats func() FetchSpansStats
}

type FetchSpansOnlyResponse struct {
    // existing fields
    Results SpanIterator
    // proposal: replaces the existing `Bytes func() uint64` callback
    Stats func() FetchSpansStats
}
```

```go
// pkg/tempopb/tempo.pb.go (generated from tempo.proto)

type SearchMetrics struct {
    // existing fields
    InspectedTraces uint32 `protobuf:"..." json:"inspectedTraces,omitempty"`
    InspectedBytes  uint64 `protobuf:"..." json:"inspectedBytes,omitempty"`
    TotalBlocks     uint32 `protobuf:"..." json:"totalBlocks,omitempty"`
    CompletedJobs   uint32 `protobuf:"..." json:"completedJobs,omitempty"`
    TotalJobs       uint32 `protobuf:"..." json:"totalJobs,omitempty"`
    TotalBlockBytes uint64 `protobuf:"..." json:"totalBlockBytes,omitempty"`
    InspectedSpans  uint64 `protobuf:"..." json:"inspectedSpans,omitempty"`
    // proposed additional fields
    BackendReads       uint64 `protobuf:"..." json:"backendReads,omitempty"`
    BackendBytes       uint64 `protobuf:"..." json:"backendBytes,omitempty"`
    // AdditionalMetrics collects
    // - rowGroupsInspected
    // - rowGroupsSkipped
    // - pagesInspected
    // - pagesSkipped
    // - cacheHits
    // - cacheMisses
    // - cacheBytes
    AdditionalMetrics  map[string]int64 `protobuf:"..." json:"additionalMetrics,omitempty"`
}

type TraceByIDMetrics struct {
    // existing fields
    InspectedBytes uint64 `protobuf:"..." json:"inspectedBytes,omitempty"`
    // proposed additional fields
    BackendReads       uint64 `protobuf:"..." json:"backendReads,omitempty"`
    BackendBytes       uint64 `protobuf:"..." json:"backendBytes,omitempty"`
    // AdditionalMetrics collects
    // - cacheHits
    // - cacheMisses
    // - cacheBytes
    AdditionalMetrics  map[string]int64 `protobuf:"..." json:"additionalMetrics,omitempty"`
}

type MetadataMetrics struct {
    // existing fields
    InspectedBytes  uint64 `protobuf:"..." json:"inspectedBytes,omitempty"`
    TotalJobs       uint32 `protobuf:"..." json:"totalJobs,omitempty"`
    CompletedJobs   uint32 `protobuf:"..." json:"completedJobs,omitempty"`
    TotalBlocks     uint32 `protobuf:"..." json:"totalBlocks,omitempty"`
    TotalBlockBytes uint64 `protobuf:"..." json:"totalBlockBytes,omitempty"`
    // proposed additional fields
    BackendReads       uint64 `protobuf:"..." json:"backendReads,omitempty"`
    BackendBytes       uint64 `protobuf:"..." json:"backendBytes,omitempty"`
    // AdditionalMetrics collects
    // - cacheHits
    // - cacheMisses
    // - cacheBytes
    AdditionalMetrics  map[string]int64 `protobuf:"..." json:"additionalMetrics,omitempty"`
}
```

```go
// tempodb/backend/cache/cache.go

var cacheRequests = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: "tempodb",
    Name:      "cache_requests_total",
    Help:      "Cache lookup outcome by role.",
}, []string{"role", "outcome"}) // outcome in {hit, miss}

var cacheRequestBytes = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: "tempodb",
    Name:      "cache_request_bytes_total",
    Help:      "Bytes served by cache (hit) or fetched on miss, by role.",
}, []string{"role", "outcome"})

// see PR https://github.com/grafana/tempo/pull/7137
var pageCacheErrorBytes = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "tempodb",
	Name:      "cache_store_error_bytes_total",
	Help:      "Parquet cache bytes lost to backend read errors",
}, []string{"class", "role"})
```

New per-query fields on the frontend spans and response logs (`search response`, `query instant results`, `query range response`, `trace id response`, `search tag response`, `search tag values response`): `query_weight`, `query_conditions`, `query_regex_conditions`, `query_has_or`, `query_needs_full_trace`, `query_select_all`, `query_type`.

New span `parquet.backendBlock.Fetch` at `vparquetX/block_traceql.go`. Attributes set on span open: `blockID`, `tenantID`, `blockSize`, `numConditions`, `allConditions`, `secondPassSelectAll`. Attributes set at span end: `rowGroupsInspected`, `rowGroupsSkipped`, `pagesInspected`, `pagesSkipped`, `cacheHits`, `cacheMisses`, `inspectedBytes`. Equivalent spans on `block_traceql_fetch.go:FetchSpans`, `block_autocomplete.go:FetchTagNames` and `FetchTagValues`, and the corresponding `wal_block.go` methods.

## Proposal 2: Deep per-layer instrumentation

Fill in the per-method and per-iterator visibility gaps so any slow query can be attributed end-to-end through frontend, querier, livestore, fetch, and engine layers. Three parts, mirroring the subsections below: span coverage, block-level work counters, and per-query log enrichment.

### Per-layer span coverage

**A) Querier spans for currently-uninstrumented methods:**   
Several querier methods have no span of their own and inherit only from the HTTP handler. Add `tracer.Start` to the methods that lack one, in `querier/querier.go` and `querier_query_range.go`. Methods to instrument: `SearchBlock`, `SearchRecent`, `SearchTags*`, `SearchTagValues*`, `internalTagsSearchBlockV2`, `internalTagValuesSearchBlock(V2)`, `QueryRange`, `queryRangeRecent`, `queryBlock`.  
Attach `tenant`, query text (where applicable), range, and the final response stats (already computed locally) as attributes.

**B) Span on `MetricsEvaluator.Do[SpansOnly]`:**   
The metrics-query evaluation entry points in `traceql/engine_metrics.go` create no span today, so the metrics fan-out is invisible in traces. Wrap each entry with a span carrying `pipeline`, `needsFullTrace`, `spanOnlyFetch`, `maxExemplars`, and the start/end timestamps on open. Set `spansTotal`, `bytes`, and `exemplarCount` at span end.

**C) Livestore span coverage and per-op latency histogram:**   
Top-level live-store RPC methods on `LiveStore` and several `instance.*` read paths (`FindByTraceID`, `SearchTags`/`SearchTagValues` v1, `QueryRange` top-level) have no spans. Add the missing spans and enrich the existing per-block spans with `blockSize`, `bytesInspected` (using the stats struct from Proposal 1), and error attributes. Add `tempo_live_store_request_duration_seconds{op}` and `tempo_live_store_request_errors_total{op, reason}` so per-method latency and error rate are observable in the `tempo_live_store_*` namespace. Add a `tenant` label to `tempo_live_store_lagged_requests_total`.

### Block-level instrumentation

**D) Backend transport role label and size histogram:**  
`tempodb_backend_request_duration_seconds` has no `role` label, and its `operation` label is actually the HTTP method. Add a `role` label sourced from a context value the cache layer sets when it dispatches a backend read on miss. Add a new `tempodb_backend_request_size_bytes{operation, role, status_code}` histogram so a 16 MiB page miss can be distinguished from a 1 KiB index read. Plan a label-name migration for the misleading `operation` field; keep the current name for now to avoid breakage.

**E) Iterator counters:**   
The decisions that drive row-group and page skipping in `parquetquery/iters.go` (`seekRowGroup`/`KeepColumnChunk`, `seekPages`/`KeepPage`) are silent today. Add per-iterator counters of row groups inspected/skipped, pages inspected/skipped, values matched, and dictionary short-circuits. Surface them via the `FetchSpansStats` struct from Proposal 1 for per-query correlation. Also surface them as cell-wide Prometheus counters: `tempo_block_rowgroups_total{outcome}`, `tempo_block_pages_total{outcome}`, and `tempo_block_dictionary_shortcircuit_total`.

**F) Bloom-filter hit/miss:**  
`checkBloom` at `tempodb/encoding/vparquet5/block_findtracebyid.go:36` creates a span but does not record the `found` bool from `filter.Test(id)`. Add `bloom_hit` (boolean) as a span attribute and add `tempo_block_bloom_lookups_total{outcome}` where outcome is `positive`, `negative`, or `skipped`. The `skipped` outcome captures the compaction-level-based suppression in `tempodb/backend/cache/cache.go`.

### Per-query log enrichment

**G) Per-query log fields from fetch layer:**  
Extend the per-request response log lines on the frontend with the new `SearchMetrics` fields introduced in Proposal 1\. Add: `cache_hits`, `cache_misses`, `cache_bytes`, `backend_reads`, `backend_bytes`, `row_groups_inspected`, `row_groups_skipped`, `pages_inspected`, `pages_skipped`, plus a derived `cache_hit_ratio`. This is the proposal's main user-visible payoff: a slow-query investigation becomes a single Loki query.

### Central code changes

```go
// pkg/parquetquery/iters.go (new counter fields on SyncIterator)

type SyncIterator struct {
    // existing fields ...
    rowGroupsInspected      uint32
    rowGroupsSkipped        uint32
    pagesInspected          uint32
    pagesSkipped            uint32
    valuesMatched           uint64
    dictionaryShortCircuits uint64
}
```

```go
// tempodb/backend/instrumentation/backend_transports.go

var backendRequestSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
    Namespace: "tempodb",
    Name:      "backend_request_size_bytes",
    Help:      "Size of backend storage requests by role.",
    Buckets:   prometheus.ExponentialBuckets(64, 2, 24), // 64B .. 128MiB
}, []string{"operation", "role", "status_code"})

// existing tempodb_backend_request_duration_seconds gets the same `role` label.
```

```go
// pkg/traceql/engine_metrics.go

func (e *MetricsEvaluator) Do(ctx context.Context, ...) {
    ctx, span := tracer.Start(ctx, "traceql.MetricsEvaluator.Do",
        trace.WithAttributes(
            attribute.String("pipeline", e.metricsPipeline.String()),
            attribute.Bool("needsFullTrace", e.needsFullTrace),
            attribute.Bool("spanOnlyFetch", e.spanOnlyFetch),
            attribute.Int("maxExemplars", e.maxExemplars),
        ))
    defer span.End()
    // ... existing body ...
    span.SetAttributes(
        attribute.Int64("spansTotal", int64(e.spansTotal)),
        attribute.Int64("bytes", int64(e.bytes)),
        attribute.Int("exemplarCount", e.exemplarCount),
    )
}
```

New livestore Prometheus metrics: `tempo_live_store_request_duration_seconds{op}` (histogram, native); `tempo_live_store_request_errors_total{op, reason}` (counter); `tempo_live_store_lagged_requests_total` gains a `tenant` label.

New fetch-layer Prometheus metrics: `tempo_block_rowgroups_total{outcome}` (counter, outcome in {scanned, skipped}); `tempo_block_pages_total{outcome}` (counter, outcome in {scanned, skipped}); `tempo_block_dictionary_shortcircuit_total` (counter); `tempo_block_bloom_lookups_total{outcome}` (counter, outcome in {positive, negative, skipped}).

New frontend response-log fields (appended to the existing `level.Info` lines): `cache_hits`, `cache_misses`, `cache_bytes`, `object_store_reads_total`, `backend_bytes`, `row_groups_inspected`, `row_groups_skipped`, `pages_inspected`, `pages_skipped`.

## Proposal 3: Correctness fixes and supplementary visibility

Close remaining correctness gaps and add small bits of visibility that round out the picture. Two parts: correctness corrections (live-traces InspectedBytes, missing metric metadata) and additional visibility (sampler scaling, worker-pool work).

### Correctness corrections

**A) Trace-by-ID live-traces InspectedBytes:**  
The in-memory live-traces branch of `instance.FindByTraceID` at `modules/livestore/instance_search.go:653-666` adds nothing to `metrics.InspectedBytes`. The existing code comment acknowledges this as intentional but inaccurate. Compute a best-effort byte estimate from the matched live trace and add it to the response `InspectedBytes` and to the `tempo_live_store_query_inspected_bytes_total{op="trace_by_id"}` counter. This restores accuracy for trace-by-ID throughput charts when results are served from live data.

### Additional visibility

**B) Sampler scaling visibility:**  
The TraceQL metrics evaluator applies `TraceSampler.FinalScalingFactor()` and `SpanSampler.FinalScalingFactor()` to produced series at `traceql/engine_metrics.go:1497`. Neither the factor nor the pre-scaling counts are surfaced anywhere. Add `trace_sampler_factor`, `span_sampler_factor`, and `spans_total_pre_scaling` as attributes on the metrics-evaluator span introduced by Proposal 2 (B).

**D) Pool work observability:**   
The tempodb worker pool at `tempodb/pool/pool.go` exposes only queue length and max as gauges.Per-job duration and per-job errors are not measured today, so total job cost is invisible: the existing `tempodb_backend_request_duration_seconds` shows object-storage latency, but not the rest of a job (cache lookups, parquet decoding, TraceQL evaluation, per-job overhead). Wrap the job-execution function with a duration histogram and an error counter.

### Central code changes

```go
// tempodb/pool/pool.go

var workDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
    Namespace:                       "tempodb",
    Name:                            "work_duration_seconds",
    Help:                            "Per-job duration and outcome of the tempodb worker pool.",
    NativeHistogramBucketFactor:     1.1,
    NativeHistogramMaxBucketNumber:  100,
    NativeHistogramMinResetDuration: time.Hour,
}, []string{"outcome"}) // outcome in {success, error, cancelled}

var workErrors = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: "tempodb",
    Name:      "work_errors_total",
    Help:      "Tempodb worker pool job errors by reason.",
}, []string{"reason"})
```

```go
// modules/livestore/instance_search.go (live-traces branch of FindByTraceID)

if matched, ok := i.liveTraces.Traces[hash]; ok {
    // ... existing combiner.Consume call ...
    inspectedBytes := estimateLiveTraceBytes(matched)
    metrics.InspectedBytes += inspectedBytes
    instance.tempoLiveStoreQueryInspectedBytes.
        WithLabelValues(i.instanceID, opTraceByID).
        Add(float64(inspectedBytes))
}
```

New metrics-evaluator span attributes (added at the span introduced by Proposal 2 (b)): `trace_sampler_factor` (float), `span_sampler_factor` (float), `spans_total_pre_scaling` (uint64).

# Consensus

*To be drafted after proposals are written and reviewed.*

# Appendix

## Existing signals

Summary of the observability surface present on the read path today, by layer. Three tables per layer: Prometheus metrics, OpenTelemetry traces (spans), and structured logs.

Naming convention in the log tables: each `level.Info` "request"/"response" line pair counts as one row. The surrounding error/warn lines are grouped by family (for example, "access-control failures") rather than enumerated individually.

### Query-Frontend

**Metrics** (`modules/frontend/` \+ dskit middleware on the query-frontend container)

| Metric | Type | Labels | What it measures |
| :---- | :---- | :---- | :---- |
| `tempo_query_frontend_queries_total` | Counter | `tenant`, `op`, `result` | Total queries received; pair with `…_within_slo_total` for SLO burn. |
| `tempo_query_frontend_queries_within_slo_total` | Counter | `tenant`, `op`, `result` | Queries classified as within their SLO. |
| `tempo_query_frontend_bytes_inspected_total` | Counter | `tenant`, `op` | Sum of `SearchMetrics.InspectedBytes` per query. |
| `tempo_query_frontend_bytes_processed_per_second` | Counter | `tenant`, `op` | Per-query throughput accumulated into a counter. |
| `tempo_query_frontend_jobs_per_query` | Histogram | `op` | Number of shard-jobs planned per query. |
| `tempo_query_frontend_retries` | Histogram | none | Retry count per request. |
| `tempo_query_frontend_queue_length` | Gauge | `user` | Queue depth per tenant. |
| `tempo_query_frontend_queue_duration_seconds` | Histogram | none | Time spent in queue before dispatch. |
| `tempo_query_frontend_batch_weight` | Histogram | `user` | Weight per dispatched batch (not per request). |
| `tempo_query_frontend_actual_batch_size` | Histogram | none | Requests per dispatched batch. |
| `tempo_query_frontend_discarded_requests_total` | Counter | `user` | Requests discarded due to full queue. |
| `tempo_query_frontend_connected_clients` | Gauge | none | Querier workers currently connected. |
| `tempo_query_frontend_mcp_calls_total` | Counter | `tool` | MCP tool dispatch counter. |
| `tempo_request_duration_seconds` | Histogram | `method`, `route`, `status_code`, `ws` | Per-route latency on every HTTP+gRPC endpoint. Canonical RED metric. |
| `tempo_inflight_requests` | Gauge | `method`, `route` | Current inflight per route. |

**Traces** (spans created by `modules/frontend/` code)

| Span | Attributes / events | What it represents |
| :---- | :---- | :---- |
| `frontend.ShardSearch` / `frontend.ShardQuery` / `frontend.ShardSearchTags` | none | Per-query sharding work (no attributes today). |
| `frontend.QueryRangeSharder.range` / `…instant` | `totalJobs`, `totalBlocks`, `totalBlockBytes` | The only sharder span that carries response aggregates. |
| `httpCollector.RoundTrip` / `GRPCCollector.RoundTrip` | events: `next.RoundTrip done`, `consumeAndCombineResponses done`, `combiner.HTTPFinal done` / `combiner.GRPCFinal() done` | Per-query collector lifetime spanning retries \+ combines. |
| `frontend.Retry` | per-retry event with `try`, `status_code`, `errMsg` | Wraps the inner pipeline; one span per request, one event per retry. |
| `queued` | none | Span lifetime ≈ queue dwell time. |

**Logs** (structured logs emitted by frontend handlers)

| Log family | Level | `msg` value(s) | Key fields |
| :---- | :---- | :---- | :---- |
| Generic per-HTTP-request stats | Info | `"query stats"` | `tenant`, `method`, `traceID`, `url`, `duration`, `status`, `response_size`, optional configured headers |
| Search | Info | `"search request"` → `"search response"` (+ `"… - no resp"`, `"… - no metrics"`) | `query`, `range_seconds`, `duration_seconds`, `request_throughput`, `inspected_bytes`, `inspected_traces`, `inspected_spans`, `total_*`, `completed_*`, `status_code` |
| Query instant | Info | `"query instant request"` → `"query instant results"` | Same set \+ `partial_status`, `partial_message`, `num_response_series` |
| Query range | Info | `"query range request"` → `"query range response"` | Same set \+ `max_series`, `mode`, `step` |
| Trace-by-ID | Info | `"trace id request"` → `"trace id response"` | `path`, `inspected_bytes`, `request_throughput`, `duration_seconds`, `err` |
| Tags / tag values | Info | `"search tag request"` → `"search tag response"`; `"search tag values request"` → `"search tag values response"` | `handler`, `scope` / `tag`\+`query`, `range_seconds`, `inspected_bytes`, `request_throughput` |
| Error / access-control / parse failures | Error | varies by handler (e.g. `"search streaming: access control handling failed"`) | `err` only |
| Per-block sharder errors | Error / Warn | `"failed to convert dedicated columns in query range sharder. skipping"`, `"invalid start/step end. skipping"`, `"failed to cloneRequestForQuerirs in the query range sharder. skipping"` | `block`, `err` (and range fields for the Warn) |

### Querier

**Metrics** (`modules/querier/` \+ dskit middleware on the querier container)

| Metric | Type | Labels | What it measures |
| :---- | :---- | :---- | :---- |
| `tempo_querier_livestore_clients` | Gauge | none | Live-store client pool size. |
| `tempo_querier_external_endpoint_request_duration_seconds` | Histogram | `status_code` | Latency of external trace-by-ID forwarding. |
| `tempo_querier_worker_request_executed_total` | Counter | none | Process-wide request count; not labeled by tenant or op. |
| `tempo_request_duration_seconds{container="querier"}` | Histogram | `method`, `route`, `status_code`, `ws` | Per-route latency (13 querier routes). Same metric family as frontend. |

**Traces** (spans from querier handlers \+ worker)

| Span | Attributes / events | What it represents |
| :---- | :---- | :---- |
| `Querier.TraceByIDHandler[V2]` | event `validated request` with `blockStart/End`, `queryMode`, `apiVersion` | HTTP handler entry. |
| `Querier.SearchHandler` | `requestURI`, `isSearchBlock`, `SearchRequest`/`SearchRequestBlock` (full string) |  |
| `Querier.SearchTagsHandler` (used by V1 and V2; name collision) | none |  |
| `Querier.QueryRangeHandler` | `query`, `step`, `interval`, `inspectedBytes`, `inspectedSpans` (only handler that attaches response stats) |  |
| `Querier.FindTraceByID` | `queryMode`; events `searching live-stores`, `done searching live-stores` (`found`, `combinedSpans`, `combinedTraces`), `searching store`, `done searching store` (`foundPartialTraces`), `searching external`, `done searching external` (`spansFound`) | Per-call FindTraceByID work. |
| `Querier.forLiveStoreRing` / `forLiveStoreMetricsRing` | none | Ring fan-out wrappers. |
| `querier_processor_runRequest` | none | Worker-to-handler bridge span. |
| (auto) gRPC client spans on querier↔livestore | (otelgrpc) | One per RPC; emitted automatically via `otelgrpc.NewClientHandler()`. |
| (auto) HTTP client spans for external trace-by-ID | (otelhttp) | Emitted automatically via `otelhttp.NewTransport()`. |

**Logs** (structured logs emitted by querier code)

| Log family | Level | `msg` value(s) | Key fields |
| :---- | :---- | :---- | :---- |
| Tag/tag-value collector limit warnings | Warn | `"Search tags exceeded limit, reduce cardinality or size of tags"`, `"Search of tag values exceeded limit, reduce cardinality or size of tags"` | `orgID`, `stopReason`, sometimes `tag` |
| `forLiveStoreRing` context error (malformed: passes msg as key, not `"msg"`) | Debug | (key) `"forLiveStoreRing context error"` / `"forLiveStoreMetricsRing context error"` | `ctx.Err()` |
| Query-range live-store fan-out error | Error | `"error querying live-stores in Querier.queryRangeRecent"` | `err` |
| Worker connection lifecycle | Info / Error | several msg strings (see `modules/querier/worker/`) | `addr`, `err`, `totalConcurrency` |
| Worker request loop errors | Error / Debug | `"failed to notify querier shutdown to query-frontend"`, `"error contacting frontend"`, `"error processing requests"`, `"error running requests"`, `"error running  batched requests"` (double space typo), `"error processing query"` (oversize response) | `address`, `err` |

Note: the querier emits **no `level.Info` summary** of a successful read at this layer. Success-path observability comes from the frontend log lines above and the gRPC server middleware (`tempo_request_duration_seconds`).

### Livestore

**Metrics** (`modules/livestore/`, read-path-relevant subset)

| Metric | Type | Labels | What it measures |
| :---- | :---- | :---- | :---- |
| `tempo_live_store_query_inspected_bytes_total` | Counter | `tenant`, `op` | Bytes inspected per tenant per op (`search`, `search_tags`, `search_tag_values`, `trace_by_id`, `query_range`). `search_tag_values` op shared between V1 and V2. |
| `tempo_live_store_lagged_requests_total` | Counter | `route` | Read RPCs that queried past the Kafka consumer horizon. Only `SearchRecent` and `QueryRange` increment. No `tenant` label. |
| `tempo_live_store_ready` | Gauge | none | Whether the live-store will serve reads. |
| `tempo_live_store_catch_up_duration_seconds` | Gauge | none | Startup catch-up duration. |
| `tempo_live_store_live_traces`, `tempo_live_store_live_trace_bytes` | Gauge | `tenant` | Snapshot of in-memory live-trace count/bytes (set on cut). Indirectly bounds the "live traces" branch in FindTraceByID. |
| `tempo_live_store_partition_owned` | Gauge | `partition`, `zone` | Which partition this live-store owns (crucial for routing). |
| `tempo_ingest_storage_reader_receive_delay_seconds` | Histogram | none | Consumer freshness, per process. Live-store owns one partition per process, so this is effectively per-partition but the partition is not on the series. |
| `tempo_ingest_group_partition_lag[_seconds]` | Gauge | `group`, `partition` | Per-partition Kafka lag. |
| `tempo_request_duration_seconds{container="live-store"}` | Histogram | `method`, `route`, `status_code`, `ws` | Per-gRPC-method latency on every read RPC (15 live-store routes). The only place per-method live-store latency is observable today. |

**Traces** (spans from `modules/livestore/`)

| Span | Attributes / events | What it represents |
| :---- | :---- | :---- |
| `instance.iterateBlocks` | `tenant` | Parent for every per-block fan-out. |
| `process.headBlock` / `process.walBlock` / `process.completeBlock` | `blockID` only | Per-block work. No size, duration, error, or bytes attributes. |
| `instance.Search` | event `SearchRequest` with full request string (high cardinality) | Search per-instance. |
| `instance.SearchTagsV2` / `instance.SearchTagValuesV2` | none directly (V2 tag-value's `cached` attribute lands on the per-block span, not this one) | Tag/tag-value per-instance. |
| `instance.QueryRange.WALBlock` / `instance.QueryRange.CompleteBlock` | `block`, `blockSize`, `cached` (complete-block only) | Per-block metrics-query work. |
| `LocalBlock.*` (`FindTraceByID`, `Search`, `Fetch`, `FetchSpans`, `SearchTags*`, `SearchTagValues*`, `FetchTagValues`, `FetchTagNames`) | none | Wrapper spans on complete blocks; no attributes. |

Notable absences: the top-level `LiveStore.*` RPC methods, `instance.FindByTraceID`, `instance.SearchTags`/`SearchTagValues` v1, and `instance.QueryRange` (top-level) do not create their own spans.

**Logs** (structured logs from `modules/livestore/`)

| Log family | Level | `msg` value(s) | Key fields |
| :---- | :---- | :---- | :---- |
| Per-block panic recovery | Error | `"panic in iterateBlocks head block"`, `"panic in iterateBlocks wal block"`, `"panic in iterateBlocks complete block"` | `blockID`, `panic`, `stack` |
| Read-path top-level errors | Error | `"error in Search"`, `"error in SearchTagsV2"`, `"error in SearchTagValues"`, `"error in SearchTagValuesV2"`, `"error in FindTraceByID"`, `"error in QueryRange"` | `err` |
| Tag-search collector limit warnings | Warn | `"Search of tags exceeded limit, …"`, `"Search of tag values exceeded limit, …"`, `"size of tag values exceeded limit, …"` | `tag`, `orgID`/`tenant`, `stopReason`/`limit`/`size` |
| Block-level capability warnings | Warn | `"block does not support search"` | `blockID` |
| Disk-cache failures | Warn | `"GetDiskCache failed"`, `"GetDiskCache unmarshal failed"`, `"SetDiskCache failed"`, `"reading local query cache failed"`, `"writing local query cache failed"` | `block`, `err` |
| Kafka lag tripped (only emitted for `SearchRecent` and `QueryRange`) | Info | `"isLagged tripped"` | `route`, `query`, `end_unix_nano`, `now_unix_nano`, `time_lag_sec`, `offset_lag`, `last_record_unix_nano` |

Note: no `level.Info` log line covers a successful query at the livestore layer. All log emissions are error/warn paths.

## Added signals

Signals introduced or extended by Proposals 1, 2, and 3, organized in the same layout as the Existing signals section above: three layer subsections, each with Metrics, Traces, and Logs tables. Fetch-layer, cache, backend-transport, engine, and pool additions are folded into the Livestore subsection (the layer where they fire on the read path). Rows tagged `[modified]` extend a signal that already exists in the Existing signals tables; all others are brand-new.

### Query-Frontend

**Metrics**: no new Prometheus metrics at the frontend. Frontend additions land on existing sharder spans and on existing per-query response log lines (see below).

**Traces**

| Span | Attributes / events | What it represents |
| :---- | :---- | :---- |
| `frontend.ShardSearch` / `frontend.ShardQuery` / `frontend.ShardSearchTags` / `frontend.QueryRangeSharder.range` / `…instant` | adds `query_weight`, `query_conditions`, `query_regex_conditions`, `query_has_or`, `query_needs_full_trace`, `query_select_all`, `query_type` | Query shape exposed on sharder spans (Proposal 1D). [modified] |

**Logs**

| Log family | Level | `msg` value(s) | Key fields |
| :---- | :---- | :---- | :---- |
| Per-query response lines (Search, Query instant, Query range, Trace-by-ID, Tags, Tag values) | Info | unchanged: `"search response"`, `"query instant results"`, `"query range response"`, `"trace id response"`, `"search tag response"`, `"search tag values response"` | adds query shape (Proposal 1D): `query_weight`, `query_conditions`, `query_regex_conditions`, `query_has_or`, `query_needs_full_trace`, `query_select_all`, `query_type`. adds per-query work (Proposal 2G): `cache_hits`, `cache_misses`, `cache_bytes`, `cache_hit_ratio`, `object_store_reads_total`, `backend_bytes`, `row_groups_inspected`, `row_groups_skipped`, `pages_inspected`, `pages_skipped`. [modified] |

### Querier

**Metrics**: no new Prometheus metrics at the querier layer. The proposed Prometheus signals all fire below this layer; see Livestore.

**Traces**

| Span | Attributes / events | What it represents |
| :---- | :---- | :---- |
| `Querier.SearchBlock`, `Querier.SearchRecent`, `Querier.SearchTags`, `Querier.SearchTagsV2`, `Querier.SearchTagValues`, `Querier.SearchTagValuesV2`, `Querier.internalTagsSearchBlockV2`, `Querier.internalTagValuesSearchBlock`, `Querier.internalTagValuesSearchBlockV2`, `Querier.QueryRange`, `Querier.queryRangeRecent`, `Querier.queryBlock` | `tenant`, query text (where applicable), range, final response stats | Per-method querier spans for methods that currently inherit only from the HTTP handler (Proposal 2A). |
| `traceql.MetricsEvaluator.Do` and `traceql.MetricsEvaluator.DoSpansOnly` | open: `pipeline`, `needsFullTrace`, `spanOnlyFetch`, `maxExemplars`. end: `spansTotal`, `bytes`, `exemplarCount`, `trace_sampler_factor`, `span_sampler_factor`, `spans_total_pre_scaling` | Metrics-query evaluator span (Proposal 2B), with sampler-scaling attributes added on top (Proposal 3B). |

**Logs**: no new querier-side log lines.

### Livestore

Folds in fetch-layer, cache, backend-transport, engine-evaluator, and pool additions, since they all fire on the livestore read path.

**Metrics**

| Metric | Type | Labels | What it measures |
| :---- | :---- | :---- | :---- |
| `tempodb_cache_requests_total` | Counter | `role`, `outcome` (`hit`, `miss`) | Cache lookups per role and outcome at the role-aware router (Proposal 1C). |
| `tempodb_cache_request_bytes_total` | Counter | `role`, `outcome` | Bytes served by cache on hit / fetched on miss, by role (Proposal 1C). |
| `tempodb_cache_store_error_bytes_total` | Counter | `class`, `role` | Parquet cache bytes lost to backend read errors (Proposal 1C, PR #7137). |
| `tempodb_backend_request_size_bytes` | Histogram | `operation`, `role`, `status_code` | Size distribution of backend storage requests; lets a 16 MiB page miss be distinguished from a 1 KiB index read (Proposal 2D). |
| `tempodb_backend_request_duration_seconds` | Histogram | `operation`, `role`, `status_code` | `role` label added so per-role S3 latency is observable (Proposal 2D). [modified] |
| `tempo_block_rowgroups_total` | Counter | `outcome` (`scanned`, `skipped`) | Row groups inspected vs skipped during iteration (Proposal 2E). |
| `tempo_block_pages_total` | Counter | `outcome` (`scanned`, `skipped`) | Pages inspected vs skipped during iteration (Proposal 2E). |
| `tempo_block_dictionary_shortcircuit_total` | Counter | none | Dictionary fast-path short-circuits during column-chunk evaluation (Proposal 2E). |
| `tempo_block_bloom_lookups_total` | Counter | `outcome` (`positive`, `negative`, `skipped`) | Bloom-filter lookup outcomes for trace-by-ID (Proposal 2F). |
| `tempo_live_store_request_duration_seconds` | Histogram (native) | `op` | Per-method livestore RPC latency in the `tempo_live_store_*` namespace (Proposal 2C). |
| `tempo_live_store_request_errors_total` | Counter | `op`, `reason` | Per-method livestore RPC errors by reason (Proposal 2C). |
| `tempo_live_store_lagged_requests_total` | Counter | `route`, `tenant` | `tenant` label added so lag can be attributed per tenant (Proposal 2C). [modified] |
| `tempo_live_store_query_inspected_bytes_total{op="trace_by_id"}` | Counter | `tenant`, `op` | Now also includes the live-traces branch contribution; the metric becomes accurate when trace-by-ID hits in-memory live data (Proposal 3A). [modified] |
| `tempodb_work_duration_seconds` | Histogram (native) | `outcome` (`success`, `error`, `cancelled`) | Per-job duration of the tempodb worker pool (Proposal 3D). |
| `tempodb_work_errors_total` | Counter | `reason` | Tempodb worker pool job errors by reason (Proposal 3D). |

**Traces**

| Span | Attributes / events | What it represents |
| :---- | :---- | :---- |
| `LiveStore.FindTraceByID`, `LiveStore.SearchRecent`, `LiveStore.SearchBlock`, `LiveStore.SearchTags`, `LiveStore.SearchTagsV2`, `LiveStore.SearchTagValues`, `LiveStore.SearchTagValuesV2`, `LiveStore.QueryRange` | tenant, request, response stats (where applicable) | Top-level livestore RPC spans, none of which exist today (Proposal 2C). |
| `instance.FindByTraceID`, `instance.SearchTags` (v1), `instance.SearchTagValues` (v1), `instance.QueryRange` (top-level) | tenant, request, response stats | Instance-level read entry spans currently missing (Proposal 2C). |
| `process.headBlock` / `process.walBlock` / `process.completeBlock` | adds `blockSize`, `bytesInspected`, error attributes (existing spans currently carry only `blockID`) | Richer per-block work attribution (Proposal 2C). [modified] |
| `parquet.backendBlock.Fetch` at `vparquetX/block_traceql.go` | open: `blockID`, `tenantID`, `blockSize`, `numConditions`, `allConditions`, `secondPassSelectAll`. end: `rowGroupsInspected`, `rowGroupsSkipped`, `pagesInspected`, `pagesSkipped`, `cacheHits`, `cacheMisses`, `inspectedBytes` | Per-query, per-block fetch span (Proposal 1E). |
| `block_traceql_fetch.go:FetchSpans` | same attribute set as `parquet.backendBlock.Fetch` | Span-only fetch path used by TraceQL metrics queries (Proposal 1E). |
| `block_autocomplete.go:FetchTagNames`, `block_autocomplete.go:FetchTagValues` | same attribute set | Autocomplete-driven parquet scans currently untraced (Proposal 1E). |
| Equivalent `wal_block.go` counterparts of all four spans above | same attribute sets | Uniform trace coverage across WAL and complete blocks (Proposal 1E). |
| `checkBloom` at `block_findtracebyid.go` (existing span) | adds `bloom_hit` (boolean) | Bloom-filter outcome recorded per trace-by-ID (Proposal 2F). [modified] |

**Logs**: no new log lines proposed at the livestore layer. Proposal 3A surfaces as a counter correction (see `tempo_live_store_query_inspected_bytes_total` in the Metrics table above), not a log change.
