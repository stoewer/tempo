# Implementation plan: PR 2 — block-level counters (2.E + 2.F)

| |                                                                                                       |
| :--- |:------------------------------------------------------------------------------------------------------|
| **Issue** | grafana/tempo-squad#1274                                                                              |
| **Epic** | grafana/tempo-squad#1263 (Project Sleipnir: Observability Improvements)                               |
| **Author** | Adrian Stoewer                                                                                        |
| **Created** | 2026-07-01                                                                                            |
| **Status** | Draft                                                                                                 |
| **Source design doc** | `design-doc-info/Design Doc: Observability for Tempo Read-Path Performance.md` (Proposals 2.E, 2.F)   |

Iterator counters and bloom-filter hit/miss. Ships as one PR titled
*"Block-level read-path counters: iterator work + bloom outcomes"*.

## Scope

- **Proposal 2.E** — per-iterator row-group / page / values-matched /
  dictionary-short-circuit counters. Surfaced on `FetchSpansStats` for
  per-query correlation and as process-wide Prometheus counters.
- **Proposal 2.F** — bloom-filter hit/miss span attribute + process-wide
  Prometheus counter.

Companion PRs (see `implementation-plan-2D.md` and
`implementation-plan-proposal1-gaps.md`) cover the backend transport
role label (2.D) and the Proposal 1 producer gaps respectively.

## Relationship to PR 1

Independent commit chain — no code ordering dependency on PR 1. Both PRs
extend `FetchSpansStats`; if PR 1 lands first, PR 2 rebases through a
mechanical conflict on the struct. Either order is fine.

## Pre-conditions

- `traceql.FetchSpansStats` in `pkg/traceql/storage.go` carries
  `RowGroupsInspected`, `RowGroupsSkipped`, `PagesInspected`,
  `PagesSkipped`, `ValuesMatched` fields. **Zero today** — this PR
  populates them.
- `populateMetricsFromFetchStats` already routes the row-group / page
  fields into `SearchMetrics.AdditionalMetrics` via `addIfNonZero`.
  `ValuesMatched` is **not yet** routed; this PR adds the key + wiring.
- Span attribute names follow camelCase; log field names follow
  snake_case.
- Cell-wide `tempodb_cache_*` counters exist (#7504). This PR adds the
  `tempo_block_*` family.

## Phase split

Two implementation phases plus verification. 2.F is smaller and
introduces the shared `blockmetrics` package; **Phase 2 depends on the
`blockmetrics` package landed in Phase 1**, so the order is required,
not merely convenient.

| Phase | Scope | Risk |
|---|---|---|
| 1 | 2.F — bloom hit/miss span attr + counter (positive/negative only). Introduces `blockmetrics` package. | Cheap; per-block trace-by-ID path only. |
| 2 | 2.E — predicate interface widening + iterator counters. | Larger; touches every Predicate impl and the `SyncIterator` hot path. |
| 3 | Distributed docker-compose verification | n/a |

---

## Phase 1 — Proposal 2.F: bloom-filter hit/miss

### Files

- `tempodb/encoding/blockmetrics/blockmetrics.go` (new package) —
  process-wide counter registrations shared between vp4 and vp5.
- `tempodb/encoding/vparquet5/block_findtracebyid.go`
- `tempodb/encoding/vparquet4/block_findtracebyid.go` (backport,
  matches the vp4/vp5 symmetry from #7504)

### Change shape

1. **New package** `tempodb/encoding/blockmetrics`. Single import
   surface for encoding-level Prometheus counters:
   ```go
   package blockmetrics

   var BloomLookups = promauto.NewCounterVec(prometheus.CounterOpts{
       Namespace: "tempo",
       Name:      "block_bloom_lookups_total",
       Help:      "Bloom-filter lookup outcomes for trace-by-ID.",
   }, []string{"outcome"})
   ```
   Outcomes shipped: `positive`, `negative`. `skipped` is deferred (see
   open question).

2. **Hit/miss attribute + counter.** In vp5 `checkBloom`:
   ```go
   found = filter.Test(id)
   span.SetAttributes(attribute.Bool("bloomHit", found))
   if found {
       blockmetrics.BloomLookups.WithLabelValues("positive").Inc()
   } else {
       blockmetrics.BloomLookups.WithLabelValues("negative").Inc()
   }
   return found, nil
   ```
   `bloomHit` is camelCase, matching the #7504 span-attribute
   convention. Design-doc text writes `bloom_hit`; note the deviation
   in the PR description.

3. **vp4 backport.** Same change in
   `vparquet4/block_findtracebyid.go::checkBloom`. Same counter (shared
   package, single registration).

4. **Tests.** Unit test running `checkBloom` against a known bloom
   filter (positive ID + random ID) and asserting counter increments
   via `testutil.ToFloat64`. Mirror in vp4.

5. **TODO sweep.** End of phase:
   `grep -rn 'TODO(issue/1274)' tempodb/encoding/vparquet{4,5}/block_findtracebyid.go`.
   Confirm zero matches.

### Risks / open questions

- **`skipped` outcome deferred.** Design doc says "skipped captures the
  compaction-level-based suppression in cache.go", but that suppression
  bypasses *the cache*, not the bloom test — `checkBloom` always runs
  `filter.Test`. Shipping `skipped` today would make the three outcomes
  incoherent. Follow-up: clarify with the design-doc author whether the
  intent was to also short-circuit the test when bloom caching is
  rejected (behavior change).
- **vp4 backport exceeds design-doc scope.** Design doc names vp5 only;
  we backport to preserve Phase 1 symmetry. Note in PR description.

---

## Phase 2 — Proposal 2.E: iterator counters

### Files

- `pkg/parquetquery/predicates.go` — widen `Predicate.KeepColumnChunk`
  to return a `KeepResult` enum.
- `pkg/parquetquery/iters.go` — shared counter aggregator struct +
  increment sites + `SyncIteratorOptCounters` option + counter-hook
  registration (`RegisterCounterHooks`).
- `pkg/parquetquerygen/predicates.go` — update templates.
  **Regenerate with `make gen-parquet-query`** (3 templates emitting
  26 concrete predicates in `pkg/parquetquery/predicates.gen.go`).
- `tempodb/encoding/blockmetrics/blockmetrics.go` — grow with row-group
  / page / dictionary-short-circuit counters + `init()` wiring hooks
  into `pkg/parquetquery`.
- `pkg/traceql/storage.go` — add `DictionaryShortCircuits uint64` to
  `FetchSpansStats`; extend `populateMetricsFromFetchStats` (and the
  engine's `accumulateFetchStats`) to route `ValuesMatched` and
  `DictionaryShortCircuits` via `addIfNonZero`.
- `pkg/tempopb/additional_metrics_keys.go` — new constants
  `AdditionalMetricValuesMatched` and
  `AdditionalMetricDictionaryShortCircuits` (camelCase keys).
- `tempodb/encoding/vparquet5/block_search.go` — `makeIterFunc` creates
  the counters aggregator and passes it via `SyncIteratorOptCounters`.
- `tempodb/encoding/vparquet5/{block_traceql,block_traceql_fetch,wal_block,block_autocomplete}.go`
  — wire counters into `Stats()`. Drop the per-iterator half of the
  `TODO(issue/1274)` markers.
- `tempodb/encoding/vparquet3/block_search.go` — mechanical predicate
  update (vp3 has `reportValuesPredicate`).
- `tempodb/encoding/vparquet4/*.go` — vp4 backport (block_search,
  block_traceql, block_autocomplete, wal_block).

### Change shape

1. **`SyncIteratorCounters` — shared aggregator, no per-iterator fields.**
   Per-iterator state dropped in favor of one shared struct injected
   via `SyncIteratorOpt` and read at `Stats()` time.
   ```go
   // pkg/parquetquery/iters.go
   type SyncIteratorCounters struct {
       RowGroupsInspected      uint32
       RowGroupsSkipped        uint32
       PagesInspected          uint32
       PagesSkipped            uint32
       ValuesMatched           uint64
       DictionaryShortCircuits uint64
   }

   func SyncIteratorOptCounters(c *SyncIteratorCounters) SyncIteratorOpt { ... }
   ```
   `SyncIterator` is documented as not safe for concurrent use — no
   atomics needed. `LeftJoinIterator` and friends consume sibling
   iterators sequentially in one goroutine.

2. **Predicate interface widening.**
   ```go
   // pkg/parquetquery/predicates.go
   type KeepResult uint8

   const (
       KeepResultKeep KeepResult = iota
       KeepResultSkip                 // Rejected for reasons other than dictionary.
       KeepResultSkipDictionary       // Rejected because keepDictionary returned false.
   )

   type Predicate interface {
       KeepColumnChunk(*ColumnChunkHelper) KeepResult
       KeepPage(parquet.Page) bool
       KeepValue(parquet.Value) bool
   }
   ```
   No I/O cost; exact attribution.

   **Implementer sites — all enumerated:**
   - `pkg/parquetquery/predicates.go` (12 predicate types):
     - Consult dictionary (return `KeepResultSkipDictionary` when
       `keepDictionary` returns false): `ByteInPredicate`,
       `ByteNotInPredicate`, `regexPredicate`, `SubstringPredicate`,
       `GenericPredicate[T]`. (Five total.)
     - Never consult the dictionary: `IntBetweenPredicate` (column-
       index only), `SkipNilsPredicate`, `CallbackPredicate`,
       `NilValuePredicate`, `IncludeNilStringEqualPredicate`.
     - Delegating: `OrPredicate`, `InstrumentedPredicate`.
   - `pkg/parquetquerygen/predicates.go`: 3 templates emitting 26
     concrete predicates in `pkg/parquetquery/predicates.gen.go`.
     Regenerate with `make gen-parquet-query`.
   - `tempodb/encoding/vparquet3/block_search.go`:
     `reportValuesPredicate` (vp3 mechanical update).
   - `tempodb/encoding/vparquet4/block_search.go`:
     `reportValuesPredicate`.
   - `tempodb/encoding/vparquet4/block_traceql.go`: `samplingPredicate`
     (delegates to `p.inner.KeepColumnChunk`).
   - `tempodb/encoding/vparquet5/block_search.go`:
     `reportValuesPredicate`.
   - `tempodb/encoding/vparquet5/block_traceql.go`: `samplingPredicate`.
   - Sanity check:
     `grep -rn 'func.*KeepColumnChunk' --include='*.go' .` — every hit
     returns `KeepResult`, not `bool`.

   **Delegating-predicate semantics:**
   - `OrPredicate`:
     - **Nil child** is treated as `KeepResultKeep` — preserves the
       existing behavior where a `nil` predicate in `preds` means "no
       constraint from this branch, keep everything". Any non-nil child
       returning Keep also propagates as Keep.
     - `KeepResultSkipDictionary` **only** if **every** non-nil child
       returned `KeepResultSkipDictionary` (and at least one non-nil
       child exists). Matches the reading "the dictionary alone was
       sufficient to reject the entire OR". Conservative; avoids
       over-attribution.
     - `KeepResultSkip` in every other rejection case.
     - **Edge case:** empty `preds` returns `KeepResultSkip` (mirrors
       existing behavior — empty OR skips today). Add a unit test
       covering both empty `preds` and a mix of nil + non-nil children.
   - `InstrumentedPredicate`: pass through the child's result
     unchanged when `p.Pred != nil`. When `p.Pred == nil`, existing
     behavior short-circuits with keep-all — return `KeepResultKeep`.
   - `samplingPredicate` (vp4/vp5): pass through the inner predicate's
     result.

   **Test doubles to update:**
   - `pkg/parquetquery/predicates_test.go`:
     - `mockPredicate`: switch return type from `bool` to `KeepResult`.
     - `TestOrPredicateCallsKeepColumnChunk`: needs a **semantic pass**
       under the new OR rule (its assertions may change).
   - Sanity check:
     `grep -rn 'KeepColumnChunk' --include='*_test.go' .` for other test
     doubles.
   - Callers of `KeepColumnChunk` migrate to switch on the result.

3. **Increment sites in `iters.go`.** Two `KeepColumnChunk` call sites
   in row-group scanning; two mutually exclusive skip paths per row
   group (row-number vs predicate). Account at each site separately.
   (Line numbers omitted — will drift on rebase. Locate by function +
   branch description.)
   - **Row-number skip:** in `seekRowGroup`, the
     `CompareRowNumbers(seekTo, maxRN) != -1` branch that `continue`s
     past the current row group; and the analogous `popRowGroup`-driven
     branch in `next()`. Bump `RowGroupsSkipped` and `continue`.
   - **Predicate skip — two call sites:** the `filter.KeepColumnChunk`
     invocation in `seekRowGroup` (immediately after popping a row
     group) and the equivalent invocation in `next()`'s `popRowGroup`
     path. `seekPages` does not call `KeepColumnChunk`; it only calls
     `KeepPage`. Each `KeepColumnChunk` site calls a new
     `tryKeepColumnChunk` helper:
     ```go
     func (c *SyncIterator) tryKeepColumnChunk(cc *ColumnChunkHelper) bool {
         res := KeepResultKeep
         if c.filter != nil {
             res = c.filter.KeepColumnChunk(cc)
         }
         if c.counters != nil {
             switch res {
             case KeepResultKeep:
                 c.counters.RowGroupsInspected++
             case KeepResultSkipDictionary:
                 c.counters.RowGroupsSkipped++
                 c.counters.DictionaryShortCircuits++
             default:
                 c.counters.RowGroupsSkipped++
             }
         }
         return res == KeepResultKeep
     }
     ```
     Same shape for pages via `tryKeepPage` (wraps `KeepPage`, bumps
     `PagesInspected` / `PagesSkipped`). Row-number page skips bump
     `PagesSkipped` at their own `continue` site.
   - **Value matched** at the value-return site in `next()` — the
     `return c.curr, v, nil` line inside the inner value-consumption
     loop, reached only when a value passes `c.filter.KeepValue`:
     ```go
     if c.counters != nil {
         c.counters.ValuesMatched++
     }
     return c.curr, v, nil
     ```
     Only per-value increment. Cost is one predicted branch + one
     nil-check + one uint64 add. Benchmark sanity check in step 8.

4. **Process-wide counters** added to `blockmetrics`. Counters declared
   there with the `tempo_block_*` namespace; `pkg/parquetquery` stays
   generic. Wiring via a registration callback:
   ```go
   // pkg/parquetquery/iters.go
   var counterHooks CounterHooks

   type CounterHooks struct {
       RowGroupScanned        func()
       RowGroupSkipped        func()
       PageScanned            func()
       PageSkipped            func()
       DictionaryShortCircuit func()
   }

   // RegisterCounterHooks wires process-wide Prometheus increments from
   // outside the package. Zero hooks left as nil are a no-op.
   func RegisterCounterHooks(h CounterHooks) { counterHooks = h }
   ```
   ```go
   // tempodb/encoding/blockmetrics/blockmetrics.go
   var (
       RowGroupsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
           Namespace: "tempo",
           Name:      "block_rowgroups_total",
           Help:      "Row groups inspected or skipped during iteration.",
       }, []string{"outcome"})
       PagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
           Namespace: "tempo",
           Name:      "block_pages_total",
           Help:      "Pages inspected or skipped during iteration.",
       }, []string{"outcome"})
       DictionaryShortCircuit = promauto.NewCounter(prometheus.CounterOpts{
           Namespace: "tempo",
           Name:      "block_dictionary_shortcircuit_total",
           Help:      "Dictionary-based column-chunk short-circuits.",
       })
   )

   func init() {
       parquetquery.RegisterCounterHooks(parquetquery.CounterHooks{
           RowGroupScanned:        func() { RowGroupsTotal.WithLabelValues("scanned").Inc() },
           RowGroupSkipped:        func() { RowGroupsTotal.WithLabelValues("skipped").Inc() },
           PageScanned:            func() { PagesTotal.WithLabelValues("scanned").Inc() },
           PageSkipped:            func() { PagesTotal.WithLabelValues("skipped").Inc() },
           DictionaryShortCircuit: func() { DictionaryShortCircuit.Inc() },
       })
   }
   ```
   `tryKeepColumnChunk` calls `counterHooks.RowGroupScanned()` /
   `RowGroupSkipped()` / `DictionaryShortCircuit()` (nil-checked)
   alongside the per-query counter bumps. Nil-default hooks let unit
   tests that don't import `blockmetrics` continue to work.

5. **`FetchSpansStats` extension.**
   ```go
   type FetchSpansStats struct {
       // ... existing ...
       DictionaryShortCircuits uint64
   }
   ```
   `ValuesMatched` field already exists.

6. **Engine-side wiring** in `pkg/traceql/storage.go` and
   `engine_metrics.go`. Add `ValuesMatched` and
   `DictionaryShortCircuits` to both `populateMetricsFromFetchStats`
   and `accumulateFetchStats` via `addIfNonZero`. Two new constants in
   `pkg/tempopb/additional_metrics_keys.go`:
   `AdditionalMetricValuesMatched` and
   `AdditionalMetricDictionaryShortCircuits` (camelCase keys).

7. **Wiring in vparquet5/vparquet4.**
   - In `block_search.go::makeIterFunc`, create one
     `*SyncIteratorCounters` per fetch call and pass it via
     `SyncIteratorOptCounters` to every `NewSyncIterator`. Hand the
     same pointer to the fetch caller.
   - In each of `block_traceql_fetch.go` (`FetchSpans`),
     `block_traceql.go` (`Fetch`), `block_autocomplete.go`
     (`FetchTagNames` / `FetchTagValues`), and `wal_block.go`
     equivalents:
     ```go
     counters := &pq.SyncIteratorCounters{}
     // pass to makeIterFunc / NewSyncIterator via SyncIteratorOptCounters
     return traceql.FetchSpansOnlyResponse{
         Results: iter,
         Stats: func() traceql.FetchSpansStats {
             return traceql.FetchSpansStats{
                 Bytes:                   rr.BytesRead(),
                 RowGroupsInspected:      counters.RowGroupsInspected,
                 RowGroupsSkipped:        counters.RowGroupsSkipped,
                 PagesInspected:          counters.PagesInspected,
                 PagesSkipped:            counters.PagesSkipped,
                 ValuesMatched:           counters.ValuesMatched,
                 DictionaryShortCircuits: counters.DictionaryShortCircuits,
             }
         },
     }, nil
     ```
     If PR 1 has already landed, merge its per-role fields into the
     same literal. If PR 2 lands first, PR 1 rebases and adds them.
   - Remove the `TODO(issue/1274)` markers now that both halves are
     wired. Existing markers live in `block_traceql.go`,
     `block_traceql_fetch.go`, and `wal_block.go` in vp4/vp5. If PR 1
     landed first, those markers now read `TODO(issue/1274): populate
     per-iterator counters` — this PR removes them entirely. If PR 2
     lands first, the markers still say "per-iterator and per-role" —
     rewrite to just "per-role" and PR 1 removes them. Verify with:
     `grep -rn 'TODO(issue/1274)' tempodb/encoding/vparquet{4,5}`.
   - `block_autocomplete.go` has no markers today — nothing to remove
     there.

8. **Tests.**
   - `pkg/parquetquery/predicates_test.go`:
     - Update `mockPredicate` (and any other Predicate test doubles found
       by `grep -rn 'KeepColumnChunk' --include='*_test.go' .`) to return
       `KeepResult`.
     - Add in-memory dictionary fixture helper:
       ```go
       // newDictColumnChunk writes a single dictionary-encoded column
       // chunk to an in-memory parquet.File and returns a
       // *ColumnChunkHelper for it. Configures the writer to prefer
       // dictionary encoding (low cardinality inputs + small
       // PageBufferSize) and asserts cc.Dictionary() != nil before
       // returning; t.Fatal on failure so misconfiguration surfaces
       // immediately.
       func newDictColumnChunk(t *testing.T, values []string) *ColumnChunkHelper
       ```
     - For each `keepDictionary`-using predicate
       (`ByteInPredicate`, `ByteNotInPredicate`, `regexPredicate`,
       `SubstringPredicate`, `GenericPredicate[T]`), assert:
       (a) dictionary-path rejection → `KeepResultSkipDictionary`;
       (b) column-index / bounds rejection → `KeepResultSkip`;
       (c) acceptance → `KeepResultKeep`.
     - `OrPredicate` tests exercise the "all-children" rule and the
       empty-preds edge case.
   - `pkg/parquetquery/iters_test.go`:
     - Drive a `SyncIterator` over a synthetic parquet file with
       predicates that reject at each level (row group via row-number,
       row group via predicate, row group via dictionary, page, value)
       and assert `*SyncIteratorCounters` carries the expected values.
     - Verify process-wide Prometheus counters via
       `testutil.ToFloat64` after wiring a test-local set of hooks.
       Note: `pkg/parquetquery` **cannot** import
       `tempodb/encoding/blockmetrics` (would be a layering inversion),
       so tests that assert Prometheus counters must either wire their
       own hooks via `RegisterCounterHooks` or blank-import blockmetrics
       (`_ "github.com/grafana/tempo/tempodb/encoding/blockmetrics"`)
       to trigger its `init()`. Tests that don't need Prometheus
       assertions can rely on the nil-hook no-op and skip both.
     - Benchmark sanity check: run any existing `BenchmarkSyncIterator*`
       (or add a minimal one) with and without a non-nil
       `*SyncIteratorCounters` to bound the overhead of the `next()`
       path. Report deltas in the PR description.
   - vparquet5 fetch test that exercises `Stats()` and asserts the new
     fields propagate through to `SearchMetrics.AdditionalMetrics`.
   - **Walblock has no dedicated test.** The shared `pkg/parquetquery`
     iterator tests cover the counter logic; wal block uses the same
     iterator infrastructure with no special handling.

9. **TODO sweep.** End of phase:
   `grep -rn 'TODO(issue/1274)' pkg/traceql pkg/tempopb tempodb/encoding/vparquet{4,5}`.
   All per-iterator markers must be gone. If PR 1 has also landed, the
   grep should return no `1274` markers at all — indicating both halves
   of the wiring are done.

### Risks / open questions

- **Hot-path overhead.** All per-query increments are scalar adds on a
  not-concurrent struct — negligible. Process-wide Prometheus counter
  increments are atomic but cheap at row-group / page granularity (not
  per-value). Benchmark step in Phase 2 step 8 confirms.
- **`Predicate` interface change.** Widening `KeepColumnChunk` from
  `bool` to `KeepResult` touches every predicate implementation (12 in
  `predicates.go` + 26 in `predicates.gen.go` via 3 templates + 5 sites
  in vparquet3/4/5). Risk is mechanical churn, not correctness.
- **Counter-hook nil safety.** Hooks default to nil and are no-ops in
  the call sites. Tests that need process-wide-counter assertions
  import `blockmetrics` to wire hooks.

---

## Phase 3 — Verification via distributed docker-compose

Same setup as PR 1's Phase 4 (Prometheus at `:9090`, Tempo at `:3202`).

### 2.E — iterator counters

```
curl -s 'http://localhost:3202/api/metrics/query_range?q=%7B%7D%20%7C%20rate()&start=...&end=...&step=15s'

curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum by (outcome) (tempo_block_rowgroups_total)'
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum by (outcome) (tempo_block_pages_total)'
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=tempo_block_dictionary_shortcircuit_total'
```
Both `outcome="scanned"` and `outcome="skipped"` should be non-zero
after queries that mix selective and unselective predicates. Dictionary
short-circuit counter is non-zero on string columns with selective
predicates.

Per-query `AdditionalMetrics`:
```
curl -s 'http://localhost:3202/api/search?q=%7B%7D&limit=20' | jq '.metrics.additionalMetrics'
```
Expect (as string-encoded int64s): `rowGroupsInspected`,
`rowGroupsSkipped`, `pagesInspected`, `pagesSkipped`, `valuesMatched`,
`dictionaryShortCircuits`.

### 2.F — bloom-filter hit/miss

```
# Negative lookups (random IDs).
# bash:
for i in $(seq 10); do
    curl -s -o /dev/null -w '%{http_code}\n' \
      "http://localhost:3202/api/traces/$(uuidgen | tr -d -)"
done
# fish:
for i in (seq 10)
    curl -s -o /dev/null -w '%{http_code}\n' \
      http://localhost:3202/api/traces/(uuidgen | tr -d -)
end

# Positive lookup:
curl -s 'http://localhost:3202/api/traces/<real-id>'

# Counter:
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum by (outcome) (tempo_block_bloom_lookups_total)'
```
Expect `outcome="negative"` non-zero from random IDs and
`outcome="positive"` non-zero from real ones. `outcome="skipped"` not
shipped in this PR.

### Trace inspection

Tempo self-traces via the exporter configured in #1273's compose. In
Grafana (`:3000`):
- Search for `parquet.backendBlock.checkBloom` and confirm `bloomHit`
  boolean attribute.
- Open a `parquet.backendBlock.*` span and confirm the Phase 1 attrs
  (`blockID`, `numConditions`, `inspectedBytes`, etc.) didn't regress.

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

- **Proposal 2.A-C** — colleague's PR.
- **Proposal 2.D** — companion PR 1a
  (`implementation-plan-2D.md`).
- **Proposal 1 producer gaps** — companion PR 1b
  (`implementation-plan-proposal1-gaps.md`).
- **Proposal 2.G** — per-query log enrichment, deferred.
- **Bloom `skipped` outcome** — semantics ambiguous; ship
  positive/negative only, revisit after design-doc clarification.
- **Per-query dictionary-shortcircuit under `OrPredicate`** — the
  "all-children" rule is conservative; edge cases where a mixed OR
  rejects entirely via dictionary in aggregate but not in every child
  are recorded as `KeepResultSkip`.

## Chloggen entry

`enhancement` / `tempodb`:
> Add block-level read-path counters: iterator row-group / page /
> values-matched / dictionary-short-circuit counters (Prometheus +
> per-query `SearchMetrics.additionalMetrics`); bloom-filter hit/miss
> counter and span attribute.
