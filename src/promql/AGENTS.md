# promql — Agent & Contributor Guide

Navigation map for `src/promql`. Repo-wide invariants:
[`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

`promql` owns PromQL evaluation: the series/evaluated data model, range and
instant evaluation, function kernels, vector matching, and the PromQL extension
operators. Statement-to-plan translation lives in `src/query/src/promql/`.

## Module map

| Area | Path | Entry point |
| --- | --- | --- |
| Data model | `src/promql/src/grid_series.rs`, `src/promql/src/evaluated.rs` | Grid series, evaluated chunks |
| Evaluation | `src/promql/src/range_eval.rs`, `src/promql/src/instant_eval.rs` | Range and instant evaluation |
| Kernels | `src/promql/src/functions.rs`, `src/promql/src/functions/` | PromQL functions |
| Matching | `src/promql/src/vector_match.rs` | Binary vector matching |
| Extension operators | `src/promql/src/extension_plan.rs`, `src/promql/src/extension_plan/` | Logical nodes and execution plans |
| Native model | `src/promql/src/native/` | Compiled tree, chunks, display, metrics and execution |

## Evaluation paths

- **Extension operators:** the existing PromQL path plans operators such as
  `SeriesDivide`, `SeriesNormalize`, `RangeManipulate`, `InstantManipulate`,
  `EmptyMetric` and `UnionDistinctOn`. `PromExtensionPlanner`
  (`src/promql/src/extension_plan/planner.rs`) maps the logical nodes to their
  execution plans.
- **Experimental fused native-series POC (default off):** the query planner offers
  a PromQL statement only when the engine's `in_process_region_transport` and
  `QueryOptions::experimental_native_promql` options are enabled and the per-request
  `query.experimental_native_promql` extension is true. Admission is limited to a
  bare vector selector (parentheses allowed) and unary `rate`, `increase`,
  `sum_over_time`, and `abs` calls over a selector; range selectors occur only
  inside those calls. It is a standalone route: unsupported expressions and all
  other cases use the existing planner. Accepted plans use process-local
  `FusedSeries`, with ordinary `RecordBatch` output collected through DataFusion.
  Scan N→Fused N retains region/partition parallelism, but this POC does not merge
  one logical series across regions or scan partitions, guarantee canonical final
  labels across them, or use global label-set occupancy; `NativeExecutor` retains
  its partition-local `Emitted` state. Repartitioning redistributes rows but does not perform
  semantic series merging. A future gather/repartition plus merge/finalize is
  outside this POC. Metric scans retain the static `__table_id` predicate.
- **Admission and SQL output:** the guards live in
  `src/query/src/promql/planner/native.rs`. Non-`Float64` fields, alternative
  (`or`) matchers and `rate`/`increase` on temporality-tagged tables keep the
  existing plan; the latter preserves legacy per-series delta summation.
  Allowing query fallback through the engine option, session variable or
  `query_fallback` hint also disables native admission. Native SQL/TQL output
  has column order `time index, fields, tags`, raw field names with nullable
  `Float64` values, and nullable `Utf8` tags (including integer table tags).
  Unlike a legacy raw selector, it does not preserve tag types; legacy function
  expressions can also use different field names and column order. The native
  time index is a non-nullable millisecond timestamp without a time zone.

## Change coupling

- Distributed extension operators need matching handling in the Substrait
  serializer (`src/common/substrait/src/extension_serializer.rs`), the physical
  `PromExtensionPlanner`, and `check_extension_plan`
  (`src/query/src/dist_plan/commutativity.rs`). Process-local `FusedSeries` is
  the exception: it is not serialized to a datanode. Its distributed placement
  is restricted to a direct table scan, a bound metric name and trees that read
  one series at a time. Native admission requires partition columns to be tags,
  but region/partition-local execution does not merge a logical series across
  regions or scan partitions; do not treat that placement guard as a global
  series-merge or final-label guarantee.
- A new logical node also needs its physical planner reachable from
  `DfQueryPlanner::create_physical_plan`, assembled in
  `src/query/src/query_engine/state.rs`.

## Gotchas

- **One operation, two implementations; build on the live one.** `InstantManipulate`
  routes to `InstantManipulateExec`. The native alternative
  `NativeInstantManipulateExec` is crate-private and
  `InstantManipulate::try_to_native_execution_plan` has no caller on any branch —
  its doc is written for a caller that does not exist.
- **A native window requires non-interleaved input.** Only `Contiguous` and
  `KeyThenTime` are accepted; `Interleaved` is refused when compiled.
- **A series closes exactly once**, on the chunk that says so — per input series,
  not per label set. A node that rewrites labels can hand two input series over
  under one label set, and each closes on its own chunk, so a label set may be
  closed more than once; a merge above them must not read the second close as a
  defect. That closing chunk is the only way a consumer distinguishes "nothing more
  here" from "no more input": dropping it leaves a series open, and a child that
  merges regions must recompute finality globally rather than pass a region-local
  flag through.
- **Raw-frame labels come from the binding, not the schema.** A raw series frame
  carries an opaque encoded primary key while its schema metadata describes only
  the format: `greptime.series.layout`, `.content`, `.labels` and `.time`, plus
  the producer ordering the adapter adds. Nothing in that metadata binds the key
  to a decode — the frame decoder compares those format keys and the field list,
  and never a label identity. `unpack_raw_series` therefore requires a
  caller-supplied `RawLabelSource`: decode with the source that was actually
  bound when the stream was produced, because the frame schema does not replace
  that binding. The metadata describes format, not label identity.
- **A wrong label binding is silently wrong, not an error.** Storage validates the
  binding it is given (metadata identity, tag-ness, name matches, no duplicates)
  but cannot tell whether that set is the one the query meant, and the metric name
  is not part of that check.

## Testing

```bash
cargo nextest run -p promql
cargo nextest run -p query          # planner and native-compile changes
cargo sqlness bare -t <case>        # PromQL cases under tests/cases/standalone/common/promql/
```
