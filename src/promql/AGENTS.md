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
- **Fused native-series route:** the query planner offers a PromQL statement to
  `native_compile::compile_selectors` only when the engine's
  `in_process_region_transport` and `QueryOptions::experimental_native_promql`
  options are enabled and the per-request `query.experimental_native_promql`
  extension is true. Accepted plans use process-local `FusedSeries`; declined
  or unsupported shapes fall back to the existing planner. `compile_selectors`
  binds each selector to its own scan and stamps that scan's position on the
  tree input it creates (`BoundInput::source_index`), so `FusedSeries` holds one
  logical child per index and the tree itself says which child feeds which input.
  The route accepts one selector over a direct scan with a bound metric name, or
  two occurrences of one selector when both read one identical final scan,
  matched one-to-one over the whole series identity with a point-wise operand on
  each side. Label matchers are not a reason to decline: each scan applies its
  own. Only trees that read one series at a time are admitted, so an aggregate
  or a whole-vector operation (`sort`, `absent`, ...) anywhere in the tree falls
  back, as does an ineligible raw-series table/field set or an unsupported
  expression/grid. Metric scans retain the static `__table_id` predicate. Hybrid
  CTEs and CREATE VIEW TQL creation currently fall back to the existing planner.

## Change coupling

- Distributed extension operators need matching handling in the Substrait
  serializer (`src/common/substrait/src/extension_serializer.rs`), the physical
  `PromExtensionPlanner`, and `check_extension_plan`
  (`src/query/src/dist_plan/commutativity.rs`). Process-local `FusedSeries` is
  the exception: it is not serialized to a datanode. Its distributed placement
  is restricted to a direct table scan, a bound metric name and trees that read
  one series at a time. The single-region placement warning in
  `commutativity.rs` remains important: do not assume a series stays in one
  region.
- A new logical node also needs its physical planner reachable from
  `DfQueryPlanner::create_physical_plan`, assembled in
  `src/query/src/query_engine/state.rs`.
- A fused plan's inputs are described once. `BoundInput::source_index` names which
  source of one `NativeExecutor` invocation produced an input's chunks;
  `FusedSeries` maps that index to its DataFusion child by position, and the
  raw-series schema the input was compiled against is the layout its frame is
  decoded with. Neither the logical node nor `FusedSeriesExec` therefore keeps a
  per-child schema or destination list, while the binding's grid, time column,
  tags and metric name and the executor's `(plan, RawLabelSource, SeriesOrdering)`
  tuple stay as they are. A change to how a source is bound touches `BoundInput`
  (its `ordering` included, which the executor checks against the producer),
  `FusedSeries`'s source walk and the executor's `push_raw_source` together.

## Gotchas

- **One operation, two implementations; build on the live one.** `InstantManipulate`
  routes to `InstantManipulateExec`. The native alternative
  `NativeInstantManipulateExec` is crate-private and
  `InstantManipulate::try_to_native_execution_plan` has no caller on any branch —
  its doc is written for a caller that does not exist.
- **A native window requires non-interleaved input.** Only `Contiguous` and
  `KeyThenTime` are accepted; `Interleaved` is refused when compiled.
- **A fused tree is logical over N sources, physical over one.** `FusedSeries`
  accepts one child per source index, but `FusedSeriesExec` runs exactly one
  child, at that child's own partition count, and queues its chunks with
  `NativeExecutor::push_raw_source(0, ..)`. A second source (needs a
  label-pairing exchange) and a constant scalar (needs a scalar-to-batch
  conversion) are refused when the physical node is built, never answered with
  an empty result.
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
