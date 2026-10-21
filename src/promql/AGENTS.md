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
| Native model | `src/promql/src/native/` | Compiled tree, chunks, display, metrics |

## Two evaluation paths, and their status

The easiest thing to get wrong here. Both exist; only one runs.

- **Extension operators are live.** `PromPlanner` builds `SeriesDivide`,
  `SeriesNormalize`, `RangeManipulate`, `InstantManipulate`, `EmptyMetric` and
  `UnionDistinctOn`; `PromExtensionPlanner`
  (`src/promql/src/extension_plan/planner.rs`) maps each logical node to its
  execution plan, one branch per node, `None` for anything else.
- **Nothing plans into the native model yet.** `src/query/src/promql/native_compile.rs`
  compiles a statement into a `NativeTree`, but no planner calls it, so a tree
  built here reaches no execution. `src/promql/src/native/` holds the tree, the
  chunk protocol, display and metrics, and — once a runtime lands — the driver
  that runs a tree node by node. Compiling and running are separate concerns
  owned by separate files; do not assume a tree is executable because it
  compiles.

## Change coupling

- **An extension node reaches a datanode only if three things line up**: a payload
  branch in `src/common/substrait/src/extension_serializer.rs`, a physical branch
  in `PromExtensionPlanner`, and a `Commutativity::Commutative` arm in
  `check_extension_plan` (`src/query/src/dist_plan/commutativity.rs`). Missing the
  third is silent: the node falls through to `Unsupported`, the distributed
  boundary is placed above it, and it never reaches a region — with no error.
- A new logical node also needs its physical planner reachable from
  `DfQueryPlanner::create_physical_plan`, assembled in
  `src/query/src/query_engine/state.rs`.
- User-visible changes need planner coverage plus a sqlness case under
  `tests/cases/standalone/common/promql/`.

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
  caller-supplied `RawLabelSource`: decode with the source that was actually bound
  when the stream was produced, because the frame schema does not replace that
  binding. The metadata describes format, not label identity.
- **A wrong label binding is silently wrong, not an error.** Storage validates the
  binding it is given (metadata identity, tag-ness, name matches, no duplicates)
  but cannot tell whether that set is the one the query meant, and the metric name
  is not part of that check.

## Testing

```bash
cargo nextest run -p promql
cargo nextest run -p query          # planner and native-compile changes
cargo sqlness bare -t <case>        # PromQL cases live under tests/cases/standalone/common/promql/
```

## Not wired up yet

The storage side of the raw path exists; what is missing is planner wiring. Both
gaps below are structural, not bugs.

- **The native tree has no planner entry point.** `native_compile::compile` has no
  caller, and a `NativeTree` becomes a physical plan only once something builds it
  while it still holds the AST and the selector binding — the point where the
  matchers, `__field__` and the lookback are still available. Compiling later, from
  a plan, loses them.
- **No planner reaches a raw scan.** mito2 implements
  `RegionScanner::raw_series_schema` (`mito2/src/read/series_scan.rs`), so
  `RegionScanExec::try_to_series_scan` can build `SeriesScanExec` from a prepared
  scanner; the trait's `Ok(None)` default only refuses engines that have not
  implemented it. What is missing is the planner side: nothing selects that
  conversion, and `SeriesScanExec::children()` is empty, so wrapping a
  `RegionScanExec` before partition preparation leaves `ParallelizeScan` unable to
  assign partitions. Either give the partitioner a child it can see, or convert to
  the native leaf only after partition preparation — the second is the contract
  `RegionScanExec::try_to_series_scan` documents.
