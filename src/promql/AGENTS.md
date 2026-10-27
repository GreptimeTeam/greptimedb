# promql — Agent & Contributor Guide

Navigation map for `src/promql`. Repo-wide invariants:
[`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

`promql` owns PromQL evaluation: series/evaluated data, range and instant
functions, vector matching and extension operators. Statement planning lives in
`src/query/src/promql/`.

## Module map

| Area | Path | Entry point |
| --- | --- | --- |
| Data model | `src/promql/src/grid_series.rs`, `src/promql/src/evaluated.rs` | Grid series and evaluated chunks |
| Evaluation | `src/promql/src/range_eval.rs`, `src/promql/src/instant_eval.rs` | Range and instant evaluation |
| Kernels | `src/promql/src/functions.rs`, `src/promql/src/functions/` | PromQL functions |
| Matching | `src/promql/src/vector_match.rs` | Binary vector matching |
| Extension operators | `src/promql/src/extension_plan.rs`, `src/promql/src/extension_plan/` | Logical and physical extension nodes |
| Native model | `src/promql/src/native/` | Bound tree, execution, metrics and display |
| Native compiler | `src/query/src/promql/native_compile.rs` | PromQL AST to native tree |

## Native execution boundary

Native PromQL is an experimental route alongside the existing DataFusion PromQL
path. It evaluates each independent input series, and an admitted single-source
scan gathers its selected storage regions on the frontend and merges their raw
samples by complete raw identity and time before the tree runs. That raw merge is
not a canonical evaluated merge: cross-region canonical `SeriesMerge`/`Exchange`
and global final-label semantics are still unimplemented, so do not describe
partition-local evaluation as a global series merge.

The fan-in opens every selected region read at once, consumes each opened stream
continuously, and drops it at its own EOF so its datanode read permit returns
progressively; a region still opening never stops a sibling from being drained.
Inputs reach the merge as buffered-then-live streams, so it streams instead of
materializing every region first. Buffered chunks and the in-flight raw batch are
charged to the query memory pool, and a refusal is an explicit resource error.
The datanode read limit counts in-flight region read streams per datanode rather
than per query, so a query selecting more regions on one datanode than its permit
count is refused at admission: an explicit error, every permit released, never a
partial result. Do not document "raise the limit" as the remedy; giving one
query's whole read set a shared admission scope is admission-layer follow-up.

Vector operations use the existing vector-matching semantics; do not describe
them as pairing only corresponding child series. A single-input operation can
independently map N series to N outputs. Aggregation placement must respect its
stage: `Complete` is valid only when the execution sees all contributions to
each group. Partitioned aggregation requires `Partial` state per input partition
followed by `Final` aggregation of that state. Do not claim that a complete
aggregate on each scan partition is globally complete.

A one-child fused node - the value-producing per-series tree and the producer
that exports a partial fold's state alike - may be evaluated in a region only
when every partition column of the table is one of the raw binding's tags;
otherwise it stays on the frontend above the fan-in. The rule is a conservative
sufficient condition read from the scan's own table metadata, and unresolvable
metadata is refused rather than read as an unpartitioned table.

The native `timestamp` operation evaluates on its input grid, and literal
scalar constants (including foldable constant expressions) bind on the
statement grid. This does not imply support for time-varying `time()` or
`scalar(v)`. Native `count` produces Float64 values, unlike the legacy PromQL
path's Int64 count. Native SQL/TQL output is not necessarily schema-identical
to legacy output.

The native tree and its compiler describe the implemented model, not the whole
PromQL surface. Keep comments aligned with executable nodes and kernels rather
than listing an exhaustive function allowlist. In range functions,
`double_exponential_smoothing`, `mad_over_time`, and `ts_of_*_over_time` are
experimental in Prometheus; `first_over_time` is not.

## Change coupling

Distributed extension operators need matching handling in the Substrait
serializer (`src/common/substrait/src/extension_serializer.rs`), the physical
`PromExtensionPlanner`, and `check_extension_plan`
(`src/query/src/dist_plan/commutativity.rs`). Process-local native nodes must
not be treated as serializable or distributed operators. New logical extension
nodes also need a physical planner reachable from
`DfQueryPlanner::create_physical_plan`, assembled in
`src/query/src/query_engine/state.rs`.

A `FusedSeries` over two tables stays on the frontend over one merge scan per
table, each carrying a `RawSeriesSource` that answers explicit-label raw frames.
The frame schema is compared exactly at the merge scan, so its declaration and
the region's output change together.

## Gotchas

A series closes exactly once per input series, on the chunk that says so—not
once per final label set. A label-rewriting operation may cause distinct input
series to share final labels, and each still closes on its own chunk. The
closing chunk is the only way a consumer distinguishes “nothing more here”
from “no more input”; dropping it leaves a series open. Consumers must
preserve this per-series finality, including sparse trailing empty chunks. A
merge above them must not mistake a second close for a defect. A child that
merges regions must recompute finality globally rather than pass through a
region-local flag.

## Validation

```bash
cargo nextest run -p promql
cargo nextest run -p query          # planner and native-compile changes
cargo sqlness bare -t <case>        # PromQL cases under tests/cases/standalone/common/promql/
```
