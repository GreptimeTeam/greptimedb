// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Inserts the canonical merge a consumer of whole series needs - and only where one is needed.
//!
//! A fused input hands over the pieces of a metric one scan partition holds, so a consumer cannot
//! assume its input is grouped by canonical label set. [`NativeExpr::Merge`] is the node that
//! collects the pieces of one canonical label set into one series, and this module is where a tree
//! gains one. It is a narrow pass: a merge holds a whole label set's rows before anything above it
//! runs, and most consumers read the pieces correctly as they arrive, so a merge is inserted at a
//! handful of places and refused everywhere else.
//!
//! # The three conditions
//!
//! A merge is inserted at a consumer only when all three hold, asked in this order:
//!
//! 1. **The consumer needs one unified canonical series.** Some kernels fold a group one point at a
//!    time, some read a series' rows as they come, some pick series against each other: only a
//!    consumer that needs *one series* per key, rather than one contribution at a time, can use a
//!    merge at all.
//! 2. **The input does not already provide it.** [`NativeProperties::grouping`] is that fact: a
//!    [`CanonicalGrouping::CanonicalComplete`] input already answers one series per canonical key,
//!    so a merge over it would collect nothing.
//! 3. **Co-location is proven for this key.** A merge reads every piece of one canonical label set,
//!    which only an execution holding all of them can answer. That is
//!    [`merges_are_placeable_in`](crate::native::plan::merges_are_placeable_in)'s question, and it
//!    is the caller's: this pass has no placement to read, so it answers conditions 1 and 2 and
//!    leaves condition 3 to the route that knows which execution the tree will run in. A merge is
//!    never inserted to make an unplaceable tree placeable - a shape the placement cannot answer is
//!    declined, not rewritten.
//!
//! None of these is a reason by itself: that the input is
//! [`CanonicalGrouping::SourcePiecesOnly`], that the operator is stateful, that the operator
//! [`reads_across_series`](crate::native::plan::reads_across_series), or that the node is the root.
//! Each of them is true of consumers that answer their input correctly one contribution at a time.
//!
//! # What each consumer needs
//!
//! | consumer | merge? | why |
//! | --- | --- | --- |
//! | [`NativeExpr::Map`], a scalar-broadcast [`NativeExpr::Binary`] | no | point-wise over one series at a time; a contribution is one series' rows, and what the operator does to a *label set* is a fact about its output, to be answered at the consumer of that output |
//! | [`NativeExpr::Aggregate`], stage `Complete` | no | the fold is per point: `AggregateRuntime::accumulate` folds each chunk into states indexed by `(group, grid position)`, so several contributions to one group at one position are the same accumulation as one contiguous series would give. Completeness of a *group* is the placement's contract (`needs_single_execution_in`), not a merge's |
//! | [`NativeExpr::Aggregate`], stages `Partial`/`Final` | no | `Partial` answers aggregate state, which is not an evaluated vector and has no series for a merge to collect; `Final` folds the state of every partition and is the stage that *is* the merge of contributions |
//! | [`NativeExpr::Window`] | **yes, over a child that proves nothing** | a window spans its input's contributions of one label set and needs one unified pass over them. The runtime does that pass through `MergeInput`, but it builds one only for a child that reports its fragments, and a child that hands series back unchanged says nothing - it reports `shared = false` like a node that has already unified - so nothing collects the fragments and the window answers one series per fragment. [`unifies_its_own_contributions`] is the one rule that separates "already unified" from "cannot say", and this arm collects exactly the children that answer "cannot say": the boundary itself, and a `Vector` with `topk`/`limit_ratio`/`sort` or a filtering `Broadcast` above it |
//! | [`NativeExpr::Binary`] over two vectors, [`NativeExpr::Set`] | no | the kernel files its rows by *result label set* and hands over one series per set, refusing two values of a field at one point (`Results::sort_rows`, `same_labelset`). Matching-key completeness is `matching_co_located`/placement's question, and per-point cardinality is what that refusal enforces |
//! | [`NativeExpr::Vector`], `topk`/`limit_ratio`/`sort` | no, but a window above it collects | each ranks, keeps or orders *series*, and hands back the series it read with their labels: several contributions of one label set are ranked by the value each holds at a point, which is what one whole series would hold there. What it hands over therefore aliases exactly as its input does, which is why it passes [`unifies_its_own_contributions`]'s answer on - and why a window over one of these is the shape the next section's third row is about |
//! | [`NativeExpr::Vector`], `limitk` | **yes** | `k` counts *series* in input order (`select::limitk` walks the group's members and stops at `k`), so pieces of one label set would occupy several slots and be counted against `k`. The runtime merges its input only when the child shares runs, and a scan leaf does not, so the tree has to carry the merge |
//! | [`NativeExpr::Vector`], `histogram_quantile`/`histogram_fraction` | no | the kernel merges label-set pieces itself before it reads buckets (`vector_series` calls `merge_same_labelset`) |
//! | [`NativeExpr::Vector`], `count_values` | no | it counts per group and value at each point, so contributions are folded the way an aggregate folds them |
//! | [`NativeExpr::Vector`], `absent` | no | it reads presence per point over the whole vector, so the union of the contributions is the presence it needs |
//! | [`NativeExpr::Info`] | no | the kernel indexes the info side by `(name, identifying values)` and keeps the newest sample per point, then merges the results that share a label set and refuses a field held twice at one point |
//! | [`NativeExpr::Instant`], [`NativeExpr::Timestamp`] | no | a selection answers one series from one series' rows |
//! | the root | no | a caller of a native tree reads chunks, and a map at the root hands over pieces on purpose: `next_chunk` states that a caller needing one series per label set merges them. Canonicalizing the root would only hold the whole answer back |
//!
//! `MergeInput` is what performs the window's joint processing - it orders the contributions, keeps
//! one state per label set, and decides end-of-series from the `is_last` of the collected fragments
//! (`MergeInput::check`, then `merge_same_labelset`, then the evaluator). Whether a window gets one
//! is decided by [`unifies_its_own_contributions`], which `RuntimeNode::build` and this pass both
//! call, so the two cannot disagree about a child. It answers in three values, and this pass acts on
//! exactly one of them:
//!
//! | answer | the nodes that answer it | the runtime | this pass |
//! | --- | --- | --- | --- |
//! | one series per canonical label set | `Aggregate`, `Binary` over two vectors, `Set`, `Info`, `Merge`, `Vector` with `limitk`/`absent`/`histogram_*`/`count_values`, a `Window` whose child reports its fragments | `shared = false`: nothing above collects | nothing to insert - the child already answers what the window needs |
//! | several, and the node says so | `Map`, `Timestamp`, `Instant::Timestamp`, a label-dropping `Broadcast`, a `Window` that neither drops the metric name nor has a child that reports fragments, `Vector` with `topk`/`limit_ratio`/`sort` over one of these | `shared = true`: the window's `MergeInput` collects them | nothing to insert - a merge here would turn that collection off (`Merge` answers "already unified") and move the stream checks to a node without the evaluators |
//! | several, and the node says nothing | a boundary ([`NativeExpr::Input`]), and every node that hands its input's series back unchanged: a filtering `Broadcast`, a `Vector` with `topk`/`limit_ratio`/`sort` | `shared = false`: the window gets no `MergeInput` | **the one the pass collects** |
//!
//! The third row is why the pass exists. A boundary binds the pieces some upstream execution
//! produced and claims nothing about them: a scan hands over one whole storage series at a time -
//! which the streaming evaluator follows chunk by chunk, `RangeEvaluator::push` refusing a chunk
//! that changes the series' primary key or goes back inside it - while a tree split at a repartition
//! (`A -> Repartition -> B`, `B`'s tree `Window(Input)`) starts at an input whose fragments are the
//! pieces of one canonical label set, spread over the executions the repartition fanned them out to.
//! The flag alone cannot tell those apart either: a leaf is not shared because a merge binds samples
//! evaluated on a grid and a scan's are raw, not because its pieces are one series per label set. So
//! the answer is "cannot say" rather than a proof, and every node that hands its input's series back
//! unchanged passes that answer on. [`canonical`] then settles the rest: a raw boundary
//! cannot take a merge and needs none, and a boundary whose [`NativeProperties`] already claim
//! [`CanonicalGrouping::CanonicalComplete`] is not collected again.
//!
//! An `Instant` selection is not one of them: it reads raw samples (`NativeNode::validate` refuses
//! an evaluated input), so it cannot sit between an evaluated boundary and a window at all, and a
//! window over one is the fixed shape rather than a defeating one.
//!
//! Two nodes hand series back unchanged between an evaluated boundary and a window, and both were
//! measured answering two series where one is owed: a `Vector` that keeps the series it ranked,
//! `topk`/`limit_ratio`/`sort` (`avg_over_time(topk(1, x)[5m:1m])`), and a scalar `Broadcast` whose
//! filtering comparison keeps the label set it read (`avg_over_time((x > 0)[5m:1m])`). Both answer
//! "cannot say" and both are collected by [`rewrite`]'s `Window` arm, which states the rule next to
//! the code that applies it. That arm collects them *above* the child, which is the earliest place a
//! merge can sit: `topk` ranks the fragments it was handed, so the merge reunites the pieces the
//! window reads but cannot make `topk` rank them as one series. A collection below `topk` is a
//! different question - it would change what `topk` keeps - and is not what this pass inserts.
//!
//! A *different* label-set collision - two storage series that resolve to one canonical label set -
//! is an error rather than a unification, and every node that can produce one refuses it: the
//! window's own `Emitted` table reports "no merge resolves two values of one field at one position"
//! for two series writing the same position, and the evaluator refuses a changed primary key inside
//! one series. That is Prometheus' rule too (duplicate series is an error), and it is why no
//! collection is warranted there: a merge would have added the two values up instead.
//!
//! # What removing a merge takes with it: the duplicate check
//!
//! [`crate::grid_series::merge_same_labelset`] does two things at once: it collects the pieces of
//! one label set, and it refuses two values of one field at one position under one label set
//! ("vector cannot contain metrics with the same labelset"). The second is not a side effect - it
//! is the error a query owes when two source series resolve to one label set - so every merge this
//! pass used to insert was also the place the tree reported it.
//!
//! Every path this change stopped collecting therefore has to say where that check went, and the
//! answer is per consumer:
//!
//! - `Binary` over two vectors, `Set`: the kernel still makes it. `Results::sort_rows` reports two
//!   rows of one result label set at one point, and the label sets it files under are canonical
//!   (`key_of`/`label_set`), so the error survives. It is narrower than a merge's - it fires where
//!   both operands hold a value, not where either does - which is what Prometheus checks too.
//! - `Map`, `Window`, `Timestamp`, a label-dropping broadcast: `Emitted` (in `execute`) still makes
//!   it. Those nodes keep an occupancy table per output label set and refuse a second series
//!   writing a position a first one wrote (`occupy`), which is the same check for the same reason.
//! - `Vector` with `histogram_quantile`/`histogram_fraction`: the kernel calls
//!   `merge_same_labelset` itself, so the check is exactly where it was.
//! - `Info`: the kernel keeps it. Its results are filed by `label_set` and each label set's rows
//!   are sorted by point, where a field held twice records "vector cannot contain metrics with the
//!   same labelset" itself.
//! - `Vector` with `limitk`: the merge this pass keeps *is* the check. `MergeInput::check` refuses
//!   a stream that breaks its contract and `merge_same_labelset` refuses two values of one field at
//!   one position, so nothing is lost here.
//! - **`Aggregate`, `Vector` with `topk`/`limit_ratio`/`sort`/`absent`/`count_values`, and the
//!   root: nothing checks.** Those kernels fold, rank, count or emit whatever they are handed, and
//!   two fragments of one label set now pass through unremarked. That is a real loss of an error
//!   check, and it is the reason this change is not the whole of the work: Part C has to separate
//!   the duplicate check from the collection - one pass over the evaluated input that refuses
//!   `(label set, position)` held twice - and it is required before the removal is safe. The spots
//!   are marked in [`rewrite`]'s arms.
//!
//! How exposed each of those is depends on where a same-position duplicate can even come from. One
//! metric's own pieces cannot make one: storage holds one value per (series, timestamp), so two
//! fragments of one canonical label set hold disjoint positions. A duplicate needs either a label
//! rewrite upstream - and every rewriting node keeps its `Emitted` table above - or two scan
//! partitions that overlap and hand over the same sample twice. The second is the case no node
//! below a rewrite covers, and it is the one Part C has to answer.
//!
//! The pass reads [`NativeProperties`] from the tree rather than storing them on it, so a rewritten
//! tree keeps no fact that the rewrite itself invalidated: each node's facts are derived again from
//! its rewritten children.

use datafusion::common::Result as DfResult;

use crate::native::plan::{
    BoundVectorOp, CanonicalGrouping, NativeExpr, NativeNode, NativeProperties, NativeTree,
    NativeType, unifies_its_own_contributions,
};

#[cfg(test)]
pub(crate) mod tests;

/// Rewrites `tree` so that every consumer that needs one canonical series is handed one.
///
/// `inputs` holds what the caller knows about the fused inputs the tree was compiled over, indexed
/// by the `source_index` of the [`BoundInput`](crate::native::plan::BoundInput) each leaf was bound
/// to; a boundary the caller says nothing about proves nothing. The index is what a leaf reads, so a
/// tree with several sources is judged source by source: one scan's proof is never another's. What
/// this adds is only a [`NativeExpr::Merge`] over a node whose fragments may hold one label set:
/// every operation binding and every `source_index` stays where it was, so the rewrite changes which
/// series a consumer reads and not what the query computes.
///
/// Where a merge goes is decided by what each node needs of its input, and the module documentation
/// is the table: two consumers need one series per canonical key that their input cannot already
/// provide. `limitk` counts series in input order, and a window spanning its input's contributions
/// needs one unified pass and gets none from the runtime when it sits on a fused input. Every other
/// node reads its input as it arrives, and what a merge *above* such a node would collect is a
/// question about that node's output, asked at the consumer of the output and not here.
///
/// Running the pass over its own output changes nothing: each insertion puts a merge over an input
/// that then reports [`CanonicalGrouping::CanonicalComplete`], so a second run finds nothing to add,
/// and the window's merge is not re-inserted either - the window then sits on a
/// [`NativeExpr::Merge`] and not on the boundary this pass collects.
///
/// The result is checked by [`NativeTree::try_new`], because a merge has to sit over an evaluated
/// vector and the pass may only put it there.
///
/// See the module documentation for what removing a merge takes with it: the duplicate-label-set
/// check a collection used to perform. Part C has to restore it for the paths this pass stopped
/// collecting, and the arms of [`rewrite`] mark which those are.
pub fn insert_native_merge_inside_current_fused(
    tree: &NativeTree,
    inputs: &[NativeProperties],
) -> DfResult<NativeTree> {
    // The root is a consumer too, and it is the one consumer whose caller is known to read pieces:
    // canonicalizing it would hold back the answer without a consumer asking for that.
    //
    // PART C: the merge that used to sit here was the only duplicate-label-set check over the root's
    // output. A caller that merges the pieces itself gets the error from
    // `merge_same_labelset`; a caller that reads them as they come does not.
    let root = rewrite(tree.root(), inputs)?;
    NativeTree::try_new(root)
}

/// Wraps `node` in a [`NativeExpr::Merge`] unless it already answers one series per canonical label
/// set.
///
/// This is conditions 1 and 2 of the module's rule asked of one node: the caller has already decided
/// that this consumer needs one unified series (condition 1), and what is left is whether the input
/// provides it (condition 2) - [`CanonicalGrouping`] is that fact. Condition 3, co-location, is the
/// placement's question and never this function's.
///
/// A node whose output is not an evaluated vector keeps its place: a merge binds samples evaluated on
/// a grid, so a scalar, raw samples and aggregate state have nothing a merge could take, and no
/// consumer asks for one over those.
///
/// The facts are read through [`NativeNode::properties`], which derives them from the rewritten node
/// and the boundary every time. A merge proves one series per label set from the pieces it was
/// handed, and it cannot claim that a fragment which never arrived was collected: that is why the
/// ownership it reports stays its input's, unchanged.
fn canonical(node: NativeNode, boundary: &[NativeProperties]) -> NativeNode {
    let properties = node.properties(boundary);
    if properties.grouping == CanonicalGrouping::CanonicalComplete {
        return node;
    }
    if !matches!(&node.output, NativeType::Samples(schema) if schema.eval_grid.is_some()) {
        return node;
    }
    // A merge reports its input's layout: the same fields on the same grid.
    let output = node.output.clone();
    NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(node),
        },
        output,
    }
}

/// Rewrites `node` bottom-up: its children first, then the merge the consumer needs of them.
///
/// `boundary` is what the caller knows about the fused inputs this tree was compiled over, indexed by
/// `source_index`. It is the one thing every node's facts are derived from, leaves included, so the
/// rewrite never has to say which of its own lists a node should read: [`NativeNode::properties`]
/// asks a leaf for its own entry and every other node for its children's.
///
/// Each arm states why that consumer needs or does not need a merge; the module documentation is the
/// same table in one place.
fn rewrite(node: &NativeNode, boundary: &[NativeProperties]) -> DfResult<NativeNode> {
    let expr = match &node.expr {
        // A source is the boundary it was bound to and there is nothing below it to rewrite. A
        // constant reads nothing.
        NativeExpr::Input(_) | NativeExpr::Scalar(_) => node.expr.clone(),
        // A selection answers one series from one series' rows, so it reads the pieces as they are.
        NativeExpr::Instant { input, op } => NativeExpr::Instant {
            input: Box::new(rewrite(input, boundary)?),
            op: op.clone(),
        },
        // A merge is the canonicalizer itself, so it asks nothing of its input: fragments are what it
        // was written to receive.
        NativeExpr::Merge { input } => NativeExpr::Merge {
            input: Box::new(rewrite(input, boundary)?),
        },
        // A window may receive several independent contributions of one label set, and a window that
        // spans them needs one correct unified pass. The runtime does that pass through `MergeInput`
        // when its child reports `shared`, and `unifies_its_own_contributions` is what decides both
        // that flag and this arm, so the two cannot disagree about a child. Its three answers split
        // the children into three groups, and only the third needs a merge:
        //
        // - `Some(true)`: the child has established one series per label set (`Aggregate`, `Binary`
        //   over two vectors, `Set`, `Info`, `Merge`, a `Vector` that reads the whole vector), or it
        //   is a window that collects its own fragments. Nothing collects what needs no collecting.
        // - `Some(false)`: the child may hand over several and *says so* - a `Map`, a `Timestamp`, an
        //   `Instant` selection reporting a timestamp, a label-dropping `Broadcast`, a `Window`, a
        //   `Vector` over such a child. `RuntimeNode::build` gives the window its `MergeInput` here,
        //   and a merge in the tree would turn that off (`NativeExpr::Merge` reports
        //   `unifies_its_own_contributions = Some(true)`, so the window would stream an already-merged
        //   series) and move the stream checks to a node without the evaluators.
        // - `None`: the child cannot say. A boundary ([`NativeExpr::Input`]) is the node that answers
        //   this, and a node that hands its input's series back unchanged passes the answer on: a
        //   `Vector` with `topk`/`limit_ratio`/`sort` or a filtering `Broadcast`. Nothing below such
        //   a child unified anything and nothing above it will, so this is the one child the pass has
        //   to collect - and `canonical` still asks the rest: whether the caller's facts already claim
        //   [`CanonicalGrouping::CanonicalComplete`] for that boundary (a boundary that proves one
        //   series per label set is not collected again) and whether what the child hands over is a
        //   shape a merge can take at all (an evaluated vector; a scan's raw samples cannot take one,
        //   and they need none, because the runtime streams one whole storage series at a time and
        //   the evaluator follows it chunk by chunk).
        //
        // The two measured shapes this closes are `avg_over_time(topk(1, x)[5m:1m])`
        // (`Window(Vector(Input))`) and `avg_over_time((x > 0)[5m:1m])`
        // (`Window(Binary(Input, Scalar))`), each of which answered one series per fragment before:
        // both children are `None` here, because both hand the boundary's fragments on unchanged.
        NativeExpr::Window { input, op } => {
            let input = rewrite(input, boundary)?;
            // The child this pass has to collect: one that neither proves it hands over one series
            // per label set nor reports that it hands over several. Read as a negation of
            // "proves" - `Some(false)` as well as `None` - this would collect a `Map`, a
            // `Timestamp`, a name-dropping `Window` and a label-dropping `Broadcast` too, and turn
            // off the `MergeInput` the runtime builds for each of them: the blanket rule this pass
            // exists to be narrower than. The two answers `false` covers are different facts, and
            // only `None` is the one nothing else collects; the merge-count table in this module's
            // tests pins both sides of that line.
            //
            // `canonical` asks the rest of the question - whether the caller's facts already say
            // this input answers one series per label set, and whether what it hands over is a
            // shape a merge can take. Co-location, the third condition, stays the caller's.
            let input = if unifies_its_own_contributions(&input).is_none() {
                canonical(input, boundary)
            } else {
                input
            };
            NativeExpr::Window {
                input: Box::new(input),
                op: op.clone(),
            }
        }
        // A map is point-wise, so it reads each piece as it is; what it does to a label set is a fact
        // about its output, not a reason to collect its input.
        NativeExpr::Map { input, op } => NativeExpr::Map {
            input: Box::new(rewrite(input, boundary)?),
            op: op.clone(),
        },
        // A timestamp reads one series' positions, so it reads each piece as it is.
        NativeExpr::Timestamp { input } => NativeExpr::Timestamp {
            input: Box::new(rewrite(input, boundary)?),
        },
        // `info` indexes the info side by `(name, identifying values)` and keeps the newest sample of
        // each point, then merges the results that share a label set: both sides are read whole by a
        // kernel that does the grouping itself, so neither side is collected here.
        //
        // The duplicate check is the kernel's on both sides: its results are filed by `label_set`
        // and a field held twice at one point records "vector cannot contain metrics with the same
        // labelset", while the info side's own collision records "found duplicate series for info
        // metric". Removing the merges here therefore took no check with it.
        NativeExpr::Info {
            base,
            info,
            selector,
        } => NativeExpr::Info {
            base: Box::new(rewrite(base, boundary)?),
            info: Box::new(rewrite(info, boundary)?),
            selector: selector.clone(),
        },
        // An aggregate folds a group one point at a time: `AggregateRuntime::accumulate` folds every
        // chunk into states indexed by `(group, grid position)`, so several contributions to one
        // group at one position are that group's accumulation and not a series that has to be
        // assembled first. That a `Complete` aggregate needs *one execution* is the placement's
        // contract (`needs_single_execution_in`), which is a different answer from a merge.
        //
        // PART C: the merge that used to sit here was the only duplicate-label-set check on this
        // path. `FloatAggregator` folds whatever it is handed, so two fragments of one label set now
        // pass through unremarked and their values are summed. A check separated from the collection
        // has to be added here before this removal is safe.
        NativeExpr::Aggregate { input, op } => NativeExpr::Aggregate {
            input: Box::new(rewrite(input, boundary)?),
            op: op.clone(),
        },
        // A vector operation is decided one operator at a time, because only one of them needs its
        // input collected: `limitk` counts series in input order, so pieces of one label set would
        // be counted against `k` separately. `topk`, `limit_ratio` and `sort` rank, keep or order
        // series by what each holds at a point, `histogram_*` merges label-set pieces itself, and
        // `count_values` and `absent` fold every contribution at a point. The module documentation
        // is the table with the kernel each line was read from.
        //
        // `limitk`'s merge is the exception, and it keeps the check: `MergeInput::check` refuses a
        // stream that breaks its contract and `merge_same_labelset` refuses two values of one field
        // at one position.
        //
        // PART C: for the other six the merge that used to sit here was the only
        // duplicate-label-set check on this path. `topk`, `limit_ratio` and `sort` hand the pieces
        // back as separate series, `count_values` and `absent` fold them, and only `histogram_*`
        // calls `merge_same_labelset` itself.
        NativeExpr::Vector { input, op } => {
            let input = rewrite(input, boundary)?;
            NativeExpr::Vector {
                input: Box::new(match op {
                    BoundVectorOp::LimitK { .. }
                        if matches!(unifies_its_own_contributions(&input), Some(false)) =>
                    {
                        input
                    }
                    BoundVectorOp::LimitK { .. } => canonical(input, boundary),
                    BoundVectorOp::TopK { .. }
                    | BoundVectorOp::LimitRatio { .. }
                    | BoundVectorOp::Sort(_)
                    | BoundVectorOp::Absent { .. }
                    | BoundVectorOp::Histogram(_)
                    | BoundVectorOp::CountValues { .. } => input,
                }),
                op: op.clone(),
            }
        }
        // A binary operation is point-wise: a scalar side is broadcast over the vector side's series
        // one at a time, and two vectors are matched by the kernel, which files its rows by *result*
        // label set and refuses two values of a field at one point. Neither needs its input collected
        // into one series per label set, and matching-key completeness is the placement's question
        // (`matching_co_located`), not a merge's.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => NativeExpr::Binary {
            left: Box::new(rewrite(left, boundary)?),
            right: Box::new(rewrite(right, boundary)?),
            matching: matching.clone(),
        },
        // A set operation keeps each side's samples as they are and files them by result label set,
        // refusing two values of a field at one point, so it reads contributions directly too.
        NativeExpr::Set { left, right, op } => NativeExpr::Set {
            left: Box::new(rewrite(left, boundary)?),
            right: Box::new(rewrite(right, boundary)?),
            op: op.clone(),
        },
    };
    Ok(NativeNode {
        expr,
        output: node.output.clone(),
    })
}
