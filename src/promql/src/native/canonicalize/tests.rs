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

//! What the canonical-merge pass has to get right: it collects exactly where a rewrite could give
//! two series one label set, it never collects a shape a merge cannot take, and running it twice
//! changes nothing.
//!
//! The trees are built node by node over the one-field schemas of a `rate(x[5m])`-style fixture, and
//! the labels each source reports are supplied by the test: the pass's whole decision is whether a
//! rewrite writes a label its input already carries, so a test that says nothing about the input's
//! labels is testing the conservative branch and has to say so.

use std::sync::Arc;

use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};

use super::merge_label_rewrites;
use crate::histogram_eval::HistogramFunction;
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundVectorOp, BoundWindow, Grouping, InstantValue, MapOp, NativeExpr, NativeNode, NativeTree,
    NativeType,
};
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels, SetOp,
};

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn fields(count: usize) -> Arc<[FieldSpec]> {
    (0..count)
        .map(|index| FieldSpec {
            name: format!("v{index}").into(),
        })
        .collect()
}

fn evaluated(schema_fields: Arc<[FieldSpec]>, grid: Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema_fields,
        eval_grid: Some(grid),
    }))
}

/// A scan of raw samples, the leaf every tree here is built over.
fn raw_input(source_index: usize) -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(1),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// A constant, which a binary operation broadcasts.
fn scalar(value: f64, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(out),
    }
}

/// An instant selection over `inner` that reports what `value` says it reports, on `out`.
fn selection(inner: NativeNode, out: Arc<EvalGrid>, value: InstantValue) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(inner),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// `x` as an evaluated series: the scan, selected onto `out`.
fn selected(out: &Arc<EvalGrid>) -> NativeNode {
    selection(raw_input(0), Arc::clone(out), InstantValue::Sample)
}

/// A map that applies `ops` to `inner`, keeping its layout.
fn map_ops(inner: NativeNode, ops: Vec<MapOp>) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: ops.into(),
            },
        },
        output,
    }
}

/// `label_replace(inner, dst, "$1", src, "(.*)")`, a rewrite that writes `dst`.
fn rewrite(inner: NativeNode, dst: &str, src: &str) -> NativeNode {
    map_ops(
        inner,
        vec![MapOp::Label(Arc::new(
            LabelRewrite::replace(dst, "$1", src, "(.*)").expect("the rewrite compiles"),
        ))],
    )
}

/// `abs(inner)`: a point-wise map that keeps the label set's primary key.
fn abs_map(inner: NativeNode) -> NativeNode {
    map_ops(inner, vec![MapOp::Math(MathOp::Abs)])
}

/// `sum without() (inner)` at `stage`, which folds every series it is handed into one group.
fn aggregate_over(inner: NativeNode, out: Arc<EvalGrid>, stage: AggregateStage) -> NativeNode {
    let output = match stage {
        AggregateStage::Partial => NativeType::AggregateState(Arc::new(
            crate::native::plan::AggregateStateSchema {
                group: Grouping::By(Arc::from([])),
                accumulators: fields(1),
                grid: Arc::clone(&out),
            },
        )),
        AggregateStage::Complete | AggregateStage::Final => evaluated(fields(1), out),
    };
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::from([])),
                stage,
            },
        },
        output,
    }
}

/// A vector operation over `inner`, laid out as `inner` is.
fn vector_over(inner: NativeNode, op: BoundVectorOp) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op,
        },
        output,
    }
}

fn topk(k: f64) -> BoundVectorOp {
    BoundVectorOp::TopK {
        k,
        top: true,
        group: Grouping::By(Arc::from([])),
    }
}

/// An `adding` match with no `on`/`ignoring`, which is `x + y`.
fn add() -> BinaryMatching {
    BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    }
}

/// A binary operation over `left` and `right` on `out`, one value per input field.
fn binary_over(left: NativeNode, right: NativeNode, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: evaluated(fields(1), out),
    }
}

/// A set operation over `left` and `right` on `out`.
fn set_over(left: NativeNode, right: NativeNode, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Set {
            left: Box::new(left),
            right: Box::new(right),
            op: crate::native::plan::BoundSetOp {
                op: SetOp::Or,
                labels: MatchingLabels::Ignoring(Vec::new()),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// `rate(inner[5m])`, a window that folds `inner` onto `out` and drops the metric name.
fn window_over(inner: NativeNode, input_grid: Option<Arc<EvalGrid>>, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 300_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    }
}

/// `info(base, {})`: the base series with the data labels of the info series that share theirs.
fn info_over(base: NativeNode, info: NativeNode) -> NativeNode {
    let output = base.output.clone();
    NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(base),
            info: Box::new(info),
            selector: InfoSelector {
                name_matchers: Vec::new(),
                data_labels: Vec::new(),
                keep_unmatched: true,
            },
        },
        output,
    }
}

/// How many merges `tree` holds.
fn merges(tree: &NativeTree) -> usize {
    fn visit(node: &NativeNode) -> usize {
        let children = match &node.expr {
            NativeExpr::Input(_)
            | NativeExpr::Scalar(_)
            | NativeExpr::EvalTime { .. }
            | NativeExpr::VectorOf { .. } => Vec::new(),
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::ParameterizedMap { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Merge { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => vec![input.as_ref()],
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => vec![left.as_ref(), right.as_ref()],
        };
        usize::from(matches!(node.expr, NativeExpr::Merge { .. }))
            + children.into_iter().map(visit).sum::<usize>()
    }
    visit(tree.root())
}

/// Runs the pass over `root`, whose sources report `source_tags`.
fn passed(root: NativeNode, source_tags: &[Vec<String>]) -> NativeTree {
    let tree = NativeTree::try_new(root).expect("the fixture is a legal tree");
    merge_label_rewrites(&tree, source_tags).expect("the pass produces a legal tree")
}

fn tags(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// The labels each fixture source reports, which is what decides whether a rewrite can alias.
fn scan_tags() -> Vec<Vec<String>> {
    vec![tags(&["host"])]
}

#[test]
fn a_fresh_destination_needs_no_collection() {
    let out = grid(0, 1000, 2);
    // `dst` names no label the scan reports, so no two series can end under one label set.
    let tree = passed(rewrite(selected(&out), "dst", "host"), &scan_tags());
    assert_eq!(0, merges(&tree), "a fresh destination cannot alias");
    assert!(
        matches!(tree.root().expr, NativeExpr::Map { .. }),
        "the root stays the rewrite"
    );
}

#[test]
fn a_rewrite_of_a_scan_tag_is_collected_at_the_root() {
    let out = grid(0, 1000, 2);
    // Writing `host` - the label that told the scan's series apart - can fold them into one set.
    let tree = passed(rewrite(selected(&out), "host", "host"), &scan_tags());
    assert_eq!(1, merges(&tree));
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root is the consumer this merge answers");
    };
    assert!(
        matches!(input.expr, NativeExpr::Map { .. }),
        "the merge sits above the rewrite, not below it"
    );
}

#[test]
fn a_fold_is_handed_one_series_per_set() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        aggregate_over(
            rewrite(selected(&out), "host", "host"),
            Arc::clone(&out),
            AggregateStage::Complete,
        ),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    let NativeExpr::Aggregate { input, .. } = &tree.root().expr else {
        panic!("the root stays the fold");
    };
    assert!(
        matches!(input.expr, NativeExpr::Merge { .. }),
        "a fold sums the pieces it is handed, so it is handed one"
    );
}

#[test]
fn a_ranking_operation_is_handed_one_series_per_set() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        vector_over(rewrite(selected(&out), "host", "host"), topk(1.0)),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    let NativeExpr::Vector { input, .. } = &tree.root().expr else {
        panic!("the root stays the ranking");
    };
    assert!(
        matches!(input.expr, NativeExpr::Merge { .. }),
        "`k` counts series, so pieces of one set must not occupy several slots"
    );
}

#[test]
fn a_presence_reading_vector_operation_collects_nothing() {
    let out = grid(0, 1000, 2);
    // `absent` folds every contribution at a point, so the pieces are the presence it reads.
    let absent = || BoundVectorOp::Absent {
        labels: datatypes::series::SeriesLabels::try_new(None, Vec::new()).unwrap(),
    };
    let tree = passed(
        vector_over(rewrite(selected(&out), "host", "host"), absent()),
        &scan_tags(),
    );
    assert_eq!(0, merges(&tree));
}

#[test]
fn a_pointwise_map_reads_each_piece_as_it_is() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        abs_map(rewrite(selected(&out), "host", "host")),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    // The merge is the root, above the point-wise map: the map reads pieces, its consumer does not.
    assert!(matches!(tree.root().expr, NativeExpr::Merge { .. }));
}

#[test]
fn each_side_of_a_match_is_collected_on_its_own() {
    let out = grid(0, 1000, 2);
    let aliasing = || rewrite(selected(&out), "host", "host");
    let tree = passed(
        binary_over(aliasing(), abs_map(selected(&out)), Arc::clone(&out)),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree), "only the aliasing side needs collecting");
    let NativeExpr::Binary { left, right, .. } = &tree.root().expr else {
        panic!("the root stays the match");
    };
    assert!(matches!(left.expr, NativeExpr::Merge { .. }));
    assert!(!matches!(right.expr, NativeExpr::Merge { .. }));
}

#[test]
fn a_set_operation_collects_each_side() {
    let out = grid(0, 1000, 2);
    let aliasing = || rewrite(selected(&out), "host", "host");
    let tree = passed(
        set_over(aliasing(), aliasing(), Arc::clone(&out)),
        &scan_tags(),
    );
    assert_eq!(2, merges(&tree));
    let NativeExpr::Set { left, right, .. } = &tree.root().expr else {
        panic!("the root stays the set operation");
    };
    assert!(matches!(left.expr, NativeExpr::Merge { .. }));
    assert!(matches!(right.expr, NativeExpr::Merge { .. }));
}

#[test]
fn a_window_collects_an_aliasing_child() {
    let out = grid(0, 1000, 2);
    // A window over an evaluated vector folds one series' range, so it reads the series whole.
    let tree = passed(
        window_over(
            rewrite(selected(&out), "host", "host"),
            Some(Arc::clone(&out)),
            Arc::clone(&out),
        ),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    let NativeExpr::Window { input, .. } = &tree.root().expr else {
        panic!("the root stays the window");
    };
    assert!(matches!(input.expr, NativeExpr::Merge { .. }));
}

#[test]
fn a_window_over_a_raw_scan_collects_nothing() {
    let out = grid(0, 1000, 2);
    // The scan hands over whole storage series, and a merge cannot take raw samples anyway.
    let tree = passed(window_over(raw_input(0), None, out), &scan_tags());
    assert_eq!(0, merges(&tree));
}

#[test]
fn info_collects_the_side_that_aliases() {
    let out = grid(0, 1000, 2);
    let info = selection(raw_input(1), Arc::clone(&out), InstantValue::InfoTimestamp);
    let tree = passed(
        info_over(rewrite(selected(&out), "host", "host"), info),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    let NativeExpr::Info { base, info, .. } = &tree.root().expr else {
        panic!("the root stays the info join");
    };
    assert!(matches!(base.expr, NativeExpr::Merge { .. }));
    assert!(
        !matches!(info.expr, NativeExpr::Merge { .. }),
        "the info side reads one sample time per series and aliases nothing"
    );
}

#[test]
fn a_scalar_side_of_a_broadcast_is_not_collected() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        binary_over(scalar(1.0, Arc::clone(&out)), selected(&out), Arc::clone(&out)),
        &scan_tags(),
    );
    assert_eq!(0, merges(&tree), "a constant holds no label set");
}

#[test]
fn a_scalar_root_is_never_collected() {
    let out = grid(0, 1000, 2);
    let tree = passed(scalar(1.0, out), &scan_tags());
    assert_eq!(0, merges(&tree));
}

#[test]
fn a_raw_root_is_never_collected() {
    let tree = passed(raw_input(0), &scan_tags());
    assert_eq!(0, merges(&tree));
}

#[test]
fn an_unknown_source_proves_nothing_and_is_collected() {
    let out = grid(0, 1000, 2);
    // The caller says nothing about the labels this source reports, so a rewrite over it may well
    // have written one of them.
    let tree = passed(rewrite(selected(&out), "dst", "host"), &[]);
    assert_eq!(1, merges(&tree));
}

#[test]
fn the_pass_is_idempotent() {
    let out = grid(0, 1000, 2);
    let once = passed(
        aggregate_over(
            rewrite(selected(&out), "host", "host"),
            Arc::clone(&out),
            AggregateStage::Complete,
        ),
        &scan_tags(),
    );
    let twice = merge_label_rewrites(&once, &scan_tags()).expect("a collected tree is legal");
    assert_eq!(1, merges(&twice));
    assert_eq!(once.to_string(), twice.to_string());
}

#[test]
fn a_later_rewrite_of_an_earlier_destination_is_collected() {
    let out = grid(0, 1000, 2);
    // The first rewrite writes `dst`, which is fresh; the second writes the `dst` the first one
    // created, so it can fold what the first distinguished.
    let two = map_ops(
        selected(&out),
        vec![
            MapOp::Label(Arc::new(
                LabelRewrite::replace("dst", "$1", "host", "(.*)").unwrap(),
            )),
            MapOp::Label(Arc::new(
                LabelRewrite::replace("dst", "fixed", "host", "(.*)").unwrap(),
            )),
        ],
    );
    let tree = passed(two, &scan_tags());
    assert_eq!(1, merges(&tree));
}

#[test]
fn a_sort_keeps_the_alias_of_its_input() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        vector_over(
            rewrite(selected(&out), "host", "host"),
            BoundVectorOp::Sort(SortOrder::Asc),
        ),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
}

#[test]
fn a_histogram_fold_collects_nothing() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        vector_over(
            rewrite(selected(&out), "host", "host"),
            BoundVectorOp::Histogram(HistogramFunction::Quantile(0.5)),
        ),
        &scan_tags(),
    );
    assert_eq!(0, merges(&tree), "the kernel merges the buckets it reads");
}
