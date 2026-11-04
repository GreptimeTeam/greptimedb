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
use crate::histogram_eval::{HistogramFunction, NativeHistogramFunction};
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::map_label::LabelRewrite;
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundVectorOp, BoundWindow, Grouping, InstantValue, MapOp, NativeExpr, NativeNode, NativeTree,
    NativeType, ParameterizedOp,
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
            op: BoundMap { ops: ops.into() },
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

/// `histogram_count(inner)`: point-wise over the grid, but it drops every float sample it reads.
fn histogram_count(inner: NativeNode) -> NativeNode {
    map_ops(
        inner,
        vec![MapOp::Histogram(NativeHistogramFunction::Count)],
    )
}

/// `clamp_min(inner, argument)`: a point-wise function whose bound argument is a scalar on its grid.
fn clamp_min(inner: NativeNode, argument: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::ParameterizedMap {
            input: Box::new(inner),
            op: ParameterizedOp::ClampMin,
            parameters: vec![argument],
        },
        output,
    }
}

/// `sum without() (inner)` at `stage`, which folds every series it is handed into one group.
fn aggregate_over(inner: NativeNode, out: Arc<EvalGrid>, stage: AggregateStage) -> NativeNode {
    let output = match stage {
        AggregateStage::Partial => {
            NativeType::AggregateState(Arc::new(crate::native::plan::AggregateStateSchema {
                group: Grouping::By(Arc::from([])),
                // The state family this branch carries: the kind and its components come from the
                // kernel binding, not from a second mapping.
                kind: datatypes::series::aggregate_state::AggregateStateKind::Sum,
                components: Arc::from(
                    crate::native::plan::aggregate_state_binding(AggregateKind::Sum)
                        .expect("sum has a mergeable state")
                        .1,
                ),
                accumulators: fields(1),
                grid: Arc::clone(&out),
            }))
        }
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
fn window_over(
    inner: NativeNode,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
) -> NativeNode {
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
fn presence_and_folding_vector_operations_collect_aliasing_inputs() {
    let out = grid(0, 1000, 2);
    let ops = [
        BoundVectorOp::Absent {
            labels: datatypes::series::SeriesLabels::try_new(None, Vec::new()).unwrap(),
        },
        BoundVectorOp::Histogram(HistogramFunction::Quantile(0.5)),
        BoundVectorOp::CountValues {
            label: Arc::from("value"),
            group: Grouping::By(Arc::from([])),
        },
    ];
    for op in ops {
        // Distinct host values can become the same label set, including at an overlapping point.
        let aliasing = map_ops(
            selected(&out),
            vec![MapOp::Label(Arc::new(
                LabelRewrite::replace("host", "same", "host", ".*").unwrap(),
            ))],
        );
        let tree = passed(vector_over(aliasing, op.clone()), &scan_tags());
        assert_eq!(1, merges(&tree), "{op:?}");
        let NativeExpr::Vector { input, .. } = &tree.root().expr else {
            panic!("the root stays the vector operation: {op:?}");
        };
        let NativeExpr::Merge { input } = &input.expr else {
            panic!("{op:?} must read a canonical series");
        };
        assert!(matches!(input.expr, NativeExpr::Map { .. }), "{op:?}");

        let twice = merge_label_rewrites(&tree, &scan_tags()).unwrap();
        assert_eq!(1, merges(&twice), "{op:?}");
        assert_eq!(tree.to_string(), twice.to_string(), "{op:?}");
    }
}

#[test]
fn rewrites_after_ranking_or_grouping_are_collected() {
    let out = grid(0, 1000, 2);
    let source_tags = vec![tags(&["host", "zone"])];
    // Different hosts may both survive topk(2). With topk(1) by host, one series survives in each
    // host group. Sum by host likewise produces one output per host, not one for the whole vector.
    let top_two = vector_over(selected(&out), topk(2.0));
    let grouped_top_one = vector_over(
        selected(&out),
        BoundVectorOp::TopK {
            k: 1.0,
            top: true,
            group: Grouping::By(Arc::new([Arc::from("host")])),
        },
    );
    let mut grouped_sum =
        aggregate_over(selected(&out), Arc::clone(&out), AggregateStage::Complete);
    let NativeExpr::Aggregate { op, .. } = &mut grouped_sum.expr else {
        unreachable!("aggregate_over constructs an aggregate");
    };
    op.group = Grouping::By(Arc::new([Arc::from("host")]));

    for (name, inner) in [
        ("topk(2)", top_two),
        ("topk(1) by host", grouped_top_one),
        ("sum by host", grouped_sum),
    ] {
        // Unlike copying host to itself, a fixed destination can actually make those outputs equal.
        let renamed = map_ops(
            inner,
            vec![MapOp::Label(Arc::new(
                LabelRewrite::replace("host", "same", "host", ".*").unwrap(),
            ))],
        );
        let tree = passed(renamed, &source_tags);
        assert_eq!(1, merges(&tree), "{name}");
        let NativeExpr::Merge { input } = &tree.root().expr else {
            panic!("the statement must collect the rewrite after {name}");
        };
        let NativeExpr::Map { input, .. } = &input.expr else {
            panic!("the merge must sit directly above the rewrite after {name}");
        };
        assert!(
            matches!(
                input.expr,
                NativeExpr::Vector { .. } | NativeExpr::Aggregate { .. }
            ),
            "{name}"
        );
        let twice = merge_label_rewrites(&tree, &source_tags).unwrap();
        assert_eq!(tree.to_string(), twice.to_string(), "{name}");
        assert_eq!(1, merges(&twice), "{name}");
    }
}

#[test]
fn a_rewrite_above_a_match_collects_the_matches_own_labels() {
    let out = grid(0, 1000, 2);
    // A match answers one series per label set, but those labels are its operands': renaming `host`
    // to a constant above it puts both sides' results under one set, so the consumer above the
    // rewrite is the one that has to collect them. The match itself reads its operands as they are.
    let matched = binary_over(selected(&out), abs_map(selected(&out)), Arc::clone(&out));
    let renamed = map_ops(
        matched,
        vec![MapOp::Label(Arc::new(
            LabelRewrite::replace("host", "same", "host", ".*").unwrap(),
        ))],
    );
    let tree = passed(renamed, &scan_tags());
    assert_eq!(1, merges(&tree));
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the statement must collect the rewrite above the match");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the merge sits above the rewrite, not below it");
    };
    assert!(
        matches!(input.expr, NativeExpr::Binary { .. }),
        "the match is not handed the rewritten operand"
    );
    let twice = merge_label_rewrites(&tree, &scan_tags()).unwrap();
    assert_eq!(tree.to_string(), twice.to_string());
    assert_eq!(1, merges(&twice));
}

#[test]
fn a_rewrite_of_the_label_count_values_writes_is_collected() {
    let out = grid(0, 1000, 2);
    // `count_values` answers one series per distinct value under the label it writes, so renaming
    // that label to a constant folds every one of them into one label set.
    let counted = vector_over(
        selected(&out),
        BoundVectorOp::CountValues {
            label: Arc::from("value"),
            group: Grouping::By(Arc::from([])),
        },
    );
    let renamed = map_ops(
        counted,
        vec![MapOp::Label(Arc::new(
            LabelRewrite::replace("value", "same", "value", ".*").unwrap(),
        ))],
    );
    let tree = passed(renamed, &scan_tags());
    assert_eq!(1, merges(&tree));
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the statement must collect the rewrite of the label `count_values` writes");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the merge sits above the rewrite, not above count_values");
    };
    assert!(matches!(input.expr, NativeExpr::Vector { .. }));
}

#[test]
fn a_fresh_destination_above_count_values_needs_no_collection() {
    let out = grid(0, 1000, 2);
    // `dst` is not a label `count_values` reports - its grouping's are, and so is the one it writes
    // - so no two of its series can end under one label set, and deriving that set is what says a
    // fresh destination above it has nothing to collect.
    let counted = vector_over(
        selected(&out),
        BoundVectorOp::CountValues {
            label: Arc::from("value"),
            group: Grouping::By(Arc::from([])),
        },
    );
    let tree = passed(rewrite(counted, "dst", "value"), &scan_tags());
    assert_eq!(0, merges(&tree), "a fresh destination cannot alias");
    assert!(
        matches!(tree.root().expr, NativeExpr::Map { .. }),
        "the root stays the rewrite"
    );
}

#[test]
fn a_pointwise_map_reads_each_piece_as_it_is() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        abs_map(rewrite(selected(&out), "host", "host")),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    // The map stays the root and its input is the merge: a map reads pieces point-wise, but a point
    // it reads can be one it drops, so the collection has to happen below it.
    let NativeExpr::Map { input, .. } = &tree.root().expr else {
        panic!("the root stays the point-wise map");
    };
    let NativeExpr::Merge { input } = &input.expr else {
        panic!("the map is handed one series per label set");
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }));
    let twice = merge_label_rewrites(&tree, &scan_tags()).unwrap();
    assert_eq!(1, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
}

/// `histogram_count(label_replace(x, "host", "same", "host", ".*"))`: the map that erases the
/// samples the collision would be judged on, so the merge has to sit between the rewrite and it.
#[test]
fn a_sample_dropping_map_collects_below_itself() {
    let out = grid(0, 1000, 2);
    let aliasing = map_ops(
        selected(&out),
        vec![MapOp::Label(Arc::new(
            LabelRewrite::replace("host", "same", "host", ".*").unwrap(),
        ))],
    );
    let tree = passed(histogram_count(aliasing), &scan_tags());
    assert_eq!(1, merges(&tree), "one merge, below the sample-dropping map");
    let NativeExpr::Map { input, .. } = &tree.root().expr else {
        panic!("the root stays `histogram_count`");
    };
    let NativeExpr::Merge { input } = &input.expr else {
        panic!("`histogram_count` reads one series per label set rather than the pieces");
    };
    let NativeExpr::Map { input, op } = &input.expr else {
        panic!("the merge sits above the rewrite and below the map");
    };
    assert!(
        op.ops.iter().any(|op| matches!(op, MapOp::Label(_))),
        "the collected node is the rewrite"
    );
    assert!(
        matches!(input.expr, NativeExpr::Instant { .. }),
        "the rewrite still reads the selection it read before"
    );
    // A second pass over the collected tree changes nothing: the merge answers one series per label
    // set, so nothing below the map aliases any more.
    let twice = merge_label_rewrites(&tree, &scan_tags()).unwrap();
    assert_eq!(1, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn a_pointwise_map_over_a_clean_input_gains_no_merge() {
    let out = grid(0, 1000, 2);
    // No rewrite anywhere in the tree, so no two pieces can share a label set.
    let tree = passed(abs_map(abs_map(selected(&out))), &scan_tags());
    assert_eq!(0, merges(&tree));
    let NativeExpr::Map { input, .. } = &tree.root().expr else {
        panic!("the root stays the point-wise map");
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }));
    let twice = merge_label_rewrites(&tree, &scan_tags()).unwrap();
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn a_sample_dropping_map_over_a_fresh_destination_gains_no_merge() {
    let out = grid(0, 1000, 2);
    // `dst` names no label the scan reports, so the rewrite cannot alias and the map's input needs
    // no collection even though the map drops samples.
    let tree = passed(
        histogram_count(rewrite(selected(&out), "dst", "host")),
        &scan_tags(),
    );
    assert_eq!(0, merges(&tree), "a fresh destination cannot alias");
    let NativeExpr::Map { input, .. } = &tree.root().expr else {
        panic!("the root stays `histogram_count`");
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }));
}

#[test]
fn a_parameterized_map_collects_below_itself() {
    let out = grid(0, 1000, 2);
    // A clamp may empty a point as well, so its input is collected below it exactly as a map's is;
    // the bound argument is a scalar and carries no label set to collect.
    let tree = passed(
        clamp_min(
            rewrite(selected(&out), "host", "host"),
            scalar(0.0, Arc::clone(&out)),
        ),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    let NativeExpr::ParameterizedMap {
        input, parameters, ..
    } = &tree.root().expr
    else {
        panic!("the root stays the parameterized map");
    };
    let NativeExpr::Merge { input } = &input.expr else {
        panic!("the parameterized map is handed one series per label set");
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }));
    assert_eq!(1, parameters.len());
    assert!(
        matches!(parameters[0].expr, NativeExpr::Scalar(_)),
        "a scalar argument is not collected"
    );
    let twice = merge_label_rewrites(&tree, &scan_tags()).unwrap();
    assert_eq!(1, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
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
        binary_over(
            scalar(1.0, Arc::clone(&out)),
            selected(&out),
            Arc::clone(&out),
        ),
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
fn aggregate_state_is_not_collected() {
    let out = grid(0, 1000, 2);
    let state = aggregate_over(selected(&out), out, AggregateStage::Partial);
    let tree = passed(state, &scan_tags());
    assert_eq!(0, merges(&tree));
    assert!(matches!(tree.root().output, NativeType::AggregateState(_)));
    let twice = merge_label_rewrites(&tree, &scan_tags()).unwrap();
    assert_eq!(tree.to_string(), twice.to_string());
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
fn presence_and_folding_vector_operations_do_not_collect_distinct_inputs() {
    let out = grid(0, 1000, 2);
    for op in [
        BoundVectorOp::Absent {
            labels: datatypes::series::SeriesLabels::try_new(None, Vec::new()).unwrap(),
        },
        BoundVectorOp::Histogram(HistogramFunction::Quantile(0.5)),
        BoundVectorOp::CountValues {
            label: Arc::from("value"),
            group: Grouping::By(Arc::from([])),
        },
    ] {
        let tree = passed(
            vector_over(rewrite(selected(&out), "dst", "host"), op.clone()),
            &scan_tags(),
        );
        assert_eq!(0, merges(&tree), "{op:?}");
        let NativeExpr::Vector { input, .. } = &tree.root().expr else {
            panic!("the root stays the vector operation: {op:?}");
        };
        assert!(matches!(input.expr, NativeExpr::Map { .. }), "{op:?}");
    }
}
