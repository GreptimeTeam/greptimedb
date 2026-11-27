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

use std::sync::Arc;

use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};

use super::merge_label_rewrites;
use crate::extension_plan::FieldKind;
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

fn scalar(value: f64, out: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(out),
    }
}

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

/// `abs(inner)`.
fn abs_map(inner: NativeNode) -> NativeNode {
    map_ops(inner, vec![MapOp::Math(MathOp::Abs)])
}

/// `histogram_count(inner)`, which drops every float sample it reads.
fn histogram_count(inner: NativeNode) -> NativeNode {
    map_ops(
        inner,
        vec![MapOp::Histogram(NativeHistogramFunction::Count)],
    )
}

/// `clamp_min(inner, argument)`.
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
                kind: datatypes::series::aggregate_state::AggregateStateKind::Sum,
                components: Arc::from(
                    crate::native::plan::aggregate_state_binding(
                        AggregateKind::Sum,
                        FieldKind::Float,
                    )
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
                field_kind: FieldKind::Float,
            },
        },
        output,
    }
}

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

/// `x + y` with no `on`/`ignoring`.
fn add() -> BinaryMatching {
    BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    }
}

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

/// `last_over_time(inner[5m])`, which keeps the metric name.
fn keeping_window_over(
    inner: NativeNode,
    input_grid: Option<Arc<EvalGrid>>,
    out: Arc<EvalGrid>,
) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function: RangeFunction::LastOverTime,
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

/// `rate(inner[5m])`, which drops the metric name.
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

/// `info(base, {})`.
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
            | NativeExpr::ParameterizedTopK { input, .. }
            | NativeExpr::ParameterizedHistogram { input, .. }
            | NativeExpr::ScalarOf { input }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Merge { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => vec![input.as_ref()],
            NativeExpr::ParameterizedAggregate {
                input, parameter, ..
            } => vec![input.as_ref(), parameter.as_ref()],
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

/// Runs the pass over `root` with no metric facts, so every metadata drop is unproved.
fn passed(root: NativeNode, source_tags: &[Vec<String>]) -> NativeTree {
    passed_over(root, source_tags, &[])
}

/// Runs the pass over `root` with `source_tags` and `source_metrics` indexed by `source_index`.
fn passed_over(
    root: NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> NativeTree {
    let tree = NativeTree::try_new(root).expect("the fixture is a legal tree");
    merge_label_rewrites(&tree, source_tags, source_metrics)
        .expect("the pass produces a legal tree")
}

/// A named scan's runtime labels: its tag and `__name__`, which is not a tag column.
fn named_scan_tags() -> Vec<Vec<String>> {
    vec![tags(&["host", "__name__"])]
}

fn tags(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// One tag, `host`, and no metric name.
fn scan_tags() -> Vec<Vec<String>> {
    vec![tags(&["host"])]
}

#[test]
fn a_fresh_destination_needs_no_collection() {
    let out = grid(0, 1000, 2);
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

        let twice = merge_label_rewrites(&tree, &scan_tags(), &[]).unwrap();
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
        let twice = merge_label_rewrites(&tree, &source_tags, &[]).unwrap();
        assert_eq!(tree.to_string(), twice.to_string(), "{name}");
        assert_eq!(1, merges(&twice), "{name}");
    }
}

#[test]
fn a_rewrite_above_a_match_collects_the_matches_own_labels() {
    let out = grid(0, 1000, 2);
    // Renaming `host` to a constant above the match folds its results, so the root collects them.
    // The right operand drops the metadata labels, so it is collected too.
    let matched = binary_over(selected(&out), abs_map(selected(&out)), Arc::clone(&out));
    let renamed = map_ops(
        matched,
        vec![MapOp::Label(Arc::new(
            LabelRewrite::replace("host", "same", "host", ".*").unwrap(),
        ))],
    );
    let tree = passed(renamed, &scan_tags());
    assert_eq!(
        2,
        merges(&tree),
        "the rewrite's own collection, and the one the name-dropping operand hands out: {tree}"
    );
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[]).unwrap();
    assert_eq!(tree.to_string(), twice.to_string());
    assert_eq!(2, merges(&twice));
}

#[test]
fn a_rewrite_of_the_label_count_values_writes_is_collected() {
    let out = grid(0, 1000, 2);
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
    // One merge below the map, for the rewrite, and one above it, for the map's own name drop.
    assert_eq!(2, merges(&tree), "{tree}");
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what the map hands out: {tree}");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the root stays the point-wise map");
    };
    let NativeExpr::Merge { input } = &input.expr else {
        panic!("the map is handed one series per label set");
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }));
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[]).unwrap();
    assert_eq!(2, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
}

/// `histogram_count(label_replace(x, "host", "same", "host", ".*"))`: the map erases the float
/// samples the collision would be judged on, so the merge sits between the rewrite and the map.
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
    assert_eq!(
        2,
        merges(&tree),
        "one merge below the sample-dropping map, one for the labels it drops"
    );
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what `histogram_count` hands out: {tree}");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[]).unwrap();
    assert_eq!(2, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn a_pointwise_map_collects_its_own_name_drop() {
    let out = grid(0, 1000, 2);
    // No metric facts, so each `abs` drop is unproved and collected above its map.
    let tree = passed(abs_map(abs_map(selected(&out))), &scan_tags());
    assert_eq!(2, merges(&tree), "{tree}");
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what the outer map hands out: {tree}");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the merge sits above the map, not below it: {tree}");
    };
    let NativeExpr::Merge { input } = &input.expr else {
        panic!("the outer map reads one series per label set: {tree}");
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }), "{tree}");
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[]).unwrap();
    assert_eq!(2, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn a_sample_dropping_map_over_a_fresh_destination_collects_its_own_drop() {
    let out = grid(0, 1000, 2);
    // The fresh `dst` needs no merge below the map; the map's own name drop is collected above it.
    let tree = passed(
        histogram_count(rewrite(selected(&out), "dst", "host")),
        &scan_tags(),
    );
    assert_eq!(1, merges(&tree), "a fresh destination cannot alias: {tree}");
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what `histogram_count` hands out: {tree}");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the root stays `histogram_count`");
    };
    assert!(matches!(input.expr, NativeExpr::Map { .. }));
}

#[test]
fn a_parameterized_map_collects_below_itself() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        clamp_min(
            rewrite(selected(&out), "host", "host"),
            scalar(0.0, Arc::clone(&out)),
        ),
        &scan_tags(),
    );
    // One merge below the clamp, for the rewrite, and one above it, for its name drop.
    assert_eq!(2, merges(&tree), "{tree}");
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what the clamp hands out: {tree}");
    };
    let NativeExpr::ParameterizedMap {
        input, parameters, ..
    } = &input.expr
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[]).unwrap();
    assert_eq!(2, merges(&twice));
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
    assert_eq!(
        2,
        merges(&tree),
        "the aliasing side and the name-dropping side are each collected: {tree}"
    );
    let NativeExpr::Binary { left, right, .. } = &tree.root().expr else {
        panic!("the root stays the match");
    };
    assert!(matches!(left.expr, NativeExpr::Merge { .. }));
    assert!(matches!(right.expr, NativeExpr::Merge { .. }));
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
    let tree = passed(
        keeping_window_over(
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
fn a_name_dropping_kernel_over_one_metric_keeps_its_partitions() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    let kept = |name: &str, root: NativeNode| {
        let tree = passed_over(root, &named_scan_tags(), &single);
        assert_eq!(0, merges(&tree), "`{name}` cannot alias: {tree}");
        assert!(
            !matches!(tree.root().expr, NativeExpr::Merge { .. }),
            "`{name}` hands over one series per label set already: {tree}"
        );
    };
    kept("abs", abs_map(selected(&out)));
    kept("histogram_count", histogram_count(selected(&out)));
    kept(
        "clamp_min",
        clamp_min(selected(&out), scalar(0.0, Arc::clone(&out))),
    );
    kept(
        "x * 2",
        binary_over(
            selected(&out),
            scalar(2.0, Arc::clone(&out)),
            Arc::clone(&out),
        ),
    );
}

#[test]
fn a_name_dropping_kernel_over_a_metadata_tag_is_collected() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    let collected = |name: &str, tags: &[Vec<String>], root: NativeNode| {
        let tree = passed_over(root, tags, &single);
        assert_eq!(1, merges(&tree), "`{name}` drops the name: {tree}");
        assert!(
            matches!(tree.root().expr, NativeExpr::Merge { .. }),
            "`{name}` hands out one series per label set: {tree}"
        );
    };
    for metadata in ["__type__", "__unit__"] {
        let scan = vec![tags(&["host", "__name__", metadata])];
        collected("abs", &scan, abs_map(selected(&out)));
        collected("histogram_count", &scan, histogram_count(selected(&out)));
        collected(
            "clamp_min",
            &scan,
            clamp_min(selected(&out), scalar(0.0, Arc::clone(&out))),
        );
        collected(
            "x * 2",
            &scan,
            binary_over(
                selected(&out),
                scalar(2.0, Arc::clone(&out)),
                Arc::clone(&out),
            ),
        );
    }
}

/// `rate(x[5m]) * 2` and `abs(abs(rate(x[5m])))` over one metric plan no merge.
#[test]
fn a_drop_above_a_name_dropping_kernel_needs_no_collection() {
    let out = grid(0, 1000, 2);
    let scan = [tags(&["host", "zone", "__name__"])];
    let single = [Some(Arc::<str>::from("some_metric"))];
    let rate = || window_over(raw_input(0), None, Arc::clone(&out));
    // `rate(x[5m]) * 2`.
    let broadcast = binary_over(rate(), scalar(2.0, Arc::clone(&out)), Arc::clone(&out));
    // `abs(abs(rate(x[5m])))`.
    let nested = abs_map(abs_map(rate()));
    for (query, root) in [
        ("rate(x[5m]) * 2", broadcast),
        ("abs(abs(rate(x[5m])))", nested),
    ] {
        let tree = passed_over(root, &scan, &single);
        assert_eq!(0, merges(&tree), "`{query}` plans a merge: {tree}");
    }
}

/// A merge keeps series that differ in `__name__`, `__type__` or `__unit__`, so a drop above it is
/// still collected when the source may hand over several metrics.
#[test]
fn a_merge_below_a_drop_does_not_answer_the_metadata_labels() {
    let out = grid(0, 1000, 2);
    let scan = [tags(&["host", "__name__"])];
    let tree = passed_over(
        abs_map(window_over(raw_input(0), None, Arc::clone(&out))),
        &scan,
        &[None],
    );
    assert_eq!(
        2,
        merges(&tree),
        "the window's name drop and the map's are two collisions, and the merge between them \
         cannot fold the names they remove: {tree}"
    );
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what the map hands out: {tree}");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the root stays the map: {tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Merge { .. }),
        "the window's answer is collected below the map: {tree}"
    );
}

/// A label-only rewrite passes the scan's single name through, unless it writes `__name__`.
#[test]
fn a_rewrite_of_the_metric_name_leaves_the_drop_unproved() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    // `abs(label_replace(x, "dst", "$1", "host", "(.*)"))`.
    let tree = passed_over(
        abs_map(rewrite(selected(&out), "dst", "host")),
        &named_scan_tags(),
        &single,
    );
    assert_eq!(
        0,
        merges(&tree),
        "a rewrite of another label keeps the name the scan bound: {tree}"
    );
    // `abs(label_replace(x, "__name__", "$1", "host", "(.*)"))`.
    let tree = passed_over(
        abs_map(rewrite(selected(&out), "__name__", "host")),
        &named_scan_tags(),
        &single,
    );
    assert_eq!(
        2,
        merges(&tree),
        "a rewrite of the metric name collects itself and leaves the drop above it unproved: {tree}"
    );
}

/// One collision is one collection, not one per kernel that repeats the drop.
#[test]
fn a_drop_above_a_merge_needs_no_collection_of_its_own() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    // `label_replace(x, "host", "$1", "host", "(.*)")`.
    let aliasing = || rewrite(selected(&out), "host", "host");

    // `label_replace(x, "host", "$1", "host", "(.*)") * 2`.
    let tree = passed_over(
        binary_over(aliasing(), scalar(2.0, Arc::clone(&out)), Arc::clone(&out)),
        &named_scan_tags(),
        &single,
    );
    assert_eq!(1, merges(&tree), "`label_replace(...) * 2`: {tree}");
    let NativeExpr::Binary { left, .. } = &tree.root().expr else {
        panic!("the root stays the broadcast: {tree}");
    };
    let NativeExpr::Merge { input } = &left.expr else {
        panic!("the vector side is handed one series per label set: {tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Map { .. }),
        "the merge sits above the rewrite, and no second one above the match: {tree}"
    );

    // `abs(abs(rate(label_replace(...)[5m])))`.
    let tree = passed_over(
        abs_map(abs_map(window_over(
            aliasing(),
            Some(Arc::clone(&out)),
            Arc::clone(&out),
        ))),
        &named_scan_tags(),
        &single,
    );
    assert_eq!(
        1,
        merges(&tree),
        "`abs(abs(rate(label_replace(...)[5m])))`: {tree}"
    );
    let NativeExpr::Map { input, .. } = &tree.root().expr else {
        panic!("the root stays the outer map: {tree}");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the two maps are not collected apart: {tree}");
    };
    let NativeExpr::Window { input, .. } = &input.expr else {
        panic!("the inner map reads the window: {tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Merge { .. }),
        "the one merge sits above the rewrite: {tree}"
    );
    let twice = merge_label_rewrites(&tree, &named_scan_tags(), &single).unwrap();
    assert_eq!(1, merges(&twice), "{twice}");
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn a_collision_below_a_merge_is_collected_there() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    // `rate(label_replace(x, "host", "$1", "host", "(.*)")[5m])`.
    let tree = passed_over(
        window_over(
            rewrite(selected(&out), "host", "host"),
            Some(Arc::clone(&out)),
            Arc::clone(&out),
        ),
        &named_scan_tags(),
        &single,
    );
    assert_eq!(
        1,
        merges(&tree),
        "the rewrite's own collection is what collects the collision: {tree}"
    );
    let NativeExpr::Window { input, .. } = &tree.root().expr else {
        panic!("the root stays the window: {tree}");
    };
    let NativeExpr::Merge { input } = &input.expr else {
        panic!("the window reads the collected rewrite: {tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Map { .. }),
        "the merge sits above the rewrite, not above the window: {tree}"
    );
    let twice = merge_label_rewrites(&tree, &named_scan_tags(), &single).unwrap();
    assert_eq!(1, merges(&twice), "{twice}");
    assert_eq!(tree.to_string(), twice.to_string());
}

/// `abs(label_replace(label_replace(x, "__name__", "$1", "host", "(.*)"), "host", "", "", ""))`
/// leaves the written name as the only label telling series apart, so `single_metric_name` must not
/// hand back the scan's own name.
#[test]
fn a_written_name_is_not_the_name_the_scan_binds() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    let rewritten = map_ops(
        map_ops(
            selected(&out),
            vec![MapOp::Label(Arc::new(
                LabelRewrite::replace("__name__", "$1", "host", "(.*)").unwrap(),
            ))],
        ),
        vec![MapOp::Label(Arc::new(
            LabelRewrite::replace("host", "", "", "").unwrap(),
        ))],
    );
    let tree = passed_over(abs_map(rewritten), &named_scan_tags(), &single);
    assert_eq!(3, merges(&tree), "{tree}");
    let NativeExpr::Merge { input: abs_node } = &tree.root().expr else {
        panic!("the root collects what `abs` hands out: {tree}");
    };
    let NativeExpr::Map { input: below, op } = &abs_node.expr else {
        panic!("the collected node is the map above the rewrites: {tree}");
    };
    assert!(
        op.ops.iter().any(|op| matches!(op, MapOp::Math(_))),
        "the root merge sits above `abs`, not above the rewrites: {tree}"
    );
    let NativeExpr::Merge { input: delete } = &below.expr else {
        panic!("`abs` reads the collected rewrite: {tree}");
    };
    let NativeExpr::Map {
        input: name_write, ..
    } = &delete.expr
    else {
        panic!("the merge below `abs` sits above the delete: {tree}");
    };
    assert!(
        matches!(name_write.expr, NativeExpr::Merge { .. }),
        "the name write is collected below the delete: {tree}"
    );
}

/// A name-dropping window, `timestamp(x)` and `timestamp` over an evaluated input are proved only over
/// a single-metric source without `__type__`/`__unit__` tags.
#[test]
fn a_name_dropping_node_is_proved_by_the_labels_alone() {
    let out = grid(0, 1000, 2);
    let named = named_scan_tags();
    let single = [Some(Arc::<str>::from("some_metric"))];
    let collected =
        |name: &str, tags: &[Vec<String>], metrics: &[Option<Arc<str>>], root: NativeNode| {
            let tree = passed_over(root, tags, metrics);
            assert_eq!(
                1,
                merges(&tree),
                "`{name}` drops the metadata labels: {tree}"
            );
            assert!(
                matches!(tree.root().expr, NativeExpr::Merge { .. }),
                "the root collects `{name}`: {tree}"
            );
        };
    let kept = |name: &str, root: NativeNode| {
        let tree = passed_over(root, &named, &single);
        assert_eq!(0, merges(&tree), "`{name}` cannot alias: {tree}");
        assert!(
            !matches!(tree.root().expr, NativeExpr::Merge { .. }),
            "the root stays `{name}`: {tree}"
        );
    };
    let stamps = || selection(raw_input(0), Arc::clone(&out), InstantValue::Timestamp);
    let drops = || window_over(raw_input(0), None, Arc::clone(&out));
    let evaluated_stamps = || NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(drops()),
        },
        output: evaluated(fields(1), Arc::clone(&out)),
    };

    kept("timestamp", stamps());
    kept("rate", drops());
    kept("timestamp(rate)", evaluated_stamps());
    collected("timestamp", &named, &[None], stamps());
    collected("rate", &named, &[None], drops());
    collected("timestamp(rate)", &named, &[None], evaluated_stamps());
    for metadata in ["__type__", "__unit__"] {
        let mut typed = named.clone();
        typed[0].push(metadata.to_string());
        collected("timestamp", &typed, &single, stamps());
        collected("rate", &typed, &single, drops());
        collected("timestamp(rate)", &typed, &single, evaluated_stamps());
    }
    let tree = passed_over(
        keeping_window_over(raw_input(0), None, Arc::clone(&out)),
        &named,
        &single,
    );
    assert_eq!(0, merges(&tree), "`last_over_time` keeps the name: {tree}");
    assert!(
        matches!(tree.root().expr, NativeExpr::Window { .. }),
        "the root stays the name-keeping window: {tree}"
    );
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
fn a_scalar_broadcast_collects_its_own_name_drop() {
    let out = grid(0, 1000, 2);
    let tree = passed(
        binary_over(
            scalar(1.0, Arc::clone(&out)),
            selected(&out),
            Arc::clone(&out),
        ),
        &scan_tags(),
    );
    // `1 + x`: the merge for the broadcast's name drop sits above it.
    assert_eq!(1, merges(&tree), "{tree}");
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what the broadcast hands out: {tree}");
    };
    let NativeExpr::Binary { left, right, .. } = &input.expr else {
        panic!("the root stays the broadcast");
    };
    assert!(
        matches!(left.expr, NativeExpr::Scalar(_)),
        "a constant holds no label set"
    );
    assert!(
        !matches!(right.expr, NativeExpr::Merge { .. }),
        "the vector side hands its series over as it reads them"
    );
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[]).unwrap();
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn an_unknown_source_proves_nothing_and_is_collected() {
    let out = grid(0, 1000, 2);
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
    let twice = merge_label_rewrites(&once, &scan_tags(), &[]).expect("a collected tree is legal");
    assert_eq!(1, merges(&twice));
    assert_eq!(once.to_string(), twice.to_string());
}

#[test]
fn a_later_rewrite_of_an_earlier_destination_is_collected() {
    let out = grid(0, 1000, 2);
    // The second rewrite writes the `dst` the first one created.
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
