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

/// `last_over_time(inner[5m])`, a window that folds `inner` onto `out` and keeps the metric name.
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
            | NativeExpr::ParameterizedTopK { input, .. }
            | NativeExpr::ParameterizedHistogram { input, .. }
            | NativeExpr::ScalarOf { input }
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
///
/// Nothing is stated about which metric each source reads, so a kernel that drops the metadata
/// labels over one of them proves nothing and is collected.
fn passed(root: NativeNode, source_tags: &[Vec<String>]) -> NativeTree {
    passed_over(root, source_tags, &[])
}

/// Runs the pass over `root`, whose sources report `source_tags` and read the metric each of
/// `source_metrics` names, indexed the way `merge_label_rewrites` takes it.
///
/// The scans below these fixtures key their series by columns that always decode a value, which is
/// the case a metadata drop has to be proved against the labels alone: `NULL` and `''` in one key
/// column are what [`passed_over_nullable_keys`] states instead.
fn passed_over(
    root: NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> NativeTree {
    passed_over_with_keys(root, source_tags, source_metrics, true)
}

/// The same pass over a tree whose scans key their series by a column that can hold `NULL` in one
/// series and `''` in another, so no metadata drop below it can be proved by the labels alone.
fn passed_over_nullable_keys(
    root: NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
) -> NativeTree {
    passed_over_with_keys(root, source_tags, source_metrics, false)
}

/// Runs the pass over `root` with the key fact the caller states, which is what
/// `merge_label_rewrites` takes as `keys_survive`.
fn passed_over_with_keys(
    root: NativeNode,
    source_tags: &[Vec<String>],
    source_metrics: &[Option<Arc<str>>],
    keys_survive: bool,
) -> NativeTree {
    let tree = NativeTree::try_new(root).expect("the fixture is a legal tree");
    merge_label_rewrites(&tree, source_tags, source_metrics, keys_survive)
        .expect("the pass produces a legal tree")
}

/// The labels a scan bound to one metric reports: its tag, and the name the binding gives every
/// series it hands over, which is not a tag column.
fn named_scan_tags() -> Vec<Vec<String>> {
    vec![tags(&["host", "__name__"])]
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

        let twice = merge_label_rewrites(&tree, &scan_tags(), &[], true).unwrap();
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
        let twice = merge_label_rewrites(&tree, &source_tags, &[], true).unwrap();
        assert_eq!(tree.to_string(), twice.to_string(), "{name}");
        assert_eq!(1, merges(&twice), "{name}");
    }
}

#[test]
fn a_rewrite_above_a_match_collects_the_matches_own_labels() {
    let out = grid(0, 1000, 2);
    // A match answers one series per label set, but those labels are its operands': renaming `host`
    // to a constant above it puts both sides' results under one set, so the consumer above the
    // rewrite is the one that has to collect them. The match itself reads its operands as they are;
    // the one on the right drops the metadata labels, so its own answer is collected.
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[], true).unwrap();
    assert_eq!(tree.to_string(), twice.to_string());
    assert_eq!(2, merges(&twice));
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
    // Two collections: one below the map, because a map reads pieces point-wise and a point it
    // reads can be one it drops, and one above it, for the metadata labels the map itself drops.
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[], true).unwrap();
    assert_eq!(2, merges(&twice));
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
    // A second pass over the collected tree changes nothing: the merge answers one series per label
    // set, so nothing below the map aliases any more.
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[], true).unwrap();
    assert_eq!(2, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn a_pointwise_map_collects_its_own_name_drop() {
    let out = grid(0, 1000, 2);
    // No rewrite anywhere in the tree, so no two pieces were told apart by a label one of them
    // wrote - but `abs` drops the metadata labels, and the drop is a source of duplicates of its
    // own: a key column that holds `NULL` in one stored series and `''` in another decodes to an
    // absent and to an empty label, and both are gone once the empty one goes. Each map's answer is
    // therefore collected, and both sit above the map that hands them out.
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[], true).unwrap();
    assert_eq!(2, merges(&twice));
    assert_eq!(tree.to_string(), twice.to_string());
}

#[test]
fn a_sample_dropping_map_over_a_fresh_destination_collects_its_own_drop() {
    let out = grid(0, 1000, 2);
    // `dst` names no label the scan reports, so the rewrite cannot alias and the map's input needs
    // no collection even though the map drops samples - but the metadata labels `histogram_count`
    // drops are a source of duplicates of their own, so its answer is collected.
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
    // A clamp may empty a point as well, so its input is collected below it exactly as a map's is;
    // the bound argument is a scalar and carries no label set to collect.
    let tree = passed(
        clamp_min(
            rewrite(selected(&out), "host", "host"),
            scalar(0.0, Arc::clone(&out)),
        ),
        &scan_tags(),
    );
    // One below the map, for the point a clamp can empty, and one above it, for the metadata labels
    // the kernel drops.
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[], true).unwrap();
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
    // A window over an evaluated vector folds one series' range, so it reads the series whole. The
    // window keeps the metric name here, so the rewrite below it is the only source of duplicates.
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
fn a_window_over_a_raw_scan_collects_nothing() {
    let out = grid(0, 1000, 2);
    // The scan hands over whole storage series, and a merge cannot take raw samples anyway.
    let tree = passed(keeping_window_over(raw_input(0), None, out), &scan_tags());
    assert_eq!(0, merges(&tree));
}

#[test]
fn a_name_dropping_window_is_collected_by_its_consumer() {
    let out = grid(0, 1000, 2);
    // `rate` drops the metric name, which is part of the identity a merge groups by: a caller that
    // says nothing about which metric the scan reads collects what the window hands out.
    let tree = passed(
        window_over(raw_input(0), None, Arc::clone(&out)),
        &named_scan_tags(),
    );
    assert_eq!(1, merges(&tree));
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what the window hands out: {tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Window { .. }),
        "the merge sits above the window, not below it"
    );
}

#[test]
fn a_name_dropping_window_over_one_metric_streams() {
    let out = grid(0, 1000, 2);
    // The scan is bound to one metric, so every series it hands over is named the same thing:
    // dropping that name tells none of them apart, and the window keeps every partition.
    //
    // The key columns are the other half of that proof, and this fixture states the half that holds:
    // every one of them always decodes a value, so the empty label value the drop removes with the
    // name never stood for a second stored series.
    let tree = passed_over(
        window_over(raw_input(0), None, Arc::clone(&out)),
        &named_scan_tags(),
        &[Some(Arc::from("some_metric"))],
    );
    assert_eq!(0, merges(&tree), "{tree}");
    assert!(
        matches!(tree.root().expr, NativeExpr::Window { .. }),
        "the root stays the window: {tree}"
    );
}

/// Two stored series of one metric that differ only in a tag column holding `NULL` in one and `''`
/// in the other: `{host="web-1", zone=""}` and `{host="web-1"}` are two series to the scan, and
/// `rate` drops the metric name and the empty `zone` with it, so both leave the window as
/// `{host="web-1"}`.
///
/// The labels alone cannot tell them apart - they share host, zone is about to go, and the name is
/// the one every series of the scan shares - so the scan's key columns are what decides the answer.
/// A key column that can hold that pair is the fact that says so, and the drop is collected: the
/// merge is where the second value of one label set at one position is refused, which is
/// Prometheus's answer to two series of one label set rather than two pieces of one series.
///
/// The execution half of this - that the merge refuses the pair instead of handing both over - is
/// `two_series_onto_one_label_set_after_a_window_are_rejected` in `native::execute::tests`.
#[test]
fn a_nullable_key_tag_collects_the_window_that_drops_an_empty_value() {
    let out = grid(0, 1000, 2);
    // A scan of one metric whose tags are `host` and `zone`, the second of which is a nullable key
    // column: the pair above is one storage key away from being two.
    let scan = [tags(&["host", "zone", "__name__"])];
    let single = [Some(Arc::<str>::from("some_metric"))];
    let tree = passed_over_nullable_keys(
        window_over(raw_input(0), None, Arc::clone(&out)),
        &scan,
        &single,
    );
    assert!(
        merges(&tree) >= 1,
        "a nullable key column keeps the collection `rate` needs: {tree}"
    );
    let NativeExpr::Merge { input } = &tree.root().expr else {
        panic!("the root collects what the window hands out: {tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Window { .. }),
        "the merge sits above the window: {tree}"
    );

    // The same tree over a scan whose keys always decode a value: the labels prove the drop, and
    // the window keeps every partition.
    let tree = passed_over(
        window_over(raw_input(0), None, Arc::clone(&out)),
        &scan,
        &single,
    );
    assert_eq!(
        0,
        merges(&tree),
        "non-nullable keys keep the proof, so nothing is collected: {tree}"
    );
}

#[test]
fn a_name_dropping_window_over_several_metrics_is_collected() {
    let out = grid(0, 1000, 2);
    // A selector that discovered several metrics binds no single name, so two of its series can be
    // told apart by the name alone - and dropping it folds them into one label set.
    let tree = passed_over(
        window_over(raw_input(0), None, Arc::clone(&out)),
        &named_scan_tags(),
        &[None],
    );
    assert_eq!(1, merges(&tree), "{tree}");
    assert!(
        matches!(tree.root().expr, NativeExpr::Merge { .. }),
        "the root collects what the window hands out: {tree}"
    );
}

#[test]
fn a_metadata_tag_in_the_binding_fails_closed() {
    let out = grid(0, 1000, 2);
    // `__type__` and `__unit__` go with the name, and a scan that carries one of them as a tag can
    // hold two series that differ only there. A fixed metric name proves nothing about them, so the
    // drop stays collected.
    for metadata in ["__type__", "__unit__"] {
        let scan = vec![tags(&["host", "__name__", metadata])];
        let tree = passed_over(
            window_over(raw_input(0), None, Arc::clone(&out)),
            &scan,
            &[Some(Arc::from("some_metric"))],
        );
        assert_eq!(1, merges(&tree), "{metadata}: {tree}");
    }
}

/// The name a name-dropping kernel removes is the one every series of a single-metric scan shares,
/// and nothing else it drops tells two of them apart, so a math map, a histogram map, a
/// parameterized math kernel and an arithmetic scalar broadcast handed such a source keep every
/// partition.
///
/// The scan's key columns are the other half of that proof, and this is the case they hold for:
/// every one of them always decodes a value, so the empty label value the drop removes with the name
/// never stood for a second stored series. [`a_name_dropping_kernel_over_a_nullable_key_is_collected`]
/// is the same fixtures over a scan whose keys can spell that pair.
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
    // A math map.
    kept("abs", abs_map(selected(&out)));
    // A histogram map.
    kept("histogram_count", histogram_count(selected(&out)));
    // A parameterized math kernel.
    kept(
        "clamp_min",
        clamp_min(selected(&out), scalar(0.0, Arc::clone(&out))),
    );
    // An arithmetic scalar broadcast, `x * 2`.
    kept(
        "x * 2",
        binary_over(
            selected(&out),
            scalar(2.0, Arc::clone(&out)),
            Arc::clone(&out),
        ),
    );
}

/// The same kernels over a binding that carries `__type__` or `__unit__` as a tag are collected at
/// the root.
///
/// Those two go with the name when a kernel drops it, so two series that differ only there meet once
/// the drop removes them, whatever the scan's key columns read otherwise: this is the labels half
/// failing, not the key half. It is asked before the key columns are.
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

/// A name-dropping kernel above another one asks for no collection of its own, and neither does a
/// broadcast above a window that drops the name: `rate(x[5m]) * 2` plans one window and one
/// broadcast, and `abs(abs(rate(x[5m])))` one window and two maps, with no merge in either.
///
/// That is the answer over a scan whose key columns always decode a value. Over one that can hold
/// `NULL` in one series and `''` in the other, the drop the labels cannot prove is collected - but
/// only once, at the bottom of the chain: the merge answers the empty-value half of every drop above
/// it, because it hands over one series per label set under the comparison that treats an empty
/// value as no label. Each extra merge would buffer its whole input to collect a pair the drop above
/// it cannot fold. The labels half is a different matter and is still asked - see
/// [`a_merge_below_a_drop_answers_only_the_empty_values`].
#[test]
fn a_drop_above_a_name_dropping_kernel_needs_no_collection() {
    let out = grid(0, 1000, 2);
    // What the planner states for a scan of one metric: its tag columns and the name the binding
    // gives every series it hands over.
    let scan = [tags(&["host", "zone", "__name__"])];
    let single = [Some(Arc::<str>::from("some_metric"))];
    let rate = || window_over(raw_input(0), None, Arc::clone(&out));
    // `rate(x[5m]) * 2`: the broadcast drops the metadata labels the window dropped already.
    let broadcast = binary_over(rate(), scalar(2.0, Arc::clone(&out)), Arc::clone(&out));
    // `abs(abs(rate(x[5m])))`: two math kernels above the window, neither of which can fold a pair
    // the window left distinct.
    let nested = abs_map(abs_map(rate()));
    for (query, root) in [
        ("rate(x[5m]) * 2", broadcast),
        ("abs(abs(rate(x[5m])))", nested),
    ] {
        let tree = passed_over(root, &scan, &single);
        assert_eq!(0, merges(&tree), "`{query}` plans a merge: {tree}");
    }
}

/// The same two queries over a scan whose keys can spell one series as `NULL` and another as `''`:
/// the drop is collected once, at the bottom of the chain, and the drops above it cost nothing.
///
/// One merge is what a nullable key column costs a statement, whatever the length of the chain above
/// it. The merge sits above the window - the bottom-most drop - so every map and broadcast above it
/// is asked nothing: what they read is already one series per canonical label set.
#[test]
fn a_nullable_key_column_collects_a_chain_of_drops_once() {
    let out = grid(0, 1000, 2);
    // The same facts as the test above, with a key column that can hold `NULL` in one series and
    // `''` in the other, which no labels can prove apart.
    let scan = [tags(&["host", "zone", "__name__"])];
    let single = [Some(Arc::<str>::from("some_metric"))];
    let rate = || window_over(raw_input(0), None, Arc::clone(&out));
    let broadcast = binary_over(rate(), scalar(2.0, Arc::clone(&out)), Arc::clone(&out));
    let nested = abs_map(abs_map(rate()));
    for (query, root) in [
        ("rate(x[5m]) * 2", broadcast),
        ("abs(abs(rate(x[5m])))", nested),
    ] {
        let tree = passed_over_nullable_keys(root, &scan, &single);
        assert_eq!(
            1,
            merges(&tree),
            "`{query}` pays for the one merge its key columns need, not one per drop: {tree}"
        );
    }
}

/// A nullable key column collects the drop it is about, and that merge sits above the drop rather
/// than anywhere else in the chain: the window's answer is what the two stored series arrive at.
#[test]
fn a_nullable_key_column_collects_the_bottom_most_drop() {
    let out = grid(0, 1000, 2);
    let scan = [tags(&["host", "zone", "__name__"])];
    let single = [Some(Arc::<str>::from("some_metric"))];
    // `abs(abs(rate(x[5m])))` over the nullable key: the merge is the window's consumer, and both
    // maps above it read what it hands over.
    let tree = passed_over_nullable_keys(
        abs_map(abs_map(window_over(raw_input(0), None, Arc::clone(&out)))),
        &scan,
        &single,
    );
    assert_eq!(1, merges(&tree), "{tree}");
    let NativeExpr::Map { input, .. } = &tree.root().expr else {
        panic!("the root stays the outer map: {tree}");
    };
    let NativeExpr::Map { input, .. } = &input.expr else {
        panic!("the two maps are not collected apart: {tree}");
    };
    let NativeExpr::Merge { input } = &input.expr else {
        panic!("the merge sits above the window: {tree}");
    };
    assert!(
        matches!(input.expr, NativeExpr::Window { .. }),
        "the merge sits above the window, not below it: {tree}"
    );
}

/// A merge below a drop answers the empty-value half of the proof only: a merge keeps two series
/// that differ in `__name__`, `__type__` or `__unit__`, and removing those is exactly what a
/// metadata drop does. Two metrics whose ordinary labels are equal are therefore still collected
/// above a merge, one merge per collision.
///
/// A selector that discovered several metrics is that shape: each metric's window is collected for
/// the name it drops, and the map above the merge is collected again for the name the merge kept.
#[test]
fn a_merge_below_a_drop_answers_only_the_empty_values() {
    let out = grid(0, 1000, 2);
    let scan = [tags(&["host", "__name__"])];
    // The source may hand over several metrics, which is what leaves the labels half unproved above
    // the window's merge.
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

/// A `Merge` hands over one series per canonical label set - the one that treats an empty label
/// value as no label - so no drop above it can fold two of its series by removing an empty value.
/// The kernels above a merge therefore need no collection of their own for the empty-value half, one
/// merge per collision rather than one per kernel that repeats the collapse; the labels half is a
/// different matter (`a_merge_below_a_drop_answers_only_the_empty_values`).
#[test]
fn a_drop_above_a_merge_needs_no_collection_of_its_own() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    // `label_replace(x, "host", "$1", "host", "(.*)")`: every host is renamed to one value, so the
    // rewrite folds the scan's series into one label set and the pass collects it there.
    let aliasing = || rewrite(selected(&out), "host", "host");

    // `label_replace(x, "host", "$1", "host", "(.*)") * 2`: the broadcast drops the metadata labels
    // above a merge, which cannot fold two series the merge already told apart.
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

    // `abs(abs(rate(label_replace(...)[5m])))`: the rewrite's collision is the one collection, and
    // neither map above it asks for another - their own name drop is over series a merge holds.
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
    // A second pass changes nothing: the merge answers one series per label set, and nothing above
    // it collects again.
    let twice = merge_label_rewrites(&tree, &named_scan_tags(), &single, true).unwrap();
    assert_eq!(1, merges(&twice), "{twice}");
    assert_eq!(tree.to_string(), twice.to_string());
}

/// A collection below a merge still happens: two series the tree folds into one label set there are
/// collected by the merge the pass puts above the rewrite, and the drop above them asks for nothing
/// more - the merge is what makes the two arrive as one.
#[test]
fn a_collision_below_a_merge_is_collected_there() {
    let out = grid(0, 1000, 2);
    let single = [Some(Arc::<str>::from("some_metric"))];
    // `rate(label_replace(x, "host", "$1", "host", "(.*)")[5m])`: every host is renamed to one
    // value, so the rewrite folds the scan's series into one label set - below the window, whose
    // `rate` drops the metric name on top of that.
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
    let twice = merge_label_rewrites(&tree, &named_scan_tags(), &single, true).unwrap();
    assert_eq!(1, merges(&twice), "{twice}");
    assert_eq!(tree.to_string(), twice.to_string());
}

/// A name write is collected even when a merge already holds the series below it: rewriting a name
/// can still put two metrics whose ordinary labels are equal under one identity, which is the rule
/// the kernels' name-drop proof states.
///
/// `abs(label_replace(label_replace(x, "__name__", "$1", "host", "(.*)"), "host", "", "", ""))`
/// deletes the host the name was derived from, so the name is the only label still telling two
/// series apart. `single_metric_name` in `native::plan` is what refuses to carry a written name: it
/// would otherwise hand the scan's own name back, and a binding whose key columns always decode a
/// value would then make the drop look injective.
#[test]
fn a_written_name_is_not_the_name_the_scan_binds() {
    let out = grid(0, 1000, 2);
    // The scan reads one metric, so nothing but the written name is left to tell two of its series
    // apart.
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
    // The rewrite of the name and the rewrite of the `host` it was derived from are collected in
    // their own right, and the root merge is the `abs` above them - the node whose answer the
    // destination check decides.
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

#[test]
fn a_timestamp_is_collected_by_its_consumer() {
    let out = grid(0, 1000, 2);
    // `timestamp` reports a sample's own time under the metric name dropped, so a caller that says
    // nothing about the scan's metric collects its answer at the root as well.
    let stamped = || NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(selected(&out)),
        },
        output: evaluated(fields(1), Arc::clone(&out)),
    };
    let tree = passed(stamped(), &named_scan_tags());
    assert_eq!(1, merges(&tree));
    assert!(
        matches!(tree.root().expr, NativeExpr::Merge { .. }),
        "the root collects what the timestamp hands out: {tree}"
    );

    // The scan reads one metric, so the name it drops told none of its series apart: the timestamp
    // keeps every partition.
    let tree = passed_over(
        stamped(),
        &named_scan_tags(),
        &[Some(Arc::from("some_metric"))],
    );
    assert_eq!(0, merges(&tree), "{tree}");
    assert!(
        matches!(tree.root().expr, NativeExpr::Timestamp { .. }),
        "the root stays the timestamp: {tree}"
    );
}

/// The three nodes that drop the metadata labels on their own are judged by the labels: one metric
/// name over every series, and no metadata tag among them.
///
/// A name-dropping window, `timestamp(x)` and a `timestamp` over an evaluated input all drop the
/// metadata labels. A source bound to one metric names every series it hands over, so that name told
/// none of them apart and the drop is proved; a source that may hand over several is collected, and
/// so is one that carries `__type__` or `__unit__` among its tags. A window that keeps the name
/// drops nothing and hands its input's identity on.
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
    // `timestamp(x)` over the scan, `rate(x[5m])`, and `timestamp(rate(x[5m]))` over an evaluated
    // input: the three nodes that drop the metadata labels themselves.
    let stamps = || selection(raw_input(0), Arc::clone(&out), InstantValue::Timestamp);
    let drops = || window_over(raw_input(0), None, Arc::clone(&out));
    let evaluated_stamps = || NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(drops()),
        },
        output: evaluated(fields(1), Arc::clone(&out)),
    };

    // The labels hold: one metric names every series, and no metadata tag is among the tags.
    kept("timestamp", stamps());
    kept("rate", drops());
    kept("timestamp(rate)", evaluated_stamps());
    // A source that may hand over several metrics is not proved: the name it drops is what tells
    // two of them apart.
    collected("timestamp", &named, &[None], stamps());
    collected("rate", &named, &[None], drops());
    collected("timestamp(rate)", &named, &[None], evaluated_stamps());
    // A binding that keys a series by a metadata label is collected: that label goes with the name,
    // so two series that differ only there meet.
    for metadata in ["__type__", "__unit__"] {
        let mut typed = named.clone();
        typed[0].push(metadata.to_string());
        collected("timestamp", &typed, &single, stamps());
        collected("rate", &typed, &single, drops());
        collected("timestamp(rate)", &typed, &single, evaluated_stamps());
    }
    // A window that keeps the name drops nothing, so nothing is asked of it.
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
    // `x + 2` drops the metadata labels, so what it hands out is collected - above the match, not
    // beside it - and neither operand is collected on its own.
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
    let twice = merge_label_rewrites(&tree, &scan_tags(), &[], true).unwrap();
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
    let twice =
        merge_label_rewrites(&once, &scan_tags(), &[], true).expect("a collected tree is legal");
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
