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

//! What the split has to keep right: which aggregate is cut out, what stays above the merge, and
//! which shapes are declined so the caller can keep the plan it had.
//!
//! The trees are built node by node with the same bindings the PromQL compiler produces, because the
//! compiler lives in the `query` crate, which depends on this one.

use super::*;
use datafusion::arrow::datatypes::{Field, TimeUnit};
use datafusion::common::DFSchema;
use datatypes::series::samples::FieldSpec;
use datatypes::series::time::{EvalGrid, GridSpec};

use crate::native::plan::BoundVectorOp;
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

/// The statement's grid, which every evaluated chunk is addressed on.
fn grid() -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 60_000, 4).unwrap()).unwrap())
}

/// The raw fields the tree's input binds, which a scan below it has to be able to produce.
fn raw_fields() -> Arc<[FieldSpec]> {
    Arc::from([FieldSpec { name: "v0".into() }])
}

/// The raw samples a scan hands over: one field, no grid.
fn raw() -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: raw_fields(),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// The evaluated layout a selection or a window reports.
fn evaluated() -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec { name: "v0".into() }]),
        eval_grid: Some(grid()),
    }))
}

/// `rate(v0[5m])`, the per-series prefix of every aggregate below.
fn window() -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(raw()),
            op: crate::native::plan::BoundWindow {
                function: crate::range_eval::RangeFunction::Rate,
                window: crate::range_eval::RangeWindow {
                    range_ms: 300_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: None,
                output_grid: grid(),
            },
        },
        output: evaluated(),
    }
}

/// An instant selection, the other per-series prefix the split admits.
fn instant() -> NativeNode {
    NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(raw()),
            op: crate::native::plan::BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                timestamp: false,
                output_grid: grid(),
            },
        },
        output: evaluated(),
    }
}

fn map_over(inner: NativeNode, ops: Vec<MathOp>) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: crate::native::plan::BoundMap {
                ops: ops.into_iter().map(MapOp::Math).collect::<Vec<_>>().into(),
            },
        },
        output,
    }
}

fn aggregate_over(
    inner: NativeNode,
    kind: AggregateKind,
    group: Grouping,
    stage: AggregateStage,
) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate { kind, group, stage },
        },
        output,
    }
}

fn complete(kind: AggregateKind, group: Grouping) -> impl Fn(NativeNode) -> NativeNode {
    move |inner| aggregate_over(inner, kind, group.clone(), AggregateStage::Complete)
}

fn by(labels: &[&str]) -> Grouping {
    Grouping::By(
        labels
            .iter()
            .map(|label| Arc::<str>::from(*label))
            .collect(),
    )
}

/// The plan below the producer: a scan producing the raw fields the tree's input binds.
///
/// The producer checks that its child can produce every field its tree input declares, so the scan
/// names them as nullable float columns; the merge itself reads none of them.
fn scan() -> LogicalPlan {
    let columns = raw_fields()
        .iter()
        .map(|field| {
            (
                None,
                Arc::new(Field::new(field.name.as_ref(), DataType::Float64, true)),
            )
        })
        .collect::<Vec<_>>();
    LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::new_with_metadata(columns, Default::default()).unwrap()),
    })
}

fn binding() -> FusedSeriesBinding {
    FusedSeriesBinding {
        grid: grid(),
        metric_name: Some(Arc::from("m")),
        time_index: "ts".to_string(),
        tags: vec!["k0".to_string(), "k1".to_string()],
    }
}

/// The rows a finalized aggregate reports: the time index, one value column and the grouped labels.
fn output_schema(values: &[&str], tags: &[&str]) -> DFSchemaRef {
    let mut columns = vec![(
        None,
        Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )),
    )];
    for name in values {
        columns.push((None, Arc::new(Field::new(*name, DataType::Float64, true))));
    }
    for name in tags {
        columns.push((None, Arc::new(Field::new(*name, DataType::Utf8, true))));
    }
    Arc::new(DFSchema::new_with_metadata(columns, Default::default()).unwrap())
}

fn finalize(root: NativeNode, output: DFSchemaRef) -> DfResult<AggregateFinalize> {
    AggregateFinalize::try_new(
        Arc::from("sum(rate(m[5m]))"),
        NativeTree::try_new(root)?,
        binding(),
        scan(),
        output,
    )
}

/// The labels this node writes, read off its schema the way the physical half reads them.
fn output_tags(node: &AggregateFinalize) -> Vec<String> {
    node.schema()
        .fields()
        .iter()
        .filter(|field| field.data_type() == &DataType::Utf8)
        .map(|field| field.name().clone())
        .collect()
}

/// The producer this node merges.
fn producer(node: &AggregateFinalize) -> &FusedSeries {
    let LogicalPlan::Extension(Extension { node }) = &node.input else {
        panic!("the merge should read a producer");
    };
    node.as_any()
        .downcast_ref::<FusedSeries>()
        .expect("the producer is a fused series node")
}

/// The innermost aggregate is the one cut out, and only the nodes that may sit above it keep their
/// place: the outer aggregate runs in the suffix, over the merged values.
#[test]
fn the_innermost_aggregate_is_cut_out_of_a_nested_tree() {
    let inner = complete(AggregateKind::Avg, by(&["k0"]))(window());
    let root = complete(AggregateKind::Sum, by(&[]))(inner);
    let node = finalize(root, output_schema(&["sum(avg(v0))"], &[])).unwrap();

    // The producer runs the per-series prefix and exports the inner aggregate's state.
    let producer = producer(&node);
    assert_eq!(
        Some(AggregateKind::Avg),
        producer.partial_fold().map(|op| op.kind)
    );
    let prefix = producer.tree().to_string();
    assert!(prefix.contains("Window"), "{prefix}");
    assert!(!prefix.contains("Aggregate"), "{prefix}");

    // The suffix starts at the merged values and holds the outer aggregate.
    let suffix = node.suffix.to_string();
    assert!(suffix.contains("Aggregate"), "{suffix}");
    assert!(suffix.contains("Input"), "{suffix}");
    assert!(!suffix.contains("Window"), "{suffix}");
}

/// A map above the merge is point-wise, so it is the same operation over merged values.
#[test]
fn an_outer_map_stays_in_the_suffix() {
    let root = map_over(
        complete(AggregateKind::Sum, by(&["k0"]))(instant()),
        vec![MathOp::Ceil],
    );
    let node = finalize(root, output_schema(&["ceil(sum(v0))"], &["k0"])).unwrap();

    let suffix = node.suffix.to_string();
    assert!(suffix.contains("Map"), "{suffix}");
    assert!(suffix.contains("Input"), "{suffix}");
    assert_eq!(output_tags(&node), ["k0".to_string()]);
}

/// `left <op> right` with a constant on one side, bound the way the compiler binds a broadcast: the
/// operation reports the vector side's samples.
fn broadcast(left: NativeNode, right: NativeNode, op: BinaryOp) -> NativeNode {
    let output = if matches!(left.output, NativeType::Scalar(_)) {
        right.output.clone()
    } else {
        left.output.clone()
    };
    NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: BinaryMatching {
                op,
                return_bool: false,
                labels: MatchingLabels::Ignoring(Vec::new()),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output,
    }
}

/// The constant scalar `value`, on the statement's grid.
fn scalar_on(value: f64) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(crate::native::plan::BoundScalar { value }),
        output: NativeType::Scalar(grid()),
    }
}

/// A constant operand is broadcast over the merged values, so the aggregate under the other side is
/// still the one cut: the suffix keeps the operation, the operand order and the matching, with the
/// merged values where the aggregate was.
#[test]
fn a_constant_operand_keeps_the_cut_below_it() {
    for (constant_on_the_left, value) in [(true, "100 - avg(v0)"), (false, "avg(v0) - 100")] {
        let aggregate = complete(AggregateKind::Avg, by(&["k0"]))(window());
        let root = if constant_on_the_left {
            broadcast(scalar_on(100.0), aggregate, BinaryOp::Sub)
        } else {
            broadcast(aggregate, scalar_on(100.0), BinaryOp::Sub)
        };
        let node = finalize(root, output_schema(&[value], &["k0"])).unwrap();

        // The producer still runs only the per-series prefix: the constant is above the cut, so the
        // window is all the prefix holds and the constant is applied in the suffix.
        let prefix = producer(&node).tree().to_string();
        assert!(prefix.contains("Window"), "{prefix}");
        assert!(!prefix.contains("Binary"), "{prefix}");
        assert!(!prefix.contains("Scalar"), "{prefix}");

        // The suffix is the operation over the merged values, which enter it as its own source `0`:
        // the prefix's and the suffix's inputs are separate executions, not one exchange.
        let NativeExpr::Binary {
            left,
            right,
            matching,
        } = &node.suffix.root().expr
        else {
            panic!("the operation stays above the cut: {}", node.suffix);
        };
        assert_eq!(BinaryOp::Sub, matching.op, "the operator is kept");
        let (constant, merged) = if constant_on_the_left {
            (left, right)
        } else {
            (right, left)
        };
        assert!(
            matches!(constant.expr, NativeExpr::Scalar(_)),
            "the constant keeps its side: {}",
            node.suffix
        );
        assert!(
            matches!(&merged.expr, NativeExpr::Input(input) if input.source_index == 0),
            "the merged values enter where the aggregate was: {}",
            node.suffix
        );
        assert!(!node.suffix.to_string().contains("Window"));
    }
}

fn vector_over(input: NativeNode, op: BoundVectorOp) -> NativeNode {
    let output = input.output.clone();
    NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(input),
            op,
        },
        output,
    }
}

#[test]
fn topk_and_bottomk_run_after_the_aggregate_cut() {
    for top in [true, false] {
        let aggregate = complete(AggregateKind::Sum, by(&["k0"]))(window());
        let root = vector_over(
            aggregate,
            BoundVectorOp::TopK {
                k: 10.0,
                top,
                group: by(&[]),
            },
        );
        let tree = NativeTree::try_new(root).unwrap();
        assert!(AggregateFinalize::can_split(&tree, &raw_fields(), "ts"));
        let node = finalize(tree.root().clone(), output_schema(&["sum(v0)"], &["k0"])).unwrap();
        let NativeExpr::Vector {
            input,
            op: BoundVectorOp::TopK {
                top: actual_top, ..
            },
        } = &node.suffix.root().expr
        else {
            panic!("{}", node.suffix);
        };
        assert_eq!(top, *actual_top);
        assert!(
            matches!(&input.expr, NativeExpr::Input(bound) if bound.source_index == 0 && bound.output.samples().is_some_and(|schema| schema.fields.len() == 1 && schema.fields[0].name.as_ref() == "v0" && schema.eval_grid.as_ref().is_some_and(|actual| actual.spec() == grid().spec())))
        );
    }
}

#[test]
fn limitk_above_the_cut_is_declined_because_merge_does_not_preserve_input_order() {
    let error = finalize(
        vector_over(
            complete(AggregateKind::Sum, by(&["k0"]))(window()),
            BoundVectorOp::LimitK {
                k: 1.0,
                group: by(&[]),
            },
        ),
        output_schema(&["sum(v0)"], &["k0"]),
    )
    .unwrap_err();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );
}

#[test]
fn topk_without_aggregate_is_not_split() {
    let tree = NativeTree::try_new(vector_over(
        window(),
        BoundVectorOp::TopK {
            k: 10.0,
            top: true,
            group: by(&[]),
        },
    ))
    .unwrap();
    assert!(!AggregateFinalize::can_split(&tree, &raw_fields(), "ts"));
    assert!(split(tree.root(), &raw_fields(), "ts").unwrap().is_none());
}

/// A binary operation above the cut is only the operation over merged values when its other side is a
/// constant: two aggregates are two cuts, a vector sibling would have to be matched with the merged
/// values by label, and a prefix that pairs two vectors reads the whole vector rather than one series.
#[test]
fn a_binary_operation_without_a_constant_operand_is_refused() {
    let folded = || {
        complete(AggregateKind::Sum, by(&[]))(complete(AggregateKind::Avg, by(&["k0"]))(window()))
    };
    for root in [
        // Two sides fold, and one cut cannot answer two aggregates.
        broadcast(folded(), folded(), BinaryOp::Add),
        // A vector sibling is another series set, which nothing below the merge brings together.
        broadcast(
            complete(AggregateKind::Sum, by(&["k0"]))(window()),
            window(),
            BinaryOp::Add,
        ),
        broadcast(
            window(),
            complete(AggregateKind::Sum, by(&["k0"]))(window()),
            BinaryOp::Div,
        ),
        // A prefix that pairs two vectors reads the whole vector at a point, which one partition
        // cannot do: admitting a constant operand does not admit this.
        complete(AggregateKind::Sum, by(&["k0"]))(broadcast(window(), window(), BinaryOp::Div)),
    ] {
        let error = finalize(root, output_schema(&["sum(v0)"], &["k0"])).unwrap_err();
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{error}"
        );
    }
}

/// The output's labels are the schema's string columns, which is what the merge writes.
#[test]
fn the_output_labels_are_the_schema_labels() {
    let root = complete(AggregateKind::Sum, by(&["k0", "k1"]))(window());
    let node = finalize(root, output_schema(&["sum(v0)"], &["k0", "k1"])).unwrap();

    assert_eq!(output_tags(&node), ["k0".to_string(), "k1".to_string()]);
}

/// An aggregate directly over the raw input never reaches the split: the tree's own validation
/// refuses a `Complete` aggregate that consumes raw samples, so nothing here has a prefix to run.
#[test]
fn a_raw_aggregate_is_refused_at_the_tree_boundary() {
    let error = NativeTree::try_new(complete(AggregateKind::Sum, by(&["k0"]))(raw())).unwrap_err();
    assert!(matches!(error, DataFusionError::Internal(_)), "{error}");
    assert!(
        error.to_string().contains("evaluated vector"),
        "the refusal names the input a complete aggregate needs: {error}"
    );
}

/// A window over an evaluated input is a subquery, evaluated on its own grid, so what the merge
/// produces is not its input: the split keeps the existing plan.
#[test]
fn a_subquery_above_an_aggregate_is_refused() {
    let root = complete(AggregateKind::Sum, by(&["k0"]))(NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(complete(AggregateKind::Sum, by(&["k0"]))(window())),
            op: crate::native::plan::BoundWindow {
                function: crate::range_eval::RangeFunction::Rate,
                window: crate::range_eval::RangeWindow {
                    range_ms: 300_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: Some(grid()),
                output_grid: grid(),
            },
        },
        output: evaluated(),
    });
    let error = finalize(root, output_schema(&["sum(v0)"], &["k0"])).unwrap_err();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );
}

/// `clamp` with its maximum below its minimum answers nothing, so the fold would see a group with no
/// sample rather than the query the reader wrote.
#[test]
fn an_empty_clamp_is_refused() {
    let empty = map_over(instant(), vec![MathOp::Clamp { min: 2.0, max: 1.0 }]);
    let error = finalize(
        complete(AggregateKind::Sum, by(&["k0"]))(empty),
        output_schema(&["sum(v0)"], &["k0"]),
    )
    .unwrap_err();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );

    // The same clamp the other way round is a range, and the prefix keeps it.
    let range = map_over(instant(), vec![MathOp::Clamp { min: 0.0, max: 1.0 }]);
    assert!(
        finalize(
            complete(AggregateKind::Sum, by(&["k0"]))(range),
            output_schema(&["sum(v0)"], &["k0"])
        )
        .is_ok()
    );
}

/// A group that varies with a value field or the grid point is not a group the state rows can name.
#[test]
fn an_aggregate_grouped_by_a_value_field_or_the_time_index_is_refused() {
    for group in [by(&["v0"]), by(&["ts"])] {
        let error = finalize(
            complete(AggregateKind::Sum, group)(window()),
            output_schema(&["sum(v0)"], &["v0"]),
        )
        .unwrap_err();
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{error}"
        );
    }
    // A label the input does not carry is simply absent, as in the existing planner.
    assert!(
        finalize(
            complete(AggregateKind::Sum, by(&["absent"]))(window()),
            output_schema(&["sum(v0)"], &["absent"])
        )
        .is_ok()
    );
}

/// A quantile's state is the group's samples, which neither half of this node can carry.
#[test]
fn a_quantile_aggregate_is_refused() {
    let root = complete(AggregateKind::Quantile { quantile: 0.5 }, by(&["k0"]))(window());
    let error = finalize(root, output_schema(&["q(v0)"], &["k0"])).unwrap_err();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );
}

/// A tree with no aggregate at all is not this node's.
#[test]
fn a_tree_without_an_aggregate_is_refused() {
    let error = finalize(window(), output_schema(&["v0"], &[])).unwrap_err();
    assert!(
        matches!(error, DataFusionError::NotImplemented(_)),
        "{error}"
    );
}

/// The caller's schema has to describe the rows this node reports: one value column per field the
/// aggregate folds.
#[test]
fn the_output_schema_has_to_match_the_cut() {
    let root = || complete(AggregateKind::Sum, by(&["k0"]))(window());
    for output in [
        // No value column where the aggregate folds one field.
        output_schema(&[], &["k0"]),
        // A value column per field the aggregate does not fold.
        output_schema(&["sum(v0)", "sum(v1)"], &["k0"]),
    ] {
        let error = finalize(root(), output).unwrap_err();
        assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    }
    assert!(finalize(root(), output_schema(&["sum(v0)"], &["k0"])).is_ok());
}

/// The two halves of a split name their own sources, and neither reuses the other's.
///
/// The prefix still reads the one raw scan at execution source `0` and declares no grid - it is raw
/// samples. The suffix is a tree of its own, built at the cut, so its one input is source `0` of
/// *its* execution and carries the cut aggregate's evaluated output, on the same grid the node was
/// bound to: the merged values enter there, as the aggregate's own output shape.
#[test]
fn the_prefix_and_the_suffix_each_name_their_own_source_zero() {
    let root = complete(AggregateKind::Sum, by(&["k0"]))(window());
    let node = finalize(root, output_schema(&["sum(v0)"], &["k0"])).unwrap();

    // The prefix is what the producer runs: one raw input on source 0, and no evaluation grid.
    fn inputs(node: &NativeNode, found: &mut Vec<(usize, Option<Arc<EvalGrid>>)>) {
        match &node.expr {
            NativeExpr::Input(input) => found.push((
                input.source_index,
                input
                    .output
                    .samples()
                    .cloned()
                    .and_then(|schema| schema.eval_grid.clone()),
            )),
            NativeExpr::Scalar(_) => {}
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => inputs(input, found),
            NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
                inputs(left, found);
                inputs(right, found);
            }
        }
    }

    let prefix = producer(&node).tree().clone();
    let mut raw_inputs = Vec::new();
    inputs(prefix.root(), &mut raw_inputs);
    let [raw_input] = raw_inputs.as_slice() else {
        panic!("the prefix reads one raw input, got {raw_inputs:?}");
    };
    assert_eq!(0, raw_input.0, "the prefix reads execution source 0");
    assert!(
        raw_input.1.is_none(),
        "the prefix's input is raw samples, so it has no grid: {raw_input:?}"
    );

    // The suffix is its own tree: one input, named by its own execution's source 0, evaluated on
    // the grid the node is bound to.
    let mut suffix_inputs = Vec::new();
    inputs(node.suffix.root(), &mut suffix_inputs);
    let [suffix_input] = suffix_inputs.as_slice() else {
        panic!("the suffix reads one input, got {suffix_inputs:?}");
    };
    assert_eq!(
        0, suffix_input.0,
        "the suffix's merged values are source 0 of its own execution"
    );
    let grid = suffix_input
        .1
        .as_ref()
        .expect("the merged values are evaluated, so they are on a grid");
    assert_eq!(
        node.binding.grid.spec(),
        grid.spec(),
        "the suffix runs on the grid the node is bound to"
    );
}
