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

use datafusion::common::DFSchema;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};

use super::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundVectorOp, BoundWindow, Grouping, MapOp, NativeExpr, NativeNode, NativeTree, NativeType,
    needs_single_execution, reads_across_series,
};
use crate::extension_plan::{FusedSeries, FusedSeriesBinding};
use crate::info::InfoSelector;
use crate::map::MathOp;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn evaluated(
    schema_fields: Arc<[datatypes::series::samples::FieldSpec]>,
    grid: Arc<EvalGrid>,
) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: schema_fields,
        eval_grid: Some(grid),
    }))
}

/// `count` fields named `f0`, `f1`, ..., which is the layout a multi-field source has.
fn fields(count: usize) -> Arc<[FieldSpec]> {
    (0..count)
        .map(|index| FieldSpec {
            name: Arc::from(format!("f{index}")),
        })
        .collect::<Vec<_>>()
        .into()
}

fn raw() -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: Arc::new([]),
        eval_grid: None,
    }))
}

fn input() -> NativeNode {
    let output = raw();
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

fn input_on(grid: Arc<EvalGrid>) -> NativeNode {
    // One schema allocation shared by the binding and the output: `validate` checks that the output
    // is the binding it was bound to, which is about the binding itself, not the grid's spec.
    let output = evaluated(Arc::new([]), grid);
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output,
    }
}

/// A scan of `count` fields, so a consumer's field check has something to disagree with.
fn input_with_fields(count: usize) -> NativeNode {
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(count),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

/// A scan of raw samples whose chunks of different series may interleave.
fn interleaved_input() -> NativeNode {
    let output = raw();
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Interleaved,
        }),
        output,
    }
}

/// A constant scalar, which is a scalar type evaluated on the statement's grid.
fn scalar_on(value: f64, grid: Arc<EvalGrid>) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value }),
        output: NativeType::Scalar(grid),
    }
}

/// The grid a state node is indexed on.
fn state_grid(node: &NativeNode) -> Arc<EvalGrid> {
    let NativeType::AggregateState(schema) = &node.output else {
        unreachable!("expected aggregate state")
    };
    Arc::clone(&schema.grid)
}

fn partial_aggregate(inner: NativeNode) -> NativeNode {
    let grid = inner
        .output
        .grid()
        .expect("a partial aggregate consumes an evaluated vector")
        .clone();
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: Arc::new([]),
            grid,
        })),
    }
}

fn final_aggregate(state: NativeNode) -> NativeNode {
    let grid = state_grid(&state);
    NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(state),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Final,
            },
        },
        output: evaluated(Arc::new([]), grid),
    }
}

fn window_over(inner: NativeNode, input_grid: Option<Arc<EvalGrid>>) -> NativeNode {
    window_on(inner, input_grid, grid(0, 60_000, 4))
}

/// A window over `inner` that produces `count` fields on `out`, so a consumer of it can be given a
/// layout the window itself holds legally.
fn window_fields(inner: NativeNode, out: Arc<EvalGrid>, count: usize) -> NativeNode {
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
                input_grid: None,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(count), out),
    }
}

/// A window over `inner` that produces `out`, so two windows can be given the same grid.
fn window_on(
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
        output: evaluated(Arc::new([]), out),
    }
}

fn map_over(inner: NativeNode) -> NativeNode {
    let out = inner.output.grid().expect("a map needs a grid").clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(Arc::new([]), out),
    }
}

/// A canonical merge over `inner`, which reports its input's layout.
fn merge_over(inner: NativeNode) -> NativeNode {
    let output = inner.output.clone();
    NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(inner),
        },
        output,
    }
}

fn add() -> BinaryMatching {
    BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
        fill: FillValues::default(),
    }
}

/// A compiled tree is accepted when every node's contract holds.
#[test]
fn a_window_over_raw_samples_is_valid() {
    let root = window_over(input(), None);
    assert!(NativeTree::try_new(root).is_ok());
}

/// Adding a map to a window changes the tree and nothing else: no new shape is recognised.
#[test]
fn adding_a_map_only_changes_the_tree() {
    let bare = window_over(input(), None);
    let mapped = map_over(window_over(input(), None));
    // Both trees are legal, so the comparison below is about the shape and not about both failing.
    assert!(bare.validate().is_ok(), "{}", bare.validate().unwrap_err());
    assert!(
        mapped.validate().is_ok(),
        "{}",
        mapped.validate().unwrap_err()
    );
    // The mapped tree is that map over that window over that input, and nothing else: the map on
    // top is the one node the bare tree gained, and what is below it is the tree it was built from.
    let NativeExpr::Map { input, .. } = &mapped.expr else {
        unreachable!("the mapped tree's root is a map")
    };
    let NativeExpr::Window { input: leaf, .. } = &input.expr else {
        unreachable!("the map is not over the window the bare tree is")
    };
    assert!(
        matches!(leaf.expr, NativeExpr::Input(_)),
        "the window is not over the input the bare tree reads"
    );
}

#[test]
fn merge_rejects_raw_and_mismatched_input_shapes() {
    let raw_merge = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(input()),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    assert!(raw_merge.validate().is_err());

    let output_grid = grid(0, 60_000, 4);
    let supported = [
        input_on(Arc::clone(&output_grid)),
        window_over(
            input_on(Arc::clone(&output_grid)),
            Some(Arc::clone(&output_grid)),
        ),
        map_over(input_on(Arc::clone(&output_grid))),
    ];
    for inner in supported {
        let merge = NativeNode {
            expr: NativeExpr::Merge {
                input: Box::new(inner),
            },
            output: evaluated(Arc::new([]), Arc::clone(&output_grid)),
        };
        assert!(
            merge.validate().is_ok(),
            "{}",
            merge.validate().unwrap_err()
        );
        assert!(reads_across_series(&merge));
    }

    let inner = window_over(
        input_on(Arc::clone(&output_grid)),
        Some(Arc::clone(&output_grid)),
    );
    assert!(
        inner.validate().is_ok(),
        "{}",
        inner.validate().unwrap_err()
    );
    let wrong_grid = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(inner.clone()),
        },
        output: evaluated(Arc::new([]), grid(1, 60_000, 4)),
    };
    let grid_error = wrong_grid.validate().unwrap_err();
    assert!(grid_error.to_string().contains("grid"), "{grid_error}");

    let wrong_fields = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(inner),
        },
        output: evaluated(fields(1), output_grid),
    };
    let fields_error = wrong_fields.validate().unwrap_err();
    assert!(fields_error.to_string().contains("field"), "{fields_error}");
}

/// A merge canonicalizes evaluated fragments and names no operator below it, so any tree whose output
/// is an evaluated vector is one it may sit over: a scan's samples, a window's rate, an aggregate's
/// fold, a vector operation's choice and a binary operation's match all hand over the same thing.
/// An input that is not an evaluated vector at all is still refused.
#[test]
fn a_merge_sits_over_any_evaluated_vector() {
    let shared = grid(0, 60_000, 4);
    let rate = || window_on(input(), None, Arc::clone(&shared));
    let aggregate = |inner: NativeNode| NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let vector = |inner: NativeNode| NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op: BoundVectorOp::Sort(SortOrder::Asc),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let binary = |left: NativeNode, right: NativeNode| NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };

    // The prefix the merge was first written for, then the shapes it now also sits over: each of them
    // answers evaluated values, which is what the collection reads.
    for node in [
        merge_over(input_on(Arc::clone(&shared))),
        merge_over(rate()),
        merge_over(map_over(rate())),
        merge_over(aggregate(rate())),
        merge_over(vector(rate())),
        merge_over(binary(rate(), rate())),
    ] {
        assert!(node.validate().is_ok(), "{}", node.validate().unwrap_err());
        assert!(reads_across_series(&node));
    }

    // Raw samples carry their own timestamps instead of positions on a grid, so there is nothing to
    // merge by: a scan is the one input a merge refuses however many fields it carries.
    for inner in [input(), input_with_fields(1)] {
        let error = merge_over(inner).validate().unwrap_err();
        assert!(error.to_string().contains("evaluated vector"), "{error}");
    }
    // A scalar is not a vector at all, and a merge reports samples.
    let scalar_input = NativeNode {
        expr: NativeExpr::Merge {
            input: Box::new(scalar_on(1.0, Arc::clone(&shared))),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let error = scalar_input.validate().unwrap_err();
    assert!(error.to_string().contains("evaluated vector"), "{error}");
}

/// A map is point-wise over values, so raw samples are not something it can consume.
#[test]
fn a_map_cannot_consume_raw_samples() {
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(input()),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("an evaluated vector"), "{error}");
}

#[test]
fn timestamp_requires_evaluated_vector_and_preserves_its_layout() {
    let evaluated_input = window_over(input(), None);
    let node = NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(evaluated_input.clone()),
        },
        output: evaluated_input.output.clone(),
    };
    assert!(node.validate().is_ok());
    let raw = NativeNode {
        expr: NativeExpr::Timestamp {
            input: Box::new(input()),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    assert!(
        raw.validate()
            .unwrap_err()
            .to_string()
            .contains("evaluated vector")
    );
}

/// A scalar is one value per grid point, not a vector, so there are no fields to map.
#[test]
fn a_map_cannot_consume_a_scalar() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(scalar_on(1.0, Arc::clone(&out))),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("not a scalar"), "{error}");
}

/// A map with no operation would be a node that does nothing, which is a compiler mistake.
#[test]
fn a_map_needs_an_operation() {
    let inner = window_over(input(), None);
    let out = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap { ops: Arc::new([]) },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("no operation"), "{error}");
}

/// A map reports one value per input field, so it cannot declare a layout its input does not
/// have. Without this check the tree would validate and only fail when the runtime compared the
/// node's schema against the one its input actually produces.
#[test]
fn a_map_cannot_change_the_field_count() {
    let inner = window_over(input(), None);
    let out = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: Arc::new([MapOp::Math(MathOp::Abs)]),
            },
        },
        output: evaluated(
            Arc::new([datatypes::series::samples::FieldSpec {
                name: Arc::from("value"),
            }]),
            out,
        ),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a map declares 1 field(s) but its input provides 0"),
        "{error}"
    );
}

/// A window reports one value per input field, so it carries the same rule as a map.
#[test]
fn a_window_cannot_change_the_field_count() {
    let inner = window_over(input(), None);
    // The output grid is the input's, so the grid check passes and the field check is what the
    // test actually exercises.
    let out = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function: RangeFunction::Rate,
                window: RangeWindow {
                    range_ms: 30_000,
                    offset_ms: 0,
                    anchor_ms: None,
                    modifier: None,
                },
                input_grid: Some(Arc::clone(&out)),
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(
            Arc::new([datatypes::series::samples::FieldSpec {
                name: Arc::from("value"),
            }]),
            out,
        ),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a window declares 1 field(s) but its input provides 0"),
        "{error}"
    );
}

/// An instant selection picks one row of the source per grid point, so it carries one value per
/// input field. A tree that declares anything else would only fail at run time, against a schema
/// the tree was supposed to have validated.
#[test]
fn an_instant_selection_cannot_change_the_field_count() {
    let out = grid(0, 60_000, 4);
    // The source has two fields; the selection declares one value.
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(input_with_fields(2)),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(fields(1), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("an instant selection declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// An instant selection reads raw samples; feeding it evaluated values would re-anchor a grid
/// that is already fixed.
#[test]
fn an_instant_selection_reads_raw_samples() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(window_over(input(), None)),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("reads evaluated"), "{error}");
}

/// Aggregate state is not a sample stream, so an instant selection cannot read it.
#[test]
fn an_instant_selection_cannot_consume_state() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(partial_aggregate(window_on(
                input(),
                None,
                Arc::clone(&out),
            ))),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("aggregate state"), "{error}");
}

/// A window's declared input grid has to be the grid its input actually produces.
#[test]
fn a_window_checks_its_declared_input_grid() {
    let inner_grid = grid(0, 60_000, 4);
    let other_grid = grid(0, 30_000, 8);
    // The input produces `inner_grid`, but the window claims a different one.
    let node = window_over(input_on(inner_grid), Some(other_grid));
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("input grid is not the input's grid"),
        "{error}"
    );
}

/// A range evaluator reads one raw series to its last chunk before it starts the next, so raw
/// chunks of different series may not interleave.
#[test]
fn a_window_cannot_read_interleaved_input() {
    let error = window_over(interleaved_input(), None)
        .validate()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("interleaved chunks cannot be fed to it"),
        "{error}"
    );
}

/// An instant selection's consumer collects one series' chunks into one buffer, so it reads a
/// series to its last chunk before the next: it refuses an interleaved input for the reason a
/// window does.
#[test]
fn an_instant_selection_cannot_read_interleaved_input() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(interleaved_input()),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                at_ms: None,
                value: InstantValue::Sample,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::new([]), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("interleaved chunks cannot be fed to it"),
        "{error}"
    );
}

/// A window whose range is empty or negative reads nothing at every grid point.
#[test]
fn a_window_reads_a_non_empty_range() {
    let mut node = window_over(input(), None);
    let NativeExpr::Window { op, .. } = &mut node.expr else {
        unreachable!("a window tree has a window root")
    };
    op.window.range_ms = 0;
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("empty range"), "{error}");
}

/// A node that declares an output grid its output does not carry would be addressed on one grid
/// and hand out positions on another.
#[test]
fn a_window_checks_its_output_grid() {
    let mut node = window_over(input(), None);
    let NativeExpr::Window { op, .. } = &mut node.expr else {
        unreachable!("a window tree has a window root")
    };
    op.output_grid = grid(0, 30_000, 8);
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("declares a grid its output does not carry"),
        "{error}"
    );
}

/// A window consumes samples: state holds accumulators, not a sample stream to range over.
#[test]
fn a_window_cannot_consume_state() {
    let out = grid(0, 60_000, 4);
    let node = window_on(
        partial_aggregate(window_on(input(), None, Arc::clone(&out))),
        None,
        grid(0, 60_000, 4),
    );
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("aggregate state"), "{error}");
}

/// A window ranges over timestamps, and a scalar has none of its own.
#[test]
fn a_window_cannot_consume_a_scalar() {
    let out = grid(0, 60_000, 4);
    let node = window_on(
        scalar_on(1.0, Arc::clone(&out)),
        Some(out),
        grid(0, 60_000, 4),
    );
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("not a scalar"), "{error}");
}

/// A binary node aligns two inputs, so two different grids cannot be aligned implicitly.
#[test]
fn a_binary_operation_rejects_two_grids() {
    let left = window_over(input(), None);
    let left_grid = left.output.grid().unwrap().clone();
    let right = window_on(input(), None, grid(0, 30_000, 8));
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: evaluated(Arc::new([]), left_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("different grids"), "{error}");
}

/// Two inputs on the same grid are a legal binary operation, which is how
/// `rate(a[5m]) + rate(b[5m])` becomes an ordinary tree.
#[test]
fn a_binary_operation_over_one_grid_is_valid() {
    // Both sides evaluate on the statement's grid, so the compiler binds one grid to both.
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, Arc::clone(&shared))),
            right: Box::new(window_on(input(), None, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), shared),
    };
    assert!(node.validate().is_ok());
}

/// A compiler that allocates one grid per window still produces one grid: the kernel aligns
/// operands by `EvalGrid::spec` (`vector_match::same_grid`), so `validate` compares specs too and
/// does not reject a tree the runtime would run correctly.
#[test]
fn a_binary_operation_aligns_separately_allocated_equal_specs() {
    // Two windows, one fresh `EvalGrid` each, and the statement's grid a third: equal specs, no
    // shared allocation anywhere in the tree.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, grid(0, 60_000, 4))),
            right: Box::new(window_on(input(), None, grid(0, 60_000, 4))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    assert!(node.validate().is_ok(), "{}", node.validate().unwrap_err());

    // The same tree with a different spec is still rejected: the kernel rejects that pair too.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, grid(0, 60_000, 4))),
            right: Box::new(window_on(input(), None, grid(30_000, 60_000, 4))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("different grids"), "{error}");
}

/// A scalar side is broadcast, not matched: `sum by(job)(rate(x[5m])) + time()` has no labels to
/// line up, so `matching` is not consulted and the output keeps the vector side's fields.
#[test]
fn a_binary_operation_broadcasts_a_scalar() {
    let shared = grid(0, 60_000, 4);
    let vector = window_on(input(), None, Arc::clone(&shared));
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(vector),
            right: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    assert!(node.validate().is_ok());

    // The mirror case: the scalar on the left keeps the right side's fields.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            right: Box::new(window_on(input(), None, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    assert!(node.validate().is_ok());
}

/// A scalar has no fields of its own, so a broadcast that declares a different count than the
/// vector side would report a layout the kernel does not produce.
#[test]
fn a_scalar_broadcast_cannot_change_the_field_count() {
    let shared = grid(0, 60_000, 4);
    // The vector side has two fields; the broadcast declares one.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_fields(input_with_fields(2), Arc::clone(&shared), 2)),
            right: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(fields(1), shared),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not produce the vector side's fields"),
        "{error}"
    );
}

/// Two vectors pair their fields by position, so the match reports the shorter side's count
/// (`vector_match::binary_output_schema` takes `min(lhs, rhs)`); a tree declaring anything else is
/// rejected instead of failing at run time.
#[test]
fn a_binary_operation_over_two_vectors_matches_the_shorter_side() {
    let shared = grid(0, 60_000, 4);
    // Two fields matched against one: the match produces one field, not two.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_fields(input_with_fields(2), Arc::clone(&shared), 2)),
            right: Box::new(window_fields(input_with_fields(1), Arc::clone(&shared), 1)),
            matching: add(),
        },
        output: evaluated(fields(2), Arc::clone(&shared)),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains(
            "a binary operation over two vectors declares 2 field(s) but its input provides 1"
        ),
        "{error}"
    );

    // Declaring the shorter side's count is the legal tree, so the check is not vacuous.
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_fields(input_with_fields(2), Arc::clone(&shared), 2)),
            right: Box::new(window_fields(input_with_fields(1), Arc::clone(&shared), 1)),
            matching: add(),
        },
        output: evaluated(fields(1), shared),
    };
    assert!(node.validate().is_ok(), "{}", node.validate().unwrap_err());
}

/// Two scalars make a scalar, because there is nothing to attach labels or fields to.
#[test]
fn two_scalars_make_a_scalar() {
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            right: Box::new(scalar_on(2.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: NativeType::Scalar(Arc::clone(&shared)),
    };
    assert!(node.validate().is_ok());
}

/// Two scalars have nothing to attach labels or fields to, so they cannot produce a vector.
#[test]
fn two_scalars_cannot_produce_a_vector() {
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(scalar_on(1.0, Arc::clone(&shared))),
            right: Box::new(scalar_on(2.0, Arc::clone(&shared))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), shared),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("two scalars do not produce a scalar"),
        "{error}"
    );
}

/// A constant operand is broadcast to each series the other side answers, so such an operation reads
/// one series at a time exactly when that other side does.
///
/// Two vectors are a match over the whole vector, and an aggregate folds a group, so either of them
/// under the constant still reads across series. Two constants answer the same value for every series
/// there is, so they read none.
#[test]
fn a_constant_operand_reads_one_series_at_a_time() {
    let shared = grid(0, 60_000, 4);
    let vector = || window_on(input(), None, Arc::clone(&shared));
    let binary = |left: NativeNode, right: NativeNode| NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: vector().output,
    };
    let aggregate = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(vector()),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };

    // `100 - rate(v0[5m])`, and the same constant written on the other side.
    for node in [
        binary(scalar_on(100.0, Arc::clone(&shared)), vector()),
        binary(vector(), scalar_on(100.0, Arc::clone(&shared))),
    ] {
        assert!(
            !reads_across_series(&node),
            "a broadcast reaches each series the vector side answers: {node:?}"
        );
    }
    // `sum(rate(v0[5m])) + 100` folds a group first, so the constant is broadcast to each group.
    assert!(reads_across_series(&binary(
        aggregate,
        scalar_on(100.0, Arc::clone(&shared))
    )));
    // `rate(v0[5m]) + rate(v0[5m])` matches two vectors by label.
    assert!(reads_across_series(&binary(vector(), vector())));
    // `1 + 2` answers one value for every series there is.
    assert!(!reads_across_series(&binary(
        scalar_on(1.0, Arc::clone(&shared)),
        scalar_on(2.0, Arc::clone(&shared))
    )));
}

/// A tree needs one execution over every scan partition exactly when one of its operators answers a
/// value for the whole vector; every other tree answers each series on its own and keeps running one
/// execution per partition.
#[test]
fn only_a_whole_vector_tree_needs_one_execution() {
    let shared = grid(0, 60_000, 4);
    let rate = || window_on(input(), None, Arc::clone(&shared));
    let vector = |inner: NativeNode, op: BoundVectorOp| NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(inner),
            op,
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let binary = |left: NativeNode, right: NativeNode| NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(left),
            right: Box::new(right),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let aggregate = |inner: NativeNode, stage: AggregateStage| NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage,
            },
        },
        output: if stage == AggregateStage::Partial {
            partial_aggregate(rate()).output
        } else {
            evaluated(Arc::new([]), Arc::clone(&shared))
        },
    };

    // One series' rows answer one series: the input, `rate(x[5m])` and `abs(rate(x[5m]))`.
    for node in [input(), rate(), map_over(rate())] {
        assert!(
            !needs_single_execution(&node),
            "a per-series tree keeps one execution per partition: {node:?}"
        );
    }
    // A broadcast reaches each series of the vector side, and a state aggregate is contractually
    // one execution's contribution.
    assert!(!needs_single_execution(&binary(
        scalar_on(1.0, Arc::clone(&shared)),
        rate()
    )));
    assert!(!needs_single_execution(&binary(rate(), rate())));
    assert!(!needs_single_execution(&aggregate(
        rate(),
        AggregateStage::Partial
    )));

    // `topk(1, rate(x[5m]))`, `sort(rate(x[5m]))` and `limitk(1, rate(x[5m]))` read every series
    // against the others, as does a complete aggregate.
    let group = Grouping::By(Arc::new(["job".into()]));
    for op in [
        BoundVectorOp::TopK {
            k: 1.0,
            top: true,
            group: group.clone(),
        },
        BoundVectorOp::LimitK {
            k: 1.0,
            group: group.clone(),
        },
        BoundVectorOp::Sort(SortOrder::Asc),
    ] {
        assert!(
            needs_single_execution(&vector(rate(), op)),
            "a global vector operation needs every series at once"
        );
    }
    assert!(needs_single_execution(&aggregate(
        rate(),
        AggregateStage::Complete
    )));
    // Depth does not hide it: `sum(rate(x[5m])) + 100` still folds a group, and so does a per-series
    // left side over a whole-vector right side.
    assert!(needs_single_execution(&binary(
        aggregate(rate(), AggregateStage::Complete),
        scalar_on(100.0, Arc::clone(&shared))
    )));
    assert!(needs_single_execution(&binary(
        rate(),
        vector(rate(), BoundVectorOp::Sort(SortOrder::Asc))
    )));
}

/// A binary operation is point-wise on a grid, so a side of raw samples has no positions to line
/// up with the other side.
#[test]
fn a_binary_operation_cannot_consume_raw_samples() {
    let shared = grid(0, 60_000, 4);
    let vector = || window_on(input(), None, Arc::clone(&shared));

    let left_raw = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(input()),
            right: Box::new(vector()),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let error = left_raw.validate().unwrap_err();
    assert!(
        error.to_string().contains("not raw samples on its left"),
        "{error}"
    );

    let right_raw = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(vector()),
            right: Box::new(input()),
            matching: add(),
        },
        output: evaluated(Arc::new([]), shared),
    };
    let error = right_raw.validate().unwrap_err();
    assert!(
        error.to_string().contains("not raw samples on its right"),
        "{error}"
    );
}

/// State is not a value, so neither side of a binary operation may be state.
#[test]
fn a_binary_operation_cannot_consume_state() {
    let shared = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(window_on(input(), None, Arc::clone(&shared))),
            right: Box::new(partial_aggregate(window_on(
                input(),
                None,
                Arc::clone(&shared),
            ))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), Arc::clone(&shared)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("aggregate state"), "{error}");
}

/// A partial aggregate produces state, not a value; a complete one produces a value on the same
/// grid as its input.
#[test]
fn an_aggregate_output_matches_its_stage() {
    let inner = window_over(input(), None);
    let value_grid = inner.output.grid().unwrap().clone();

    let complete = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner.clone()),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&value_grid)),
    };
    assert!(complete.validate().is_ok());

    let partial = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: Arc::new([]),
            grid: value_grid,
        })),
    };
    assert!(partial.validate().is_ok());
}

/// A complete aggregate folds each field on its own, so it reports one value per input field
/// (`aggregate::aggregate_float` builds its rows from the input schema's fields).
#[test]
fn a_complete_aggregate_cannot_change_the_field_count() {
    let out = grid(0, 60_000, 4);
    // The input is a legal two-field window; the aggregate declares one value.
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_fields(input_with_fields(2), Arc::clone(&out), 2)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(fields(1), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a complete aggregate declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// Declaring a value where the stage produces state is a compiler mistake, and is rejected.
#[test]
fn an_aggregate_stage_must_agree_with_its_type() {
    let inner = window_over(input(), None);
    let value_grid = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: evaluated(Arc::new([]), value_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("produces aggregate state"),
        "{error}"
    );
}

/// A `Partial` aggregate keeps one accumulator per input field (`aggregate_float` lays its state
/// out in `schema.fields.len()`-sized points), so state declaring another count is rejected.
#[test]
fn a_partial_aggregate_cannot_change_the_accumulator_count() {
    let out = grid(0, 60_000, 4);
    // The input is a legal two-field window; the state declares one accumulator.
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_fields(input_with_fields(2), Arc::clone(&out), 2)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: fields(1),
            grid: out,
        })),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a partial aggregate declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// A `Partial` aggregate's state is grouped by the binding's group labels, so state declaring
/// other labels is not the state this aggregate produces.
#[test]
fn a_partial_aggregate_groups_its_state_like_its_binding() {
    let mut node = partial_aggregate(window_over(input(), None));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a partial aggregate tree has an aggregate root")
    };
    op.group = Grouping::By(Arc::new(["instance".into()]));
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("groups differ from the binding"),
        "{error}"
    );
}

/// A `Partial` aggregate indexes its state on the grid its input produces, so state on another
/// grid is addressed with positions the input never wrote.
#[test]
fn a_partial_aggregate_indexes_its_state_on_the_inputs_grid() {
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_over(input(), None)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: Arc::new([]),
            grid: grid(0, 30_000, 8),
        })),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("indexed on a grid the input does not produce"),
        "{error}"
    );
}

/// A `Final` aggregate merges into the binding's group labels, so the state it reads has to be
/// grouped the same way.
#[test]
fn a_final_aggregate_groups_its_state_like_its_binding() {
    let mut node = final_aggregate(partial_aggregate(window_over(input(), None)));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a final aggregate tree has an aggregate root")
    };
    op.group = Grouping::By(Arc::new(["instance".into()]));
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("groups differ from the binding"),
        "{error}"
    );
}

/// `by` and `without` are not interchangeable, even over the same label.
///
/// `by(job)` keeps `job` and drops every other label; `without(job)` drops `job` and keeps the
/// rest. A binding that spelled `without` as a label list could only produce the first, so state
/// built for one has to be refused for the other rather than merged under the wrong labels.
#[test]
fn a_grouping_cannot_swap_by_for_without() {
    let mut node = final_aggregate(partial_aggregate(window_over(input(), None)));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a final aggregate tree has an aggregate root")
    };
    op.group = Grouping::Without(Arc::new(["job".into()]));
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("groups differ from the binding"),
        "{error}"
    );

    // The same labels under the same shape validate, so the refusal is about the shape.
    let mut node = final_aggregate(partial_aggregate(window_over(input(), None)));
    let NativeExpr::Aggregate { op, .. } = &mut node.expr else {
        unreachable!("a final aggregate tree has an aggregate root")
    };
    op.group = Grouping::By(Arc::new(["job".into()]));
    assert!(node.validate().is_ok());
}

/// A `Final` aggregate consumes the state a `Partial` produced, which is what an exchange
/// boundary hands over between the two stages.
#[test]
fn a_final_aggregate_merges_partial_state() {
    let shared = grid(0, 60_000, 4);
    let state = partial_aggregate(window_on(input(), None, Arc::clone(&shared)));
    let merged = final_aggregate(state);
    assert!(merged.validate().is_ok());
    assert!(
        matches!(&merged.output, NativeType::Samples(schema) if schema.eval_grid.as_ref().is_some_and(|grid| Arc::ptr_eq(grid, &shared)))
    );
}

/// A `Final` aggregate merges the state into one value per accumulator, so it reports the state's
/// count and nothing else.
#[test]
fn a_final_aggregate_cannot_change_the_field_count() {
    let out = grid(0, 60_000, 4);
    // A legal partial aggregate: two input fields, two accumulators.
    let state = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_fields(input_with_fields(2), Arc::clone(&out), 2)),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Grouping::By(Arc::new(["job".into()])),
            accumulators: fields(2),
            grid: Arc::clone(&out),
        })),
    };
    assert!(
        state.validate().is_ok(),
        "{}",
        state.validate().unwrap_err()
    );

    // The merge declares one value for the state's two accumulators.
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(state),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Final,
            },
        },
        output: evaluated(fields(1), out),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("a final aggregate declares 1 field(s) but its input provides 2"),
        "{error}"
    );
}

/// A `Final` aggregate consumes state, so a vector is not something it can merge.
#[test]
fn a_final_aggregate_cannot_consume_a_window() {
    let inner = window_over(input(), None);
    let value_grid = inner.output.grid().unwrap().clone();
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(inner),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Final,
            },
        },
        output: evaluated(Arc::new([]), value_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("consumes aggregate state"),
        "{error}"
    );
}

/// A `Complete` aggregate aggregates vectors, so state is not something it can consume.
#[test]
fn a_complete_aggregate_cannot_consume_state() {
    let state = partial_aggregate(window_over(input(), None));
    let state_grid = state_grid(&state);
    let node = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(state),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), state_grid),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("consumes an evaluated vector"),
        "{error}"
    );
}

/// The first-phase walkthrough shapes compile to trees, which is the acceptance the RFC states
/// for composition.
#[test]
fn the_walkthrough_shapes_are_trees() {
    // Every node in one query evaluates on the statement's grid; `validate` compares the specs of
    // the grids, not their allocations, which is the rule the kernels apply.
    let statement_grid = grid(0, 60_000, 4);

    // `abs(rate(x[5m]))`
    let abs_rate = map_over(window_over(input(), None));
    assert!(abs_rate.validate().is_ok());

    // `sum by(job)(rate(x[5m]))`, complete because one execution sees every contribution.
    let sum = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_on(input(), None, Arc::clone(&statement_grid))),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::new([]), Arc::clone(&statement_grid)),
    };
    assert!(sum.validate().is_ok());

    // `sum by(job)(rate(x[5m])) + 1`: the aggregate is complete, and the scalar side is
    // broadcast, so `matching` is not consulted for it.
    let plus_one = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(sum.clone()),
            right: Box::new(scalar_on(1.0, Arc::clone(&statement_grid))),
            matching: add(),
        },
        output: evaluated(Arc::new([]), statement_grid),
    };
    assert!(plus_one.validate().is_ok());
}

/// A scalar node's output is the scalar type, which is what keeps a broadcast from turning into
/// label matching.
#[test]
fn a_scalar_node_is_valid() {
    let node = scalar_on(1.0, grid(0, 60_000, 4));
    assert!(node.validate().is_ok());
}

/// A scalar node declared as a vector would let a broadcast be planned as a match.
#[test]
fn a_scalar_node_must_produce_a_scalar() {
    let node = NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value: 1.0 }),
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(
        error.to_string().contains("does not produce a scalar"),
        "{error}"
    );
}

/// An input whose declared output is not the binding it was bound to cannot be trusted.
#[test]
fn an_input_must_report_the_binding_it_was_bound_to() {
    let node = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: raw(),
            source_index: 0,
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output: raw(),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("not the binding"), "{error}");
}

/// An exchange boundary binds state, and the node has to report that same state.
#[test]
fn an_input_can_bind_aggregate_state() {
    let state = partial_aggregate(window_over(input(), None));
    let bound = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: state.output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: state.output.clone(),
    };
    assert!(bound.validate().is_ok());

    // A `Final` aggregate can then consume it, which is the shape of a distributed aggregate.
    assert!(final_aggregate(bound).validate().is_ok());

    // A fresh state allocation is a different binding, so it cannot be reported as the bound one.
    let other = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: state.output.clone(),
            source_index: 0,
            ordering: SeriesOrdering::Contiguous,
        }),
        output: partial_aggregate(window_over(input(), None)).output,
    };
    let error = other.validate().unwrap_err();
    assert!(error.to_string().contains("not the binding"), "{error}");
}

/// A vector operation reads the values at each grid point, so raw samples, which have no shared points,
/// are not something it can consume.
#[test]
fn a_vector_operation_cannot_consume_raw_samples() {
    let node = NativeNode {
        expr: NativeExpr::Vector {
            input: Box::new(input()),
            op: BoundVectorOp::LimitRatio { ratio: 0.5 },
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("an evaluated vector"), "{error}");
}

/// A node that reports samples while holding no input node is refused where the node is built.
///
/// The tree is constructed directly here — a child module of `native::plan` may — because the node
/// is one the public constructors decline: a scalar root is the only zero-input shape a fused node
/// accepts, and samples without a source would be an empty vector reported as an answer. Every
/// public constructor therefore stays a checked one.
#[test]
fn a_samples_root_without_an_input_is_refused() {
    let statement_grid = grid(0, 60_000, 4);
    let tree = NativeTree {
        root: NativeNode {
            expr: NativeExpr::Scalar(BoundScalar { value: 1.0 }),
            output: evaluated(Arc::new([]), Arc::clone(&statement_grid)),
        },
    };
    let binding = FusedSeriesBinding {
        grid: statement_grid,
        time_index: "time".to_string(),
        tags: vec![],
        metric_name: None,
    };

    let error = FusedSeries::try_new(
        Arc::from("1"),
        tree,
        binding,
        Vec::new(),
        Arc::new(DFSchema::empty()),
    )
    .expect_err("a samples root reads no source while reporting samples");

    assert!(
        error.to_string().contains("constant scalar"),
        "the refusal must name the shape a zero-child tree has to be: {error}"
    );
}

/// `info` reads its info side's values as sample times, so it accepts only the instant selection
/// that reports them: one that reports the samples themselves would compare every info series as
/// equally new.
#[test]
fn info_reads_the_sample_times_of_one_field() {
    let out = grid(0, 60_000, 4);
    let info_of = |value: InstantValue, fields: usize| NativeNode {
        expr: NativeExpr::Info {
            base: Box::new(window_fields(input_with_fields(1), Arc::clone(&out), 1)),
            info: Box::new(NativeNode {
                expr: NativeExpr::Instant {
                    input: Box::new(input_with_fields(fields)),
                    op: BoundInstant {
                        lookback_ms: 300_000,
                        offset_ms: 0,
                        at_ms: None,
                        value,
                        output_grid: Arc::clone(&out),
                    },
                },
                output: evaluated(self::fields(fields), Arc::clone(&out)),
            }),
            selector: InfoSelector {
                name_matchers: Vec::new(),
                data_labels: Vec::new(),
                keep_unmatched: true,
            },
        },
        output: evaluated(self::fields(1), Arc::clone(&out)),
    };
    info_of(InstantValue::InfoTimestamp, 1).validate().unwrap();
    for (value, fields, expected) in [
        (InstantValue::Sample, 1, "not their values"),
        (InstantValue::InfoTimestamp, 2, "carry 2 fields"),
    ] {
        let error = info_of(value, fields).validate().unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}
