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
use datatypes::series::samples::SeriesSchema;
use datatypes::series::time::{EvalGrid, GridSpec};

use super::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundWindow, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::map::MathOp;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, MatchingLabels};

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
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

fn input_on(grid: Arc<EvalGrid>) -> NativeNode {
    // One allocation shared by the binding and the output: `validate` checks allocation identity,
    // because a compiler that meant one grid has no reason to allocate it twice.
    let output = evaluated(Arc::new([]), grid);
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            output: output.clone(),
            ordering: SeriesOrdering::Contiguous,
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
                group: Arc::new(["job".into()]),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Arc::new(["job".into()]),
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
                group: Arc::new(["job".into()]),
                stage: AggregateStage::Final,
            },
        },
        output: evaluated(Arc::new([]), grid),
    }
}

fn window_over(inner: NativeNode, input_grid: Option<Arc<EvalGrid>>) -> NativeNode {
    window_on(inner, input_grid, grid(0, 60_000, 4))
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
                ops: Arc::new([MathOp::Abs]),
            },
        },
        output: evaluated(Arc::new([]), out),
    }
}

fn add() -> BinaryMatching {
    BinaryMatching {
        op: BinaryOp::Add,
        return_bool: false,
        labels: MatchingLabels::Ignoring(Vec::new()),
        cardinality: Cardinality::OneToOne,
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
    assert_eq!(bare.validate().is_ok(), mapped.validate().is_ok());
    // The mapped tree has one more node, and it is the same window below it.
    let mut depth = 0;
    super::super::metrics::walk(&mapped, &mut |_, _| depth += 1);
    assert_eq!(depth, 3);
}

/// A map is point-wise over values, so raw samples are not something it can consume.
#[test]
fn a_map_cannot_consume_raw_samples() {
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(input()),
            op: BoundMap {
                ops: Arc::new([MathOp::Abs]),
            },
        },
        output: evaluated(Arc::new([]), grid(0, 60_000, 4)),
    };
    let error = node.validate().unwrap_err();
    assert!(error.to_string().contains("an evaluated vector"), "{error}");
}

/// A scalar is one value per grid point, not a vector, so there are no fields to map.
#[test]
fn a_map_cannot_consume_a_scalar() {
    let out = grid(0, 60_000, 4);
    let node = NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(scalar_on(1.0, Arc::clone(&out))),
            op: BoundMap {
                ops: Arc::new([MathOp::Abs]),
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
                ops: Arc::new([MathOp::Abs]),
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
    assert!(error.to_string().contains("field(s)"), "{error}");
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
    assert!(error.to_string().contains("field(s)"), "{error}");
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
                group: Arc::new(["job".into()]),
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
                group: Arc::new(["job".into()]),
                stage: AggregateStage::Partial,
            },
        },
        output: NativeType::AggregateState(Arc::new(super::AggregateStateSchema {
            group: Arc::new(["job".into()]),
            accumulators: Arc::new([]),
            grid: value_grid,
        })),
    };
    assert!(partial.validate().is_ok());
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
                group: Arc::new(["job".into()]),
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

/// A `Partial` aggregate turns a vector into state: the output is the state type, indexed on the
/// vector's grid.
#[test]
fn a_partial_aggregate_produces_state() {
    let state = partial_aggregate(window_over(input(), None));
    assert!(state.validate().is_ok());
    assert!(matches!(state.output, NativeType::AggregateState(_)));
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
                group: Arc::new(["job".into()]),
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
                group: Arc::new(["job".into()]),
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
    // Every node in one query evaluates on the statement's grid, so the compiler binds one grid
    // and shares it; `validate` checks that identity.
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
                group: Arc::new(["job".into()]),
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
            ordering: SeriesOrdering::Contiguous,
        }),
        output: partial_aggregate(window_over(input(), None)).output,
    };
    let error = other.validate().unwrap_err();
    assert!(error.to_string().contains("not the binding"), "{error}");
}
