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

use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};
use datatypes::series::{Label, MetricName, SeriesOrdering};

use super::super::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundInstant, BoundMap, BoundScalar,
    BoundVectorOp, BoundWindow, Grouping, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::map::MathOp;
use crate::native::plan::MapOp;
use crate::range_eval::{RangeFunction, RangeWindow};
use crate::select::SortOrder;
use crate::vector_match::{BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels};

fn grid(start: i64, step: i64, len: u64) -> Arc<EvalGrid> {
    Arc::new(EvalGrid::try_new(GridSpec::try_new(start, step, len).unwrap()).unwrap())
}

fn fields() -> Arc<[FieldSpec]> {
    Arc::new([FieldSpec {
        name: "value".into(),
    }])
}

/// The samples a node produces on `grid`.
fn evaluated(grid: Arc<EvalGrid>) -> NativeType {
    NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(),
        eval_grid: Some(grid),
    }))
}

/// An unqualified scan over raw samples, which is what a compiled query starts from.
fn raw_input() -> NativeNode {
    raw_input_from(0)
}

/// The same scan bound to child `source_index` of the final execution tree.
fn raw_input_from(source_index: usize) -> NativeNode {
    // One allocation shared by the binding and the output: `validate` checks allocation identity.
    // The fields are the ones a window over this input reports, because a window keeps its input's
    // layout and `validate` rejects a node that declares a different count.
    let output = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(),
        eval_grid: None,
    }));
    NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index,
            output: output.clone(),
            ordering: SeriesOrdering::KeyThenTime,
        }),
        output,
    }
}

fn window_over(
    inner: NativeNode,
    function: RangeFunction,
    range_ms: i64,
    offset_ms: i64,
    anchor_ms: Option<i64>,
    out: Arc<EvalGrid>,
) -> NativeNode {
    NativeNode {
        expr: NativeExpr::Window {
            input: Box::new(inner),
            op: BoundWindow {
                function,
                window: RangeWindow {
                    range_ms,
                    offset_ms,
                    anchor_ms,
                },
                input_grid: None,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(out),
    }
}

fn map(inner: NativeNode, ops: Vec<MathOp>) -> NativeNode {
    let out = inner.output.grid().expect("a map needs a grid").clone();
    NativeNode {
        expr: NativeExpr::Map {
            input: Box::new(inner),
            op: BoundMap {
                ops: ops.into_iter().map(MapOp::Math).collect(),
            },
        },
        output: evaluated(out),
    }
}

/// The expected display of a tree, one string per line, so a change in wording, order or
/// indentation fails the test rather than passing as a substring.
fn shown(lines: &[&str]) -> String {
    lines.join("\n")
}

/// The header comes first and is exactly `NativeTree`, so the output says what it is before it
/// says anything about the tree.
#[test]
fn the_first_line_names_the_tree() {
    let tree = NativeTree::try_new(raw_input()).unwrap();

    assert_eq!(
        tree.to_string(),
        shown(&["NativeTree", "  root Input source=0 raw samples"])
    );
    assert_eq!(tree.to_string().lines().next(), Some("NativeTree"));
}

/// A compiled tree is one line per node, outermost first: the window is the root and the scan it
/// folds is shown beneath it.
#[test]
fn a_window_over_raw_samples_is_shown_once_per_node() {
    let tree = NativeTree::try_new(window_over(
        raw_input(),
        RangeFunction::Rate,
        300_000,
        0,
        None,
        grid(0, 60_000, 4),
    ))
    .unwrap();

    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Window rate [300000ms] offset 0 anchor - on grid 0/60000x4",
            "    input Input source=0 raw samples",
        ])
    );
}

/// A map nests one level deeper than the window it reads, and lists its operations in the order
/// they are applied.
#[test]
fn a_map_nests_below_the_window_it_maps() {
    let tree = NativeTree::try_new(map(
        window_over(
            raw_input(),
            RangeFunction::Rate,
            300_000,
            60_000,
            None,
            grid(0, 60_000, 4),
        ),
        vec![MathOp::Abs, MathOp::Round { nearest: 3.0 }],
    ))
    .unwrap();

    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Map abs, round 3",
            "    input Window rate [300000ms] offset 60000 anchor - on grid 0/60000x4",
            "      input Input source=0 raw samples",
        ])
    );
}

/// A parameterised window names the constant it was bound to, and an anchored one names its
/// anchor instead of printing the absent case.
#[test]
fn a_window_names_its_constant_argument_and_its_anchor() {
    let tree = NativeTree::try_new(window_over(
        raw_input(),
        RangeFunction::QuantileOverTime(0.9),
        60_000,
        0,
        Some(42),
        grid(1_000, 60_000, 4),
    ))
    .unwrap();

    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Window quantile_over_time 0.9 [60000ms] offset 0 anchor 42 on grid 1000/60000x4",
            "    input Input source=0 raw samples",
        ])
    );
}

/// An input says whether its samples are raw or already evaluated, and reports the ordering it
/// guarantees when that is weaker than the scan default.
#[test]
fn an_input_reports_whether_it_is_raw_and_what_ordering_it_guarantees() {
    let contiguous = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(),
        eval_grid: Some(grid(0, 60_000, 4)),
    }));
    let evaluated_input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: contiguous.clone(),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: contiguous,
    };
    let tree = NativeTree::try_new(evaluated_input).unwrap();
    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Input source=0 evaluated samples, contiguous",
        ])
    );

    let interleaved = NativeType::Samples(Arc::new(SeriesSchema {
        fields: fields(),
        eval_grid: None,
    }));
    let raw_input = NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: interleaved.clone(),
            ordering: SeriesOrdering::Interleaved,
        }),
        output: interleaved,
    };
    let tree = NativeTree::try_new(raw_input).unwrap();
    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Input source=0 raw samples, interleaved"
        ])
    );
}

/// The child an input reads is part of its line: two trees over different children, or over one
/// child shared by both inputs, do not render as the same text.
#[test]
fn an_input_names_the_child_it_reads() {
    let out = grid(0, 60_000, 4);
    let selected = |source_index: usize| NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(raw_input_from(source_index)),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 0,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(Arc::clone(&out)),
    };
    let tree = |left: usize, right: usize| {
        NativeTree::try_new(NativeNode {
            expr: NativeExpr::Binary {
                left: Box::new(selected(left)),
                right: Box::new(selected(right)),
                matching: BinaryMatching {
                    op: BinaryOp::Add,
                    return_bool: false,
                    labels: MatchingLabels::Ignoring(Vec::new()),
                    cardinality: Cardinality::OneToOne,
                    fill: FillValues::default(),
                },
            },
            output: evaluated(Arc::clone(&out)),
        })
        .unwrap()
    };

    let distinct = tree(0, 1).to_string();
    assert_eq!(
        distinct,
        shown(&[
            "NativeTree",
            "  root Binary + ignoring [] one-to-one",
            "    left Instant lookback 300000ms offset 0 on grid 0/60000x4",
            "      input Input source=0 raw samples",
            "    right Instant lookback 300000ms offset 0 on grid 0/60000x4",
            "      input Input source=1 raw samples",
        ])
    );

    let shared = tree(0, 0).to_string();
    assert_eq!(
        shared
            .lines()
            .filter(|line| line.ends_with("Input source=0 raw samples"))
            .count(),
        2,
        "{shared}"
    );
    assert_ne!(
        shared, distinct,
        "two trees differ in the child each input reads"
    );
}

/// A leaf kind renders what it holds, whether or not anything could run it: the line names the
/// node it is and makes no claim about executability.
#[test]
fn a_scalar_and_an_instant_line_show_what_they_hold() {
    let out = grid(0, 60_000, 4);
    let scalar = NativeNode {
        expr: NativeExpr::Scalar(BoundScalar { value: 2.0 }),
        output: NativeType::Scalar(Arc::clone(&out)),
    };
    let tree = NativeTree::try_new(scalar).unwrap();
    assert_eq!(tree.to_string(), shown(&["NativeTree", "  root Scalar 2"]));

    let instant = NativeNode {
        expr: NativeExpr::Instant {
            input: Box::new(raw_input()),
            op: BoundInstant {
                lookback_ms: 300_000,
                offset_ms: 60_000,
                output_grid: Arc::clone(&out),
            },
        },
        output: evaluated(out),
    };
    let tree = NativeTree::try_new(instant).unwrap();
    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Instant lookback 300000ms offset 60000 on grid 0/60000x4",
            "    input Input source=0 raw samples",
        ])
    );
}

/// An aggregate and a binary operation name the binding they were compiled with, which is what
/// makes the line worth reading.
#[test]
fn an_aggregate_and_a_binary_name_their_binding() {
    let out = grid(0, 60_000, 4);
    let aggregate = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_over(
                raw_input(),
                RangeFunction::Rate,
                300_000,
                0,
                None,
                Arc::clone(&out),
            )),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::By(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::clone(&out)),
    };
    let tree = NativeTree::try_new(aggregate).unwrap();
    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Aggregate sum by [job] complete",
            "    input Window rate [300000ms] offset 0 anchor - on grid 0/60000x4",
            "      input Input source=0 raw samples",
        ])
    );

    // A scalar broadcast, which is the one binary shape whose matching rules are never consulted:
    // the display still prints them, because the binding is what the compiler decided.
    let binary = NativeNode {
        expr: NativeExpr::Binary {
            left: Box::new(NativeNode {
                expr: NativeExpr::Scalar(BoundScalar { value: 2.0 }),
                output: NativeType::Scalar(Arc::clone(&out)),
            }),
            right: Box::new(window_over(
                raw_input(),
                RangeFunction::Rate,
                300_000,
                0,
                None,
                Arc::clone(&out),
            )),
            matching: BinaryMatching {
                op: BinaryOp::Add,
                return_bool: false,
                labels: MatchingLabels::Ignoring(Vec::new()),
                cardinality: Cardinality::OneToOne,
                fill: FillValues::default(),
            },
        },
        output: evaluated(out),
    };
    let tree = NativeTree::try_new(binary.clone()).unwrap();
    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Binary + ignoring [] one-to-one",
            "    left Scalar 2",
            "    right Window rate [300000ms] offset 0 anchor - on grid 0/60000x4",
            "      input Input source=0 raw samples",
        ])
    );

    // Fill values change the result, so a set one is part of the binding shown.
    let mut filled = binary;
    let NativeExpr::Binary { matching, .. } = &mut filled.expr else {
        unreachable!()
    };
    matching.fill.rhs = Some(0.5);
    assert_eq!(
        NativeTree::try_new(filled)
            .unwrap()
            .to_string()
            .lines()
            .nth(1),
        Some("  root Binary + ignoring [] one-to-one fill_right 0.5")
    );
}

/// A `without` grouping reads as `without [...]`, not as `by [...]`.
///
/// The two are different queries over the same label list: `by(job)` keeps `job` and drops the
/// rest, `without(job)` drops `job` and keeps the rest. A rendering that spelled both `by` would
/// show two different queries as one.
#[test]
fn a_without_grouping_does_not_read_as_by() {
    let out = grid(0, 60_000, 4);
    let aggregate = NativeNode {
        expr: NativeExpr::Aggregate {
            input: Box::new(window_over(
                raw_input(),
                RangeFunction::Rate,
                300_000,
                0,
                None,
                Arc::clone(&out),
            )),
            op: BoundAggregate {
                kind: AggregateKind::Sum,
                group: Grouping::Without(Arc::new(["job".into()])),
                stage: AggregateStage::Complete,
            },
        },
        output: evaluated(Arc::clone(&out)),
    };
    let tree = NativeTree::try_new(aggregate).unwrap();
    assert_eq!(
        tree.to_string(),
        shown(&[
            "NativeTree",
            "  root Aggregate sum without [job] complete",
            "    input Window rate [300000ms] offset 0 anchor - on grid 0/60000x4",
            "      input Input source=0 raw samples",
        ])
    );
}

/// A vector operation names its operator, constant and grouping.
#[test]
fn a_vector_operation_names_its_binding() {
    let out = grid(0, 60_000, 4);
    let window = window_over(
        raw_input(),
        RangeFunction::Rate,
        300_000,
        0,
        None,
        Arc::clone(&out),
    );
    let lines = [
        (
            BoundVectorOp::TopK {
                k: 3.0,
                top: false,
                group: Grouping::Without(Arc::new(["job".into()])),
            },
            "  root Vector bottomk 3 without [job]",
        ),
        (
            BoundVectorOp::LimitK {
                k: 2.0,
                group: Grouping::By(Arc::new([])),
            },
            "  root Vector limitk 2 by []",
        ),
        (
            BoundVectorOp::LimitRatio { ratio: -0.5 },
            "  root Vector limit_ratio -0.5",
        ),
        (
            BoundVectorOp::Sort(SortOrder::ByLabelDesc(vec!["job".into(), "le".into()])),
            "  root Vector sort_by_label_desc [job, le]",
        ),
        (
            BoundVectorOp::Absent {
                labels: MetricName::try_new(
                    None,
                    vec![Label {
                        name: "job".into(),
                        value: "x".into(),
                    }],
                )
                .unwrap(),
            },
            r#"  root Vector absent {job="x"}"#,
        ),
        (
            BoundVectorOp::CountValues {
                label: "value".into(),
                group: Grouping::Without(Arc::new(["job".into()])),
            },
            r#"  root Vector count_values "value" without [job]"#,
        ),
    ];
    for (op, line) in lines {
        let tree = NativeTree::try_new(NativeNode {
            expr: NativeExpr::Vector {
                input: Box::new(window.clone()),
                op,
            },
            output: evaluated(Arc::clone(&out)),
        })
        .unwrap();
        assert_eq!(tree.to_string().lines().nth(1), Some(line));
    }
}
