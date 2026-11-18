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

//! What the guard has to answer: the operators that can hand chunks over, and nothing else.
//!
//! The consumer's fast path is only as safe as this answer, so the cases that keep it structural
//! are the ones that must say no: a region scan answering frames, a router over a content this
//! crate does not produce, and a wrapper that forwards whatever its input produces. Every content a
//! router does produce - raw, evaluated, aggregate state - is answered for, because every one of
//! them is a value this crate holds.

use std::sync::Arc;

use common_query::series_frame::frame_schema_with;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::empty::EmptyExec;
use datatypes::series::SeriesOrdering;
use datatypes::series::aggregate_state::{
    AggregateStateKind, AggregateStateSchema, StateComponent,
};
use datatypes::series::frame::{FrameLabelMode, aggregate_state_frame_schema};
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec};

use super::series_execution_plan;
use crate::extension_plan::raw_series_source::tests::{per_series_scan, raw_source};
use crate::extension_plan::{RawSeriesSourceExec, SeriesRepartitionExec};

#[test]
fn the_raw_source_hands_its_chunks_over() {
    let source = raw_source(per_series_scan(1));
    assert!(
        series_execution_plan(source.as_ref()).is_some(),
        "the region's raw source decodes the scan's frames itself, so it answers series"
    );
}

/// The answer is the operator itself, and `as_any` is how a caller says which one.
///
/// A downcast to `dyn SeriesExecutionPlan` is not expressible - [`Any::downcast_ref`] wants a sized
/// target - so the trait exposes its object as `dyn Any` and a caller that wants the operator back
/// names it. That is what makes the guard useful beyond a yes or no: a consumer holding the answer
/// can still ask whether it was handed a source or a router.
#[test]
fn the_answer_is_the_operator_that_was_asked() {
    let source = raw_source(per_series_scan(1));
    let answered =
        series_execution_plan(source.as_ref()).expect("the raw source hands chunks over");
    assert!(
        answered
            .as_any()
            .downcast_ref::<RawSeriesSourceExec>()
            .is_some(),
        "the guard answered with the raw source it was asked about"
    );

    let router: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(raw_source(per_series_scan(2)), 2)
            .expect("the raw source publishes a routeable frame"),
    );
    let answered = series_execution_plan(router.as_ref()).expect("a raw router hands chunks over");
    assert_eq!(
        2,
        answered
            .as_any()
            .downcast_ref::<SeriesRepartitionExec>()
            .expect("the guard answered with the router it was asked about")
            .outputs(),
        "the operator the caller got back is the one it asked"
    );
}

/// A router hands over the contents it publishes as chunks: the raw identity and the evaluated
/// series an owner merged, and neither of the two as a batch.
#[test]
fn a_router_hands_over_the_contents_it_publishes_as_chunks() {
    // A raw router merges each stored identity on the owner it routed it to, and what it publishes
    // is that owner's chunk: a value this crate holds, and the same value its frame encodes.
    let raw: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(raw_source(per_series_scan(2)), 2)
            .expect("the raw source publishes a routeable frame"),
    );
    assert!(
        series_execution_plan(raw.as_ref()).is_some(),
        "a router over raw frames publishes merged raw chunks, which a native consumer reads"
    );

    // The same for evaluated content: an owner publishes each label set once, already merged.
    let evaluated: Arc<dyn ExecutionPlan> = Arc::new(
        SeriesRepartitionExec::try_new(evaluated_frames(), 2)
            .expect("the child declares an evaluated frame"),
    );
    assert!(
        series_execution_plan(evaluated.as_ref()).is_some(),
        "an owner's evaluated publication is a chunk, not a batch"
    );
}

/// Every content a router publishes is handed over, aggregate state included.
///
/// This is the shape the trait states: a state publication is the row of a group its producer
/// exported, and the router carries it exactly as it carries a raw identity or an evaluated series -
/// on the same queue, the same lease and the same charge. Nothing is left on a frame path of its own,
/// which is why the router answers for all three rather than holding one of them back.
#[test]
fn every_content_a_router_publishes_is_handed_over() {
    let contents: [(&str, Arc<dyn ExecutionPlan>); 3] = [
        ("raw", raw_source(per_series_scan(2))),
        ("evaluated", evaluated_frames()),
        ("aggregate state", state_frames()),
    ];
    for (name, child) in contents {
        let router: Arc<dyn ExecutionPlan> = Arc::new(
            SeriesRepartitionExec::try_new(child, 2)
                .expect("the fixture publishes a routeable frame"),
        );
        let answered = series_execution_plan(router.as_ref())
            .unwrap_or_else(|| panic!("a router over {name} hands its publication over"));
        assert!(
            answered
                .as_any()
                .downcast_ref::<SeriesRepartitionExec>()
                .expect("the answer is the router itself")
                .hands_over_series(),
            "the router over {name} says it hands over"
        );
    }
}

#[test]
fn a_plan_that_carries_frames_is_not_answered_for() {
    // The region's raw-series leaf: it writes the frames this node reads, and what it produces is a
    // stream of batches, so a consumer has nothing in-process to read.
    let scan = per_series_scan(1);
    assert!(
        series_execution_plan(scan.as_ref()).is_none(),
        "a scan answers frames, not chunks"
    );

    // A wrapper forwards whatever its input answers, and forwarding a chunk is not its contract:
    // it is a frame-shaped operator above a native one.
    let wrapped: Arc<dyn ExecutionPlan> =
        Arc::new(CooperativeExec::new(raw_source(per_series_scan(1))));
    assert!(
        series_execution_plan(wrapped.as_ref()).is_none(),
        "a wrapper around a native operator is not itself one"
    );

    // A plan that publishes batches is read from its batches, whatever its schema declares.
    let frames: Arc<dyn ExecutionPlan> =
        Arc::new(EmptyExec::new(raw_source(per_series_scan(1)).schema()));
    assert!(
        series_execution_plan(frames.as_ref()).is_none(),
        "a batch source answers batches"
    );
}

/// The four-position grid every frame of these tests is declared on.
fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(0, 60_000, 4).expect("the grid is legal"))
            .expect("the grid is legal"),
    )
}

/// The one-field series schema those frames declare.
fn series_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: Some(grid()),
    })
}

/// A plan declaring an evaluated frame of one field.
fn evaluated_frames() -> Arc<dyn ExecutionPlan> {
    let schema = series_schema();
    let frame = frame_schema_with(
        &schema,
        FrameLabelMode::Explicit,
        None,
        SeriesOrdering::Contiguous,
    )
    .expect("explicit labels are a frame");
    Arc::new(EmptyExec::new(frame))
}

/// A plan declaring an aggregate-state frame of one accumulator.
fn state_frames() -> Arc<dyn ExecutionPlan> {
    let schema = series_schema();
    let state = AggregateStateSchema::try_new(
        AggregateStateKind::Sum,
        vec![StateComponent::Value, StateComponent::Compensation],
    )
    .expect("a sum carries a value and its compensation");
    let frame = aggregate_state_frame_schema(&state, &schema, FrameLabelMode::Explicit)
        .expect("a state schema declares a state frame");
    Arc::new(EmptyExec::new(frame))
}
