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

//! One node that runs a compiled native tree over raw series.
//!
//! [`FusedSeries`] is the logical half: it holds the PromQL text the tree was compiled from, the
//! tree itself and the binding its input was compiled against. [`FusedSeriesExec`] is the physical
//! half: it owns one execution of that tree per partition and decodes its input frames into raw
//! series chunks and converts its evaluated chunks back into batches.
//!
//! The two halves show different things in `EXPLAIN`, which is the point of having both. The
//! logical node prints the query a reader wrote ([`FusedSeries`]'s `fmt_for_explain`), and the
//! physical node prints the tree it will run ([`FusedSeriesExec`]'s `fmt_as`, which embeds
//! [`NativeTree`]'s own display rather than a second renderer). A reader comparing the two plans
//! can therefore see whether a query was compiled the way they expected without decoding node
//! names.
//!
//! Scope of this layer: the input is a stream of native raw series frames
//! ([`raw_series_frame_schema`](datatypes::series::frame::raw_series_frame_schema)) — one row per
//! chunk, carrying the series' encoded key, its finality, its timestamps and its fields, plus the
//! complete labels when the frame is explicit — with each series' rows contiguous and its
//! timestamps ascending, which is what the caller's [`SeriesOrdering`] names. The output is one
//! batch per evaluated chunk, with the statement's grid timestamps in the declared time column.
//! Each source contributes exactly one partition per output partition; this node has no internal
//! gather. Physical planning must normalize global consumers to one partition, and matching locality
//! requires a verified matching-key router on both operands. Everything the tree cannot do is
//! rejected when the execution plan is built, not when the first
//! batch arrives: a tree the native runtime does not execute, a frame whose schema the tree's input
//! was not compiled against, or an output schema the conversion cannot classify.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    SendableEvaluatedSeriesStream, SendableStateSeriesStream, ensure_frame_matches_label_source,
    pack_aggregate_state, pack_evaluated_series, series_frame_ordering, unpack_aggregate_state,
    unpack_evaluated_series, unpack_raw_series,
};
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation, UnboundedMemoryPool};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, SeriesFrameDeclaration, aggregate_state_frame_schema,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{DenseSamplesRef, FieldSpec, SampleRef, Samples, SeriesSchema};
use datatypes::series::time::EvalGrid;
use futures::stream::SelectAll;
use futures::{Stream, StreamExt};
use store_api::region_engine::RawLabelSource;
use table::table::scan::SeriesScanExec;

use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::SeriesRouting;
use crate::extension_plan::series_repartition::SeriesRepartitionExec;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, Grouping, InstantValue, NativeExpr, NativeNode,
    NativeTree, NativeType, reads_across_series,
};
use crate::native::state::{StateChunk, state_row_from_frame_row};
use crate::source::materialize_raw_labels;
use crate::vector_match::{DuplicateEvidence, MatchingLabels};

#[cfg(test)]
mod tests;

/// The label a metric name is stored under, which is not a tag column of the input.
const METRIC_NAME: &str = "__name__";

/// How many input steps one poll of [`FusedSeriesStream`] runs before it wakes itself.
///
/// A step polls the input once and hands what it settled to the executor. Settling is not
/// guaranteed: raw samples before the first evaluation point settle nothing, so an input of many
/// such chunks would otherwise be read to its end inside one poll, holding the task's thread for
/// an unbounded time and delaying both other tasks on it and the cancellation the consumer asked
/// for. The budget makes that a poll that yields instead, and the waker makes sure it is polled
/// again, so nothing is lost by stopping early.
const POLL_BUDGET: usize = 64;

/// Maximum number of tags the envelope carries, shared by [`FusedSeries::serialize`] and
/// [`FusedSeries::deserialize`] so a serialized node is always one this version can rebuild.
const MAX_TAG_COUNT: usize = 1024;

/// The shape of [`FusedSeries::serialize`]'s payload.
///
/// Bumped from 1 when the payload stopped carrying a partial fold. A reader accepts this version and
/// no other, so a payload written under the older shape is refused rather than reinterpreted with
/// the field boundaries of the newer one.
const ENVELOPE_VERSION: u32 = 2;

/// The statement grid and output naming that travel with a compiled tree.
///
/// Source schemas belong to the tree's input nodes. This binding supplies the grid that addresses
/// evaluated chunks, the time column and the metric and full raw tag names a region scan child is
/// bound with. A child that already produces raw frames, a [`RawSeriesSource`] per table of a
/// cross-table tree, carries its own binding instead. The output tags are the output schema's. The
/// logical node checks that the root produces values on that grid; frames carry each series'
/// actual labels and timestamps.
///
/// [`RawSeriesSource`]: super::RawSeriesSource
#[derive(Clone, Debug)]
pub struct FusedSeriesBinding {
    /// The statement's grid, which is the grid every evaluated chunk is addressed on.
    pub grid: Arc<EvalGrid>,
    /// The metric name every input series is bound to, when the input reads one named metric.
    ///
    /// The logical node requires it when [`tags`](Self::tags) declares [`METRIC_NAME`]: a reader of
    /// the output has to be able to tell that a range function dropped the name, which needs a name
    /// to drop. The physical node takes the names from the frames, which carry each series' own.
    pub metric_name: Option<Arc<str>>,
    /// The output column holding the grid timestamps.
    pub time_index: String,
    /// The full logical tag names used to bind region scan children, before output-label projection.
    ///
    /// [`METRIC_NAME`] is special: it is resolved to the series' metric name instead of a column,
    /// which is how a reader sees that a range function dropped it.
    pub tags: Vec<String>,
}

impl PartialEq for FusedSeriesBinding {
    fn eq(&self, other: &Self) -> bool {
        self.metric_name == other.metric_name
            && self.time_index == other.time_index
            && self.tags == other.tags
            && self.grid.spec() == other.grid.spec()
    }
}

impl Eq for FusedSeriesBinding {}

impl Hash for FusedSeriesBinding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.metric_name.hash(state);
        self.time_index.hash(state);
        self.tags.hash(state);
        let spec = self.grid.spec();
        (spec.start_ms, spec.step_ms, spec.len).hash(state);
    }
}

impl PartialOrd for FusedSeriesBinding {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            (
                self.metric_name.as_deref(),
                &self.time_index,
                &self.tags,
                self.grid.spec().start_ms,
                self.grid.spec().step_ms,
                self.grid.spec().len,
            )
                .cmp(&(
                    other.metric_name.as_deref(),
                    &other.time_index,
                    &other.tags,
                    other.grid.spec().start_ms,
                    other.grid.spec().step_ms,
                    other.grid.spec().len,
                )),
        )
    }
}

/// What one bound child of this node produces.
///
/// Aggregate state is not a sample input: it carries mergeable accumulators rather than query
/// values, even though both state and evaluated samples are addressed on a grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FusedInputKind {
    Raw,
    Evaluated,
    AggregateState,
}

impl FusedInputKind {
    fn from_native_type(native_type: &NativeType) -> DataFusionResult<Self> {
        match native_type {
            NativeType::Samples(schema) if schema.eval_grid.is_none() => Ok(Self::Raw),
            NativeType::Samples(_) => Ok(Self::Evaluated),
            NativeType::AggregateState(_) => Ok(Self::AggregateState),
            NativeType::Scalar(_) => Err(DataFusionError::Plan(
                "FusedSeries scalar cannot be a source".into(),
            )),
        }
    }
}

/// The output representation fixed when a fused plan is built.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum FusedOutputFormat {
    /// The existing tabular query result.
    PlainRows,
    /// A self-describing evaluated SeriesFrame.
    SeriesFrame,
}

fn evaluated_output(tree: &NativeTree) -> DataFusionResult<Arc<SeriesSchema>> {
    match &tree.root().output {
        NativeType::Samples(schema) if schema.eval_grid.is_some() => Ok(Arc::clone(schema)),
        NativeType::Samples(_) => Err(DataFusionError::Plan(
            "FusedSeries frame output requires evaluated samples with a grid".into(),
        )),
        NativeType::AggregateState(_) => Err(DataFusionError::Plan(
            "FusedSeries frame output does not support an aggregate-state root".into(),
        )),
        NativeType::Scalar(_) => Err(DataFusionError::Plan(
            "FusedSeries frame output does not support a scalar root".into(),
        )),
    }
}

/// The declaration is the shared schema; group, fields and grid live in the native binding and
/// are checked when a frame is decoded against an expected binding.
fn state_frame_output_schema(tree: &NativeTree) -> DataFusionResult<SchemaRef> {
    let NativeType::AggregateState(state) = &tree.root().output else {
        return Err(DataFusionError::Plan(
            "FusedSeries Partial root does not produce aggregate state".into(),
        ));
    };
    let schema = Arc::new(SeriesSchema {
        fields: state.accumulators.clone(),
        eval_grid: Some(Arc::clone(&state.grid)),
    });
    let state_schema = datatypes::series::aggregate_state::AggregateStateSchema::try_new(
        state.kind,
        state.components.to_vec(),
    )
    .map_err(|error| {
        DataFusionError::Plan(format!(
            "FusedSeries cannot declare state frame schema: {error}"
        ))
    })?;
    aggregate_state_frame_schema(&state_schema, &schema, FrameLabelMode::Explicit).map_err(
        |error| {
            DataFusionError::Plan(format!(
                "FusedSeries cannot declare state frame schema: {error}"
            ))
        },
    )
}

fn output_frame_schema(
    tree: &NativeTree,
    root_kind: FusedInputKind,
) -> DataFusionResult<SchemaRef> {
    match &tree.root().expr {
        NativeExpr::Aggregate { op, .. }
            if op.stage == crate::native::plan::AggregateStage::Partial =>
        {
            if root_kind != FusedInputKind::AggregateState {
                return Err(DataFusionError::Plan(
                    "FusedSeries Partial root must produce aggregate state".into(),
                ));
            }
            state_frame_output_schema(tree)
        }
        _ => series_frame_output_schema(evaluated_output(tree)?.as_ref()),
    }
}

fn series_frame_output_schema(schema: &SeriesSchema) -> DataFusionResult<SchemaRef> {
    let stream =
        pack_evaluated_series(Box::pin(futures::stream::empty()), Arc::new(schema.clone()))
            .map_err(|error| {
                DataFusionError::Plan(format!("FusedSeries cannot declare frame schema: {error}"))
            })?;
    Ok(stream.schema())
}

/// The logical plan of one node that runs a compiled native tree.
///
/// The node adds nothing to the tree: it records the query the tree was compiled from, so
/// `EXPLAIN` shows the reader their own query rather than the shape it compiled to, and it carries
/// the binding the tree's input was compiled against so the execution plan can be built from it.
#[derive(Debug)]
pub struct FusedSeries {
    /// The PromQL text this tree was compiled from.
    promql: Arc<str>,
    /// The compiled tree to run.
    tree: NativeTree,
    /// What the tree's input was compiled against.
    binding: FusedSeriesBinding,
    /// Plans producing the inputs, in source-index order.
    children: Vec<LogicalPlan>,
    /// The columns this node produces.
    output_schema: DFSchemaRef,
    /// Fixed output representation, chosen when the node is built.
    format: FusedOutputFormat,
}

impl FusedSeries {
    /// Serializes the native tree and the binding required to reconstruct this plan node.
    /// The PromQL text is included for diagnostics only and never drives execution; children
    /// remain ordinary Substrait inputs. Only SeriesFrame is carried: decoding must rebuild its
    /// output schema, and only SeriesFrame derives that schema from the tree.
    ///
    /// The leading version is [`ENVELOPE_VERSION`]. It changed when the payload stopped carrying a
    /// partial fold, so a payload written under the older shape is refused before its body is
    /// decoded: reading it would reinterpret the absence of that field as the next field's bytes.
    pub fn serialize(&self) -> DataFusionResult<Vec<u8>> {
        if self.format != FusedOutputFormat::SeriesFrame {
            return Err(DataFusionError::NotImplemented(
                "FusedSeries serialization does not support PlainRows output schema".into(),
            ));
        }
        fn text(out: &mut Vec<u8>, s: &str) -> DataFusionResult<()> {
            let n = u32::try_from(s.len())
                .map_err(|_| DataFusionError::Plan("FusedSeries string too long".into()))?;
            out.extend(n.to_le_bytes());
            out.extend(s.as_bytes());
            Ok(())
        }
        let tree = crate::native::wire::encode_prefix(&self.tree)
            .map_err(|e| DataFusionError::Plan(format!("cannot encode FusedSeries tree: {e}")))?;
        let mut out = Vec::new();
        out.extend(ENVELOPE_VERSION.to_le_bytes());
        out.extend(
            u32::try_from(tree.len())
                .map_err(|_| DataFusionError::Plan("FusedSeries tree too large".into()))?
                .to_le_bytes(),
        );
        out.extend(tree);
        let spec = self.binding.grid.spec();
        out.extend(spec.start_ms.to_le_bytes());
        out.extend(spec.step_ms.to_le_bytes());
        out.extend(spec.len.to_le_bytes());
        match &self.binding.metric_name {
            Some(name) => {
                out.push(1);
                text(&mut out, name)?;
            }
            None => out.push(0),
        }
        text(&mut out, &self.binding.time_index)?;
        if self.binding.tags.len() > MAX_TAG_COUNT {
            return Err(DataFusionError::Plan(format!(
                "too many FusedSeries tags: {} exceeds maximum {MAX_TAG_COUNT}",
                self.binding.tags.len()
            )));
        }
        out.extend(
            u32::try_from(self.binding.tags.len())
                .map_err(|_| DataFusionError::Plan("too many FusedSeries tags".into()))?
                .to_le_bytes(),
        );
        for tag in &self.binding.tags {
            text(&mut out, tag)?;
        }
        out.push(1); // explicit stable tag: SeriesFrame
        text(&mut out, &self.promql)?;
        Ok(out)
    }

    /// Rebuilds a serialized SeriesFrame node. Substrait attaches its child plans separately.
    pub fn deserialize(bytes: &[u8]) -> DataFusionResult<Self> {
        struct Reader<'a> {
            b: &'a [u8],
            p: usize,
        }
        impl<'a> Reader<'a> {
            fn take(&mut self, n: usize) -> DataFusionResult<&'a [u8]> {
                let e = self
                    .p
                    .checked_add(n)
                    .ok_or_else(|| DataFusionError::Plan("FusedSeries length overflow".into()))?;
                if e > self.b.len() {
                    return Err(DataFusionError::Plan(
                        "truncated FusedSeries payload".into(),
                    ));
                }
                let r = &self.b[self.p..e];
                self.p = e;
                Ok(r)
            }
            fn u8(&mut self) -> DataFusionResult<u8> {
                Ok(self.take(1)?[0])
            }
            fn u32(&mut self) -> DataFusionResult<u32> {
                let bytes = self.take(4)?;
                Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
                    DataFusionError::Plan("truncated FusedSeries integer".into())
                })?))
            }
            fn i64(&mut self) -> DataFusionResult<i64> {
                let bytes = self.take(8)?;
                Ok(i64::from_le_bytes(bytes.try_into().map_err(|_| {
                    DataFusionError::Plan("truncated FusedSeries integer".into())
                })?))
            }
            fn u64(&mut self) -> DataFusionResult<u64> {
                let bytes = self.take(8)?;
                Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
                    DataFusionError::Plan("truncated FusedSeries integer".into())
                })?))
            }
            fn text(&mut self) -> DataFusionResult<String> {
                let n = self.u32()? as usize;
                String::from_utf8(self.take(n)?.to_vec())
                    .map_err(|e| DataFusionError::Plan(format!("invalid FusedSeries UTF-8: {e}")))
            }
        }
        let mut r = Reader { b: bytes, p: 0 };
        // Read and check the version before anything else touches the body: a payload written under
        // another shape has different fields, so decoding it first would read the wrong bytes as the
        // wrong field instead of reporting what actually disagreed.
        let version = r.u32()?;
        if version != ENVELOPE_VERSION {
            return Err(DataFusionError::Plan(format!(
                "unsupported FusedSeries envelope version {version}"
            )));
        }
        let n = r.u32()? as usize;
        let tree = crate::native::wire::decode_prefix(r.take(n)?)
            .map_err(|e| DataFusionError::Plan(format!("cannot decode FusedSeries tree: {e}")))?;
        let grid = Arc::new(
            EvalGrid::try_deferred(
                datatypes::series::time::GridSpec::try_new(r.i64()?, r.i64()?, r.u64()?)
                    .map_err(|e| DataFusionError::Plan(format!("invalid FusedSeries grid: {e}")))?,
            )
            .map_err(|e| DataFusionError::Plan(format!("invalid FusedSeries grid: {e}")))?,
        );
        let metric_name = match r.u8()? {
            0 => None,
            1 => Some(Arc::<str>::from(r.text()?)),
            x => {
                return Err(DataFusionError::Plan(format!(
                    "invalid FusedSeries metric-name tag {x}"
                )));
            }
        };
        let time_index = r.text()?;
        let count = r.u32()? as usize;
        if count > MAX_TAG_COUNT {
            return Err(DataFusionError::Plan(format!(
                "too many FusedSeries tags: {count} exceeds maximum {MAX_TAG_COUNT}"
            )));
        }
        let mut tags = Vec::with_capacity(count);
        for _ in 0..count {
            tags.push(r.text()?)
        }
        let format = match r.u8()? {
            1 => FusedOutputFormat::SeriesFrame,
            x => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported FusedSeries output format tag {x}"
                )));
            }
        };
        let promql = Arc::<str>::from(r.text()?);
        if r.p != bytes.len() {
            return Err(DataFusionError::Plan(
                "trailing bytes in FusedSeries payload".into(),
            ));
        }
        let children = vec![
            LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(datafusion::common::DFSchema::empty())
            });
            collect_source_schemas(&tree)?.len()
        ];
        // A frame output declares the layout of the tree's evaluated root, which the tree itself
        // carries. `try_new_with_format` compares the two, so passing an empty schema here would
        // make every serialized frame node impossible to rebuild.
        let output_schema = if format == FusedOutputFormat::SeriesFrame {
            Arc::new(datafusion::common::DFSchema::try_from(
                output_frame_schema(
                    &tree,
                    FusedInputKind::from_native_type(&tree.root().output)?,
                )?,
            )?)
        } else {
            Arc::new(datafusion::common::DFSchema::empty())
        };
        Self::try_new_with_format(
            promql,
            tree,
            FusedSeriesBinding {
                grid,
                metric_name,
                time_index,
                tags,
            },
            children,
            output_schema,
            format,
        )
    }

    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "FusedSeries"
    }

    /// Creates the node, checking that `tree` is legal against `binding`.
    pub fn try_new(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        output_schema: DFSchemaRef,
    ) -> DataFusionResult<Self> {
        Self::try_new_with_format(
            promql,
            tree,
            binding,
            children,
            output_schema,
            FusedOutputFormat::PlainRows,
        )
    }

    /// Creates the state-exporting half of a split aggregate.
    ///
    /// The node runs a `Partial` tree and answers aggregate-state frames, so its output layout is
    /// the one `tree` declares rather than a caller's: the alternative is a caller free to declare a
    /// state frame the tree does not produce, which `try_new_with_format` would then only catch for
    /// some of its columns.
    pub fn try_new_state_export(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        let schema = state_frame_output_schema(&tree)?;
        let output_schema = DFSchema::try_from(schema).map_err(|error| {
            DataFusionError::Plan(format!(
                "FusedSeries cannot declare its state schema: {error}"
            ))
        })?;
        Self::try_new_with_format(
            promql,
            tree,
            binding,
            children,
            Arc::new(output_schema),
            FusedOutputFormat::SeriesFrame,
        )
    }

    /// The two trees a native aggregate over `tree` is split into, if it can be split at all.
    ///
    /// The first runs where the series are read and answers the state each partition folded; the
    /// second runs where every partition's state has arrived and answers the statement's series.
    /// `time_index` is the column the tree's grid points are reported under.
    pub fn split_aggregate(
        tree: &NativeTree,
        time_index: &str,
    ) -> DataFusionResult<Option<(NativeTree, NativeTree)>> {
        let fields = crate::native::split::prefix_fields(tree)?;
        let split = match crate::native::split::split(tree.root(), &fields, time_index) {
            Ok(Some(split)) => split,
            Ok(None) => return Ok(None),
            // A shape the split cannot carry is not a failure: the caller keeps the plan it would
            // have built without this route.
            Err(DataFusionError::NotImplemented(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        Ok(split
            .into_trees()?
            .map(|trees| (trees.partial, trees.final_tree)))
    }

    /// The evaluated series-frame schema declared by a native tree's root.
    pub fn series_frame_schema(tree: &NativeTree) -> DataFusionResult<SchemaRef> {
        series_frame_output_schema(evaluated_output(tree)?.as_ref())
    }

    /// Creates a node with an explicitly selected output format.
    pub fn try_new_with_format(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        output_schema: DFSchemaRef,
        format: FusedOutputFormat,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding, children.len())?;
        // State is a frame, not a row table: a `Partial` root has no row layout to publish, so the
        // mismatch is refused where the format is chosen rather than where the exec is built.
        if matches!(
            &tree.root().expr,
            NativeExpr::Aggregate { op, .. } if op.stage == AggregateStage::Partial
        ) && format != FusedOutputFormat::SeriesFrame
        {
            return Err(DataFusionError::Plan(
                "FusedSeries Partial stage requires SeriesFrame output".into(),
            ));
        }
        if format == FusedOutputFormat::SeriesFrame {
            let expected = output_frame_schema(
                &tree,
                FusedInputKind::from_native_type(&tree.root().output)?,
            )?;
            if output_schema.as_arrow() != expected.as_ref() {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries SeriesFrame schema does not match: declared {output_schema:?}, expected {expected:?}"
                )));
            }
        }
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
            format,
        })
    }

    /// The PromQL text this node was compiled from.
    pub fn promql(&self) -> &str {
        &self.promql
    }

    /// The selected output representation.
    pub fn format(&self) -> FusedOutputFormat {
        self.format
    }

    /// Whether this node's tree reads its input as aggregate state rather than as series.
    ///
    /// A state root is the `Final` half of a split aggregate, which reads what the `Partial` half
    /// exported. The kind is the tree's own: its inputs' bindings decide what each child must
    /// produce, so a caller that needs to recognize the second half of a split does not have to
    /// read the first one's labels.
    pub fn reads_aggregate_state(&self) -> bool {
        tree_inputs(&self.tree)
            .iter()
            .any(|input| matches!(input.output, NativeType::AggregateState(_)))
    }

    /// The compiled tree this node runs.
    pub fn tree(&self) -> &NativeTree {
        &self.tree
    }

    /// Child plans in source-index order.
    pub fn children(&self) -> &[LogicalPlan] {
        &self.children
    }

    /// What the tree's input was compiled against.
    pub fn binding(&self) -> &FusedSeriesBinding {
        &self.binding
    }

    /// The physical node that runs raw frame sources as this node's inputs.
    ///
    /// Each tuple binds a frame producer to its label source and ordering. Building the node
    /// rejects incompatible frames or an output
    /// schema the conversion cannot classify.
    pub fn to_execution_plan(
        &self,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.to_execution_plan_with_parallelism_option(inputs, None)
    }

    /// Builds the physical plan using an explicit target width for matching-local work.
    pub fn to_execution_plan_with_parallelism(
        &self,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        target_partitions: usize,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.to_execution_plan_with_parallelism_option(inputs, Some(target_partitions))
    }

    fn to_execution_plan_with_parallelism_option(
        &self,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        target_partitions: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        if inputs.len() != self.children.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries physical input count {} does not match its {} logical children",
                inputs.len(),
                self.children.len()
            )));
        }
        let tags = if matches!(self.tree.root().output, NativeType::AggregateState(_)) {
            self.binding.tags.clone()
        } else {
            self.output_tags()?
        };
        let evaluated = inputs.iter().any(|(input, _, _)| {
            SeriesFrameDeclaration::parse(input.schema().as_ref())
                .is_ok_and(|declaration| declaration.content() == FrameContent::Evaluated)
        });
        let locality = input_locality(self.tree.root());
        let state_group = state_routing(self.tree.root());
        let normalize = matches!(locality, InputLocality::SinglePartition);
        let matching_policy = match &locality {
            InputLocality::MatchingLocal(policy) => Some(policy.clone()),
            _ => None,
        };
        let matching_partitions = matching_policy.as_ref().map(|_| {
            target_partitions.map_or_else(
                || {
                    if let Some(source) = same_source(self.tree.root()) {
                        inputs
                            .get(source)
                            .map(|(input, _, _)| input.properties().partitioning.partition_count())
                            .unwrap_or(1)
                    } else {
                        inputs
                            .iter()
                            .map(|(input, _, _)| input.properties().partitioning.partition_count())
                            .max()
                            .unwrap_or(1)
                    }
                },
                |target| target.max(1),
            )
        });
        let window_sources = if evaluated
            && !normalize
            && inputs
                .iter()
                .any(|(input, _, _)| input.properties().partitioning.partition_count() > 1)
        {
            partitioned_window_sources(self.tree.root())?
        } else {
            Vec::new()
        };
        // Validate every declared frame and ordering before inserting any exchanges.
        let inputs = inputs
            .into_iter()
            .enumerate()
            .map(|(index, (input, label_source, ordering))| {
                let declaration = SeriesFrameDeclaration::parse(input.schema().as_ref())
                    .map_err(|error| DataFusionError::Plan(error.to_string()))?;
                let actual_ordering = series_frame_ordering(input.schema().as_ref())?;
                if actual_ordering != ordering {
                    return Err(DataFusionError::Plan(
                        "FusedSeries source supplied ordering does not match its frame declaration"
                            .into(),
                    ));
                }
                if declaration.content() == FrameContent::AggregateState {
                    // State routing is its own path: a `Final` aggregate reads the rows of one group
                    // in whatever order they arrive, so it needs every row of every group to reach
                    // one owner, not the matching-key classes a label match co-locates.
                    let group = state_group.ok_or_else(|| {
                        DataFusionError::Plan(
                            "FusedSeries reads aggregate state but holds no Final aggregate".into(),
                        )
                    })?;
                    let count = input.properties().partitioning.partition_count();
                    // One group has one owner: two would be two partial folds of one answer. More
                    // than one group splits the work the way the session asks, and a caller that
                    // names no width keeps the width the producers already have.
                    let required = match group {
                        StateGroup::Single => 1,
                        StateGroup::Grouped => target_partitions.unwrap_or(count).max(1),
                    };
                    let has_router = input
                        .downcast_ref::<SeriesRepartitionExec>()
                        .is_some_and(|router| router.outputs() == required);
                    if count == required && (has_router || required == 1) {
                        return Ok((input, label_source, ordering));
                    }
                    let routed = Arc::new(SeriesRepartitionExec::try_new(input, required)?)
                        as Arc<dyn ExecutionPlan>;
                    let ordering = series_frame_ordering(routed.schema().as_ref())?;
                    return Ok((routed, RawLabelSource::Explicit, ordering));
                }
                if declaration.content() != FrameContent::Evaluated {
                    let count = input.properties().partitioning.partition_count();
                    if normalize && count != 1 {
                        let input = Arc::new(SeriesRepartitionExec::try_new(input, 1)?)
                            as Arc<dyn ExecutionPlan>;
                        let ordering = series_frame_ordering(input.schema().as_ref())?;
                        return Ok((input, RawLabelSource::Explicit, ordering));
                    }
                    if let (Some(policy), Some(required)) =
                        (matching_policy.as_ref(), matching_partitions)
                    {
                        if required == 1 && count != 1 {
                            let input = Arc::new(SeriesRepartitionExec::try_new(input, 1)?)
                                as Arc<dyn ExecutionPlan>;
                            let ordering = series_frame_ordering(input.schema().as_ref())?;
                            return Ok((input, RawLabelSource::Explicit, ordering));
                        }
                        let has_router =
                            input
                                .downcast_ref::<SeriesRepartitionExec>()
                                .is_some_and(|router| {
                                    router.outputs() == required
                                        && router.routing_policy()
                                            == &SeriesRouting::Matching(policy.clone())
                                });
                        if required > 1 && !has_router {
                            let input = Arc::new(SeriesRepartitionExec::try_new_with_routing(
                                input,
                                required,
                                SeriesRouting::Matching(policy.clone()),
                            )?) as Arc<dyn ExecutionPlan>;
                            let ordering = series_frame_ordering(input.schema().as_ref())?;
                            return Ok((input, RawLabelSource::Explicit, ordering));
                        }
                    }
                    return Ok((input, label_source, ordering));
                }
                let count = input.properties().partitioning.partition_count();
                if count == 0 {
                    return Err(DataFusionError::Plan(format!(
                        "FusedSeries input {index} has no partitions"
                    )));
                }
                let required = if normalize {
                    1
                } else {
                    matching_partitions.unwrap_or(count)
                };
                let needs_owner = window_sources.contains(&index);
                let contiguous = actual_ordering == SeriesOrdering::Contiguous;
                let has_router =
                    input
                        .downcast_ref::<SeriesRepartitionExec>()
                        .is_some_and(|router| {
                            router.outputs() == required
                                && matching_policy.as_ref().is_some_and(|policy| {
                                    router.routing_policy()
                                        == &SeriesRouting::Matching(policy.clone())
                                })
                        });
                if normalize && count != required {
                    let input = Arc::new(SeriesRepartitionExec::try_new(input, required)?)
                        as Arc<dyn ExecutionPlan>;
                    Ok((input, label_source, SeriesOrdering::Contiguous))
                } else if normalize {
                    // Already the one partition a global tree needs; wrapping it would add a node
                    // that changes nothing.
                    Ok((input, label_source, ordering))
                } else if let Some(policy) = matching_policy.as_ref()
                    && required > 1
                    && (count != required || !has_router)
                {
                    // Either there is no router yet or the one there routes by another key. The
                    // second case re-routes what the first produced: a router's output is plain
                    // partitions, so another one above it is a legitimate shuffle rather than a
                    // no-op, and refusing the query would be worse than paying for the move.
                    let input = Arc::new(SeriesRepartitionExec::try_new_with_routing(
                        input,
                        required,
                        SeriesRouting::Matching(policy.clone()),
                    )?) as Arc<dyn ExecutionPlan>;
                    Ok((input, label_source, SeriesOrdering::Contiguous))
                } else if needs_owner
                    && !(count == 1 && contiguous)
                    && input.downcast_ref::<SeriesRepartitionExec>().is_none()
                {
                    let input = Arc::new(SeriesRepartitionExec::try_new(input, required)?)
                        as Arc<dyn ExecutionPlan>;
                    Ok((input, label_source, SeriesOrdering::Contiguous))
                } else {
                    Ok((input, label_source, ordering))
                }
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(Arc::new(FusedSeriesExec::build(
            Arc::clone(&self.promql),
            inputs,
            Arc::new(self.tree.clone()),
            output_schema,
            self.binding.time_index.clone(),
            tags,
            self.format,
            target_partitions,
        )?))
    }

    /// The label columns of the output: every column that is neither the time index nor a field of
    /// the root. A scalar root has no labels; its one other column is the value.
    ///
    /// The planner decides them from the whole tree - a match narrows them, `or` joins both sides'
    /// and a wrapper around either keeps them - and the output schema is where it records that.
    ///
    fn output_tags(&self) -> DataFusionResult<Vec<String>> {
        let Some(root) = self.tree.root().output.samples() else {
            return Ok(Vec::new());
        };
        Ok(self
            .output_schema
            .fields()
            .iter()
            .map(|field| field.name())
            .filter(|name| {
                *name != &self.binding.time_index
                    && !root
                        .fields
                        .iter()
                        .any(|field| field.name.as_ref() == name.as_str())
            })
            .cloned()
            .collect())
    }
}

impl UserDefinedLogicalNodeCore for FusedSeries {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.children.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // The tree's operands are bound inside it; there is no expression for an optimizer to
        // rewrite separately from the tree that owns it.
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        match self.format {
            FusedOutputFormat::SeriesFrame => {
                write!(f, "FusedSeries: {} (output: series_frame)", self.promql)
            }
            FusedOutputFormat::PlainRows => write!(f, "FusedSeries: {}", self.promql),
        }
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        validate_binding(&self.tree, &self.binding, inputs.len())?;
        Ok(Self {
            promql: Arc::clone(&self.promql),
            tree: self.tree.clone(),
            binding: self.binding.clone(),
            children: inputs,
            output_schema: Arc::clone(&self.output_schema),
            format: self.format,
        })
    }
}

impl PartialEq for FusedSeries {
    fn eq(&self, other: &Self) -> bool {
        // `NativeTree` carries no structural equality, so the rendered tree stands in for it. The
        // renderer names every node with its bound operation and grids, which is what makes two
        // nodes the same computation; comparing the text alone would call two different trees
        // equal whenever their PromQL happens to read the same.
        self.promql == other.promql
            && self.tree.to_string() == other.tree.to_string()
            && tree_input_fields(&self.tree) == tree_input_fields(&other.tree)
            && self.binding == other.binding
            && self.children == other.children
            && self.output_schema == other.output_schema
            && self.format == other.format
    }
}

impl Eq for FusedSeries {}

impl Hash for FusedSeries {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.promql.hash(state);
        self.binding.hash(state);
        self.children.hash(state);
        self.output_schema.hash(state);
        self.format.hash(state);
    }
}

impl PartialOrd for FusedSeries {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.promql.partial_cmp(&other.promql) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.binding.partial_cmp(&other.binding) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.children.partial_cmp(&other.children) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.tree.to_string().partial_cmp(&other.tree.to_string()) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.format.cmp(&other.format) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
        match tree_input_fields(&self.tree).cmp(&tree_input_fields(&other.tree)) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
        if self.output_schema == other.output_schema {
            Some(Ordering::Equal)
        } else {
            None
        }
    }
}

/// The raw field names bound to each input leaf, retaining source association and leaf order.
fn tree_input_fields(tree: &NativeTree) -> Vec<(usize, Vec<String>)> {
    tree_inputs(tree)
        .into_iter()
        .map(|input| {
            let fields = match &input.output {
                NativeType::Samples(schema) => schema
                    .fields
                    .iter()
                    .map(|field| field.name.to_string())
                    .collect(),
                NativeType::Scalar(_) | NativeType::AggregateState(_) => Vec::new(),
            };
            (input.source_index, fields)
        })
        .collect()
}

/// The schemas read by distinct raw sources, in dense source-index order.
fn collect_source_schemas(tree: &NativeTree) -> DataFusionResult<Vec<Arc<SeriesSchema>>> {
    collect_source_schemas_from_inputs(tree_inputs(tree))
}

fn collect_source_schemas_from_inputs(
    inputs: Vec<&crate::native::plan::BoundInput>,
) -> DataFusionResult<Vec<Arc<SeriesSchema>>> {
    let mut schemas: Vec<(usize, Arc<SeriesSchema>, FusedInputKind)> = Vec::new();
    for input in inputs {
        let kind = FusedInputKind::from_native_type(&input.output)?;
        let found = match &input.output {
            NativeType::Samples(schema) => Arc::clone(schema),
            NativeType::AggregateState(state) => Arc::new(SeriesSchema {
                fields: state.accumulators.clone(),
                eval_grid: Some(Arc::clone(&state.grid)),
            }),
            NativeType::Scalar(_) => {
                return Err(DataFusionError::Plan(
                    "FusedSeries scalar cannot be a source".into(),
                ));
            }
        };
        if let Some((_, expected, expected_kind)) = schemas
            .iter()
            .find(|(index, _, _)| *index == input.source_index)
        {
            if *expected_kind != kind {
                return Err(DataFusionError::Plan(
                    "FusedSeries repeated source disagrees on input kind".into(),
                ));
            }
            let same_grid = match (&expected.eval_grid, &found.eval_grid) {
                (None, None) => true,
                (Some(left), Some(right)) => left.spec() == right.spec(),
                _ => false,
            };
            if !same_fields(expected, &found) || !same_grid {
                return Err(DataFusionError::Plan(
                    "FusedSeries repeated source has different fields or evaluation grid".into(),
                ));
            }
        } else {
            schemas.push((input.source_index, found, kind));
        }
    }
    schemas.sort_unstable_by_key(|(index, _, _)| *index);
    if schemas
        .iter()
        .enumerate()
        .any(|(expected, (actual, _, _))| expected != *actual)
    {
        return Err(DataFusionError::Plan(
            "FusedSeries source indexes are not dense".into(),
        ));
    }
    Ok(schemas.into_iter().map(|(_, schema, _)| schema).collect())
}

#[derive(Clone, Debug)]
pub enum InputLocality {
    /// Each series is answered on its own, so the tree runs on whatever partitions its input has.
    PartitionLocal,
    /// The tree pairs series of whole vectors by one match key, so those classes must meet.
    MatchingLocal(MatchingLabels),
    /// The tree reads across series, so it needs every input on one partition.
    SinglePartition,
}

/// The one source every leaf of `node` reads, when the tree keeps the matching key of that source.
fn same_source(node: &NativeNode) -> Option<usize> {
    match &node.expr {
        NativeExpr::Input(input)
            if node.output.grid().is_some()
                || node
                    .output
                    .samples()
                    .is_some_and(|schema| schema.eval_grid.is_none()) =>
        {
            Some(input.source_index)
        }
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Timestamp { input } => same_source(input),
        NativeExpr::Map { input, op }
            if !op
                .ops
                .iter()
                .any(|op| matches!(op, crate::native::plan::MapOp::Label(_))) =>
        {
            same_source(input)
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } if parameters.iter().all(|parameter| {
            matches!(
                parameter.expr,
                NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. }
            )
        }) =>
        {
            same_source(input)
        }
        NativeExpr::Binary {
            left,
            right,
            matching,
        } if matches!(left.output, NativeType::Scalar(_)) => {
            let _ = matching;
            same_source(right)
        }
        NativeExpr::Binary {
            left,
            right,
            matching,
        } if matches!(right.output, NativeType::Scalar(_)) => {
            let _ = matching;
            same_source(left)
        }
        NativeExpr::Binary {
            left,
            right,
            matching,
        } if matching.labels == MatchingLabels::Ignoring(Vec::new())
            && matching.cardinality == crate::vector_match::Cardinality::OneToOne =>
        {
            let source = same_source(left)?;
            (same_source(right) == Some(source)).then_some(source)
        }
        NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::VectorOf { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Info { .. }
        | NativeExpr::Merge { .. }
        | NativeExpr::Binary { .. }
        | NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::Map { .. }
        | NativeExpr::ParameterizedMap { .. } => None,
    }
}

/// Whether every operation in `node` preserves the labels used by `matching`.
///
/// This is deliberately separate from `same_source`: a subtree can read one source while
/// changing the labels used to route its eventual vector match.
fn preserves_matching_key(node: &NativeNode, matching: &MatchingLabels) -> bool {
    const COMPUTED_LABELS: [&str; 3] = [METRIC_NAME, "__type__", "__unit__"];
    let key_contains = |label: &str| match matching {
        MatchingLabels::On(labels) => labels.iter().any(|candidate| candidate.as_ref() == label),
        MatchingLabels::Ignoring(labels) => {
            label != METRIC_NAME && !labels.iter().any(|candidate| candidate.as_ref() == label)
        }
    };
    let labels_removed = |labels: &[&str]| labels.iter().any(|label| key_contains(label));

    match &node.expr {
        NativeExpr::Input(_) => true,
        NativeExpr::Instant { input, op } => {
            (op.value != InstantValue::Timestamp || !labels_removed(&COMPUTED_LABELS))
                && preserves_matching_key(input, matching)
        }
        NativeExpr::Map { input, op } => {
            op.ops.iter().all(|op| match op {
                crate::native::plan::MapOp::Math(_) => !labels_removed(&COMPUTED_LABELS),
                // Label rewrites and histogram maps are not proven to retain the requested key.
                crate::native::plan::MapOp::Label(_) | crate::native::plan::MapOp::Histogram(_) => {
                    false
                }
            }) && preserves_matching_key(input, matching)
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => {
            !labels_removed(&COMPUTED_LABELS)
                && preserves_matching_key(input, matching)
                && parameters.iter().all(|parameter| {
                    matches!(
                        parameter.expr,
                        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. }
                    )
                })
        }
        NativeExpr::Timestamp { input } => {
            !labels_removed(&COMPUTED_LABELS) && preserves_matching_key(input, matching)
        }
        NativeExpr::Window { input, op } => {
            (!op.function.drops_metric_name() || !labels_removed(&COMPUTED_LABELS))
                && preserves_matching_key(input, matching)
        }
        NativeExpr::Binary {
            left,
            right,
            matching: binary,
        } if matches!(left.output, NativeType::Scalar(_)) => {
            let drops_labels = binary.return_bool
                || !matches!(
                    binary.op,
                    crate::vector_match::BinaryOp::Eq
                        | crate::vector_match::BinaryOp::Ne
                        | crate::vector_match::BinaryOp::Gt
                        | crate::vector_match::BinaryOp::Lt
                        | crate::vector_match::BinaryOp::Ge
                        | crate::vector_match::BinaryOp::Le
                );
            (!drops_labels || !labels_removed(&COMPUTED_LABELS))
                && preserves_matching_key(right, matching)
        }
        NativeExpr::Binary {
            left,
            right,
            matching: binary,
        } if matches!(right.output, NativeType::Scalar(_)) => {
            let drops_labels = binary.return_bool
                || !matches!(
                    binary.op,
                    crate::vector_match::BinaryOp::Eq
                        | crate::vector_match::BinaryOp::Ne
                        | crate::vector_match::BinaryOp::Gt
                        | crate::vector_match::BinaryOp::Lt
                        | crate::vector_match::BinaryOp::Ge
                        | crate::vector_match::BinaryOp::Le
                );
            (!drops_labels || !labels_removed(&COMPUTED_LABELS))
                && preserves_matching_key(left, matching)
        }
        // These operations can change labels or select/merge series; absent a node-specific
        // proof, do not claim that the routed key survives.
        _ => false,
    }
}

/// Whether matching operands can be segmented as independent single-source computations and
/// at least one fails to preserve the requested key.
///
/// This is the key-change case the planner can handle by cutting the operands before matching.
/// A failed preservation proof alone does not establish that a key was changed. The planner and
/// [`input_locality`] share this predicate so that this admission rule cannot drift from locality.
pub fn matching_operands_drop_key(root: &NativeNode) -> bool {
    matching_site(root).is_some_and(|(left, right, labels)| {
        same_source(left).is_some()
            && same_source(right).is_some()
            && (!preserves_matching_key(left, labels) || !preserves_matching_key(right, labels))
    })
}

/// Derives the partition contract from the bound root, not from a physical property registry.
/// Whether `node` contains a vector-vector binary, which matches series of the whole vector.
///
/// A tree of unary nodes evaluates each series on its own, so its partitions need nothing from
/// each other. A binary with two vector sides pairs by match key instead, and every series of one
/// key class has to reach the same partition.
fn needs_vector_matching(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Binary { left, right, .. } => {
            let scalar = |side: &NativeNode| matches!(side.output, NativeType::Scalar(_));
            if scalar(left) || scalar(right) {
                needs_vector_matching(left) || needs_vector_matching(right)
            } else {
                true
            }
        }
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::VectorOf { input }
        | NativeExpr::Merge { input }
        | NativeExpr::Aggregate { input, .. }
        | NativeExpr::Vector { input, .. } => needs_vector_matching(input),
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => needs_vector_matching(input) || parameters.iter().any(needs_vector_matching),
        NativeExpr::Set { left, right, .. } => {
            needs_vector_matching(left) || needs_vector_matching(right)
        }
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => false,
        NativeExpr::Info { .. } => true,
    }
}

/// The vector/vector match this tree performs, looking through the pointwise nodes wrapped on top.
///
/// A match pairs series of two whole vectors, so where it sits — including under a scalar
/// broadcast or a pointwise wrapper — is the place that needs matching-key co-location.
fn matching_site(node: &NativeNode) -> Option<(&NativeNode, &NativeNode, &MatchingLabels)> {
    match &node.expr {
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let scalar = |side: &NativeNode| matches!(side.output, NativeType::Scalar(_));
            if scalar(left) {
                matching_site(right)
            } else if scalar(right) {
                matching_site(left)
            } else {
                Some((left, right, &matching.labels))
            }
        }
        NativeExpr::Instant { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Timestamp { input } => matching_site(input),
        _ => None,
    }
}

pub fn input_locality(root: &NativeNode) -> InputLocality {
    // A `Partial` aggregate folds only the rows its own partition reads and exports the state it
    // folded; the `Final` half above the exchange is what needs every contribution in one place.
    // Reading it as a global fold instead would gather the scan into one partition and throw away
    // the parallelism the split exists to keep.
    if matches!(
        &root.expr,
        NativeExpr::Aggregate { op, .. } if op.stage == AggregateStage::Partial
    ) {
        return InputLocality::PartitionLocal;
    }
    if let Some(index) = same_source(root) {
        let raw = root.output.samples().is_some_and(|_| {
            // The root's grid does not reliably indicate source content after windowing; inspect its bound inputs.
            tree_inputs_for_locality(root)
                .into_iter()
                .find(|input| input.source_index == index)
                .is_some_and(|input| {
                    input
                        .output
                        .samples()
                        .is_some_and(|schema| schema.eval_grid.is_none())
                })
        });
        // Only a tree that matches two vectors needs its matching-key classes to meet in one
        // partition. A tree of unary nodes answers each series on its own, and asking for the
        // router there would both cost a shuffle and hide the ownership a Window needs.
        if !needs_vector_matching(root) {
            return InputLocality::PartitionLocal;
        }
        if raw {
            if matching_operands_drop_key(root) {
                return InputLocality::SinglePartition;
            }
            return InputLocality::PartitionLocal;
        }
        return InputLocality::MatchingLocal(MatchingLabels::Ignoring(Vec::new()));
    }
    // A match between two different sources needs its match keys co-located, wherever the match
    // sits under the pointwise nodes above it.
    if let Some((left, right, labels)) = matching_site(root)
        && let (Some(_left_source), Some(_right_source)) = (same_source(left), same_source(right))
        && preserves_matching_key(left, labels)
        && preserves_matching_key(right, labels)
    {
        return InputLocality::MatchingLocal(labels.clone());
    }
    if reads_across_series(root) {
        InputLocality::SinglePartition
    } else {
        InputLocality::PartitionLocal
    }
}

fn tree_inputs_for_locality(root: &NativeNode) -> Vec<&crate::native::plan::BoundInput> {
    fn visit<'a>(node: &'a NativeNode, inputs: &mut Vec<&'a crate::native::plan::BoundInput>) {
        match &node.expr {
            NativeExpr::Input(input) => inputs.push(input),
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::VectorOf { input }
            | NativeExpr::Merge { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. } => visit(input, inputs),
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => {
                visit(input, inputs);
                for parameter in parameters {
                    visit(parameter, inputs);
                }
            }
            NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
                visit(left, inputs);
                visit(right, inputs);
            }
            NativeExpr::Info { base, info, .. } => {
                visit(base, inputs);
                visit(info, inputs);
            }
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {}
        }
    }
    let mut inputs = Vec::new();
    visit(root, &mut inputs);
    inputs
}

/// The grouping a `Final` aggregate over state routes by, and how many owners it needs.
///
/// A tree whose input is aggregate state holds exactly one such aggregate: the `Final` the split
/// put at the cut. Its grouping is the key a router sends the rows of a group by, and its own
/// arity decides how many owners there are to have - a single group needs one, since two would be
/// two partial folds of the same answer - so the width is fixed here at plan time rather than
/// coalesced after the fact. The walk only answers the aggregate; whether this tree reads state at
/// all is the input frame declaration's business.
fn state_routing(root: &NativeNode) -> Option<StateGroup> {
    fn find(node: &NativeNode) -> Option<&Grouping> {
        match &node.expr {
            NativeExpr::Aggregate { op, .. } if op.stage == AggregateStage::Final => {
                Some(&op.group)
            }
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::VectorOf { input }
            | NativeExpr::ParameterizedMap { input, .. }
            | NativeExpr::Aggregate { input, .. } => find(input),
            NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
                find(left).or_else(|| find(right))
            }
            NativeExpr::Info { base, info, .. } => find(base).or_else(|| find(info)),
            NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => None,
        }
    }
    Some(match find(root)? {
        Grouping::By(labels) if labels.is_empty() => StateGroup::Single,
        _ => StateGroup::Grouped,
    })
}

/// How many owners the groups of a state input have to be split across.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StateGroup {
    /// One group: every row belongs to it, so one owner folds the answer.
    Single,
    /// More than one group: the rows can be split by their group's labels.
    Grouped,
}

fn validate_binding(
    tree: &NativeTree,
    binding: &FusedSeriesBinding,
    child_count: usize,
) -> DataFusionResult<()> {
    if binding.tags.iter().any(|tag| tag == METRIC_NAME) && binding.metric_name.is_none() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries declares {METRIC_NAME:?} but binds no metric name"
        )));
    }
    let schemas = collect_source_schemas(tree)?;
    // A tree built without its constructor carries no proof of its contracts, and one without an
    // input has no scan to refuse it later.
    if schemas.is_empty() {
        tree.root().validate()?;
    }
    if schemas.len() != child_count {
        return Err(DataFusionError::Plan(
            "FusedSeries source indexes must densely match its children".into(),
        ));
    }
    let root_grid = match &tree.root().output {
        NativeType::Samples(schema) => schema.eval_grid.as_ref(),
        NativeType::AggregateState(state) => Some(&state.grid),
        NativeType::Scalar(grid) => Some(grid),
    };
    if !root_grid.is_some_and(|grid| grid.spec() == binding.grid.spec()) {
        return Err(DataFusionError::Plan(
            "FusedSeries output is not on the statement grid".into(),
        ));
    }
    Ok(())
}

fn tree_inputs(tree: &NativeTree) -> Vec<&crate::native::plan::BoundInput> {
    fn collect<'a>(node: &'a NativeNode, inputs: &mut Vec<&'a crate::native::plan::BoundInput>) {
        match &node.expr {
            NativeExpr::Input(input) => inputs.push(input),
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {}
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::VectorOf { input } => collect(input, inputs),
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => {
                collect(input, inputs);
                for parameter in parameters {
                    collect(parameter, inputs);
                }
            }
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => {
                collect(left, inputs);
                collect(right, inputs);
            }
        }
    }
    let mut inputs = Vec::new();
    collect(tree.root(), &mut inputs);
    inputs
}

/// Sources whose evaluated series must be kept together for partitioned windows.
fn partitioned_window_sources(node: &NativeNode) -> DataFusionResult<Vec<usize>> {
    fn visit(node: &NativeNode, sources: &mut Vec<usize>) -> DataFusionResult<()> {
        match &node.expr {
            NativeExpr::Window { input, .. } => {
                let NativeExpr::Input(bound) = &input.expr else {
                    return Err(DataFusionError::Plan(
                        "partitioned FusedSeries windows require a direct Input under Window"
                            .into(),
                    ));
                };
                if !sources.contains(&bound.source_index) {
                    sources.push(bound.source_index);
                }
                visit(input, sources)?;
            }
            NativeExpr::Instant { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::VectorOf { input } => visit(input, sources)?,
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => {
                visit(input, sources)?;
                for parameter in parameters {
                    visit(parameter, sources)?;
                }
            }
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => {
                visit(left, sources)?;
                visit(right, sources)?;
            }
            NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => {}
        }
        Ok(())
    }
    let mut sources = Vec::new();
    visit(node, &mut sources)?;
    Ok(sources)
}

fn ordering_satisfies(producer: SeriesOrdering, required: SeriesOrdering) -> bool {
    match required {
        SeriesOrdering::KeyThenTime => producer == SeriesOrdering::KeyThenTime,
        SeriesOrdering::Contiguous => producer != SeriesOrdering::Interleaved,
        SeriesOrdering::Interleaved => true,
    }
}

fn same_fields(left: &SeriesSchema, right: &SeriesSchema) -> bool {
    left.fields.len() == right.fields.len()
        && left
            .fields
            .iter()
            .zip(right.fields.iter())
            .all(|(a, b)| a.name == b.name)
}

/// Fields read by each distinct raw source in source-index order.
pub fn tree_source_fields(tree: &NativeTree) -> DataFusionResult<Vec<Vec<String>>> {
    Ok(collect_source_schemas(tree)?
        .into_iter()
        .map(|schema| {
            schema
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect()
        })
        .collect())
}

/// Validates the tree's bound inputs and carries the root kind with their schemas.
///
/// The root kind must travel with the schemas because an evaluated sample schema and an
/// aggregate-state schema both have a grid, but require different physical frame decoders.
fn validate_physical_tree(
    tree: &NativeTree,
) -> DataFusionResult<(Vec<Arc<SeriesSchema>>, Vec<FusedInputKind>, FusedInputKind)> {
    let schemas = collect_source_schemas(tree)?;
    let mut source_kinds = Vec::new();
    for input in tree_inputs(tree) {
        let kind = FusedInputKind::from_native_type(&input.output)?;
        if source_kinds.len() <= input.source_index {
            source_kinds.resize(input.source_index + 1, kind);
        }
    }
    let root_kind = match &tree.root().output {
        NativeType::Scalar(_) => FusedInputKind::Evaluated,
        native_type => FusedInputKind::from_native_type(native_type)?,
    };
    match root_kind {
        FusedInputKind::Evaluated | FusedInputKind::AggregateState => {}
        FusedInputKind::Raw => {
            return Err(DataFusionError::Plan(
                "FusedSeries output is raw samples, not an evaluated or aggregate-state result"
                    .into(),
            ));
        }
    }
    Ok((schemas, source_kinds, root_kind))
}

/// Where one output column of [`FusedSeriesExec`] comes from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OutputColumn {
    /// The evaluation grid timestamps of the current chunk.
    Time,
    /// Series field by index in the bound series schema.
    Field(usize),
    /// Declared logical tag by index in the caller-supplied tag list.
    Tag(usize),
}

/// The caller-named value field used by the ordinary row conversion for a label-free scalar.
fn scalar_root_fields(
    output_schema: &SchemaRef,
    tags: &[String],
    time_column: &str,
) -> DataFusionResult<Vec<FieldSpec>> {
    if !tags.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeriesExec scalar root cannot produce {} tag columns",
            tags.len()
        )));
    }
    let mut values = output_schema
        .fields()
        .iter()
        .filter(|field| field.name().as_str() != time_column);
    let Some(value) = values.next() else {
        return Err(DataFusionError::Plan(
            "missing value column in scalar root output schema".to_string(),
        ));
    };
    if values.next().is_some() {
        return Err(DataFusionError::Plan(
            "scalar root output schema must contain exactly one value column".into(),
        ));
    }
    Ok(vec![FieldSpec {
        name: Arc::from(value.name().as_str()),
    }])
}

/// Classifies every output column, rejecting anything the conversion cannot produce.
pub(crate) fn classify_output(
    output_schema: &SchemaRef,
    series_fields: &[FieldSpec],
    tags: &[String],
    time_column: &str,
) -> DataFusionResult<Vec<OutputColumn>> {
    for (index, tag) in tags.iter().enumerate() {
        if tags[..index].contains(tag) {
            return Err(DataFusionError::Plan(format!(
                "duplicate tag column {tag:?} in FusedSeriesExec output schema"
            )));
        }
        if tag == time_column {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec cannot be the time column"
            )));
        }
        if series_fields.iter().any(|field| field.name.as_ref() == tag) {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec clashes with a series field"
            )));
        }
    }

    let mut time_seen = false;
    let mut field_seen = vec![false; series_fields.len()];
    let mut tag_seen = vec![false; tags.len()];
    let mut classification = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        let name = field.name();
        if name == time_column {
            if std::mem::replace(&mut time_seen, true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate time column {name:?} in FusedSeriesExec output schema"
                )));
            }
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, None) => {}
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "time column {name:?} of FusedSeriesExec must be Timestamp(Millisecond, \
                         None), got {other:?}"
                    )));
                }
            }
            if field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "time column {name:?} of FusedSeriesExec must not be nullable"
                )));
            }
            classification.push(OutputColumn::Time);
            continue;
        }
        if let Some(index) = series_fields
            .iter()
            .position(|field| field.name.as_ref() == name)
        {
            if std::mem::replace(&mut field_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate field column {name:?} in FusedSeriesExec output schema"
                )));
            }
            if field.data_type() != &DataType::Float64 {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be Float64, got {:?}",
                    field.data_type()
                )));
            }
            // A grid point inside the statement's interval can have no value in this field.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be nullable, because a grid \
                     point can be missing"
                )));
            }
            classification.push(OutputColumn::Field(index));
            continue;
        }
        if let Some(index) = tags.iter().position(|tag| tag == name) {
            if std::mem::replace(&mut tag_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate tag column {name:?} in FusedSeriesExec output schema"
                )));
            }
            if field.data_type() != &DataType::Utf8 {
                return Err(DataFusionError::Plan(format!(
                    "tag column {name:?} of FusedSeriesExec must be Utf8, got {:?}",
                    field.data_type()
                )));
            }
            // `rate` and the map kernels drop `__name__`, so a tag can be absent from a row.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "tag column {name:?} of FusedSeriesExec must be nullable, because range \
                     functions and map kernels drop the metric name"
                )));
            }
            classification.push(OutputColumn::Tag(index));
            continue;
        }
        return Err(DataFusionError::Plan(format!(
            "unknown column {name:?} in FusedSeriesExec output schema"
        )));
    }

    if !time_seen {
        return Err(DataFusionError::Plan(format!(
            "missing time column {time_column:?} in FusedSeriesExec output schema"
        )));
    }
    for (index, seen) in field_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing field column {:?} in FusedSeriesExec output schema",
                series_fields[index].name
            )));
        }
    }
    for (index, seen) in tag_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing tag column {:?} in FusedSeriesExec output schema",
                tags[index]
            )));
        }
    }
    Ok(classification)
}

/// Whether the tree has a binary with two vector operands that can produce a duplicate verdict.
fn native_tree_has_vector_binary(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Binary { left, right, .. } => {
            (!matches!(left.output, NativeType::Scalar(_))
                && !matches!(right.output, NativeType::Scalar(_)))
                || native_tree_has_vector_binary(left)
                || native_tree_has_vector_binary(right)
        }
        NativeExpr::VectorOf { input }
        | NativeExpr::Instant { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Aggregate { input, .. }
        | NativeExpr::Vector { input, .. } => native_tree_has_vector_binary(input),
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => {
            native_tree_has_vector_binary(input)
                || parameters.iter().any(native_tree_has_vector_binary)
        }
        NativeExpr::Set { left, right, .. } => {
            native_tree_has_vector_binary(left) || native_tree_has_vector_binary(right)
        }
        NativeExpr::Info { base, info, .. } => {
            native_tree_has_vector_binary(base) || native_tree_has_vector_binary(info)
        }
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => false,
    }
}

/// Evidence shared by the output partitions of this plan's single execution.
struct EvidenceClaims {
    evidence: Option<Arc<DuplicateEvidence>>,
    claimed: Vec<bool>,
}

fn claim_evidence(
    claims: &Mutex<EvidenceClaims>,
    partition: usize,
    partitions: usize,
    pool: &Arc<dyn MemoryPool>,
) -> DataFusionResult<Arc<DuplicateEvidence>> {
    let mut claims = claims.lock().unwrap_or_else(|e| e.into_inner());
    if claims.evidence.is_none() {
        claims.evidence = Some(Arc::new(DuplicateEvidence::new(pool, partitions)));
        claims.claimed = vec![false; partitions];
    }
    if claims.claimed.get(partition).copied().unwrap_or(false) {
        return Err(DataFusionError::Execution(
            "FusedSeries plan instance was executed more than once for the same partition".into(),
        ));
    }
    claims.claimed[partition] = true;
    Ok(Arc::clone(claims.evidence.as_ref().unwrap()))
}

/// The physical node that runs a compiled native tree over its input's raw series.
pub struct FusedSeriesExec {
    /// The PromQL text the tree was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The tree this node runs, shared by every partition's execution.
    tree: Arc<NativeTree>,
    /// Each source's compiled raw schema, used to decode only that source's frames.
    series_schemas: Vec<Arc<SeriesSchema>>,
    source_kinds: Vec<FusedInputKind>,
    /// Physical input plans, preserving each source's frame binding and ordering.
    inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The output column holding the grid timestamps, kept so this node can be rebuilt over a new
    /// child without losing which column its time came from.
    time_column: String,
    /// The tag column names, indexed by [`OutputColumn::Tag`].
    tags: Arc<[String]>,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// The evidence claimed by this plan's output partitions.
    duplicate_evidence: Mutex<EvidenceClaims>,
    /// Per-node metrics, registered by each execution of the tree.
    metric: ExecutionPlanMetricsSet,
    format: FusedOutputFormat,
    matching_parallelism: Option<usize>,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

fn preserves_partition_identity(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) => true,
        NativeExpr::Instant { input, op } => {
            op.value == InstantValue::Sample && preserves_partition_identity(input)
        }
        _ => false,
    }
}

/// Returns whether the scan's bound logical tags cover every user-visible primary-key column.
///
/// Internal metric-engine keys are not output labels. The per-series scan guarantees `__tsid` is
/// derived from user tags; within one logical table, distinct series are distinguished by their
/// user-visible primary-key columns. Requiring all of those columns prevents distinct stored series
/// from collapsing onto one output identity.
fn bound_tags_cover_primary_key(label_source: &RawLabelSource) -> bool {
    let RawLabelSource::PrimaryKey {
        physical_metadata,
        logical_tags,
        ..
    } = label_source
    else {
        return false;
    };
    let bound_ids = logical_tags
        .iter()
        .map(|tag| tag.column_id)
        .collect::<std::collections::HashSet<_>>();
    let table_id = store_api::storage::consts::ReservedColumnId::table_id();
    let tsid = store_api::storage::consts::ReservedColumnId::tsid();
    physical_metadata.primary_key_columns().all(|column| {
        column.column_id == table_id
            || column.column_id == tsid
            || bound_ids.contains(&column.column_id)
    })
}

impl FusedSeriesExec {
    /// Returns whether each output series identity is confined to one input partition.
    ///
    /// This is true only for a single per-series scan with every primary-key tag bound (or another
    /// proven fused chain) whose native tree consists solely of identity-preserving instant
    /// selections and inputs.
    pub fn has_partition_disjoint_identities(&self) -> bool {
        if self.inputs.len() != 1 || !preserves_partition_identity(self.tree.root()) {
            return false;
        }

        fn is_complete_series_input(plan: &dyn ExecutionPlan) -> bool {
            if let Some(scan) = plan.downcast_ref::<SeriesScanExec>() {
                return bound_tags_cover_primary_key(scan.label_source());
            }
            if let Some(cooperative) = plan.downcast_ref::<CooperativeExec>() {
                return cooperative
                    .children()
                    .first()
                    .is_some_and(|child| is_complete_series_input(child.as_ref()));
            }
            plan.downcast_ref::<FusedSeriesExec>()
                .is_some_and(FusedSeriesExec::has_partition_disjoint_identities)
        }

        is_complete_series_input(self.inputs[0].0.as_ref())
    }

    /// Creates the node, rejecting whatever this node cannot execute or convert.
    ///
    /// `input` produces native raw series frames: the series' encoded key, its finality, its
    /// timestamps and its fields, plus the complete labels when `label_source` is
    /// [`RawLabelSource::Explicit`]. `time_column` and `tags` name the columns of the output, which
    /// is a row table rather than a frame.
    ///
    /// The parameters stay separate because each is a fact its own caller has: the query, the plan
    /// below, the frame's label binding and ordering, the tree, and the three things the output
    /// schema is classified by. A struct over them would be the same list under another name.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        promql: Arc<str>,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        tree: Arc<NativeTree>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
    ) -> DataFusionResult<Self> {
        Self::build(
            promql,
            inputs,
            tree,
            output_schema,
            time_column,
            tags,
            FusedOutputFormat::PlainRows,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        promql: Arc<str>,
        inputs: Vec<(Arc<dyn ExecutionPlan>, RawLabelSource, SeriesOrdering)>,
        tree: Arc<NativeTree>,
        output_schema: SchemaRef,
        time_column: String,
        tags: Vec<String>,
        format: FusedOutputFormat,
        matching_parallelism: Option<usize>,
    ) -> DataFusionResult<Self> {
        let locality = input_locality(tree.root());
        let (raw_schemas, source_kinds, root_kind) = validate_physical_tree(&tree)?;
        if inputs.len() != raw_schemas.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries physical input count {} does not match its {} raw sources",
                inputs.len(),
                raw_schemas.len()
            )));
        }
        // Building a state proves the runtime executes this tree; drop the validation state now.
        let _ =
            NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()), None)?;
        let output_series = tree.root().output.samples();
        let scalar_root = matches!(tree.root().output, NativeType::Scalar(_));
        let partial_root = matches!(&tree.root().expr, NativeExpr::Aggregate { op, .. } if op.stage == AggregateStage::Partial);
        if partial_root && format != FusedOutputFormat::SeriesFrame {
            return Err(DataFusionError::Plan(
                "FusedSeries Partial stage requires SeriesFrame output".into(),
            ));
        }
        let frame_schema = if format == FusedOutputFormat::SeriesFrame {
            if inputs.iter().any(|(input, _, _)| {
                matches!(
                    input.properties().boundedness,
                    Boundedness::Unbounded { .. }
                )
            }) {
                return Err(DataFusionError::Plan(
                    "FusedSeries SeriesFrame output requires finite input to publish final series"
                        .into(),
                ));
            }
            let declared = output_frame_schema(&tree, root_kind)?;
            if declared != output_schema {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeriesExec schema does not match: declared {output_schema:?}, expected {declared:?}"
                )));
            }
            true
        } else {
            false
        };
        let classification = if partial_root || frame_schema {
            Vec::new()
        } else if scalar_root {
            let scalar_fields = scalar_root_fields(&output_schema, &tags, &time_column)?;
            classify_output(&output_schema, &scalar_fields, &tags, &time_column)?
        } else {
            classify_output(
                &output_schema,
                &output_series
                    .ok_or_else(|| {
                        DataFusionError::Plan("FusedSeries output is not samples".into())
                    })?
                    .fields,
                &tags,
                &time_column,
            )?
        };
        let partition_count = inputs.first().map_or(1, |(input, _, _)| {
            input.properties().partitioning.partition_count()
        });
        let window_sources = if raw_schemas.iter().any(|schema| schema.eval_grid.is_some())
            && inputs
                .iter()
                .any(|(input, _, _)| input.properties().partitioning.partition_count() > 1)
        {
            partitioned_window_sources(tree.root())?
                .into_iter()
                .filter(|&index| raw_schemas[index].eval_grid.is_some())
                .collect()
        } else {
            Vec::new()
        };
        for (index, (input, label_source, ordering)) in inputs.iter().enumerate() {
            let count = input.properties().partitioning.partition_count();
            if count == 0 {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries input {index} has no partitions"
                )));
            }
            if count != partition_count {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries input {index} has {count} partitions; every source must have the same positive count ({partition_count})"
                )));
            }
            // State is the one input a cross-series tree may read from more than one partition:
            // the router above the producers put every row of a group in exactly one of them, so
            // the `Final` aggregate folds that group's whole contribution where it arrives. Any
            // other global input has no such owner and has to be gathered.
            let state = source_kinds[index] == FusedInputKind::AggregateState;
            if matches!(locality, InputLocality::SinglePartition) && count != 1 && !state {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries global evaluated input {index} must have one partition"
                )));
            }
            if let InputLocality::MatchingLocal(policy) = &locality
                && count > 1
                && input
                    .downcast_ref::<SeriesRepartitionExec>()
                    .is_none_or(|router| {
                        router.outputs() != count
                            || router.routing_policy() != &SeriesRouting::Matching(policy.clone())
                    })
            {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries matching-local input {index} requires a direct matching-key SeriesRepartitionExec with {count} outputs"
                )));
            }
            if window_sources.contains(&index)
                && count > 1
                && input.downcast_ref::<SeriesRepartitionExec>().is_none()
            {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries partitioned window input {index} requires a direct SeriesRepartitionExec owner router"
                )));
            }
            let declaration = SeriesFrameDeclaration::parse(input.schema().as_ref())
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
            let actual_ordering = series_frame_ordering(input.schema().as_ref())
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
            if actual_ordering != *ordering {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries source {index} supplied ordering does not match its frame declaration"
                )));
            }
            if declaration.content() == FrameContent::Evaluated
                && actual_ordering != SeriesOrdering::Contiguous
            {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries evaluated source {index} must declare contiguous ordering"
                )));
            }
            let evaluated_global = raw_schemas[index].eval_grid.is_some()
                && matches!(locality, InputLocality::SinglePartition);
            let required_ordering = if evaluated_global || (inputs.len() > 1 && count > 1) {
                SeriesOrdering::Contiguous
            } else {
                *ordering
            };
            if tree_inputs(&tree)
                .iter()
                .filter(|node| node.source_index == index)
                .any(|node| !ordering_satisfies(required_ordering, node.ordering))
            {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries producer ordering for source {index} does not satisfy every input"
                )));
            }
            ensure_frame_matches_label_source(input.schema().as_ref(), label_source)?;
            let expected_content = match source_kinds[index] {
                FusedInputKind::Raw => FrameContent::Raw,
                FusedInputKind::Evaluated => FrameContent::Evaluated,
                FusedInputKind::AggregateState => FrameContent::AggregateState,
            };
            if declaration.content() != expected_content {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeries source {index} frame content does not match its compiled input"
                )));
            }
            let empty = || {
                Box::pin(RecordBatchStreamAdapter::new(
                    input.schema(),
                    futures::stream::empty(),
                ))
            };
            match source_kinds[index] {
                FusedInputKind::Evaluated => {
                    let _ = unpack_evaluated_series(empty(), Some(raw_schemas[index].clone()))?;
                }
                FusedInputKind::Raw => {
                    unpack_raw_series(empty(), Some(raw_schemas[index].clone()))?;
                }
                FusedInputKind::AggregateState => {
                    let bound = tree_inputs(&tree)
                        .into_iter()
                        .find(|bound| bound.source_index == index)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "FusedSeries state source {index} has no bound input"
                            ))
                        })?;
                    let NativeType::AggregateState(state) = &bound.output else {
                        return Err(DataFusionError::Plan(format!(
                            "FusedSeries source {index} is declared aggregate state but its binding is not"
                        )));
                    };
                    let _ = unpack_aggregate_state(
                        empty(),
                        Some(Arc::new(
                            datatypes::series::aggregate_state::AggregateStateSchema::try_new(
                                state.kind,
                                state.components.to_vec(),
                            )
                            .map_err(|error| DataFusionError::Plan(error.to_string()))?,
                        )),
                        Some(raw_schemas[index].clone()),
                    )?;
                }
            }
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            if partial_root || frame_schema {
                // Partial aggregate state and canonical frame series are emitted only after EOF.
                EmissionType::Final
            } else {
                // A frame's identities, and every other output, are emitted while the input is
                // still arriving, so the node's emission type is the input's.
                inputs.first().map_or(EmissionType::Final, |(input, _, _)| {
                    input.properties().emission_type
                })
            },
            inputs
                .first()
                .map_or(Boundedness::Bounded, |(input, _, _)| {
                    input.properties().boundedness
                }),
        ));
        Ok(Self {
            promql,
            tree,
            series_schemas: raw_schemas,
            source_kinds,
            inputs,
            classification: classification.into(),
            time_column,
            tags: tags.into(),
            output_schema,
            duplicate_evidence: Mutex::new(EvidenceClaims {
                evidence: None,
                claimed: Vec::new(),
            }),
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            format,
            matching_parallelism,
        })
    }

    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "FusedSeriesExec"
    }
}

impl std::fmt::Debug for FusedSeriesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FusedSeriesExec").finish()
    }
}

impl DisplayAs for FusedSeriesExec {
    /// The query, then the tree it compiles to: the tree is rendered by [`NativeTree`]'s own
    /// `Display`, so `EXPLAIN` and the node's metrics name the same nodes.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FusedSeriesExec: {}\n{}", self.promql, self.tree)?;
        if self.format == FusedOutputFormat::SeriesFrame {
            write!(f, "\noutput: series_frame")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for FusedSeriesExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// No optimizer exchange is requested here: evaluated-input partition counts and window owner
    /// routing are normalized explicitly while constructing the physical plan. Raw-input layout is
    /// left to its existing execution path.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(
            self.inputs
                .iter()
                .map(|(_, _, _)| Distribution::UnspecifiedDistribution)
                .collect(),
        )
    }

    /// Splitting the input further would not help: a second partition is a second set of series
    /// answered by a second tree, which this node already runs, and raising the count is what an
    /// optimizer does by adding a round-robin exchange that reorders nothing it can rely on.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.inputs.len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().map(|(input, _, _)| input).collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        // The tree's operands are bound inside it; this node owns no expression of its own.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != self.inputs.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec requires {} children, got {}",
                self.inputs.len(),
                children.len()
            )));
        }
        let children = children
            .into_iter()
            .zip(&self.inputs)
            .map(|(child, (_, label_source, ordering))| (child, label_source.clone(), *ordering))
            .collect();
        // Rebuilt rather than patched: the new child's schema is what the frame is validated
        // against, so a child that changed shape is refused instead of decoded with a stale layout.
        Ok(Arc::new(Self::build(
            Arc::clone(&self.promql),
            children,
            Arc::clone(&self.tree),
            self.output_schema.clone(),
            self.time_column.clone(),
            self.tags.to_vec(),
            self.format,
            self.matching_parallelism,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition >= self.properties.partitioning.partition_count() {
            return Err(DataFusionError::Execution(
                "FusedSeries output partition is out of range".into(),
            ));
        }
        let partitions = self.properties.partitioning.partition_count();
        let needs_evidence = partitions > 1 && native_tree_has_vector_binary(self.tree.root());
        let evidence = if needs_evidence {
            Some(claim_evidence(
                &self.duplicate_evidence,
                partition,
                partitions,
                &context.runtime_env().memory_pool,
            )?)
        } else {
            None
        };
        let mut claim = evidence.as_ref().map(|evidence| EvidenceClaim {
            evidence: Arc::clone(evidence),
            active: true,
        });
        let state = match NativeExecutionState::try_new(
            &self.tree,
            Arc::clone(&context.runtime_env().memory_pool),
            evidence.clone(),
        ) {
            Ok(state) => state,
            Err(error) => {
                if let Some(claim) = &mut claim {
                    claim.abort();
                }
                return Err(error);
            }
        };
        state.register_metrics(&self.metric, partition);
        let mut streams: Vec<futures::stream::BoxStream<'static, Result<FusedFeed, BoxedError>>> =
            Vec::new();
        let bound_inputs = tree_inputs(&self.tree);
        for (source, (input, _, _)) in self.inputs.iter().enumerate() {
            let frames = input.execute(partition, Arc::clone(&context))?;
            match self.source_kinds[source] {
                FusedInputKind::Evaluated => {
                    let decoded = unpack_evaluated_series(
                        frames,
                        Some(Arc::clone(&self.series_schemas[source])),
                    )?;
                    streams.push(
                        decoded
                            .map(move |item| {
                                item.map(|(series, is_last)| FusedFeed {
                                    source,
                                    chunk: FusedChunk::Evaluated(EvaluatedChunk {
                                        series,
                                        is_last,
                                    }),
                                })
                            })
                            .boxed(),
                    );
                }
                FusedInputKind::AggregateState => {
                    let bound = bound_inputs
                        .iter()
                        .find(|bound| bound.source_index == source)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "FusedSeries state source {source} has no bound input"
                            ))
                        })?;
                    let NativeType::AggregateState(binding) = bound.output.clone() else {
                        return Err(DataFusionError::Plan(format!(
                            "FusedSeries source {source} is declared aggregate state but its binding is not"
                        )));
                    };
                    let declared = Arc::new(
                        datatypes::series::aggregate_state::AggregateStateSchema::try_new(
                            binding.kind,
                            binding.components.to_vec(),
                        )
                        .map_err(|error| DataFusionError::Plan(error.to_string()))?,
                    );
                    let decoded = unpack_aggregate_state(
                        frames,
                        Some(declared.clone()),
                        Some(Arc::clone(&self.series_schemas[source])),
                    )?;
                    streams.push(
                        decoded
                            .map(move |item| match item {
                                Ok(row) => state_row_from_frame_row(row, &binding)
                                    .map(|row| FusedFeed {
                                        source,
                                        chunk: FusedChunk::State(StateChunk::Row(row)),
                                    })
                                    .map_err(|error| {
                                        BoxedError::new(common_query::error::Error::from(error))
                                    }),
                                Err(error) => Err(error),
                            })
                            .boxed(),
                    );
                }
                FusedInputKind::Raw => {
                    let raw =
                        unpack_raw_series(frames, Some(Arc::clone(&self.series_schemas[source])))?;
                    let raw = materialize_raw_labels(raw)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    streams.push(
                        raw.map(move |item| {
                            item.map(|chunk| FusedFeed {
                                source,
                                chunk: FusedChunk::Raw(chunk),
                            })
                        })
                        .boxed(),
                    );
                }
            }
        }
        let input_ids = (0..self.series_schemas.len())
            .map(|source| state.input_ids(source).collect())
            .collect();
        // One reading of the root decides both the mode and its binding, so the mode cannot
        // disagree with its binding: a Partial root without state output is a plan error.
        let state_binding = match (&self.tree.root().expr, &self.tree.root().output) {
            (NativeExpr::Aggregate { op, .. }, NativeType::AggregateState(binding))
                if op.stage == AggregateStage::Partial =>
            {
                Some(Arc::clone(binding))
            }
            (NativeExpr::Aggregate { op, .. }, _) if op.stage == AggregateStage::Partial => {
                return Err(DataFusionError::Plan(
                    "FusedSeries Partial root must produce aggregate state".into(),
                ));
            }
            _ => None,
        };
        let state_mode = state_binding.is_some();
        let state_schema = state_binding
            .as_ref()
            .map(|binding| {
                datatypes::series::aggregate_state::AggregateStateSchema::try_new(
                    binding.kind,
                    binding.components.to_vec(),
                )
                .map(Arc::new)
                .map_err(|error| DataFusionError::Plan(error.to_string()))
            })
            .transpose()?;
        let mut executor = NativeExecutor::new(state);
        let inputs = if streams.is_empty() {
            executor.end_of_input()?;
            None
        } else {
            Some(futures::stream::select_all(streams))
        };
        // A frame is encoded from the tree's own evaluated output, and only when the node answers
        // values rather than state: partial state is a third output the format does not reach.
        let frame_mode = self.format == FusedOutputFormat::SeriesFrame && !state_mode;
        let stream = FusedSeriesStream {
            inputs,
            executor: Some(executor),
            input_ids,
            classification: Arc::clone(&self.classification),
            tags: Arc::clone(&self.tags),
            output_schema: self.output_schema.clone(),
            state_mode,
            frame_mode,
            ready: VecDeque::new(),
            done: false,
            duplicate_evidence: evidence,
            evidence_completed: false,
            evidence_claim: claim,
        };
        if self.format == FusedOutputFormat::SeriesFrame {
            if state_mode {
                let binding = state_binding.as_ref().ok_or_else(|| {
                    DataFusionError::Plan(
                        "FusedSeries Partial root has no aggregate-state binding".into(),
                    )
                })?;
                let schema = Arc::new(SeriesSchema {
                    fields: binding.accumulators.clone(),
                    eval_grid: Some(Arc::clone(&binding.grid)),
                });
                let source: SendableStateSeriesStream = Box::pin(FusedStateStream {
                    source: stream,
                    pending: Vec::new().into_iter(),
                    reservation: None,
                });
                Ok(pack_aggregate_state(
                    source,
                    state_schema.ok_or_else(|| {
                        DataFusionError::Plan("FusedSeries Partial state schema is missing".into())
                    })?,
                    schema,
                )?)
            } else {
                let schema = evaluated_output(&self.tree)?;
                let source: SendableEvaluatedSeriesStream =
                    Box::pin(FusedEvaluatedStream { source: stream });
                Ok(pack_evaluated_series(source, schema)?)
            }
        } else {
            Ok(Box::pin(stream))
        }
    }
}

impl Drop for FusedSeriesStream {
    fn drop(&mut self) {
        if !self.evidence_completed
            && let Some(claim) = &mut self.evidence_claim
        {
            claim.abort();
        }
    }
}

/// Converts one evaluated chunk into one output batch.
///
/// The time column is the absolute grid positions the chunk carries, missing samples are nulls and
/// ordinary NaN stays a value.
pub(crate) fn series_to_record_batch(
    classification: &[OutputColumn],
    tags: &[String],
    output_schema: &SchemaRef,
    chunk: &EvaluatedChunk,
) -> DataFusionResult<RecordBatch> {
    let series = &chunk.series;
    // A chunk that forms one run is read from its range, which lends the grid's buffers and so
    // allocates nothing. A chunk whose rows form several runs has no single range to read
    // ([`EvaluatedChunk::range`] answers `None` for it), and the positions between its runs are
    // holes rather than rows: the axis is walked for its carried rows instead, which is the one
    // traversal that visits exactly those.
    let timestamps = match chunk.range() {
        Some(range) => range.timestamps(),
        None => series
            .time
            .timestamps()
            .map(|timestamps| ScalarBuffer::from(timestamps.into_owned())),
    }
    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let rows = timestamps.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(classification.len());
    for (position, column) in classification.iter().enumerate() {
        match column {
            OutputColumn::Time => columns.push(Arc::new(TimestampMillisecondArray::new(
                timestamps.clone(),
                None,
            ))),
            OutputColumn::Field(index) => {
                let samples = series.fields.get(*index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "evaluated series has no field at index {index}"
                    ))
                })?;
                if samples.len() != rows {
                    return Err(DataFusionError::Execution(format!(
                        "evaluated series field {index} has {} rows, time axis has {rows}",
                        samples.len()
                    )));
                }
                columns.push(field_column(samples)?);
            }
            OutputColumn::Tag(index) => {
                // `__name__` resolves to the metric name, every other tag through the label set.
                let name = tags.get(*index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "output column {position} declares unavailable tag {index}"
                    ))
                })?;
                let value = series.metric.get(name);
                columns.push(Arc::new(StringArray::from(vec![value; rows])));
            }
        }
    }
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}

/// Converts one evaluated series field into one `Float64` column.
///
/// Dense float samples are reused as they are: the array already carries exactly the right nulls
/// and keeps NaN a value, so copying it through `Samples::iter()` would only cost an allocation
/// and a re-encode. Chunked samples go through the iterator, and histograms have no float column.
fn field_column(samples: &Samples) -> DataFusionResult<ArrayRef> {
    match samples.dense() {
        Some(DenseSamplesRef::Float(values)) => Ok(Arc::new(values.clone())),
        Some(DenseSamplesRef::Histogram(_)) | Some(DenseSamplesRef::Mixed(_)) => {
            Err(DataFusionError::NotImplemented(
                "FusedSeriesExec cannot output histogram samples".into(),
            ))
        }
        None => {
            let values = samples
                .iter()
                .map(|sample| match sample {
                    SampleRef::Float(value) => Ok(Some(value)),
                    SampleRef::Missing => Ok(None),
                    SampleRef::Histogram(_) => Err(DataFusionError::NotImplemented(
                        "FusedSeriesExec cannot output histogram samples".into(),
                    )),
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
    }
}

/// One chunk a child handed over, in the form the child's frame declared it in.
#[derive(Debug)]
enum FusedChunk {
    /// Raw samples: one scan series, with its own timestamps.
    Raw(RawSeriesChunk),
    /// Values a producer already evaluated on the grid the frame declares.
    Evaluated(EvaluatedChunk),
    /// Aggregate state decoded from a declared state frame.
    State(StateChunk),
}

/// One chunk and the source index it belongs to.
struct FusedFeed {
    source: usize,
    chunk: FusedChunk,
}

/// The batches one execution of [`FusedSeriesExec`] yields.
pub struct FusedSeriesStream {
    /// Every input partition, read concurrently, or `None` once they all ended.
    ///
    /// Each decoder holds its current batch, decoded chunks and upstream source. Dropping this on
    /// error releases them even if the consumer keeps the terminated stream; a partition read on a
    /// spawned task is released once its runtime cancels the task, not by the time this returns.
    inputs: Option<SelectAll<futures::stream::BoxStream<'static, Result<FusedFeed, BoxedError>>>>,
    /// The execution of this node's tree, or `None` once it has been closed.
    executor: Option<NativeExecutor>,
    /// Runtime input IDs for each source index.
    input_ids: Vec<Vec<InputId>>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The tag column names, indexed by `OutputColumn::Tag`.
    tags: Arc<[String]>,
    /// The schema of every batch this stream yields.
    output_schema: SchemaRef,
    /// Whether the root exports aggregate-state chunks.
    state_mode: bool,
    /// Whether output is a declared SeriesFrame rather than rows.
    frame_mode: bool,
    /// The batches the executor settled since the last poll of the input.
    ready: VecDeque<RecordBatch>,
    /// Whether the input ended and everything it settled has been handed out.
    done: bool,
    duplicate_evidence: Option<Arc<DuplicateEvidence>>,
    evidence_completed: bool,
    evidence_claim: Option<EvidenceClaim>,
}

struct EvidenceClaim {
    evidence: Arc<DuplicateEvidence>,
    active: bool,
}

impl EvidenceClaim {
    fn abort(&mut self) {
        if self.active {
            self.evidence.abort();
            self.active = false;
        }
    }
    fn complete(&mut self) -> bool {
        if self.active {
            self.active = false;
            self.evidence.complete()
        } else {
            false
        }
    }
}

impl Drop for EvidenceClaim {
    fn drop(&mut self) {
        self.abort();
    }
}

impl FusedSeriesStream {
    /// Hands one raw chunk over and queues what it settled.
    fn push(&mut self, feed: FusedFeed) -> DataFusionResult<()> {
        let Some(executor) = self.executor.as_mut() else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec stream pushed a chunk after its execution closed".to_string(),
            ));
        };
        let input_ids = self.input_ids.get(feed.source).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "FusedSeries has no runtime input IDs for source {}",
                feed.source
            ))
        })?;
        let last = input_ids.len().saturating_sub(1);
        let mut chunk = Some(feed.chunk);
        for (index, input_id) in input_ids.iter().enumerate() {
            let Some(owned) = (index == last).then(|| chunk.take().unwrap()) else {
                // One source feeding several runtime inputs hands each the same value. State read
                // from a frame arrives as one row per feed, so it is copied; a producer's batch
                // carries the charge for its whole payload and cannot be shared, and no tree binds
                // one such source twice.
                match chunk.as_ref().expect("the feed is consumed once") {
                    FusedChunk::Raw(value) => {
                        executor.push_raw(*input_id, value.clone())?;
                    }
                    FusedChunk::Evaluated(value) => {
                        executor.push_evaluated(*input_id, value.clone())?;
                    }
                    FusedChunk::State(StateChunk::Row(row)) => {
                        executor.push_state(*input_id, StateChunk::Row(row.clone()))?;
                    }
                    FusedChunk::State(StateChunk::Series { .. }) => {
                        return Err(DataFusionError::Execution(
                            "one producer state batch cannot feed several runtime inputs"
                                .to_string(),
                        ));
                    }
                }
                continue;
            };
            match owned {
                FusedChunk::Raw(chunk) => executor.push_raw(*input_id, chunk)?,
                FusedChunk::Evaluated(chunk) => executor.push_evaluated(*input_id, chunk)?,
                FusedChunk::State(chunk) => executor.push_state(*input_id, chunk)?,
            }
        }
        if self.frame_mode || self.state_mode {
            Ok(())
        } else {
            let mut settled = Vec::new();
            while let Some(chunk) = executor.next_chunk()? {
                settled.push(chunk);
            }
            for chunk in settled {
                self.convert_rows(chunk)?;
            }
            Ok(())
        }
    }

    /// Ends the input, so the tree settles what it was holding.
    ///
    /// `finish()` would be the same thing plus a drain, but the drain collects: it holds the whole
    /// answer in a `Vec` before anything is handed out, so a tree that answers many chunks after its
    /// input ends would sit on all of them while the consumer waits for the first. Only the input is
    /// ended here, and the answers are pulled one per call below, so each chunk leaves as the tree
    /// settles it and what has not been pulled yet stays charged to the node holding it.
    fn close_input(&mut self) -> DataFusionResult<()> {
        match self.executor.as_mut() {
            Some(executor) => executor.end_of_input(),
            // Every answer was pulled and the execution closed itself.
            None => Ok(()),
        }
    }

    /// Pulls the next answer the tree settles after its input ended, or `None` when it has none.
    ///
    /// One chunk per call is what keeps this a stream: collecting them first would hold the whole
    /// answer while the consumer waits for the first, and would charge the pool for all of it at
    /// once. `None` is also when the execution closes and returns its reservations, so it is
    /// dropped here rather than staying alive until the stream is.
    fn pull_one(&mut self) -> DataFusionResult<Option<EvaluatedChunk>> {
        if self.state_mode {
            return Ok(None);
        }
        let Some(executor) = self.executor.as_mut() else {
            return Ok(None);
        };
        match executor.next_chunk() {
            Ok(Some(chunk)) => Ok(Some(chunk)),
            Ok(None) => {
                self.executor = None;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Ends the stream in an error, releasing the execution and the inputs.
    ///
    /// An ordinary drop of the stream releases all of this as well, so this is not about the last
    /// release: it is about a consumer that keeps the terminated stream, which would otherwise
    /// hold the evaluator, the decoder's held batch and chunks, and the plan upstream of it.
    fn fail(&mut self) {
        self.done = true;
        if !self.evidence_completed {
            if let Some(claim) = &mut self.evidence_claim {
                claim.abort();
            }
            self.evidence_completed = true;
        }
        self.executor = None;
        self.inputs = None;
        // This reservation belongs only to this execution. Release it even if the consumer keeps
        // the failed stream and the plan alive.
        // A batch queued before the error was settled by a run that has now stopped.
        self.ready.clear();
    }

    /// Converts one settled chunk into a row batch. Evaluated frames leave this driver as chunks.
    fn convert_rows(&mut self, chunk: EvaluatedChunk) -> DataFusionResult<()> {
        let batch = series_to_record_batch(
            &self.classification,
            &self.tags,
            &self.output_schema,
            &chunk,
        )?;
        self.ready.push_back(batch);
        Ok(())
    }
}

enum FusedOutput {
    Rows(RecordBatch),
    Evaluated(EvaluatedChunk),
    /// Aggregate state decoded from a declared state frame.
    State(StateChunk),
}

impl FusedSeriesStream {
    fn poll_output(&mut self, cx: &mut Context<'_>) -> Poll<Option<DataFusionResult<FusedOutput>>> {
        for _ in 0..POLL_BUDGET {
            if let Some(batch) = self.ready.pop_front() {
                return Poll::Ready(Some(Ok(FusedOutput::Rows(batch))));
            }
            if self.state_mode && !self.done {
                match self
                    .executor
                    .as_mut()
                    .map_or(Ok(None), NativeExecutor::next_state)
                {
                    Ok(Some(chunk)) => return Poll::Ready(Some(Ok(FusedOutput::State(chunk)))),
                    Ok(None) if self.inputs.is_none() => self.executor = None,
                    Ok(None) => {}
                    Err(error) => {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
            if self.frame_mode && !self.done {
                match self
                    .executor
                    .as_mut()
                    .map_or(Ok(None), NativeExecutor::next_chunk)
                {
                    Ok(Some(chunk)) => return Poll::Ready(Some(Ok(FusedOutput::Evaluated(chunk)))),
                    Ok(None) if self.inputs.is_none() => self.executor = None,
                    Ok(None) => {}
                    Err(error) => {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
            if self.done {
                return Poll::Ready(None);
            }
            let Some(inputs) = self.inputs.as_mut() else {
                match self.pull_one() {
                    Ok(Some(chunk)) if self.frame_mode => {
                        return Poll::Ready(Some(Ok(FusedOutput::Evaluated(chunk))));
                    }
                    Ok(Some(chunk)) => {
                        if let Err(error) = self.convert_rows(chunk) {
                            self.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    Ok(None) => {
                        self.done = true;
                        if self
                            .evidence_claim
                            .as_mut()
                            .is_some_and(EvidenceClaim::complete)
                            && let Some(candidate) = self
                                .duplicate_evidence
                                .as_ref()
                                .and_then(|evidence| evidence.take_verdict())
                        {
                            self.fail();
                            return Poll::Ready(Some(Err(DataFusionError::Execution(
                                candidate.message(),
                            ))));
                        }
                        self.evidence_completed = true;
                        continue;
                    }
                    Err(error) => {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                continue;
            };
            match Pin::new(inputs).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.inputs = None;
                    if let Err(error) = self.close_input() {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    self.fail();
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                }
                Poll::Ready(Some(Ok(feed))) => {
                    if let Err(error) = self.push(feed) {
                        self.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl RecordBatchStream for FusedSeriesStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for FusedSeriesStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.poll_output(cx) {
            Poll::Ready(Some(Ok(FusedOutput::Rows(batch)))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Ok(FusedOutput::Evaluated(_) | FusedOutput::State(_)))) => {
                self.fail();
                Poll::Ready(Some(Err(DataFusionError::Internal(
                    "evaluated chunk reached the row stream adapter".into(),
                ))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct FusedEvaluatedStream {
    source: FusedSeriesStream,
}

impl Stream for FusedEvaluatedStream {
    type Item = Result<(datatypes::series::samples::Series, bool), common_error::ext::BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.source).poll_output(cx) {
            Poll::Ready(Some(Ok(FusedOutput::Evaluated(chunk)))) => {
                Poll::Ready(Some(Ok((chunk.series, chunk.is_last))))
            }
            Poll::Ready(Some(Ok(FusedOutput::Rows(_) | FusedOutput::State(_)))) => {
                Pin::new(&mut self.source).fail();
                Poll::Ready(Some(Err(common_error::ext::BoxedError::new(
                    common_query::error::Error::from(DataFusionError::Internal(
                        "row batch reached the evaluated stream adapter".into(),
                    )),
                ))))
            }
            Poll::Ready(Some(Err(error))) => {
                Pin::new(&mut self.source).fail();
                Poll::Ready(Some(Err(common_error::ext::BoxedError::new(
                    common_query::error::Error::from(error),
                ))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct FusedStateStream {
    source: FusedSeriesStream,
    /// The series of the one producer chunk being handed out.
    pending: std::vec::IntoIter<datatypes::series::aggregate_state::StateSeries>,
    /// What that chunk still holds, released once its last series has been handed out.
    reservation: Option<MemoryReservation>,
}

impl FusedStateStream {
    /// Drops what this stream still held and ends it: the packer above reads nothing more.
    fn finish(&mut self) {
        self.pending = Vec::new().into_iter();
        self.reservation = None;
    }
}

impl Stream for FusedStateStream {
    type Item = Result<(datatypes::series::aggregate_state::StateSeries, bool), BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(series) = self.pending.next() {
                // is_last closes this producer's partial chunk chain; Final still merges every
                // producer's contribution before completing a group.
                return Poll::Ready(Some(Ok((series, true))));
            }
            // Every series of that chunk has been handed out, so its charge and the buffer those
            // series came from both go back before the next chunk is read. Holding the charge until
            // this point is what keeps the last series' payload accounted for while the packer
            // still reads it; dropping the exhausted iterator here keeps its spare capacity from
            // outliving the charge that covered it.
            self.reservation = None;
            self.pending = Vec::new().into_iter();
            match Pin::new(&mut self.source).poll_output(cx) {
                Poll::Ready(Some(Ok(FusedOutput::State(chunk)))) => {
                    let StateChunk::Series {
                        series,
                        reservation,
                        ..
                    } = chunk
                    else {
                        Pin::new(&mut self.source).fail();
                        self.finish();
                        return Poll::Ready(Some(Err(BoxedError::new(
                            common_query::error::Error::from(DataFusionError::Internal(
                                "row state reached state export stream".into(),
                            )),
                        ))));
                    };
                    self.pending = series.into_iter();
                    self.reservation = reservation;
                }
                Poll::Ready(Some(Ok(_))) => {
                    Pin::new(&mut self.source).fail();
                    self.finish();
                    return Poll::Ready(Some(Err(BoxedError::new(
                        common_query::error::Error::from(DataFusionError::Internal(
                            "non-state output reached state stream".into(),
                        )),
                    ))));
                }
                Poll::Ready(Some(Err(error))) => {
                    self.finish();
                    return Poll::Ready(Some(Err(BoxedError::new(
                        common_query::error::Error::from(error),
                    ))));
                }
                Poll::Ready(None) => {
                    self.finish();
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
