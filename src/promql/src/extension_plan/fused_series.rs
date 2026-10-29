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
    SendableEvaluatedSeriesStream, ensure_frame_matches_label_source, pack_evaluated_series,
    series_frame_ordering, unpack_evaluated_series, unpack_raw_series,
};
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation, UnboundedMemoryPool};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::frame::{FrameContent, SeriesFrameDeclaration};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{DenseSamplesRef, FieldSpec, SampleRef, Samples, SeriesSchema};
use datatypes::series::time::EvalGrid;
use futures::stream::SelectAll;
use futures::{Stream, StreamExt};
use store_api::region_engine::RawLabelSource;

use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::SeriesRouting;
use crate::extension_plan::aggregate_exchange::{PartialFold, aggregate_op, state_schema};
use crate::extension_plan::series_repartition::SeriesRepartitionExec;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor, operand_chunk_bytes};
use crate::native::plan::{
    AggregateKind, BoundAggregate, Grouping, NativeExpr, NativeNode, NativeTree, NativeType,
    reads_across_series,
};
use crate::source::materialize_raw_labels;
use crate::vector_match::MatchingLabels;

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
    /// The aggregate whose partition-local state this node exports instead of query rows.
    ///
    /// This row-state bridge is temporary: native final aggregation will consume series frames
    /// directly, removing state-row encoding and reconstruction.
    partial_fold: Option<BoundAggregate>,
    /// Fixed output representation; partial state export always remains PlainRows.
    format: FusedOutputFormat,
}

impl FusedSeries {
    /// Serializes the native tree and the binding required to reconstruct this plan node.
    /// The PromQL text is included for diagnostics only and never drives execution; children
    /// remain ordinary Substrait inputs. Version 1 supports only SeriesFrame without partial folds:
    /// decoding must rebuild its output schema, and only SeriesFrame derives that schema from the tree.
    pub fn serialize(&self) -> DataFusionResult<Vec<u8>> {
        if self.format != FusedOutputFormat::SeriesFrame {
            return Err(DataFusionError::NotImplemented(
                "FusedSeries serialization does not support PlainRows output schema".into(),
            ));
        }
        if self.partial_fold.is_some() {
            return Err(DataFusionError::NotImplemented(
                "FusedSeries serialization does not support partial_fold".into(),
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
        out.extend(1u32.to_le_bytes());
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
                Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
            }
            fn i64(&mut self) -> DataFusionResult<i64> {
                Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
            fn u64(&mut self) -> DataFusionResult<u64> {
                Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
            }
            fn text(&mut self) -> DataFusionResult<String> {
                let n = self.u32()? as usize;
                String::from_utf8(self.take(n)?.to_vec())
                    .map_err(|e| DataFusionError::Plan(format!("invalid FusedSeries UTF-8: {e}")))
            }
        }
        let mut r = Reader { b: bytes, p: 0 };
        let version = r.u32()?;
        if version != 1 {
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
            let evaluated = evaluated_output(&tree)?;
            Arc::new(datafusion::common::DFSchema::try_from(
                series_frame_output_schema(evaluated.as_ref())?,
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
        if format == FusedOutputFormat::SeriesFrame {
            let expected = series_frame_output_schema(evaluated_output(&tree)?.as_ref())?;
            if output_schema.as_arrow() != expected.as_ref() {
                return Err(DataFusionError::Plan(
                    "FusedSeries SeriesFrame output schema does not match the evaluated frame layout"
                        .into(),
                ));
            }
        }
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
            partial_fold: None,
            format,
        })
    }

    /// Creates a one-source node that exports partition-local state for a complete aggregate.
    pub(crate) fn try_new_partial(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        children: Vec<LogicalPlan>,
        op: BoundAggregate,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding, children.len())?;
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "FusedSeries partial fold requires one raw child".into(),
            ));
        }
        if reads_across_series(tree.root()) {
            return Err(DataFusionError::Plan(
                "FusedSeries partial fold prefix reads across series".into(),
            ));
        }
        aggregate_op(&op)?;
        let output_schema = Arc::new(datafusion::common::DFSchema::try_from(state_schema(
            &binding.tags,
        ))?);
        Ok(Self {
            promql,
            tree,
            binding,
            children,
            output_schema,
            partial_fold: Some(op),
            format: FusedOutputFormat::PlainRows,
        })
    }

    /// The aggregate whose partition-local state this node exports.
    pub fn partial_fold(&self) -> Option<&BoundAggregate> {
        self.partial_fold.as_ref()
    }

    /// The PromQL text this node was compiled from.
    pub fn promql(&self) -> &str {
        &self.promql
    }

    /// The selected output representation.
    pub fn format(&self) -> FusedOutputFormat {
        self.format
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
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        if inputs.len() != self.children.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries physical input count {} does not match its {} logical children",
                inputs.len(),
                self.children.len()
            )));
        }
        let tags = if self.partial_fold.is_some() {
            self.binding.tags.clone()
        } else {
            self.output_tags()
        };
        let evaluated = inputs.iter().any(|(input, _, _)| {
            SeriesFrameDeclaration::parse(input.schema().as_ref())
                .is_ok_and(|declaration| declaration.content() == FrameContent::Evaluated)
        });
        let locality = input_locality(self.tree.root());
        let normalize = matches!(locality, InputLocality::SinglePartition);
        let window_sources = if evaluated
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
                if declaration.content() != FrameContent::Evaluated {
                    // A global tree needs one partition whatever the input carries. A raw frame is
                    // routed by its keys, and with one output every key lands in the same place, so
                    // the same node gathers it just as it does for an evaluated frame.
                    if normalize && input.properties().partitioning.partition_count() != 1 {
                        let input = Arc::new(SeriesRepartitionExec::try_new(input, 1)?)
                            as Arc<dyn ExecutionPlan>;
                        return Ok((input, label_source, SeriesOrdering::Contiguous));
                    }
                    return Ok((input, label_source, ordering));
                }
                let count = input.properties().partitioning.partition_count();
                if count == 0 {
                    return Err(DataFusionError::Plan(format!(
                        "FusedSeries input {index} has no partitions"
                    )));
                }
                let required = if normalize { 1 } else { count };
                let needs_owner = window_sources.contains(&index);
                let contiguous = actual_ordering == SeriesOrdering::Contiguous;
                let matching_policy = match &locality {
                    InputLocality::MatchingLocal(policy) => Some(policy.clone()),
                    _ => None,
                };
                let has_router =
                    input
                        .downcast_ref::<SeriesRepartitionExec>()
                        .is_some_and(|router| {
                            router.outputs() == count
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
                } else if let Some(policy) = matching_policy
                    && count > 1
                    && !has_router
                {
                    // Either there is no router yet or the one there routes by another key. The
                    // second case re-routes what the first produced: a router's output is plain
                    // partitions, so another one above it is a legitimate shuffle rather than a
                    // no-op, and refusing the query would be worse than paying for the move.
                    let input = Arc::new(SeriesRepartitionExec::try_new_with_routing(
                        input,
                        count,
                        SeriesRouting::Matching(policy),
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
            self.partial_fold.clone(),
            self.format,
        )?))
    }

    /// The label columns of the output: every column that is neither the time index nor a field of
    /// the root. A scalar root has no labels; its one other column is the value.
    ///
    /// The planner decides them from the whole tree - a match narrows them, `or` joins both sides'
    /// and a wrapper around either keeps them - and the output schema is where it records that.
    fn output_tags(&self) -> Vec<String> {
        let Some(root) = self.tree.root().output.samples() else {
            return Vec::new();
        };
        self.output_schema
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
            .collect()
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
        match &self.partial_fold {
            Some(op) => write!(
                f,
                "FusedSeries: {} [partial {}]",
                self.promql,
                render_partial_fold(op)
            ),
            None if self.format == FusedOutputFormat::SeriesFrame => {
                write!(f, "FusedSeries: {} (output: series_frame)", self.promql)
            }
            None => write!(f, "FusedSeries: {}", self.promql),
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
            partial_fold: self.partial_fold.clone(),
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
            && partial_key(self.partial_fold.as_ref()) == partial_key(other.partial_fold.as_ref())
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
        partial_key(self.partial_fold.as_ref()).hash(state);
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
        match partial_key(self.partial_fold.as_ref())
            .partial_cmp(&partial_key(other.partial_fold.as_ref()))
        {
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

/// Renders a partial aggregate with the stable names used by the exchange plan.
fn render_partial_fold(op: &BoundAggregate) -> String {
    let group = match &op.group {
        Grouping::By(labels) => format!("by ({})", labels.join(", ")),
        Grouping::Without(labels) => format!("without ({})", labels.join(", ")),
    };
    let kind = match op.kind {
        AggregateKind::Quantile { quantile } => format!("quantile({quantile})"),
        kind => format!("{kind:?}").to_lowercase(),
    };
    format!("{kind} {group} {:?}", op.stage)
}

/// A stable comparable representation for an optional partial fold.
fn partial_key(partial: Option<&BoundAggregate>) -> Option<String> {
    partial.map(render_partial_fold)
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
    let mut schemas: Vec<(usize, Arc<SeriesSchema>)> = Vec::new();
    for input in tree_inputs(tree) {
        let NativeType::Samples(found) = &input.output else {
            return Err(DataFusionError::Plan(
                "FusedSeries input is not samples".into(),
            ));
        };
        if let Some((_, expected)) = schemas
            .iter()
            .find(|(index, _)| *index == input.source_index)
        {
            let same_grid = match (&expected.eval_grid, &found.eval_grid) {
                (None, None) => true,
                (Some(left), Some(right)) => left.spec() == right.spec(),
                _ => false,
            };
            if !same_fields(expected, found) || !same_grid {
                return Err(DataFusionError::Plan(
                    "FusedSeries repeated source has different fields or evaluation grid".into(),
                ));
            }
        } else {
            schemas.push((input.source_index, Arc::clone(found)));
        }
    }
    schemas.sort_unstable_by_key(|(index, _)| *index);
    if schemas
        .iter()
        .enumerate()
        .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(DataFusionError::Plan(
            "FusedSeries source indexes are not dense".into(),
        ));
    }
    Ok(schemas.into_iter().map(|(_, schema)| schema).collect())
}

#[derive(Clone, Debug)]
enum InputLocality {
    PartitionLocal,
    MatchingLocal(MatchingLabels),
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

fn input_locality(root: &NativeNode) -> InputLocality {
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
        if raw || !needs_vector_matching(root) {
            return InputLocality::PartitionLocal;
        }
        return InputLocality::MatchingLocal(MatchingLabels::Ignoring(Vec::new()));
    }
    // A match between two different sources needs its match keys co-located, wherever the match
    // sits under the pointwise nodes above it.
    if let Some((left, right, labels)) = matching_site(root)
        && let (Some(left_source), Some(right_source)) = (same_source(left), same_source(right))
        && left_source != right_source
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
    if !tree
        .root()
        .output
        .grid()
        .is_some_and(|grid| grid.spec() == binding.grid.spec())
    {
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
pub(crate) fn tree_source_fields(tree: &NativeTree) -> DataFusionResult<Vec<Vec<String>>> {
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

fn validate_physical_tree(tree: &NativeTree) -> DataFusionResult<Vec<Arc<SeriesSchema>>> {
    let schemas = collect_source_schemas(tree)?;
    if tree.root().output.grid().is_none() {
        return Err(DataFusionError::Plan(
            "FusedSeries output is not evaluated samples".into(),
        ));
    }
    Ok(schemas)
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

/// The physical node that runs a compiled native tree over its input's raw series.
pub struct FusedSeriesExec {
    /// The PromQL text the tree was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The tree this node runs, shared by every partition's execution.
    tree: Arc<NativeTree>,
    /// Each source's compiled raw schema, used to decode only that source's frames.
    series_schemas: Vec<Arc<SeriesSchema>>,
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
    /// Per-node metrics, registered by each execution of the tree.
    metric: ExecutionPlanMetricsSet,
    /// Aggregate state exported by each partition, when this is the partial mode.
    partial_fold: Option<BoundAggregate>,
    format: FusedOutputFormat,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

impl FusedSeriesExec {
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
            None,
            FusedOutputFormat::PlainRows,
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
        partial_fold: Option<BoundAggregate>,
        format: FusedOutputFormat,
    ) -> DataFusionResult<Self> {
        let locality = input_locality(tree.root());
        let raw_schemas = validate_physical_tree(&tree)?;
        if inputs.len() != raw_schemas.len() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeries physical input count {} does not match its {} raw sources",
                inputs.len(),
                raw_schemas.len()
            )));
        }
        // Building a state proves the runtime executes this tree; drop the validation state now.
        let _ = NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()))?;
        let output_series = tree.root().output.samples();
        let scalar_root = matches!(tree.root().output, NativeType::Scalar(_));
        if let Some(op) = &partial_fold {
            aggregate_op(op)?;
        }
        if partial_fold.is_some() && (inputs.len() != 1 || reads_across_series(tree.root())) {
            return Err(DataFusionError::Plan(
                "FusedSeries partial fold requires one raw source and a per-series prefix".into(),
            ));
        }
        let frame_schema = if partial_fold.is_none() && format == FusedOutputFormat::SeriesFrame {
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
            let declared = series_frame_output_schema(evaluated_output(&tree)?.as_ref())?;
            if declared != output_schema {
                return Err(DataFusionError::Plan(
                    "FusedSeriesExec output schema does not match the evaluated frame layout"
                        .into(),
                ));
            }
            true
        } else {
            false
        };
        let classification = if partial_fold.is_some() || frame_schema {
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
        let has_evaluated_input = raw_schemas.iter().any(|schema| schema.eval_grid.is_some());
        let has_raw_input = raw_schemas.iter().any(|schema| schema.eval_grid.is_none());
        if has_evaluated_input && has_raw_input {
            return Err(DataFusionError::Plan(
                "FusedSeries cannot mix raw and evaluated inputs".into(),
            ));
        }
        let evaluated_global =
            has_evaluated_input && matches!(locality, InputLocality::SinglePartition);
        let window_sources = if has_evaluated_input
            && inputs
                .iter()
                .any(|(input, _, _)| input.properties().partitioning.partition_count() > 1)
        {
            partitioned_window_sources(tree.root())?
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
            if matches!(locality, InputLocality::SinglePartition) && count != 1 {
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
            let expected_content = if raw_schemas[index].eval_grid.is_some() {
                FrameContent::Evaluated
            } else {
                FrameContent::Raw
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
            if expected_content == FrameContent::Evaluated {
                let _ = unpack_evaluated_series(empty(), Some(raw_schemas[index].clone()))?;
            } else {
                unpack_raw_series(empty(), Some(raw_schemas[index].clone()))?;
            }
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            if partial_fold.is_some() || frame_schema {
                // Partial aggregate state and canonical frame series are emitted only after EOF.
                EmissionType::Final
            } else {
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
            inputs,
            classification: classification.into(),
            time_column,
            tags: tags.into(),
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            partial_fold,
            format,
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
        if let Some(op) = &self.partial_fold {
            write!(f, "\npartial state: {}", render_partial_fold(op))?;
        } else if self.format == FusedOutputFormat::SeriesFrame {
            write!(f, "\noutput: series_frame")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for FusedSeriesExec {
    fn name(&self) -> &str {
        Self::name()
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
            self.partial_fold.clone(),
            self.format,
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
        let state = NativeExecutionState::try_new(
            &self.tree,
            Arc::clone(&context.runtime_env().memory_pool),
        )?;
        state.register_metrics(&self.metric, partition);
        let mut streams: Vec<futures::stream::BoxStream<'static, Result<FusedFeed, BoxedError>>> =
            Vec::new();
        for (source, (input, _, _)) in self.inputs.iter().enumerate() {
            let frames = input.execute(partition, Arc::clone(&context))?;
            if self.series_schemas[source].eval_grid.is_some() {
                let decoded = unpack_evaluated_series(
                    frames,
                    Some(Arc::clone(&self.series_schemas[source])),
                )?;
                streams.push(
                    decoded
                        .map(move |item| {
                            item.map(|(series, is_last)| FusedFeed {
                                source,
                                chunk: FusedChunk::Evaluated(EvaluatedChunk { series, is_last }),
                            })
                        })
                        .boxed(),
                );
            } else {
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
        let input_ids = (0..self.series_schemas.len())
            .map(|source| state.input_ids(source).collect())
            .collect();
        let export_budget = Arc::new(Mutex::new(None));
        let partial_fold = self
            .partial_fold
            .clone()
            .map(|op| {
                PartialFold::try_new(
                    op,
                    self.tree
                        .root()
                        .output
                        .samples()
                        .ok_or_else(|| {
                            DataFusionError::Plan("FusedSeries output is not samples".into())
                        })?
                        .clone(),
                    Arc::clone(&self.tags),
                    Arc::clone(&export_budget),
                    &context.runtime_env().memory_pool,
                )
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
        let frame_mode =
            self.partial_fold.is_none() && self.format == FusedOutputFormat::SeriesFrame;
        let stream = FusedSeriesStream {
            inputs,
            executor: Some(executor),
            input_ids,
            classification: Arc::clone(&self.classification),
            tags: Arc::clone(&self.tags),
            output_schema: self.output_schema.clone(),
            partial_fold,
            frame_mode,
            _export_budget: Arc::clone(&export_budget),
            ready: VecDeque::new(),
            done: false,
        };
        if frame_mode {
            let schema = evaluated_output(&self.tree)?;
            let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
                Arc::clone(&context.runtime_env().memory_pool);
            let reservation =
                MemoryConsumer::new(format!("FusedSeriesFrame[{partition}]")).register(&pool);
            let handed = Arc::new(Mutex::new(Vec::new()));
            let source: SendableEvaluatedSeriesStream = Box::pin(FusedEvaluatedStream::new(
                stream,
                Arc::clone(&schema),
                Arc::clone(&pool),
                reservation,
                Arc::clone(&handed),
                preserves_identity(self.tree.root()),
            ));
            Ok(Box::pin(FusedPackedStream {
                inner: pack_evaluated_series(source, schema)?,
                handed,
            }))
        } else {
            Ok(Box::pin(stream))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
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
enum FusedChunk {
    /// Raw samples: one scan series, with its own timestamps.
    Raw(RawSeriesChunk),
    /// Values a producer already evaluated on the grid the frame declares.
    Evaluated(EvaluatedChunk),
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
    /// Per-partition aggregate state fold, if this stream exports a partial result.
    partial_fold: Option<PartialFold>,
    /// Whether evaluated chunks are emitted as series frames rather than converted to rows.
    frame_mode: bool,
    /// Keeps exported state batches charged for as long as this stream holds them.
    _export_budget: Arc<Mutex<Option<MemoryReservation>>>,
    /// The batches the executor settled since the last poll of the input.
    ready: VecDeque<RecordBatch>,
    /// Whether the input ended and everything it settled has been handed out.
    done: bool,
}

impl FusedSeriesStream {
    /// Hands one raw chunk over and queues what it settled.
    fn push(&mut self, feed: FusedFeed) -> DataFusionResult<()> {
        let Some(executor) = self.executor.as_mut() else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec stream pushed a chunk after its execution closed".to_string(),
            ));
        };
        for input_id in self.input_ids.get(feed.source).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "FusedSeries has no runtime input IDs for source {}",
                feed.source
            ))
        })? {
            match &feed.chunk {
                FusedChunk::Raw(chunk) => executor.push_raw(*input_id, chunk.clone())?,
                FusedChunk::Evaluated(chunk) => {
                    executor.push_evaluated(*input_id, chunk.clone())?
                }
            }
        }
        if self.frame_mode {
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
        self.executor = None;
        self.partial_fold = None;
        self.inputs = None;
        // This reservation belongs only to this execution. Release it even if the consumer keeps
        // the failed stream and the plan alive.
        self._export_budget
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        // A batch queued before the error was settled by a run that has now stopped.
        self.ready.clear();
    }

    /// Converts one settled chunk into a row batch. Evaluated frames leave this driver as chunks.
    fn convert_rows(&mut self, chunk: EvaluatedChunk) -> DataFusionResult<()> {
        if let Some(fold) = self.partial_fold.as_mut() {
            fold.update(chunk)?;
            return Ok(());
        }
        let batch = series_to_record_batch(
            &self.classification,
            &self.tags,
            &self.output_schema,
            &chunk,
        )?;
        self.ready.push_back(batch);
        Ok(())
    }

    /// Publishes the partition's fold only after the input and native execution have drained.
    fn finish_fold(&mut self) -> DataFusionResult<()> {
        if let Some(mut fold) = self.partial_fold.take() {
            self.ready.extend(fold.finish()?);
        }
        Ok(())
    }
}

enum FusedOutput {
    Rows(RecordBatch),
    Evaluated(EvaluatedChunk),
}

impl FusedSeriesStream {
    fn poll_output(&mut self, cx: &mut Context<'_>) -> Poll<Option<DataFusionResult<FusedOutput>>> {
        for _ in 0..POLL_BUDGET {
            if let Some(batch) = self.ready.pop_front() {
                return Poll::Ready(Some(Ok(FusedOutput::Rows(batch))));
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
                        if let Err(error) = self.finish_fold() {
                            self.fail();
                            return Poll::Ready(Some(Err(error)));
                        }
                        self.done = true;
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
            Poll::Ready(Some(Ok(FusedOutput::Evaluated(_)))) => {
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

/// Whether closing a chain also closes the label set it carries.
///
/// A frame declares each series is contiguous, so one label set must appear as one chain. That holds
/// when the series that closes is the only one that will ever carry those labels.
///
/// Within one source it does hold: every series of that source shares its metric name, so a node
/// that drops `__name__`, `__type__` or `__unit__` — which is what a range function, a math map and
/// the timestamp shape do — maps distinct series to distinct labels. Rewriting an ordinary label is
/// what can make two of them equal, and that is the one thing refused here. Across sources nothing
/// is assumed: two metrics can share every ordinary label, so a name-dropping node collapses them.
///
/// The answer is conservative: a shape not recognised here keeps the end fold, which only costs
/// memory.
fn preserves_identity(node: &NativeNode) -> bool {
    same_source(node).is_some() && !rewrites_ordinary_labels(node)
}

/// Whether any node in the tree rewrites a label other than the metadata ones.
///
/// Dropping the metric name is safe within one source (see [`preserves_identity`]); replacing or
/// removing an ordinary label is not, because it can map two series onto one label set.
fn rewrites_ordinary_labels(node: &NativeNode) -> bool {
    let children: Vec<&NativeNode> = match &node.expr {
        NativeExpr::Map { input, op } => {
            if op
                .ops
                .iter()
                .any(|op| matches!(op, crate::native::plan::MapOp::Label(_)))
            {
                return true;
            }
            vec![input]
        }
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::VectorOf { input }
        | NativeExpr::Merge { input }
        | NativeExpr::Aggregate { input, .. }
        | NativeExpr::Vector { input, .. } => vec![input],
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => std::iter::once(&**input).chain(parameters.iter()).collect(),
        NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
            vec![left, right]
        }
        NativeExpr::Info { base, info, .. } => vec![base, info],
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => Vec::new(),
    };
    children.into_iter().any(rewrites_ordinary_labels)
}

struct FusedPackedStream {
    inner: SendableRecordBatchStream,
    handed: Arc<Mutex<Vec<MemoryReservation>>>,
}

impl RecordBatchStream for FusedPackedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for FusedPackedStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.inner.as_mut().poll_next(cx);
        let mut handed = self.handed.lock().unwrap_or_else(|e| e.into_inner());
        match &result {
            // Everything but the identity being packed is out of the way.
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {
                let settled = handed.len().saturating_sub(1);
                handed.drain(..settled);
            }
            Poll::Ready(None) | Poll::Ready(Some(Err(_))) => handed.clear(),
        }
        result
    }
}

impl Drop for FusedPackedStream {
    fn drop(&mut self) {
        self.handed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }
}

struct FusedEvaluatedStream {
    source: FusedSeriesStream,
    schema: Arc<SeriesSchema>,
    /// Where a fresh per-identity charge is registered.
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    reservation: MemoryReservation,
    /// Charges handed to the packer, oldest first.
    ///
    /// A charge goes here when the identity it covers is emitted, because the packer keeps the
    /// series it was handed. The packed stream frees them as it consumes, so what is billed here
    /// matches what is still resident.
    handed: Arc<Mutex<Vec<MemoryReservation>>>,
    retained: Vec<datatypes::series::samples::Series>,
    output: VecDeque<datatypes::series::samples::Series>,
    /// Whether a closed chain is also a closed label set, so it can be published at once.
    identity_per_chain: bool,
    ended: bool,
    failed: bool,
}

impl FusedEvaluatedStream {
    fn new(
        source: FusedSeriesStream,
        schema: Arc<SeriesSchema>,
        pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
        reservation: MemoryReservation,
        handed: Arc<Mutex<Vec<MemoryReservation>>>,
        identity_per_chain: bool,
    ) -> Self {
        Self {
            source,
            schema,
            pool,
            reservation,
            handed,
            retained: Vec::new(),
            output: VecDeque::new(),
            identity_per_chain,
            ended: false,
            failed: false,
        }
    }

    /// Merges the fragments of the chain that just closed and queues its output.
    ///
    /// A chain's fragments are absorbed only for as long as it stays open, so what is retained here
    /// is the pieces of one closing chain rather than a partition's whole output. Two chains that
    /// end up carrying equal labels are folded together by the merge above this node, not here.
    fn close_identity(&mut self) -> Result<(), DataFusionError> {
        if self.retained.is_empty() {
            return Ok(());
        }
        let rows = self.retained.iter().fold(0usize, |rows, series| {
            rows.saturating_add(series.time.len())
        });
        let fragments = self.retained.len();
        let fields = self.schema.fields.len();
        let workspace = crate::native::execute::merge_workspace_bytes(rows, fragments, fields);
        self.reservation.try_grow(workspace)?;
        let retained = std::mem::take(&mut self.retained);
        let merged = super::series_repartition::merge_exact_series(&self.schema, retained)?;
        self.output.extend(merged);
        Ok(())
    }

    /// Moves the charge this identity filled to the packed stream and starts a fresh one.
    fn hand_over(&mut self) {
        let fresh = MemoryConsumer::new("FusedSeriesFrame").register(&self.pool);
        let filled = std::mem::replace(&mut self.reservation, fresh);
        self.handed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(filled);
    }

    fn fail(&mut self) {
        self.retained.clear();
        self.output.clear();
        self.reservation.free();
        self.source.fail();
        self.failed = true;
    }
}

impl Stream for FusedEvaluatedStream {
    type Item = Result<(datatypes::series::samples::Series, bool), common_error::ext::BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.failed {
            return Poll::Ready(None);
        }
        if let Some(series) = self.output.pop_front() {
            if self.output.is_empty() {
                self.hand_over();
            }
            return Poll::Ready(Some(Ok((series, true))));
        }
        if self.ended {
            return Poll::Ready(None);
        }
        for _ in 0..POLL_BUDGET {
            match Pin::new(&mut self.source).poll_output(cx) {
                Poll::Ready(Some(Ok(FusedOutput::Evaluated(chunk)))) => {
                    let mut tables = 0usize;
                    for segment in chunk
                        .series
                        .fields
                        .iter()
                        .flat_map(|field| field.segments())
                    {
                        let batch = match segment.dense() {
                            Some(DenseSamplesRef::Histogram(histograms)) => {
                                Some(histograms.batch())
                            }
                            Some(DenseSamplesRef::Mixed(mixed)) => Some(mixed.histograms().batch()),
                            _ => None,
                        };
                        if let Some(batch) = batch {
                            match super::series_repartition::histogram_table_slice_bytes(batch) {
                                Ok(bytes) => tables = tables.saturating_add(bytes),
                                Err(error) => return self.error(error),
                            }
                        }
                    }
                    let charge = operand_chunk_bytes(&chunk.series).saturating_add(tables);
                    if let Err(error) = self.reservation.try_grow(charge) {
                        return self.error(error);
                    }
                    let closed = chunk.is_last && self.identity_per_chain;
                    self.retained.push(chunk.series);
                    if closed {
                        if let Err(error) = self.close_identity() {
                            return self.error(error);
                        }
                        if let Some(series) = self.output.pop_front() {
                            if self.output.is_empty() {
                                self.hand_over();
                            }
                            return Poll::Ready(Some(Ok((series, true))));
                        }
                    }
                }
                Poll::Ready(Some(Ok(FusedOutput::Rows(_)))) => {
                    return self.error(DataFusionError::Internal(
                        "row batch reached the evaluated stream adapter".into(),
                    ));
                }
                Poll::Ready(Some(Err(error))) => return self.error(error),
                Poll::Ready(None) => {
                    self.ended = true;
                    if let Err(error) = self.close_identity() {
                        return self.error(error);
                    }
                    if let Some(series) = self.output.pop_front() {
                        if self.output.is_empty() {
                            self.hand_over();
                        }
                        return Poll::Ready(Some(Ok((series, true))));
                    }
                    // Nothing was left to hand over; the packed stream frees what it holds.
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl FusedEvaluatedStream {
    fn error<T>(
        &mut self,
        error: DataFusionError,
    ) -> Poll<Option<Result<T, common_error::ext::BoxedError>>> {
        self.fail();
        Poll::Ready(Some(Err(common_error::ext::BoxedError::new(
            common_query::error::Error::from(error),
        ))))
    }
}
