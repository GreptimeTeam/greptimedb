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

//! The merge half of a split native aggregate.
//!
//! A native `Complete` aggregate folds every contribution to a group, which is only correct when one
//! execution reads all of them. A raw-series scan hands its partitions to separate executions, so the
//! aggregate is split instead: the per-series prefix of the tree runs in a [`FusedSeries`] node that
//! exports the state each partition folded, and [`AggregateFinalize`] merges that state into the
//! statement's series.
//!
//! The split is structural. The innermost aggregate of the tree is replaced by an input node at its
//! own output - that is where merged values enter - and the nodes above it become this node's suffix.
//! What cannot be split correctly is refused with [`DataFusionError::NotImplemented`], which the
//! planner reads as "plan this query the existing way".
//!
//! [`AggregateFinalize`] is the logical half and [`AggregateFinalizeExec`] the physical one: the
//! executor gathers every producer's state, merges it with the same kernel a whole-vector aggregate
//! folds, runs the suffix over the merged series and converts its chunks into the statement's rows.
//! Nothing is handed over before the input ended and the suffix closed, so a row that reached a
//! consumer is a row no contribution can still change.

use std::cmp::Ordering;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, Series, SeriesSchema};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::aggregate::{AggregateOp, FloatMerger};
use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::aggregate_exchange::{aggregate_op, decode_state, state_schema};
use crate::extension_plan::fused_series::{OutputColumn, classify_output, series_to_record_batch};
use crate::extension_plan::{FusedSeries, FusedSeriesBinding};
use crate::grid_series::series_bytes;
use crate::map::MathOp;
use crate::native::execute::{InputId, NativeExecutionState, NativeExecutor};
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, BoundInput, BoundVectorOp, Grouping, MapOp,
    NativeExpr, NativeNode, NativeTree, NativeType,
};

/// One node that merges the partition-local state of a split aggregate.
///
/// Its input is the partition-local [`FusedSeries`] producer; its schema is the statement's row shape,
/// the time index, the aggregate's value columns and the labels its grouping keeps, not the state
/// layout the producer exports.
///
/// The fields are what the merge and the suffix are built from: the aggregate, the binding the
/// producer's state is named by (its wider tag list included), the tree above the merge, and the
/// field layout of the series being merged, which is the cut aggregate's own output - evaluated on
/// the statement's grid - and not the prefix's raw input.
#[derive(Debug)]
pub struct AggregateFinalize {
    /// The PromQL text the whole tree was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The aggregate the split took out of the tree, bound as the compiler bound it.
    op: BoundAggregate,
    /// What the producer's frames were compiled against. Its `tags` are every logical tag of the
    /// input, which is what an exported state row has to be able to name.
    binding: FusedSeriesBinding,
    /// The tree above the split aggregate, with that aggregate replaced by the input its merged
    /// values enter at.
    suffix: Arc<NativeTree>,
    /// The partition-local producer, whose batches are aggregate state.
    input: LogicalPlan,
    /// The columns this node produces: its `Utf8` columns are the labels it writes.
    output_schema: DFSchemaRef,
    /// The field layout the split aggregate folds, on the grid its input is evaluated on.
    series_schema: Arc<SeriesSchema>,
}

impl AggregateFinalize {
    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "AggregateFinalize"
    }

    /// Splits `full_tree` around its innermost aggregate.
    ///
    /// `rawinput` produces the raw series frames the prefix reads, and `output_schema` is the shape
    /// the statement's rows have: the time index, the aggregate's value columns and the labels its
    /// grouping keeps, as the existing planner names them.
    ///
    /// The fields a grouping may not name are read off the tree's own raw input, not off its root:
    /// the root is the prefix and is already evaluated, so its fields are the statement's value
    /// columns rather than the raw ones a `by(...)` clause names.
    ///
    /// [`DataFusionError::NotImplemented`] says this tree cannot be split correctly - a shape whose
    /// per-partition half would not be one series at a time, or whose suffix would not run on merged
    /// values - and the caller is expected to keep the plan it would have built without this node.
    pub fn try_new(
        promql: Arc<str>,
        full_tree: NativeTree,
        binding: FusedSeriesBinding,
        rawinput: LogicalPlan,
        output_schema: DFSchemaRef,
    ) -> DfResult<Self> {
        let fields = prefix_fields(&full_tree)?;
        let Some(split) = split(full_tree.root(), &fields, &binding.time_index)? else {
            return Err(declined("the tree holds no splittable aggregate"));
        };
        let values = output_schema
            .fields()
            .iter()
            .filter(|field| field.data_type() == &DataType::Float64)
            .count();
        if values != split.schema.fields.len() {
            return Err(DataFusionError::Plan(format!(
                "the aggregate of `{promql}` folds {} field(s), and the output schema declares \
                 {values} value column(s)",
                split.schema.fields.len()
            )));
        }
        let partial = FusedSeries::try_new_partial(
            Arc::clone(&promql),
            NativeTree::try_new(split.prefix)?,
            binding.clone(),
            vec![rawinput],
            split.op.clone(),
        )?;
        Ok(Self {
            promql,
            op: split.op,
            binding,
            suffix: Arc::new(NativeTree::try_new(split.suffix)?),
            input: LogicalPlan::Extension(Extension {
                node: Arc::new(partial),
            }),
            output_schema,
            series_schema: split.schema,
        })
    }

    /// Whether `tree` holds an aggregate this node can merge.
    ///
    /// The walk is the one [`Self::try_new`] does, without building either half. A caller uses it to
    /// decide *before* planning anything else, so a shape this node declines is planned once, by the
    /// path it keeps, instead of being planned once for a schema and again after the refusal.
    ///
    /// `fields` is the raw field layout of the scan the tree was compiled against, which the caller
    /// that resolved that scan already holds: the same names the tree's own raw input carries, and
    /// the ones a `by(...)` may not name.
    pub fn can_split(tree: &NativeTree, fields: &[FieldSpec], time_index: &str) -> bool {
        matches!(split(tree.root(), fields, time_index), Ok(Some(_)))
    }

    /// The physical node that merges the state `input` produces and runs the tree above the cut.
    ///
    /// `input` is the physical plan of [`Self::input`], the partition-local producer: its batches are
    /// aggregate state, one partition per producer execution, so this node gathers them and answers
    /// one partition whatever `input`'s layout is.
    ///
    /// The row layout is read off this node's output schema the way the producer reads its frames off
    /// its binding: its `Utf8` columns are the labels it writes, and its `Float64` columns name the
    /// merged fields by position, since the merge folds the cut aggregate's fields in the order the
    /// statement declares them. Building rejects an input that is not state, a suffix the runtime
    /// does not execute (the supported suffix operations are maps, aggregates, TopK and
    /// constant-sided binary operations) and a schema [`classify_output`] cannot convert.
    pub fn to_execution_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let op = aggregate_op(&self.op)?;
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        // The same classification the producer makes over the series it writes, applied to the series
        // the merge assembles: a second copy would let the two nodes disagree about which column a
        // name reads.
        let mut tags: Vec<String> = Vec::new();
        let mut values: Vec<FieldSpec> = Vec::new();
        for field in output_schema.fields() {
            match field.data_type() {
                DataType::Utf8 => tags.push(field.name().clone()),
                DataType::Float64 => values.push(FieldSpec {
                    name: field.name().as_str().into(),
                }),
                // The time column and everything else are the classification's business below: it
                // is the authority on which columns the conversion can produce.
                _ => {}
            }
        }
        let classification =
            classify_output(&output_schema, &values, &tags, &self.binding.time_index)?;
        Ok(Arc::new(AggregateFinalizeExec::try_new(
            Arc::clone(&self.promql),
            input,
            Arc::clone(&self.suffix),
            Arc::clone(&self.series_schema),
            op,
            self.binding.tags.clone(),
            tags,
            classification,
            output_schema,
        )?))
    }

    /// The readable form of the split, for its plan line only.
    fn key(&self) -> String {
        format!("{:?} {:?} {:?}", self.op.kind, self.op.group, self.op.stage)
    }
}

impl UserDefinedLogicalNodeCore for AggregateFinalize {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // The aggregate and the tree above it are bound inside this node; there is no expression an
        // optimizer could rewrite separately from the aggregate that owns it.
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "AggregateFinalize: {} [{}]", self.promql, self.key())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "AggregateFinalize should have exactly one input".to_string(),
            ));
        }
        let input = inputs.swap_remove(0);
        let expected = state_schema(&self.binding.tags);
        if input.schema().as_arrow() != expected.as_ref() {
            return Err(DataFusionError::Plan(format!(
                "AggregateFinalize input must export aggregate state with schema {expected:?}, got {:?}",
                input.schema().as_arrow()
            )));
        }
        // A rewritten input changes where the correctly-shaped state comes from, not how it merges.
        Ok(Self {
            promql: Arc::clone(&self.promql),
            op: self.op.clone(),
            binding: self.binding.clone(),
            suffix: Arc::clone(&self.suffix),
            input,
            output_schema: Arc::clone(&self.output_schema),
            series_schema: Arc::clone(&self.series_schema),
        })
    }
}

impl PartialEq for AggregateFinalize {
    fn eq(&self, other: &Self) -> bool {
        self.promql == other.promql
            && self.op == other.op
            && self.binding == other.binding
            && self.suffix == other.suffix
            && self.input == other.input
            && self.output_schema == other.output_schema
    }
}

impl Eq for AggregateFinalize {}

impl Hash for AggregateFinalize {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.promql.hash(state);
        self.op.hash(state);
        self.binding.hash(state);
        self.suffix.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
    }
}

impl PartialOrd for AggregateFinalize {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.promql.cmp(&other.promql) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
        if self.op != other.op {
            return None;
        }
        if self.suffix != other.suffix {
            return None;
        }
        match self.binding.partial_cmp(&other.binding) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.input.partial_cmp(&other.input) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        (self.output_schema == other.output_schema).then_some(Ordering::Equal)
    }
}

/// One split: the two halves the tree was cut into, and what the cut aggregate folds.
struct Split {
    /// The per-series tree below the cut aggregate, which the producer runs.
    prefix: NativeNode,
    /// The tree above it, starting at the input the merged values enter at.
    suffix: NativeNode,
    /// The aggregate that was cut out.
    op: BoundAggregate,
    /// What that aggregate produces: the layout the exported state is indexed by.
    schema: Arc<SeriesSchema>,
}

/// Splits the innermost aggregate out of `node`.
///
/// `Ok(None)` is a subtree holding no aggregate. The currently verified suffix operations are maps,
/// aggregates, TopK/BottomK, classic histogram functions (`histogram_quantile`,
/// `histogram_fraction`) and binary operations with a constant on one side. TopK/BottomK and the
/// histogram functions run once in the suffix over the fully merged aggregate series, per evaluation
/// timestamp, and never in the partition-local producer.
///
/// A histogram function reads each classic histogram from the bucket series that share its other
/// labels, and drops `le`. The merge has already folded every partition's contribution to each
/// `(..., le)` group into one complete series on the evaluated grid, so the bucket series it reads
/// are exactly the ones this statement's input would have produced in one execution: reading `le`
/// off those merged series is the same computation, not an approximation. TopK is already accepted
/// for that reason: it runs once over the fully merged series. This admits a second operation of
/// that kind, not a new mechanism. LimitK stays declined because it depends on the storage input
/// order, which the merge does not preserve ([`crate::select::limitk`]). A map is point-wise and an
/// aggregate runs over the complete merge. Other vector operations have not yet been
/// verified/supported by this split.
///
/// An instant selection or window above the cut cannot be applied to the merged values under the
/// tree's current binding: the cut output is evaluated, whereas these nodes consume raw samples.
/// A binary operation between two vectors would require matching the cut's merged output against a
/// vector sibling, but this merge only gathers the cut's state.
fn split(node: &NativeNode, fields: &[FieldSpec], time_index: &str) -> DfResult<Option<Split>> {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => Ok(None),
        NativeExpr::Map { input, op } => {
            let Some(mut inner) = split(input, fields, time_index)? else {
                return Ok(None);
            };
            inner.suffix = NativeNode {
                expr: NativeExpr::Map {
                    input: Box::new(inner.suffix),
                    op: op.clone(),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        NativeExpr::Aggregate { input, op } => {
            check_cut(op, fields, time_index)?;
            match split(input, fields, time_index)? {
                // An aggregate above the cut runs in the suffix, over the merged values: correct
                // once every contribution is in, which is what the merge delivers.
                Some(mut inner) => {
                    inner.suffix = NativeNode {
                        expr: NativeExpr::Aggregate {
                            input: Box::new(inner.suffix),
                            op: op.clone(),
                        },
                        output: node.output.clone(),
                    };
                    Ok(Some(inner))
                }
                None => {
                    admit_prefix(input)?;
                    let NativeType::Samples(schema) = &node.output else {
                        return Err(declined("an aggregate that produces no values"));
                    };
                    if schema.eval_grid.is_none() {
                        return Err(declined("an aggregate that produces no values"));
                    }
                    Ok(Some(Split {
                        prefix: (**input).clone(),
                        // The merged values enter the suffix as the aggregate's own output: the same
                        // binding and the same value, which is what the tree validates, at source
                        // `0` of the suffix's own execution - a source index is local to one
                        // execution, so the suffix names its one input rather than inheriting the
                        // prefix's.
                        suffix: NativeNode {
                            expr: NativeExpr::Input(BoundInput {
                                source_index: 0,
                                output: node.output.clone(),
                                ordering: SeriesOrdering::Contiguous,
                            }),
                            output: node.output.clone(),
                        },
                        op: op.clone(),
                        schema: Arc::clone(schema),
                    }))
                }
            }
        }
        NativeExpr::Vector {
            input,
            op: op @ (BoundVectorOp::TopK { .. } | BoundVectorOp::Histogram(_)),
        } => {
            let Some(mut inner) = split(input, fields, time_index)? else {
                return Ok(None);
            };
            inner.suffix = NativeNode {
                expr: NativeExpr::Vector {
                    input: Box::new(inner.suffix),
                    op: op.clone(),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        NativeExpr::Merge { .. } => Err(declined("an aggregate split across a canonical merge")),
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Vector { input, .. }
        | NativeExpr::VectorOf { input }
        | NativeExpr::ParameterizedMap { input, .. } => match split(input, fields, time_index)? {
            Some(_) => Err(declined(
                "an aggregate under an operation that reads evaluated values",
            )),
            None => Ok(None),
        },
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            // Each side is split once, and at most one of them may hold the innermost aggregate: two
            // would be two cuts, which this node cannot merge.
            let (left_split, right_split) = (
                split(left, fields, time_index)?,
                split(right, fields, time_index)?,
            );
            // A constant operand is broadcast over whatever the other side answers, so applying the
            // same operation to the merged values is the operation the query wrote. Any other sibling
            // would have to be matched with those values by label, which nothing below this node
            // gathers.
            let (mut inner, constant_on_the_left) = match (left_split, right_split) {
                (None, None) => return Ok(None),
                (Some(inner), None) if matches!(right.expr, NativeExpr::Scalar(_)) => {
                    (inner, false)
                }
                (None, Some(inner)) if matches!(left.expr, NativeExpr::Scalar(_)) => (inner, true),
                _ => {
                    return Err(declined(
                        "an aggregate under a binary operation without a constant on the other side",
                    ));
                }
            };
            // The cut side's own suffix takes the place of the aggregate, on the side the query wrote
            // it: a non-commutative operator means what that order says, and the matching and the
            // output are the ones the compiler bound.
            let (left, right) = if constant_on_the_left {
                ((**left).clone(), inner.suffix)
            } else {
                (inner.suffix, (**right).clone())
            };
            inner.suffix = NativeNode {
                expr: NativeExpr::Binary {
                    left: Box::new(left),
                    right: Box::new(right),
                    matching: matching.clone(),
                },
                output: node.output.clone(),
            };
            Ok(Some(inner))
        }
        // A set operation keeps samples by whether their match group is present on the other side,
        // which the merged values of one side cannot answer.
        NativeExpr::Set { left, right, .. } => {
            match (
                split(left, fields, time_index)?,
                split(right, fields, time_index)?,
            ) {
                (None, None) => Ok(None),
                _ => Err(declined("an aggregate under a set operation")),
            }
        }
        // The suffix above a split aggregate reads the merged state alone, so it has no source for
        // the info series.
        NativeExpr::Info { base, info, .. } => {
            match (
                split(base, fields, time_index)?,
                split(info, fields, time_index)?,
            ) {
                (None, None) => Ok(None),
                _ => Err(declined("an aggregate under info")),
            }
        }
    }
}

/// The field names of the raw series the prefix reads, in source order.
///
/// Read off the tree's own raw input node, which is the one authority on what the prefix is
/// compiled against: the root is already evaluated on the statement's grid, so its fields are the
/// statement's value columns rather than the raw ones a `by(...)` clause would name. The prefix is
/// one series at a time and the tree reads exactly one source, so the first raw input answers for
/// all of them.
fn prefix_fields(tree: &NativeTree) -> DfResult<Vec<FieldSpec>> {
    fn raw_fields(node: &NativeNode) -> Option<Vec<FieldSpec>> {
        match &node.expr {
            NativeExpr::Input(input) => input
                .output
                .samples()
                .filter(|schema| schema.eval_grid.is_none())
                .map(|schema| schema.fields.to_vec()),
            NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => None,
            NativeExpr::Instant { input, .. }
            | NativeExpr::Window { input, .. }
            | NativeExpr::Map { input, .. }
            | NativeExpr::Merge { input }
            | NativeExpr::Timestamp { input }
            | NativeExpr::Aggregate { input, .. }
            | NativeExpr::Vector { input, .. }
            | NativeExpr::VectorOf { input } => raw_fields(input),
            NativeExpr::ParameterizedMap {
                input, parameters, ..
            } => raw_fields(input).or_else(|| parameters.iter().find_map(raw_fields)),
            NativeExpr::Binary { left, right, .. }
            | NativeExpr::Set { left, right, .. }
            | NativeExpr::Info {
                base: left,
                info: right,
                ..
            } => raw_fields(left).or_else(|| raw_fields(right)),
        }
    }

    raw_fields(tree.root()).ok_or_else(|| declined("a tree that reads no raw series"))
}

/// Refuses an aggregate whose state the split cannot carry, and one grouped by a value field.
///
/// Only the eight fixed-width accumulators are carried: a quantile's state is the group's samples, and
/// a non-`Complete` stage is a state or a merge rather than an answer. A `by(...)` naming a value field
/// or the time index cannot be kept either - the state rows are indexed by field and by grid point,
/// so a group that varies with either is not a group - while a label the input does not have is
/// simply absent, exactly as it is in the existing planner.
fn check_cut(op: &BoundAggregate, fields: &[FieldSpec], time_index: &str) -> DfResult<()> {
    if op.stage != AggregateStage::Complete {
        return Err(declined("an aggregate that is not `Complete`"));
    }
    if matches!(op.kind, AggregateKind::Quantile { .. }) {
        return Err(declined("a `quantile` aggregate"));
    }
    if let Grouping::By(labels) = &op.group
        && labels.iter().any(|label| {
            label.as_ref() == time_index
                || fields
                    .iter()
                    .any(|field| field.name.as_ref() == label.as_ref())
        })
    {
        return Err(declined("an aggregate grouped by a value field"));
    }
    Ok(())
}

/// Checks that the per-series prefix is a tree one partition can evaluate on its own.
///
/// The producer runs one execution per partition and folds exactly the series that partition's scan
/// returned, so the prefix has to answer one series from one series' samples: maps are point-wise, a
/// binary operation with a constant side applies that constant to each series its other side answers,
/// and a window or an instant selection reads the raw input itself. A raw input means no evaluation
/// grid - a tree whose input is already evaluated is a subquery's, which one partition cannot evaluate.
///
/// A `clamp` whose maximum is below its minimum answers no value at all, so the fold would see a group
/// with no sample of that field rather than the query the reader wrote.
fn admit_prefix(root: &NativeNode) -> DfResult<()> {
    let mut node = root;
    loop {
        match &node.expr {
            NativeExpr::Map { input, op } => {
                for map_op in op.ops.iter() {
                    if let MapOp::Math(MathOp::Clamp { min, max }) = map_op
                        && max < min
                    {
                        return Err(declined("a `clamp` whose maximum is below its minimum"));
                    }
                }
                node = input;
            }
            // Only a constant side is broadcast: it reaches each series the other side answers, so
            // that side is the one the fold runs. Two constants answer one value per grid point and
            // read no series, and two vectors are a label match over the whole vector, which is not
            // what either half of the split produces.
            NativeExpr::Binary { left, right, .. } => {
                node = match (&left.expr, &right.expr) {
                    (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => {
                        return Err(declined("a per-series prefix of two constants"));
                    }
                    (NativeExpr::Scalar(_), _) => &**right,
                    (_, NativeExpr::Scalar(_)) => &**left,
                    (_, _) => {
                        return Err(declined(
                            "a per-series prefix that matches two vectors by label",
                        ));
                    }
                };
            }
            _ => break,
        }
    }
    let raw = |node: &NativeNode| match &node.expr {
        NativeExpr::Input(input) => input
            .output
            .samples()
            .is_some_and(|schema| schema.eval_grid.is_none()),
        _ => false,
    };
    match &node.expr {
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } if raw(input) => {
            Ok(())
        }
        _ => Err(declined(
            "a per-series prefix that is not one series at a time",
        )),
    }
}

fn declined(reason: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native aggregate split does not carry {reason}"
    ))
}

/// One converted output column: its array header, the buffers that hold its length and validity, and
/// its slot in the batches vector are each well under this.
const COLUMN_OVERHEAD: usize = 256;
/// The value capacity a converted string column starts with (Arrow's `StringArray::from`), which one
/// label column is charged for before it holds a row.
const STRING_CAPACITY: usize = 1024;

/// The physical half of a split aggregate: merges the state each partition folded, runs the tree
/// above the cut over the merged series and converts the result into the statement's rows.
///
/// The answer is built before its first batch is handed over, so a late input error is the stream's
/// first item rather than a row the error invalidates after it. What the answer holds is charged to
/// that stream, which is the only owner it has: the plan answers once.
///
/// The suffix above the cut is built from the aggregate's output and may contain maps, aggregates,
/// TopK/BottomK or a binary operation with a constant on one side. [`ExecutionPlan::execute`]
/// builds its state and is where a shape the runtime cannot run is refused.
///
/// The plain state-batch input is temporary. The final design consumes series frames directly
/// from FusedSeries instead of decoding and rebuilding this intermediate state-row layout.
struct AggregateFinalizeExec {
    /// The PromQL text the whole tree was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The producer of the partition-local state, gathered to one partition.
    input: Arc<dyn ExecutionPlan>,
    /// The tree above the cut, which runs over the merged series.
    suffix: Arc<NativeTree>,
    /// The field layout the cut aggregate folds, which every decoded state row is checked against.
    series_schema: Arc<SeriesSchema>,
    /// The kernel the merge folds with.
    op: AggregateOp,
    /// The tag names the state rows are labelled by, which is what names their columns.
    state_tags: Vec<String>,
    /// The label column names this node writes, indexed by [`OutputColumn`].
    tags: Arc<[String]>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// Per-node metrics, registered by the one execution of the suffix.
    metric: ExecutionPlanMetricsSet,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

impl AggregateFinalizeExec {
    /// The name this node is shown under.
    const fn name() -> &'static str {
        "AggregateFinalizeExec"
    }

    /// Creates the node, rejecting an input that is not the producer's state and a suffix the
    /// runtime does not execute.
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        promql: Arc<str>,
        input: Arc<dyn ExecutionPlan>,
        suffix: Arc<NativeTree>,
        series_schema: Arc<SeriesSchema>,
        op: AggregateOp,
        state_tags: Vec<String>,
        tags: Vec<String>,
        classification: Vec<OutputColumn>,
        output_schema: SchemaRef,
    ) -> DfResult<Self> {
        let state_layout = state_schema(&state_tags);
        if input.schema().as_ref() != state_layout.as_ref() {
            return Err(DataFusionError::Plan(format!(
                "AggregateFinalizeExec merges the aggregate state of `{promql}`, whose layout is \
                 {state_layout:?}, not {:?}",
                input.schema()
            )));
        }
        let partitions = input.properties().partitioning.partition_count();
        if partitions == 0 {
            return Err(DataFusionError::Plan(
                "AggregateFinalizeExec input has no partitions".to_string(),
            ));
        }
        // One merge is one operation over every partition's state, so the partitions are gathered
        // first; a lone partition is used as it is.
        let input: Arc<dyn ExecutionPlan> = if partitions > 1 {
            Arc::new(CoalescePartitionsExec::new(input))
        } else {
            input
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            // One answer: every partition's state is merged into the same groups.
            Partitioning::UnknownPartitioning(1),
            // Nothing is handed over before the input ended and the suffix closed.
            EmissionType::Final,
            input.properties().boundedness,
        ));
        Ok(Self {
            promql,
            input,
            suffix,
            series_schema,
            op,
            state_tags,
            tags: tags.into(),
            classification: classification.into(),
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }
}

impl fmt::Debug for AggregateFinalizeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateFinalizeExec").finish()
    }
}

impl DisplayAs for AggregateFinalizeExec {
    /// The query, then the tree above the cut, rendered by [`NativeTree`]'s own `Display`.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "AggregateFinalizeExec: {}\n{}", self.promql, self.suffix)
    }
}

impl ExecutionPlan for AggregateFinalizeExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// The constructor gathered the input, so the single partition this node reads is already the one
    /// it requires and no exchange can improve on it.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::SinglePartition])
    }

    /// Raising the count is what an optimizer does by adding an exchange the constructor would then
    /// have to gather again.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        // The aggregate and the tree above it are bound inside this node, which owns no expression.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Plan(format!(
                "AggregateFinalizeExec requires exactly one child, got {}",
                children.len()
            ))
        })?;
        // Rebuilt rather than patched, so a child that changed shape is refused instead of decoded
        // with a stale layout. The rebuild carries the same configuration and fresh reservations,
        // which is what the next execution registers into.
        Ok(Arc::new(Self::try_new(
            Arc::clone(&self.promql),
            child,
            Arc::clone(&self.suffix),
            Arc::clone(&self.series_schema),
            self.op,
            self.state_tags.clone(),
            self.tags.to_vec(),
            self.classification.to_vec(),
            self.output_schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // One answer per statement: a second partition would answer the same groups again.
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "AggregateFinalizeExec has exactly one partition, asked for {partition}"
            )));
        }
        let pool = Arc::clone(&context.runtime_env().memory_pool);
        // What the answer holds is charged to this stream and nothing else: the plan answers once, so
        // the reservation is registered here and given back when the stream is dropped, whether the
        // answer was read, dropped half way or refused.
        let answer = Arc::new(MemoryConsumer::new("AggregateFinalizeExec output").register(&pool));
        let state = NativeExecutionState::try_new(&self.suffix, Arc::clone(&pool))?;
        state.register_metrics(&self.metric, 0);
        let input_id = state.input_ids(0).next().ok_or_else(|| {
            DataFusionError::Execution("AggregateFinalize suffix has no source-0 input".to_string())
        })?;
        if state.input_ids(0).count() != 1 {
            return Err(DataFusionError::Execution(
                "AggregateFinalize suffix must have exactly one source-0 input".to_string(),
            ));
        }
        let input = self.input.execute(0, Arc::clone(&context))?;
        let merged = merge_into_batches(
            input,
            MemoryConsumer::new("AggregateFinalizeExec state").register(&pool),
            NativeExecutor::new(state),
            input_id,
            Arc::clone(&self.series_schema),
            self.op,
            self.state_tags.clone(),
            Arc::clone(&self.classification),
            Arc::clone(&self.tags),
            self.output_schema.clone(),
            Arc::clone(&answer),
        );
        let batches = futures::stream::once(merged).flat_map(
            |result: DfResult<Vec<RecordBatch>>| -> BoxStream<'static, DfResult<RecordBatch>> {
                match result {
                    Ok(batches) => Box::pin(futures::stream::iter(
                        batches.into_iter().map(Ok::<_, DataFusionError>),
                    )),
                    // The answer never started, so the error is the stream's only item.
                    Err(error) => Box::pin(futures::stream::once(async move { Err(error) })),
                }
            },
        );
        let batches = batches.map(move |batch| {
            // A failed answer has dropped every queued batch; successful batches stay charged
            // until this stream drops.
            if batch.is_err() {
                answer.free();
            }
            batch
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            batches,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

/// Merges every partition's state, runs the suffix over the merged series and converts its chunks
/// into the statement's rows.
///
/// Each batch is decoded and merged as it arrives, so at most one batch and one partition's state are
/// live beside the merge's own. Every admission here is a total rather than a step - the kernels report
/// what their work would keep live, not what it would add - so each one is charged together with what
/// the rest of this function already holds.
#[allow(clippy::too_many_arguments)]
async fn merge_into_batches(
    mut input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    mut executor: NativeExecutor,
    input_id: InputId,
    series_schema: Arc<SeriesSchema>,
    op: AggregateOp,
    state_tags: Vec<String>,
    classification: Arc<[OutputColumn]>,
    tags: Arc<[String]>,
    output_schema: SchemaRef,
    answer: Arc<MemoryReservation>,
) -> DfResult<Vec<RecordBatch>> {
    let mut merger = FloatMerger::try_new(&series_schema, op)?;
    while let Some(batch) = input.try_next().await? {
        let batch_bytes = batch.get_array_memory_size();
        // The batch is live beside the state decoded from it, which is live beside the merge's own.
        reservation.try_resize(merger.state_bytes().saturating_add(batch_bytes))?;
        let partial = decode_state(&batch, &series_schema, op, &state_tags, |total| {
            reservation.try_resize(
                merger
                    .state_bytes()
                    .saturating_add(batch_bytes)
                    .saturating_add(total),
            )
        })?;
        // The decoded state is counted by the merge's own total, so only the batch is added here.
        merger.merge_budgeted(partial, |total| {
            reservation.try_resize(batch_bytes.saturating_add(total))
        })?;
        // The rows are folded in, so only what the merge keeps is still held.
        drop(batch);
        reservation.try_resize(merger.state_bytes())?;
    }
    // Only a successful end of the input answers: a failed read dropped the state above instead.
    let mut series = merger.finish_budgeted(|total| reservation.try_resize(total))?;
    // Partition arrival order is not a statement-level output order. Match Prometheus label
    // comparison before presenting the finalized groups so equivalent executions answer alike.
    series.sort_by(compare_series_labels);
    // The merge is consumed by its answer, which replaces what it kept. The whole answer is charged at
    // once and held until the loop ends: the vector and every series still waiting in it stay live
    // while the loop folds them one by one, so releasing them one at a time would leave the rest
    // uncharged. The amount is deliberately conservative - every series counted whole, no release.
    let retained = series
        .capacity()
        .saturating_mul(size_of::<Series>())
        .saturating_add(series.iter().map(series_bytes).sum::<usize>());
    reservation.try_resize(retained)?;
    let mut batches = Vec::new();
    let mut held = 0usize;
    for series in series {
        // Every group is one series and is complete here, so it closes its own chunk: the suffix runs
        // on merged values, not on fragments of them.
        executor.push_evaluated(
            input_id,
            EvaluatedChunk {
                series,
                is_last: true,
            },
        )?;
        drain(
            &mut executor,
            &mut batches,
            &mut held,
            &answer,
            &classification,
            &tags,
            &output_schema,
        )?;
    }
    // Every series is the suffix's now, and its own reservations account for it.
    reservation.try_resize(0)?;
    // Nothing more will arrive, so this is what a collecting node answers.
    executor.end_of_input()?;
    drain(
        &mut executor,
        &mut batches,
        &mut held,
        &answer,
        &classification,
        &tags,
        &output_schema,
    )?;
    // The rows are what stays held once the series they came from are gone.
    answer.try_resize(batches.iter().map(RecordBatch::get_array_memory_size).sum())?;
    Ok(batches)
}

/// Compares the complete Prometheus label sets in label-name order, with no temporary allocation.
fn compare_series_labels(a: &Series, b: &Series) -> Ordering {
    fn labels(series: &Series) -> impl Iterator<Item = (&str, &str)> {
        let tags = series.metric.tags();
        let name_index = tags.partition_point(|tag| tag.name.as_ref() < "__name__");
        tags[..name_index]
            .iter()
            .map(|tag| (tag.name.as_ref(), tag.value.as_ref()))
            .chain(
                series
                    .metric
                    .name()
                    .into_iter()
                    .map(|name| ("__name__", name)),
            )
            .chain(
                tags[name_index..]
                    .iter()
                    .map(|tag| (tag.name.as_ref(), tag.value.as_ref())),
            )
    }

    labels(a).cmp(labels(b))
}

/// Hands over every chunk the suffix has settled, as rows.
///
/// Each chunk is admitted before it is converted, by a coarse estimate of the storage its rows need -
/// the series itself, one copy of the timestamps and values, and each label column's initial capacity,
/// offsets and repeated text - so an answer that outgrows the pool is refused while it is built. The
/// batch's own size replaces the estimate once it exists.
#[allow(clippy::too_many_arguments)]
fn drain(
    executor: &mut NativeExecutor,
    batches: &mut Vec<RecordBatch>,
    held: &mut usize,
    answer: &MemoryReservation,
    classification: &[OutputColumn],
    tags: &[String],
    output_schema: &SchemaRef,
) -> DfResult<()> {
    while let Some(chunk) = executor.next_chunk()? {
        let series = &chunk.series;
        let rows = series.time.len();
        // The series itself, one copy of the timestamps and values at eight bytes a cell, and a header
        // for each column and its batch slot.
        let mut estimate = series_bytes(series)
            .saturating_add(rows.saturating_mul(8 * series.fields.len().saturating_add(1)))
            .saturating_add(classification.len().saturating_mul(COLUMN_OVERHEAD))
            .saturating_add(size_of::<RecordBatch>());
        // A label column is a string column: its starting capacity, an offset and a validity bit a row,
        // and the value it repeats with the room a growing buffer needs.
        for tag in tags.iter() {
            let text = rows.saturating_mul(series.metric.get(tag).map_or(0, str::len));
            estimate = estimate
                .saturating_add(STRING_CAPACITY)
                .saturating_add(rows.saturating_mul(8))
                .saturating_add(text.saturating_mul(2));
        }
        answer.try_resize(held.saturating_add(estimate))?;
        let batch = series_to_record_batch(classification, tags, output_schema, &chunk)?;
        // What the batch holds replaces the estimate: the chunk is dropped as its rows are kept.
        *held = held.saturating_add(batch.get_array_memory_size());
        answer.try_resize(*held)?;
        batches.push(batch);
    }
    Ok(())
}

#[cfg(test)]
mod exec_tests;
#[cfg(test)]
mod tests;
