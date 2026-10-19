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
//! half: it owns one execution of that tree per partition and converts its input batches into raw
//! series chunks and its evaluated chunks back into batches.
//!
//! The two halves show different things in `EXPLAIN`, which is the point of having both. The
//! logical node prints the query a reader wrote ([`FusedSeries`]'s `fmt_for_explain`), and the
//! physical node prints the tree it will run ([`FusedSeriesExec`]'s `fmt_as`, which embeds
//! [`NativeTree`]'s own display rather than a second renderer). A reader comparing the two plans
//! can therefore see whether a query was compiled the way they expected without decoding node
//! names.
//!
//! Scope of this layer: the input is a stream of ordinary record batches carrying one timestamp
//! column, the series' field columns and the series' tag columns, with each complete label set
//! contiguous and its timestamps ascending. The output is one batch per evaluated chunk, with the
//! statement's grid timestamps in the declared time column. Everything the tree cannot do is
//! rejected when the execution plan is built, not when the first batch arrives: a tree the native
//! runtime does not execute, a binding that does not match the tree, or an input or output schema
//! the conversion cannot classify.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_query::series_frame::{InputMapping, record_batches_to_raw_series};
use datafusion::arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{DenseSamplesRef, FieldSpec, SampleRef, Samples, SeriesSchema};
use datatypes::series::time::EvalGrid;
use futures::Stream;
use store_api::region_engine::SendableRawSeriesStream;

use crate::evaluated::EvaluatedChunk;
use crate::native::execute::{NativeExecutionState, NativeExecutor};
use crate::native::metrics;
use crate::native::plan::{NativeExpr, NativeTree};

#[cfg(test)]
mod tests;

/// The label a metric name is stored under, which is not a tag column of the input.
const METRIC_NAME: &str = "__name__";

/// What the tree's input was compiled against, and how its output columns are named.
///
/// The binding travels with the tree: a tree is legal only against the schema it was bound to, and
/// a reader of the output needs to know which grid a chunk's absolute positions are on. Keeping it
/// in one value is what lets both halves of the node be built from the same description.
///
/// What the node checks is that the input's columns are exactly the ones the binding accounts for:
/// the time column, the fields and the tags. What it cannot check is whether the caller meant a
/// *different* column of the same type. A binding whose [`metric_name`](Self::metric_name) is not
/// the metric the scan selects, or whose [`time_index`](Self::time_index) names another millisecond
/// timestamp column of the input, is indistinguishable from a correct one from here: the node sees
/// one timestamp column and one constant name, so picking them is the caller's obligation.
#[derive(Clone, Debug)]
pub struct FusedSeriesBinding {
    /// The raw series schema the tree's input node was bound to, without an evaluation grid.
    pub series_schema: Arc<SeriesSchema>,
    /// The statement's grid, which is the grid every evaluated chunk is addressed on.
    pub grid: Arc<EvalGrid>,
    /// The metric name every input series is bound to, when the input reads one named metric.
    ///
    /// The name is a binding rather than a column because a raw series chunk carries one metric
    /// name for all of its samples; a name that varied inside a series would not be one series.
    pub metric_name: Option<Arc<str>>,
    /// The input column holding the timestamps.
    pub time_index: String,
    /// The logical tag names the output carries, read from each series' labels.
    ///
    /// [`METRIC_NAME`] is special: it is resolved to the bound metric name instead of a column,
    /// which is how a reader sees that a range function dropped it.
    pub tags: Vec<String>,
}

impl FusedSeriesBinding {
    /// Whether `other` binds the same fields under the same names.
    fn same_fields(&self, other: &SeriesSchema) -> bool {
        self.series_schema.fields.len() == other.fields.len()
            && self
                .series_schema
                .fields
                .iter()
                .zip(other.fields.iter())
                .all(|(mine, theirs)| mine.name == theirs.name)
    }
}

impl PartialEq for FusedSeriesBinding {
    fn eq(&self, other: &Self) -> bool {
        // The schemas are compared by the field names they declare, and the grids by their spec:
        // neither type has structural equality, and neither identity nor allocation is meant here.
        self.metric_name == other.metric_name
            && self.time_index == other.time_index
            && self.tags == other.tags
            && self.grid.spec() == other.grid.spec()
            && self.same_fields(&other.series_schema)
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
        for field in self.series_schema.fields.iter() {
            field.name.hash(state);
        }
    }
}

impl PartialOrd for FusedSeriesBinding {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // A total order over the same fields equality reads, so equal bindings compare equal.
        match self
            .metric_name
            .as_deref()
            .partial_cmp(&other.metric_name.as_deref())
        {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_index.partial_cmp(&other.time_index) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        match self.tags.partial_cmp(&other.tags) {
            Some(Ordering::Equal) => {}
            ord => return ord,
        }
        let (mine, theirs) = (self.grid.spec(), other.grid.spec());
        (mine.start_ms, mine.step_ms, mine.len).partial_cmp(&(
            theirs.start_ms,
            theirs.step_ms,
            theirs.len,
        ))
    }
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
    /// The plan producing the input series.
    input: LogicalPlan,
    /// The columns this node produces.
    output_schema: DFSchemaRef,
}

impl FusedSeries {
    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "FusedSeries"
    }

    /// Creates the node, checking that `tree` is legal against `binding`.
    pub fn try_new(
        promql: Arc<str>,
        tree: NativeTree,
        binding: FusedSeriesBinding,
        input: LogicalPlan,
        output_schema: DFSchemaRef,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding)?;
        Ok(Self {
            promql,
            tree,
            binding,
            input,
            output_schema,
        })
    }

    /// The PromQL text this node was compiled from.
    pub fn promql(&self) -> &str {
        &self.promql
    }

    /// The compiled tree this node runs.
    pub fn tree(&self) -> &NativeTree {
        &self.tree
    }

    /// What the tree's input was compiled against.
    pub fn binding(&self) -> &FusedSeriesBinding {
        &self.binding
    }

    /// The physical node that runs `exec_input` as this node's input.
    ///
    /// Building it rejects a tree the runtime cannot execute and a schema the conversion cannot
    /// classify, so a plan that is built is a plan that runs.
    pub fn to_execution_plan(
        &self,
        exec_input: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        Ok(Arc::new(FusedSeriesExec::try_new(
            Arc::clone(&self.promql),
            Arc::new(self.tree.clone()),
            Arc::new(self.binding.clone()),
            exec_input,
            output_schema,
        )?))
    }
}

impl UserDefinedLogicalNodeCore for FusedSeries {
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
        // The tree's operands are bound inside it; there is no expression for an optimizer to
        // rewrite separately from the tree that owns it.
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "FusedSeries: {}", self.promql)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "FusedSeries should have at exact one input".to_string(),
            ));
        }

        let input = inputs.pop().unwrap();
        Ok(Self {
            promql: Arc::clone(&self.promql),
            tree: self.tree.clone(),
            binding: self.binding.clone(),
            input,
            output_schema: Arc::clone(&self.output_schema),
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
            && self.binding == other.binding
            && self.input == other.input
            && self.output_schema == other.output_schema
    }
}

impl Eq for FusedSeries {}

impl Hash for FusedSeries {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.promql.hash(state);
        self.binding.hash(state);
        self.input.hash(state);
        self.output_schema.hash(state);
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
        self.input.partial_cmp(&other.input)
    }
}

/// Checks that `tree` reads the schema `binding` declares, on the grid it declares.
///
/// The output grid is compared by [`EvalGrid::spec`], not by allocation: that is the rule
/// [`NativeTree`]'s own validation and the kernels apply, and a chunk's timestamps already come
/// from its absolute grid positions, so a separately allocated grid with the same spec is the same
/// grid to everything downstream.
fn validate_binding(tree: &NativeTree, binding: &FusedSeriesBinding) -> DataFusionResult<()> {
    if binding.series_schema.eval_grid.is_some() {
        return Err(DataFusionError::Plan(
            "FusedSeries binds a raw series schema, which cannot have an evaluation grid"
                .to_string(),
        ));
    }
    if binding.tags.iter().any(|tag| tag == METRIC_NAME) && binding.metric_name.is_none() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries declares a {METRIC_NAME:?} tag column but binds no metric name"
        )));
    }

    // The tree's own input node says which schema it reads, so the binding is checked against the
    // tree rather than trusted: a mismatch would evaluate one schema and label the result as
    // another.
    let mut sources = 0;
    let mut bound = false;
    metrics::walk(tree.root(), &mut |node, _| {
        let NativeExpr::Input(input) = &node.expr else {
            return;
        };
        sources += 1;
        if let Some(schema) = input.output.samples() {
            // Raw, not evaluated: this node feeds the tree from a scan, so a tree whose input was
            // compiled against an already-evaluated schema would read the wrong thing.
            bound = binding.same_fields(schema) && schema.eval_grid.is_none();
        }
    });
    if sources != 1 {
        return Err(DataFusionError::Plan(format!(
            "FusedSeries runs a tree with {sources} inputs, and it needs exactly one"
        )));
    }
    if !bound {
        return Err(DataFusionError::Plan(
            "FusedSeries binds a raw series schema the tree's input was not compiled against"
                .to_string(),
        ));
    }

    match tree.root().output.samples() {
        Some(schema)
            if schema
                .eval_grid
                .as_ref()
                .is_some_and(|grid| grid.spec() == binding.grid.spec()) =>
        {
            Ok(())
        }
        _ => Err(DataFusionError::Plan(
            "FusedSeries runs a tree whose output is not evaluated on the statement's grid"
                .to_string(),
        )),
    }
}

/// Where one output column of [`FusedSeriesExec`] comes from.
#[derive(Clone, Copy, Debug)]
enum OutputColumn {
    /// The evaluation grid timestamps of the current chunk.
    Time,
    /// Series field by index in the bound series schema.
    Field(usize),
    /// Declared logical tag by index in the caller-supplied tag list.
    Tag(usize),
}

/// Classifies every output column, rejecting anything the conversion cannot produce.
fn classify_output(
    output_schema: &SchemaRef,
    series_schema: &SeriesSchema,
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
        if series_schema
            .fields
            .iter()
            .any(|field| field.name.as_ref() == tag)
        {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec clashes with a series field"
            )));
        }
    }

    let mut time_seen = false;
    let mut field_seen = vec![false; series_schema.fields.len()];
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
        if let Some(index) = series_schema
            .fields
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
                series_schema.fields[index].name
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

/// Maps the input columns this node reads onto the raw-series roles it needs.
///
/// The input has to be exactly the columns the binding accounts for: the time column, the fields
/// and the tags. A column outside them is part of some series' identity that the raw chunks would
/// not carry, so two rows a Prometheus query keeps apart could be answered as one series with the
/// wrong labels; such an input is refused rather than read past. Whether a *different* column of
/// the same type was meant is not something this function can see — that is the caller commitment
/// [`FusedSeriesBinding`] documents.
fn input_mapping(
    binding: &FusedSeriesBinding,
    schema: &SchemaRef,
) -> DataFusionResult<InputMapping> {
    if schema.column_with_name(METRIC_NAME).is_some() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeriesExec cannot read a {METRIC_NAME:?} column: a raw series' metric name is \
             bound on the node, not varied inside one series"
        )));
    }
    let index_of = |name: &str| -> DataFusionResult<usize> {
        schema.index_of(name).map_err(|_| {
            DataFusionError::Plan(format!("FusedSeriesExec input has no column {name:?}"))
        })
    };

    // Every column of the input has to be a role the binding reads. The binding cannot express
    // "ignore this column": a column it does not read is one the raw series loses.
    for field in schema.fields() {
        let name = field.name();
        let read = name == binding.time_index.as_str()
            || binding.tags.iter().any(|tag| tag == name)
            || binding
                .series_schema
                .fields
                .iter()
                .any(|spec| spec.name.as_ref() == name);
        if !read {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec input column {name:?} is neither the time column {:?} nor one of \
                 the fields and tags its binding maps, so it would take no part in the series' \
                 identity",
                binding.time_index
            )));
        }
    }

    let tags = binding
        .tags
        .iter()
        .filter(|tag| tag.as_str() != METRIC_NAME)
        .map(|tag| Ok((Arc::from(tag.as_str()), index_of(tag)?)))
        .collect::<DataFusionResult<Vec<(Arc<str>, usize)>>>()?;
    let fields = binding
        .series_schema
        .fields
        .iter()
        .map(|spec: &FieldSpec| {
            let index = index_of(&spec.name)?;
            // A native-histogram column would reach the output converter as a histogram sample,
            // which only fails there, once execution has started. This node answers in floats, so
            // the column it reads has to be one.
            if schema.field(index).data_type() != &DataType::Float64 {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeriesExec reads field {:?} as Float64, and the input column is {}",
                    spec.name,
                    schema.field(index).data_type()
                )));
            }
            Ok((spec.clone(), index))
        })
        .collect::<DataFusionResult<Vec<(FieldSpec, usize)>>>()?;
    let mapping = InputMapping {
        time_index: index_of(&binding.time_index)?,
        metric_name: binding.metric_name.clone(),
        tags,
        fields,
    };
    // The shared converter is the authority on which column types it accepts, so it is asked
    // rather than copied: handing it an empty stream of this schema makes it validate the mapping
    // with no batch to convert.
    validate_mapping_types(&mapping, schema)?;
    Ok(mapping)
}

/// Rejects a mapping the shared converter would refuse, naming the column at fault.
///
/// The types accepted here are the converter's own, not a copy of its rules: [`converter_accepts`]
/// runs the converter's validation, so a time or tag type this node accepts is a type `execute`
/// converts, and one it refuses can only be refused there too.
fn validate_mapping_types(mapping: &InputMapping, schema: &SchemaRef) -> DataFusionResult<()> {
    let Err(error) = converter_accepts(mapping, schema) else {
        return Ok(());
    };
    // The converter reports the mapping as a whole, so the column at fault is found by asking it
    // again with the time column alone and then with one tag at a time. The field columns are
    // already checked above, and they are the only other columns this mapping maps.
    let mut without_tags = mapping.clone();
    let tags = std::mem::take(&mut without_tags.tags);
    if converter_accepts(&without_tags, schema).is_err() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeriesExec cannot read {:?} as its time column: {error}",
            schema.field(mapping.time_index).name()
        )));
    }
    for tag in tags {
        let mut one = without_tags.clone();
        one.tags = vec![tag.clone()];
        if converter_accepts(&one, schema).is_err() {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec cannot read tag {:?} from its input column: {error}",
                tag.0
            )));
        }
    }
    // Something the converter refuses that no single role accounts for: its own error says it.
    Err(error)
}

/// Whether the shared converter validates `mapping` against `schema`.
fn converter_accepts(mapping: &InputMapping, schema: &SchemaRef) -> DataFusionResult<()> {
    record_batches_to_raw_series(
        Box::pin(EmptyRecordBatchStream::new(Arc::clone(schema))),
        mapping.clone(),
    )
    .map(|_| ())
}

/// The physical node that runs a compiled native tree over its input's raw series.
pub struct FusedSeriesExec {
    /// The PromQL text the tree was compiled from, which is what the plan line shows.
    promql: Arc<str>,
    /// The tree this node runs, shared by every partition's execution.
    tree: Arc<NativeTree>,
    /// What the tree's input was compiled against, and what the output columns are called.
    binding: Arc<FusedSeriesBinding>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The plan producing the raw series.
    input: Arc<dyn ExecutionPlan>,
    /// The columns this node produces.
    output_schema: SchemaRef,
    /// Per-node metrics, registered by each execution of the tree.
    metric: ExecutionPlanMetricsSet,
    /// This node's partitioning, emission and boundedness.
    properties: Arc<PlanProperties>,
}

impl FusedSeriesExec {
    /// Creates the node, rejecting whatever this node cannot execute or convert.
    pub fn try_new(
        promql: Arc<str>,
        tree: Arc<NativeTree>,
        binding: Arc<FusedSeriesBinding>,
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> DataFusionResult<Self> {
        validate_binding(&tree, &binding)?;
        // Building a state is what proves the runtime executes this tree: a node kind it does not
        // run yet is refused here, so a plan that is built is a plan that starts. The throwaway
        // state is dropped at once, returning its (unbounded) reservations.
        let _ = NativeExecutionState::try_new(
            &tree,
            Arc::new(UnboundedMemoryPool::default()),
            &ExecutionPlanMetricsSet::new(),
            0,
        )?;
        let classification = classify_output(
            &output_schema,
            &binding.series_schema,
            &binding.tags,
            &binding.time_index,
        )?;
        // The mapping is checked against the declared input schema, so a column this node reads
        // but the input does not produce is reported when the plan is built.
        input_mapping(&binding, &input.schema())?;
        // One window holds the state of the series it is reading, and a raw series arrives as one
        // contiguous run. The scan guarantees that per partition, not across them, so more than
        // one partition could split a series between two trees that each answer half of it.
        if input.properties().partitioning.partition_count() != 1 {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec reads one partition of raw series, and its input has {}",
                input.properties().partitioning.partition_count()
            )));
        }

        let input_properties = input.properties();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            input_properties.partitioning.clone(),
            input_properties.emission_type,
            input_properties.boundedness,
        ));
        Ok(Self {
            promql,
            tree,
            binding,
            classification: classification.into(),
            input,
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
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
        write!(f, "FusedSeriesExec: {}\n{}", self.promql, self.tree)
    }
}

impl ExecutionPlan for FusedSeriesExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
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
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Plan(format!(
                "FusedSeriesExec requires exactly one child, got {}",
                children.len()
            ))
        })?;
        // Rebuilt rather than patched: the new child's schema is what the input mapping is checked
        // against, so a child that changed shape is refused instead of read with stale indices.
        Ok(Arc::new(Self::try_new(
            Arc::clone(&self.promql),
            Arc::clone(&self.tree),
            Arc::clone(&self.binding),
            child,
            self.output_schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let mapping = input_mapping(&self.binding, &self.input.schema())?;
        // Batches become raw series chunks through the shared frame converter, which is what groups
        // rows into one chunk per label set and closes each series exactly once.
        let input = record_batches_to_raw_series(input, mapping)?;
        let state = NativeExecutionState::try_new(
            &self.tree,
            Arc::clone(&context.runtime_env().memory_pool),
            &self.metric,
            partition,
        )?;
        Ok(Box::pin(FusedSeriesStream {
            input: Some(input),
            executor: Some(NativeExecutor::new(state)),
            classification: Arc::clone(&self.classification),
            binding: Arc::clone(&self.binding),
            output_schema: self.output_schema.clone(),
            ready: VecDeque::new(),
            done: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }
}

/// Converts one evaluated chunk into one output batch.
///
/// The time column is the chunk's absolute grid interval, missing samples are nulls and ordinary
/// NaN stays a value.
fn series_to_record_batch(
    classification: &[OutputColumn],
    tags: &[String],
    output_schema: &SchemaRef,
    chunk: &EvaluatedChunk,
) -> DataFusionResult<RecordBatch> {
    let series = &chunk.series;
    let range = chunk.range();
    let rows = range.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(classification.len());
    for (position, column) in classification.iter().enumerate() {
        match column {
            OutputColumn::Time => columns.push(Arc::new(TimestampMillisecondArray::new(
                range.timestamps(),
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

/// The batches one execution of [`FusedSeriesExec`] yields.
pub struct FusedSeriesStream {
    /// The input batches, read as raw series chunks, or `None` once the stream ended in an error.
    ///
    /// The converter behind it holds its current batch, the chunk it is building and its own
    /// source, so a terminated stream that kept it alive would hold upstream resources until the
    /// consumer drops the stream.
    input: Option<SendableRawSeriesStream>,
    /// The execution of this node's tree, or `None` once it has been closed.
    executor: Option<NativeExecutor>,
    /// Where each output column comes from.
    classification: Arc<[OutputColumn]>,
    /// The tag column names, indexed by `OutputColumn::Tag`.
    binding: Arc<FusedSeriesBinding>,
    /// The schema of every batch this stream yields.
    output_schema: SchemaRef,
    /// The batches the executor settled since the last poll of the input.
    ready: VecDeque<RecordBatch>,
    /// Whether the input ended and everything it settled has been handed out.
    done: bool,
}

impl FusedSeriesStream {
    /// Hands one raw chunk over and queues what it settled.
    fn push(&mut self, chunk: RawSeriesChunk) -> DataFusionResult<()> {
        let Some(executor) = self.executor.as_mut() else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec stream pushed a chunk after its execution closed".to_string(),
            ));
        };
        executor.push_raw(chunk)?;
        let mut settled = Vec::new();
        while let Some(chunk) = executor.next_chunk()? {
            settled.push(chunk);
        }
        self.convert(settled)
    }

    /// Ends the input and queues what closing settled.
    fn close(&mut self) -> DataFusionResult<()> {
        let Some(mut executor) = self.executor.take() else {
            return Ok(());
        };
        self.convert(executor.finish()?)
    }

    /// Ends the stream in an error, releasing the execution and the input at once.
    ///
    /// An ordinary drop of the stream releases all of this as well, so this is not about the last
    /// release: it is about a consumer that keeps the terminated stream, which would otherwise
    /// hold the evaluator, the converter's held batch and chunk, and the plan upstream of it.
    fn fail(&mut self) {
        self.done = true;
        self.executor = None;
        self.input = None;
        // A batch queued before the error was settled by a run that has now stopped.
        self.ready.clear();
    }

    /// Converts the chunks the executor settled into batches, in the order it produced them.
    fn convert(&mut self, chunks: Vec<EvaluatedChunk>) -> DataFusionResult<()> {
        for chunk in &chunks {
            let batch = series_to_record_batch(
                &self.classification,
                &self.binding.tags,
                &self.output_schema,
                chunk,
            )?;
            self.ready.push_back(batch);
        }
        Ok(())
    }
}

impl RecordBatchStream for FusedSeriesStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for FusedSeriesStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DataFusionResult<RecordBatch>>> {
        let this = &mut *self;
        loop {
            if let Some(batch) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if this.done {
                return Poll::Ready(None);
            }
            // An error is the only thing that clears the input, and it ends the stream with it.
            let Some(input) = this.input.as_mut() else {
                return Poll::Ready(None);
            };
            let polled = input.as_mut().poll_next(cx);
            match polled {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    // The input ended, so every series it opened has to be closed: what closing
                    // settles is handed out before the stream ends.
                    this.done = true;
                    if let Err(error) = this.close() {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    // An input error stops the execution: a stopped evaluator must not be reused.
                    this.fail();
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                }
                Poll::Ready(Some(Ok(chunk))) => {
                    if let Err(error) = this.push(chunk) {
                        this.fail();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
        }
    }
}
