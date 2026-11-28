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

//! Raw series frames built from the ordinary rows of a table whose engine has no raw series scanner.
//!
//! Such an engine returns rows in storage order, with no per-series grouping and no deduplication.
//! [`RowSeriesSourceExec`] requires its input hash-partitioned by the tags and sorted by
//! `(tags, time index)`, which the physical optimizer provides with a `RepartitionExec`
//! and a `SortExec` that spills under memory pressure, and cuts the sorted rows into series chunks
//! that carry their complete labels. Numeric fields are read as `Float64` and native histogram
//! fields as histogram samples. Timestamps are handed over in the time index's own unit; the
//! consumer is bound to that unit (`crate::raw_time`).
//!
//! A raw series holds one sample per timestamp, as Prometheus storage does. Each field is its own
//! value series, so rows that share labels and a timestamp are reduced field by field: each field
//! keeps its lowest non-null value among those rows, with NaN above every number and a native
//! histogram ordered as its struct. A field that is null on every such row stays null. The rule
//! only depends on the rows, not on the files or the order they were read in.

use std::collections::VecDeque;
use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::native_histogram::{histogram_samples_from_arrow, native_histogram_arrow_type};
use common_query::series_frame::{
    PendingBatching, pack_raw_series_with, raw_series_frame_schema_with_ordering,
};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, StringArray, UInt32Array, make_comparator,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::compute::{cast, concat_batches, take};
use datafusion::arrow::datatypes::{DataType, Float64Type, Int64Type, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::Column as ColumnExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datatypes::compute::SortOptions;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
use datatypes::timestamp::timestamp_array_to_primitive;
use futures::{Stream, StreamExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream};

/// Each output partition hands over one series after another. The key order is the sort's, which
/// is not the order of any storage key, so nothing stronger is declared.
const ORDERING: SeriesOrdering = SeriesOrdering::Contiguous;

/// The label the metric name is reported under; it is never a column of the rows.
const METRIC_NAME: &str = "__name__";

/// The logical node: rows of one table bound to one metric's raw series.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RowSeriesSource {
    metric_name: Arc<str>,
    /// The tag columns that name a series; never `__name__`, which is the metric name.
    tags: Vec<String>,
    time_index: String,
    /// The field columns each frame carries, in the order the tree reading them was compiled with.
    fields: Vec<String>,
    input: LogicalPlan,
    schema: DFSchemaRef,
}

impl RowSeriesSource {
    pub const fn name() -> &'static str {
        "RowSeriesSource"
    }

    pub fn try_new(
        metric_name: Arc<str>,
        tags: Vec<String>,
        time_index: String,
        fields: Vec<String>,
        input: LogicalPlan,
    ) -> DataFusionResult<Self> {
        if tags.iter().any(|tag| tag == METRIC_NAME) {
            return Err(DataFusionError::Plan(format!(
                "RowSeriesSource for {metric_name:?} reads a tag column named {METRIC_NAME}, which \
                 is the metric name rather than a label"
            )));
        }
        let schema = Arc::new(DFSchema::try_from(
            frame_schema(&series_schema(&fields))?.as_ref().clone(),
        )?);
        Ok(Self {
            metric_name,
            tags,
            time_index,
            fields,
            input,
            schema,
        })
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    /// The physical node over `input`, which yields the rows this node was planned over.
    ///
    /// The node declares the distribution and ordering it reads, so the physical optimizer puts
    /// the hash repartition and the sort below it.
    pub fn to_execution_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RowSeriesSourceExec::try_new(
            input,
            Arc::clone(&self.metric_name),
            self.tags.clone(),
            self.time_index.clone(),
            self.fields.clone(),
        )?))
    }
}

fn series_schema(fields: &[String]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(name.as_str()),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}

fn frame_schema(series: &SeriesSchema) -> DataFusionResult<SchemaRef> {
    raw_series_frame_schema_with_ordering(series, &RawLabelSource::Explicit, ORDERING)
}

impl PartialOrd for RowSeriesSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // The schema is derived from the fields, so it adds nothing to the order.
        (
            &self.metric_name,
            &self.tags,
            &self.time_index,
            &self.fields,
        )
            .partial_cmp(&(
                &other.metric_name,
                &other.tags,
                &other.time_index,
                &other.fields,
            ))
            .and_then(|ordering| match ordering {
                std::cmp::Ordering::Equal => self.input.partial_cmp(&other.input),
                ordering => Some(ordering),
            })
    }
}

impl UserDefinedLogicalNodeCore for RowSeriesSource {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "RowSeriesSource: metric={}, tags=[{}], time_index={}, fields=[{}]",
            self.metric_name,
            self.tags.join(", "),
            self.time_index,
            self.fields.join(", ")
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "RowSeriesSource should have exactly one input".to_string(),
            ));
        }
        Ok(Self {
            metric_name: Arc::clone(&self.metric_name),
            tags: self.tags.clone(),
            time_index: self.time_index.clone(),
            fields: self.fields.clone(),
            input: inputs.swap_remove(0),
            schema: Arc::clone(&self.schema),
        })
    }
}

/// Positions of the columns a row series reads, in the input schema.
#[derive(Debug, Clone)]
struct RowColumns {
    tags: Vec<usize>,
    time: usize,
    fields: Vec<usize>,
}

impl RowColumns {
    fn resolve(
        schema: &SchemaRef,
        tags: &[String],
        time_index: &str,
        fields: &[String],
    ) -> DataFusionResult<Self> {
        let index_of = |name: &str| {
            schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "RowSeriesSource input has no column {name:?}: {schema:?}"
                ))
            })
        };
        let tags = tags
            .iter()
            .map(|tag| index_of(tag))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let time = index_of(time_index)?;
        if !matches!(schema.field(time).data_type(), DataType::Timestamp(_, _)) {
            return Err(DataFusionError::Plan(format!(
                "RowSeriesSource time index {time_index:?} is {}, not a timestamp",
                schema.field(time).data_type()
            )));
        }
        let fields = fields
            .iter()
            .map(|field| {
                let index = index_of(field)?;
                let data_type = schema.field(index).data_type();
                if data_type.is_integer() || data_type.is_floating() || is_histogram(data_type) {
                    Ok(index)
                } else {
                    Err(DataFusionError::Plan(format!(
                        "RowSeriesSource field {field:?} is {data_type}, not a float or a native histogram"
                    )))
                }
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(Self { tags, time, fields })
    }

    fn column(schema: &SchemaRef, index: usize) -> Arc<dyn PhysicalExpr> {
        Arc::new(ColumnExpr::new(schema.field(index).name(), index))
    }

    fn tag_exprs(&self, schema: &SchemaRef) -> Vec<Arc<dyn PhysicalExpr>> {
        self.tags
            .iter()
            .map(|index| Self::column(schema, *index))
            .collect()
    }

    /// `(tags ASC, time ASC)`: series are contiguous and time is ascending within each, so the rows
    /// of a duplicated timestamp are adjacent.
    fn ordering(&self, schema: &SchemaRef) -> DataFusionResult<LexOrdering> {
        let key = |index: usize, nulls_first: bool| {
            PhysicalSortExpr::new(
                Self::column(schema, index),
                SortOptions {
                    descending: false,
                    nulls_first,
                },
            )
        };
        let exprs = self
            .tags
            .iter()
            .map(|index| key(*index, true))
            .chain(std::iter::once(key(self.time, true)))
            .collect::<Vec<_>>();
        LexOrdering::new(exprs)
            .ok_or_else(|| DataFusionError::Internal("empty RowSeriesSource ordering".into()))
    }
}

/// Whether a field column holds native histograms, which are passed on as histogram samples.
fn is_histogram(data_type: &DataType) -> bool {
    *data_type == native_histogram_arrow_type()
}

/// The physical node: cuts rows sorted by [`RowColumns::ordering`] into explicit-label raw series.
#[derive(Debug)]
pub struct RowSeriesSourceExec {
    input: Arc<dyn ExecutionPlan>,
    metric_name: Arc<str>,
    tags: Vec<String>,
    time_index: String,
    fields: Vec<String>,
    columns: RowColumns,
    /// The order `input` has to arrive in, over `input`'s schema.
    ordering: LexOrdering,
    series_schema: Arc<SeriesSchema>,
    properties: Arc<PlanProperties>,
}

impl RowSeriesSourceExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        metric_name: Arc<str>,
        tags: Vec<String>,
        time_index: String,
        fields: Vec<String>,
    ) -> DataFusionResult<Self> {
        let columns = RowColumns::resolve(&input.schema(), &tags, &time_index, &fields)?;
        let ordering = columns.ordering(&input.schema())?;
        let series_schema = series_schema(&fields);
        let frame_schema = frame_schema(&series_schema)?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(frame_schema),
            Partitioning::UnknownPartitioning(input.properties().partitioning.partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            metric_name,
            tags,
            time_index,
            fields,
            columns,
            ordering,
            series_schema,
            properties,
        })
    }
}

impl DisplayAs for RowSeriesSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "RowSeriesSourceExec: metric={}, tags=[{}], fields=[{}]",
            self.metric_name,
            self.tags.join(", "),
            self.fields.join(", ")
        )
    }
}

impl ExecutionPlan for RowSeriesSourceExec {
    fn name(&self) -> &str {
        "RowSeriesSourceExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// Every row of a series has to reach the partition that cuts it.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        let schema = self.input.schema();
        let distribution = if self.columns.tags.is_empty() {
            Distribution::SinglePartition
        } else {
            Distribution::KeyPartitioned(self.columns.tag_exprs(&schema))
        };
        InputDistributionRequirements::new(vec![distribution])
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.ordering.clone()))]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "RowSeriesSourceExec requires one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::try_new(
            children.swap_remove(0),
            Arc::clone(&self.metric_name),
            self.tags.clone(),
            self.time_index.clone(),
            self.fields.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let rows = self.input.execute(partition, context)?;
        let tag_names = self
            .tags
            .iter()
            .map(|tag| Arc::from(tag.as_str()))
            .collect();
        pack_raw_series_with(
            Box::pin(RowSeriesStream {
                input: Some(rows),
                columns: self.columns.clone(),
                metric_name: Arc::clone(&self.metric_name),
                tag_names,
                schema: Arc::clone(&self.series_schema),
                open: None,
                ready: VecDeque::new(),
                tail: None,
            }),
            PendingBatching::Immediate,
        )
    }
}

/// The series the stream is cutting.
struct OpenSeries {
    /// The tag values of the series, in tag order.
    values: Vec<String>,
    primary_key: Buffer,
    labels: SeriesLabels,
    last_time: Option<i64>,
    /// The latest chunk of this series. It is published once the stream knows whether another
    /// chunk follows, which decides its `is_last`.
    held: Option<RawSeriesChunk>,
}

impl OpenSeries {
    fn try_new(
        metric_name: &Arc<str>,
        tag_names: &[Arc<str>],
        tags: &[StringArray],
        row: usize,
    ) -> DataFusionResult<Self> {
        let values = tags
            .iter()
            .map(|tag| tag.value(row).to_string())
            .collect::<Vec<_>>();
        let mut key = Vec::new();
        for value in &values {
            key.extend_from_slice(&(value.len() as u64).to_le_bytes());
            key.extend_from_slice(value.as_bytes());
        }
        // An empty label value is an absent label in Prometheus.
        let labels = tag_names
            .iter()
            .zip(&values)
            .filter(|(_, value)| !value.is_empty())
            .map(|(name, value)| Label {
                name: Arc::clone(name),
                value: Arc::from(value.as_str()),
            })
            .collect();
        let labels = SeriesLabels::try_new(Some(Arc::clone(metric_name)), labels)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Self {
            values,
            primary_key: Buffer::from_vec(key),
            labels,
            last_time: None,
            held: None,
        })
    }

    fn holds(&self, tags: &[StringArray], row: usize) -> bool {
        self.values
            .iter()
            .zip(tags)
            .all(|(value, tag)| value == tag.value(row))
    }
}

/// Cuts one sorted partition into raw series chunks, one chunk per series per input batch.
struct RowSeriesStream {
    input: Option<SendableRecordBatchStream>,
    columns: RowColumns,
    metric_name: Arc<str>,
    tag_names: Vec<Arc<str>>,
    schema: Arc<SeriesSchema>,
    open: Option<OpenSeries>,
    ready: VecDeque<RawSeriesChunk>,
    /// The rows of the last `(labels, timestamp)` of the previous batch, held back because the next
    /// batch may hold more rows of that timestamp.
    tail: Option<RecordBatch>,
}

impl RowSeriesStream {
    /// Cuts `batch`, after the rows held back from the previous one. Unless `finished`, the rows of
    /// its last `(labels, timestamp)` are held back in turn.
    fn cut(&mut self, batch: RecordBatch, finished: bool) -> DataFusionResult<()> {
        let batch = match self.tail.take() {
            Some(tail) => concat_batches(&batch.schema(), [&tail, &batch])?,
            None => batch,
        };
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let tags = self
            .columns
            .tags
            .iter()
            .map(|index| {
                let tag = cast(batch.column(*index), &DataType::Utf8)?
                    .as_string::<i32>()
                    .clone();
                // The planner reads a null tag as `''`; both are one series and must sort together.
                if tag.null_count() > 0 {
                    return Err(DataFusionError::Execution(
                        "RowSeriesSource tag holds a null".into(),
                    ));
                }
                Ok(tag)
            })
            .collect::<DataFusionResult<Vec<StringArray>>>()?;
        let (time, _) =
            timestamp_array_to_primitive(batch.column(self.columns.time)).ok_or_else(|| {
                DataFusionError::Execution("RowSeriesSource time index is not a timestamp".into())
            })?;
        if time.null_count() > 0 {
            return Err(DataFusionError::Execution(
                "RowSeriesSource time index holds a null".into(),
            ));
        }
        let fields = self
            .columns
            .fields
            .iter()
            .map(|index| {
                let field = batch.column(*index);
                if is_histogram(field.data_type()) {
                    Ok(Arc::clone(field))
                } else {
                    cast(field, &DataType::Float64).map_err(Into::into)
                }
            })
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
        let order = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let comparators = fields
            .iter()
            .map(|field| make_comparator(field.as_ref(), field.as_ref(), order))
            .collect::<Result<Vec<_>, _>>()?;
        let same_sample = |left: usize, right: usize| {
            time.value(left) == time.value(right)
                && tags.iter().all(|tag| tag.value(left) == tag.value(right))
        };
        let mut end = rows;
        if !finished {
            while end > 0 && same_sample(end - 1, rows - 1) {
                end -= 1;
            }
        }

        let mut kept = Vec::new();
        let mut field_rows = vec![Vec::new(); fields.len()];
        let mut row = 0;
        while row < end {
            let time_value = time.value(row);
            if !self
                .open
                .as_ref()
                .is_some_and(|open| open.holds(&tags, row))
            {
                self.publish(&kept, &field_rows, &time, &fields)?;
                kept.clear();
                field_rows.iter_mut().for_each(Vec::clear);
                self.close();
                self.open = Some(OpenSeries::try_new(
                    &self.metric_name,
                    &self.tag_names,
                    &tags,
                    row,
                )?);
            }
            if let Some(open) = &mut self.open {
                if open.last_time.is_some_and(|last| last >= time_value) {
                    return Err(DataFusionError::Execution(
                        "RowSeriesSource input is not sorted by time within a series".into(),
                    ));
                }
                open.last_time = Some(time_value);
            }
            let mut run_end = row + 1;
            while run_end < end && same_sample(row, run_end) {
                run_end += 1;
            }
            kept.push(row as u32);
            for (compare, picked) in comparators.iter().zip(field_rows.iter_mut()) {
                // Nulls sort last, so the lowest row is a null only when every row is.
                let lowest = (row + 1..run_end).fold(row, |lowest, candidate| {
                    if compare(candidate, lowest).is_lt() {
                        candidate
                    } else {
                        lowest
                    }
                });
                picked.push(lowest as u32);
            }
            row = run_end;
        }
        if end < rows {
            self.tail = Some(batch.slice(end, rows - end));
        }
        self.publish(&kept, &field_rows, &time, &fields)
    }

    /// Makes the samples at the rows `kept` of the current batch the open series' latest chunk;
    /// `field_rows` names the row each field's value of those samples is read from.
    fn publish(
        &mut self,
        kept: &[u32],
        field_rows: &[Vec<u32>],
        time: &Int64Array,
        fields: &[ArrayRef],
    ) -> DataFusionResult<()> {
        let Some(open) = &mut self.open else {
            return Ok(());
        };
        if kept.is_empty() {
            return Ok(());
        }
        let indices = UInt32Array::from(kept.to_vec());
        let time = take(time, &indices, None)?;
        let time = RawTimestamps::try_new(time.as_primitive::<Int64Type>().values().clone())
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let samples = fields
            .iter()
            .zip(field_rows)
            .map(|(field, rows)| {
                let taken = take(field, &UInt32Array::from(rows.clone()), None)?;
                if is_histogram(taken.data_type()) {
                    histogram_samples_from_arrow(taken.as_struct())
                } else {
                    Ok(Samples::from_float(
                        taken.as_primitive::<Float64Type>().clone(),
                    ))
                }
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let fields = SeriesFields::try_from_vec(samples)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let chunk = RawSeriesChunk::try_new(
            &self.schema,
            open.primary_key.clone(),
            Some(open.labels.clone()),
            time,
            fields,
            false,
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if let Some(previous) = open.held.replace(chunk) {
            self.ready.push_back(previous);
        }
        Ok(())
    }

    /// Ends the open series: its held chunk is its last one.
    fn close(&mut self) {
        if let Some(mut last) = self.open.take().and_then(|open| open.held) {
            last.is_last = true;
            self.ready.push_back(last);
        }
    }
}

fn boxed(error: DataFusionError) -> BoxedError {
    BoxedError::new(common_query::error::Error::from(error))
}

impl Stream for RowSeriesStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(chunk) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(chunk)));
            }
            let Some(input) = this.input.as_mut() else {
                return Poll::Ready(None);
            };
            match input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(error) = this.cut(batch, false) {
                        this.input = None;
                        return Poll::Ready(Some(Err(boxed(error))));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    this.input = None;
                    return Poll::Ready(Some(Err(boxed(error))));
                }
                Poll::Ready(None) => {
                    this.input = None;
                    if let Some(tail) = this.tail.take()
                        && let Err(error) = this.cut(tail, true)
                    {
                        return Poll::Ready(Some(Err(boxed(error))));
                    }
                    this.close();
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RawSeriesStream for RowSeriesStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &RawLabelSource::Explicit
    }

    fn label_decode_spec(&self) -> Option<&datatypes::series::frame::LabelDecodeSpec> {
        None
    }

    fn ordering(&self) -> SeriesOrdering {
        ORDERING
    }
}

#[cfg(test)]
mod tests {
    use common_query::series_frame::unpack_raw_series;
    use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;
    use datatypes::series::samples::SampleRef;
    use futures::TryStreamExt;

    use super::*;

    /// `(host, timestamp, field values)`.
    type Row<'a> = (&'a str, i64, Vec<Option<f64>>);

    /// `(labels, timestamps, values per field, is_last)` per chunk.
    type Chunk = (Vec<(String, String)>, Vec<i64>, Vec<Vec<Option<f64>>>, bool);

    /// Cuts `batches`, already sorted by `(host, ts)`, with one `Float64` field per value column.
    async fn cut(fields: usize, batches: Vec<Vec<Row<'_>>>) -> Vec<Chunk> {
        let field_names = (0..fields)
            .map(|index| format!("f{index}"))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(
            [
                Field::new("host", DataType::Utf8, false),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
            ]
            .into_iter()
            .chain(
                field_names
                    .iter()
                    .map(|name| Field::new(name, DataType::Float64, true)),
            )
            .collect::<Vec<_>>(),
        ));
        let batches = batches
            .iter()
            .map(|rows| {
                let mut columns: Vec<ArrayRef> = vec![
                    Arc::new(StringArray::from(
                        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(TimestampMillisecondArray::from(
                        rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                    )),
                ];
                for field in 0..fields {
                    columns.push(Arc::new(Float64Array::from(
                        rows.iter().map(|row| row.2[field]).collect::<Vec<_>>(),
                    )));
                }
                RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
            })
            .collect::<Vec<_>>();
        let input = MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap();
        let exec = RowSeriesSourceExec::try_new(
            input,
            Arc::from("cpu"),
            vec!["host".to_string()],
            "ts".to_string(),
            field_names,
        )
        .unwrap();
        let frames = exec.execute(0, SessionContext::new().task_ctx()).unwrap();
        unpack_raw_series(frames, None)
            .unwrap()
            .map_ok(|chunk| {
                let labels = chunk.metric.as_ref().unwrap();
                assert_eq!(labels.name(), Some("cpu"));
                let values = chunk
                    .fields
                    .iter()
                    .map(|field| {
                        field
                            .iter()
                            .map(|sample| match sample {
                                SampleRef::Float(value) => Some(value),
                                SampleRef::Missing => None,
                                SampleRef::Histogram(_) => panic!("expected a float sample"),
                            })
                            .collect()
                    })
                    .collect();
                (
                    labels
                        .tags()
                        .iter()
                        .map(|label| (label.name.to_string(), label.value.to_string()))
                        .collect(),
                    chunk.time.as_buffer().to_vec(),
                    values,
                    chunk.is_last,
                )
            })
            .try_collect()
            .await
            .unwrap()
    }

    fn host(value: &str) -> Vec<(String, String)> {
        vec![("host".to_string(), value.to_string())]
    }

    /// A series that continues into the next batch closes only on its last chunk, and a duplicated
    /// timestamp split across two batches keeps one sample.
    #[tokio::test]
    async fn series_close_once_across_batches_and_keep_one_sample_per_timestamp() {
        let chunks = cut(
            1,
            vec![
                vec![("a", 1, vec![Some(10.0)]), ("a", 2, vec![Some(21.0)])],
                vec![
                    ("a", 2, vec![Some(20.0)]),
                    ("a", 3, vec![Some(30.0)]),
                    ("b", 1, vec![Some(5.0)]),
                ],
                vec![("b", 2, vec![Some(6.0)])],
            ],
        )
        .await;
        assert_eq!(
            chunks,
            vec![
                (host("a"), vec![1], vec![vec![Some(10.0)]], false),
                (
                    host("a"),
                    vec![2, 3],
                    vec![vec![Some(20.0), Some(30.0)]],
                    true
                ),
                (host("b"), vec![1], vec![vec![Some(5.0)]], false),
                (host("b"), vec![2], vec![vec![Some(6.0)]], true),
            ]
        );
    }

    /// Each field of a duplicated timestamp keeps its own lowest non-null value: the row the first
    /// field keeps can be null in the second, whose value then comes from another row. NaN sorts
    /// above every number, and the rows span batches.
    #[tokio::test]
    async fn duplicated_timestamps_keep_one_value_per_field() {
        let chunks = cut(
            2,
            vec![
                vec![("a", 1, vec![Some(1.0), None])],
                vec![
                    ("a", 1, vec![Some(2.0), Some(7.0)]),
                    ("a", 2, vec![Some(f64::NAN), Some(1.0)]),
                ],
                vec![("a", 2, vec![Some(3.0), None])],
            ],
        )
        .await;
        assert_eq!(
            chunks,
            vec![
                (
                    host("a"),
                    vec![1],
                    vec![vec![Some(1.0)], vec![Some(7.0)]],
                    false
                ),
                (
                    host("a"),
                    vec![2],
                    vec![vec![Some(3.0)], vec![Some(1.0)]],
                    true
                ),
            ]
        );
    }
}
