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

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use common_telemetry::warn;
use datafusion::arrow::array::{Array, AsArray, StringArray};
use datafusion::arrow::compute::{SortOptions, concat_batches};
use datafusion::arrow::datatypes::{DataType, Float64Type, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, DFSchemaRef, Statistics};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{
    EquivalenceProperties, LexRequirement, OrderingRequirements, PhysicalSortRequirement,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::expressions::{CastExpr as PhyCast, Column as PhyColumn};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PhysicalExpr, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::{Column, Expr};
use datafusion_expr::{EmptyRelation, col};
use datatypes::prelude::{ConcreteDataType, DataType as GtDataType};
use datatypes::value::{OrderedF64, Value, ValueRef};
use datatypes::vectors::{Helper, MutableVector, VectorRef};
use futures::{Stream, StreamExt, ready};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::{resolve_column_name, serialize_column_index};

/// `HistogramFold` will fold the conventional (non-native) histogram ([1]) for later
/// computing.
///
/// Specifically, it will transform the `le` and `field` column into a complex
/// type, and samples on other tag columns:
/// - `le` will become a [ListArray] of [f64]. With each bucket bound parsed
/// - `field` will become a [ListArray] of [f64]
/// - other columns will be sampled every `bucket_num` element, but their types won't change.
///
/// Due to the folding or sampling, the output rows number will become `input_rows` / `bucket_num`.
///
/// # Requirement
/// - Input should be sorted on `<tag list>, ts, le ASC`.
/// - The value set of `le` should be same. I.e., buckets of every series should be same.
///
/// [1]: https://prometheus.io/docs/concepts/metric_types/#histogram
#[derive(Debug, PartialEq, Hash, Eq)]
pub struct HistogramFold {
    /// Name of the `le` column. It's a special column in prometheus
    /// for implementing conventional histogram. It's a string column
    /// with "literal" float value, like "+Inf", "0.001" etc.
    le_column: String,
    ts_column: String,
    input: LogicalPlan,
    field_column: String,
    quantile: OrderedF64,
    output_schema: DFSchemaRef,
    unfix: Option<UnfixIndices>,
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
struct UnfixIndices {
    pub le_column_idx: u64,
    pub ts_column_idx: u64,
    pub field_column_idx: u64,
}

impl UserDefinedLogicalNodeCore for HistogramFold {
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
        if self.unfix.is_some() {
            return vec![];
        }

        let mut exprs = vec![
            col(&self.le_column),
            col(&self.ts_column),
            col(&self.field_column),
        ];
        exprs.extend(self.input.schema().fields().iter().filter_map(|f| {
            let name = f.name();
            if name != &self.le_column && name != &self.ts_column && name != &self.field_column {
                Some(col(name))
            } else {
                None
            }
        }));
        exprs
    }

    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        if self.unfix.is_some() {
            return None;
        }

        let input_schema = self.input.schema();
        let le_column_index = input_schema.index_of_column_by_name(None, &self.le_column)?;

        if output_columns.is_empty() {
            let indices = (0..input_schema.fields().len()).collect::<Vec<_>>();
            return Some(vec![indices]);
        }

        let mut necessary_indices = output_columns
            .iter()
            .map(|&output_column| {
                if output_column < le_column_index {
                    output_column
                } else {
                    output_column + 1
                }
            })
            .collect::<Vec<_>>();
        necessary_indices.push(le_column_index);
        necessary_indices.sort_unstable();
        necessary_indices.dedup();
        Some(vec![necessary_indices])
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HistogramFold: le={}, field={}, quantile={}",
            self.le_column, self.field_column, self.quantile
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.is_empty() {
            return Err(DataFusionError::Internal(
                "HistogramFold must have at least one input".to_string(),
            ));
        }

        let input: LogicalPlan = inputs.into_iter().next().unwrap();
        let input_schema = input.schema();

        if let Some(unfix) = &self.unfix {
            let le_column =
                resolve_column_name(unfix.le_column_idx, input_schema, "HistogramFold", "le")?;
            let ts_column =
                resolve_column_name(unfix.ts_column_idx, input_schema, "HistogramFold", "ts")?;
            let field_column = resolve_column_name(
                unfix.field_column_idx,
                input_schema,
                "HistogramFold",
                "field",
            )?;

            let output_schema = Self::convert_schema(input_schema, &le_column)?;

            Ok(Self {
                le_column,
                ts_column,
                input,
                field_column,
                quantile: self.quantile,
                output_schema,
                unfix: None,
            })
        } else {
            Ok(Self {
                le_column: self.le_column.clone(),
                ts_column: self.ts_column.clone(),
                input,
                field_column: self.field_column.clone(),
                quantile: self.quantile,
                output_schema: self.output_schema.clone(),
                unfix: None,
            })
        }
    }
}

impl HistogramFold {
    pub fn new(
        le_column: String,
        field_column: String,
        ts_column: String,
        quantile: f64,
        input: LogicalPlan,
    ) -> DataFusionResult<Self> {
        let input_schema = input.schema();
        Self::check_schema(input_schema, &le_column, &field_column, &ts_column)?;
        let output_schema = Self::convert_schema(input_schema, &le_column)?;
        Ok(Self {
            le_column,
            ts_column,
            input,
            field_column,
            quantile: quantile.into(),
            output_schema,
            unfix: None,
        })
    }

    pub const fn name() -> &'static str {
        "HistogramFold"
    }

    fn check_schema(
        input_schema: &DFSchemaRef,
        le_column: &str,
        field_column: &str,
        ts_column: &str,
    ) -> DataFusionResult<()> {
        let check_column = |col| {
            if !input_schema.has_column_with_unqualified_name(col) {
                Err(DataFusionError::SchemaError(
                    Box::new(datafusion::common::SchemaError::FieldNotFound {
                        field: Box::new(Column::new(None::<String>, col)),
                        valid_fields: input_schema.columns(),
                    }),
                    Box::new(None),
                ))
            } else {
                Ok(())
            }
        };

        check_column(le_column)?;
        check_column(ts_column)?;
        check_column(field_column)
    }

    pub fn to_execution_plan(&self, exec_input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let input_schema = self.input.schema();
        // safety: those fields are checked in `check_schema()`
        let le_column_index = input_schema
            .index_of_column_by_name(None, &self.le_column)
            .unwrap();
        let field_column_index = input_schema
            .index_of_column_by_name(None, &self.field_column)
            .unwrap();
        let ts_column_index = input_schema
            .index_of_column_by_name(None, &self.ts_column)
            .unwrap();

        let tag_columns = exec_input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if idx == le_column_index || idx == field_column_index || idx == ts_column_index {
                    None
                } else {
                    Some(Arc::new(PhyColumn::new(field.name(), idx)) as _)
                }
            })
            .collect::<Vec<_>>();

        let mut partition_exprs = tag_columns.clone();
        partition_exprs.push(Arc::new(PhyColumn::new(
            self.input.schema().field(ts_column_index).name(),
            ts_column_index,
        )) as _);

        let output_schema: SchemaRef = self.output_schema.inner().clone();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::Hash(
                partition_exprs.clone(),
                exec_input.output_partitioning().partition_count(),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(HistogramFoldExec {
            le_column_index,
            field_column_index,
            ts_column_index,
            input: exec_input,
            tag_columns,
            partition_exprs,
            quantile: self.quantile.into(),
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    /// Transform the schema
    ///
    /// - `le` will be removed
    fn convert_schema(
        input_schema: &DFSchemaRef,
        le_column: &str,
    ) -> DataFusionResult<DFSchemaRef> {
        let fields = input_schema.fields();
        // safety: those fields are checked in `check_schema()`
        let mut new_fields = Vec::with_capacity(fields.len() - 1);
        for f in fields {
            if f.name() != le_column {
                new_fields.push((None, f.clone()));
            }
        }
        Ok(Arc::new(DFSchema::new_with_metadata(
            new_fields,
            HashMap::new(),
        )?))
    }

    pub fn serialize(&self) -> Vec<u8> {
        let le_column_idx = serialize_column_index(self.input.schema(), &self.le_column);
        let ts_column_idx = serialize_column_index(self.input.schema(), &self.ts_column);
        let field_column_idx = serialize_column_index(self.input.schema(), &self.field_column);

        pb::HistogramFold {
            le_column_idx,
            ts_column_idx,
            field_column_idx,
            quantile: self.quantile.into(),
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_histogram_fold = pb::HistogramFold::decode(bytes).context(DeserializeSnafu)?;
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        let unfix = UnfixIndices {
            le_column_idx: pb_histogram_fold.le_column_idx,
            ts_column_idx: pb_histogram_fold.ts_column_idx,
            field_column_idx: pb_histogram_fold.field_column_idx,
        };

        Ok(Self {
            le_column: String::new(),
            ts_column: String::new(),
            input: placeholder_plan,
            field_column: String::new(),
            quantile: pb_histogram_fold.quantile.into(),
            output_schema: Arc::new(DFSchema::empty()),
            unfix: Some(unfix),
        })
    }
}

impl PartialOrd for HistogramFold {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare fields in order excluding output_schema
        match self.le_column.partial_cmp(&other.le_column) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.ts_column.partial_cmp(&other.ts_column) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.input.partial_cmp(&other.input) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.field_column.partial_cmp(&other.field_column) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.quantile.partial_cmp(&other.quantile)
    }
}

#[derive(Debug)]
pub struct HistogramFoldExec {
    /// Index for `le` column in the schema of input.
    le_column_index: usize,
    input: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    /// Index for field column in the schema of input.
    field_column_index: usize,
    ts_column_index: usize,
    /// Tag columns are all columns except `le`, `field` and `ts` columns.
    tag_columns: Vec<Arc<dyn PhysicalExpr>>,
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    quantile: f64,
    metric: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl ExecutionPlan for HistogramFoldExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let mut cols = self
            .tag_columns
            .iter()
            .map(|expr| PhysicalSortRequirement {
                expr: expr.clone(),
                options: None,
            })
            .collect::<Vec<PhysicalSortRequirement>>();
        // add ts
        cols.push(PhysicalSortRequirement {
            expr: Arc::new(PhyColumn::new(
                self.input.schema().field(self.ts_column_index).name(),
                self.ts_column_index,
            )),
            options: None,
        });
        // add le ASC
        cols.push(PhysicalSortRequirement {
            expr: Arc::new(PhyCast::new(
                Arc::new(PhyColumn::new(
                    self.input.schema().field(self.le_column_index).name(),
                    self.le_column_index,
                )),
                DataType::Float64,
                None,
            )),
            options: Some(SortOptions {
                descending: false,  // +INF in the last
                nulls_first: false, // not nullable
            }),
        });

        // Safety: `cols` is not empty
        let requirement = LexRequirement::new(cols).unwrap();

        vec![Some(OrderingRequirements::Hard(vec![requirement]))]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(self.partition_exprs.clone())]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    // cannot change schema with this method
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert!(!children.is_empty());
        let new_input = children[0].clone();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(self.output_schema.clone()),
            Partitioning::Hash(
                self.partition_exprs.clone(),
                new_input.output_partitioning().partition_count(),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            input: new_input,
            metric: self.metric.clone(),
            le_column_index: self.le_column_index,
            ts_column_index: self.ts_column_index,
            tag_columns: self.tag_columns.clone(),
            partition_exprs: self.partition_exprs.clone(),
            quantile: self.quantile,
            output_schema: self.output_schema.clone(),
            field_column_index: self.field_column_index,
            properties,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);

        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        let output_schema = self.output_schema.clone();

        let mut normal_indices = (0..input.schema().fields().len()).collect::<HashSet<_>>();
        normal_indices.remove(&self.field_column_index);
        normal_indices.remove(&self.le_column_index);
        Ok(Box::pin(HistogramFoldStream {
            le_column_index: self.le_column_index,
            field_column_index: self.field_column_index,
            quantile: self.quantile,
            normal_indices: normal_indices.into_iter().collect(),
            bucket_size: None,
            input_buffer: vec![],
            input,
            output_schema,
            input_schema: self.input.schema(),
            mode: FoldMode::Optimistic,
            safe_group: None,
            metric: baseline_metric,
            batch_size,
            input_buffered_rows: 0,
            output_buffer: HistogramFoldStream::empty_output_buffer(
                &self.output_schema,
                self.le_column_index,
            )?,
            output_buffered_rows: 0,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn partition_statistics(&self, _: Option<usize>) -> DataFusionResult<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.schema()),
        })
    }

    fn name(&self) -> &str {
        "HistogramFoldExec"
    }
}

impl DisplayAs for HistogramFoldExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "HistogramFoldExec: le=@{}, field=@{}, quantile={}",
                    self.le_column_index, self.field_column_index, self.quantile
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FoldMode {
    Optimistic,
    Safe,
}

pub struct HistogramFoldStream {
    // internal states
    le_column_index: usize,
    field_column_index: usize,
    quantile: f64,
    /// Columns need not folding. This indices is based on input schema
    normal_indices: Vec<usize>,
    bucket_size: Option<usize>,
    /// Expected output batch size
    batch_size: usize,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    mode: FoldMode,
    safe_group: Option<SafeGroup>,

    // buffers
    input_buffer: Vec<RecordBatch>,
    input_buffered_rows: usize,
    output_buffer: Vec<Box<dyn MutableVector>>,
    output_buffered_rows: usize,

    // runtime things
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
}

#[derive(Debug, Default)]
struct SafeGroup {
    tag_values: Vec<Value>,
    buckets: Vec<f64>,
    counters: Vec<f64>,
}

impl RecordBatchStream for HistogramFoldStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for HistogramFoldStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(batch) => {
                    let batch = batch?;
                    let timer = Instant::now();
                    let Some(result) = self.fold_input(batch)? else {
                        self.metric.elapsed_compute().add_elapsed(timer);
                        continue;
                    };
                    self.metric.elapsed_compute().add_elapsed(timer);
                    break Poll::Ready(Some(result));
                }
                None => {
                    self.flush_remaining()?;
                    break Poll::Ready(self.take_output_buf()?.map(Ok));
                }
            }
        };
        self.metric.record_poll(poll)
    }
}

impl HistogramFoldStream {
    /// The inner most `Result` is for `poll_next()`
    pub fn fold_input(
        &mut self,
        input: RecordBatch,
    ) -> DataFusionResult<Option<DataFusionResult<RecordBatch>>> {
        match self.mode {
            FoldMode::Safe => {
                self.push_input_buf(input);
                self.process_safe_mode_buffer()?;
            }
            FoldMode::Optimistic => {
                self.push_input_buf(input);
                let Some(bucket_num) = self.calculate_bucket_num_from_buffer()? else {
                    return Ok(None);
                };
                self.bucket_size = Some(bucket_num);

                if self.input_buffered_rows < bucket_num {
                    // not enough rows to fold
                    return Ok(None);
                }

                self.fold_buf(bucket_num)?;
            }
        }

        self.maybe_take_output()
    }

    /// Generate a group of empty [MutableVector]s from the output schema.
    ///
    /// For simplicity, this method will insert a placeholder for `le`. So that
    /// the output buffers has the same schema with input. This placeholder needs
    /// to be removed before returning the output batch.
    pub fn empty_output_buffer(
        schema: &SchemaRef,
        le_column_index: usize,
    ) -> DataFusionResult<Vec<Box<dyn MutableVector>>> {
        let mut builders = Vec::with_capacity(schema.fields().len() + 1);
        for field in schema.fields() {
            let concrete_datatype = ConcreteDataType::try_from(field.data_type()).unwrap();
            let mutable_vector = concrete_datatype.create_mutable_vector(0);
            builders.push(mutable_vector);
        }
        builders.insert(
            le_column_index,
            ConcreteDataType::float64_datatype().create_mutable_vector(0),
        );

        Ok(builders)
    }

    /// Determines bucket count using buffered batches, concatenating them to
    /// detect the first complete bucket that may span batch boundaries.
    fn calculate_bucket_num_from_buffer(&mut self) -> DataFusionResult<Option<usize>> {
        if let Some(size) = self.bucket_size {
            return Ok(Some(size));
        }

        if self.input_buffer.is_empty() {
            return Ok(None);
        }

        let batch_refs: Vec<&RecordBatch> = self.input_buffer.iter().collect();
        let batch = concat_batches(&self.input_schema, batch_refs)?;
        self.find_first_complete_bucket(&batch)
    }

    fn find_first_complete_bucket(&self, batch: &RecordBatch) -> DataFusionResult<Option<usize>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let vectors = Helper::try_into_vectors(batch.columns())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let le_array = batch.column(self.le_column_index).as_string::<i32>();

        let mut tag_values_buf = Vec::with_capacity(self.normal_indices.len());
        self.collect_tag_values(&vectors, 0, &mut tag_values_buf);
        let mut group_start = 0usize;

        for row in 0..batch.num_rows() {
            if !self.is_same_group(&vectors, row, &tag_values_buf) {
                // new group begins
                self.collect_tag_values(&vectors, row, &mut tag_values_buf);
                group_start = row;
            }

            if Self::is_positive_infinity(le_array, row) {
                return Ok(Some(row - group_start + 1));
            }
        }

        Ok(None)
    }

    /// Fold record batches from input buffer and put to output buffer
    fn fold_buf(&mut self, bucket_num: usize) -> DataFusionResult<()> {
        let batch = concat_batches(&self.input_schema, self.input_buffer.drain(..).as_ref())?;
        let mut remaining_rows = self.input_buffered_rows;
        let mut cursor = 0;

        // TODO(LFC): Try to get rid of the Arrow array to vector conversion here.
        let vectors = Helper::try_into_vectors(batch.columns())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let le_array = batch.column(self.le_column_index);
        let le_array = le_array.as_string::<i32>();
        let field_array = batch.column(self.field_column_index);
        let field_array = field_array.as_primitive::<Float64Type>();
        let mut tag_values_buf = Vec::with_capacity(self.normal_indices.len());

        while remaining_rows >= bucket_num && self.mode == FoldMode::Optimistic {
            self.collect_tag_values(&vectors, cursor, &mut tag_values_buf);
            if !self.validate_optimistic_group(
                &vectors,
                le_array,
                cursor,
                bucket_num,
                &tag_values_buf,
            ) {
                let remaining_input_batch = batch.slice(cursor, remaining_rows);
                self.switch_to_safe_mode(remaining_input_batch)?;
                return Ok(());
            }

            // "sample" normal columns
            for (idx, value) in self.normal_indices.iter().zip(tag_values_buf.iter()) {
                self.output_buffer[*idx].push_value_ref(value);
            }
            // "fold" `le` and field columns
            let mut bucket = Vec::with_capacity(bucket_num);
            let mut counters = Vec::with_capacity(bucket_num);
            for bias in 0..bucket_num {
                let position = cursor + bias;
                let le = if le_array.is_valid(position) {
                    le_array.value(position).parse::<f64>().unwrap_or(f64::NAN)
                } else {
                    f64::NAN
                };
                bucket.push(le);

                let counter = if field_array.is_valid(position) {
                    field_array.value(position)
                } else {
                    f64::NAN
                };
                counters.push(counter);
            }
            // ignore invalid data
            let result = Self::evaluate_row(self.quantile, &bucket, &counters).unwrap_or(f64::NAN);
            self.output_buffer[self.field_column_index].push_value_ref(&ValueRef::from(result));
            cursor += bucket_num;
            remaining_rows -= bucket_num;
            self.output_buffered_rows += 1;
        }

        let remaining_input_batch = batch.slice(cursor, remaining_rows);
        self.input_buffered_rows = remaining_input_batch.num_rows();
        if self.input_buffered_rows > 0 {
            self.input_buffer.push(remaining_input_batch);
        }

        Ok(())
    }

    fn push_input_buf(&mut self, batch: RecordBatch) {
        self.input_buffered_rows += batch.num_rows();
        self.input_buffer.push(batch);
    }

    fn maybe_take_output(&mut self) -> DataFusionResult<Option<DataFusionResult<RecordBatch>>> {
        if self.output_buffered_rows >= self.batch_size {
            return Ok(self.take_output_buf()?.map(Ok));
        }
        Ok(None)
    }

    fn switch_to_safe_mode(&mut self, remaining_batch: RecordBatch) -> DataFusionResult<()> {
        self.mode = FoldMode::Safe;
        self.bucket_size = None;
        self.input_buffer.clear();
        self.input_buffered_rows = remaining_batch.num_rows();

        if self.input_buffered_rows > 0 {
            self.input_buffer.push(remaining_batch);
            self.process_safe_mode_buffer()?;
        }

        Ok(())
    }

    fn collect_tag_values<'a>(
        &self,
        vectors: &'a [VectorRef],
        row: usize,
        tag_values: &mut Vec<ValueRef<'a>>,
    ) {
        tag_values.clear();
        for idx in self.normal_indices.iter() {
            tag_values.push(vectors[*idx].get_ref(row));
        }
    }

    fn validate_optimistic_group(
        &self,
        vectors: &[VectorRef],
        le_array: &StringArray,
        cursor: usize,
        bucket_num: usize,
        tag_values: &[ValueRef<'_>],
    ) -> bool {
        let inf_index = cursor + bucket_num - 1;
        if !Self::is_positive_infinity(le_array, inf_index) {
            return false;
        }

        for offset in 1..bucket_num {
            let row = cursor + offset;
            for (idx, expected) in self.normal_indices.iter().zip(tag_values.iter()) {
                if vectors[*idx].get_ref(row) != *expected {
                    return false;
                }
            }
        }
        true
    }

    /// Checks whether a row belongs to the current group (same series).
    fn is_same_group(
        &self,
        vectors: &[VectorRef],
        row: usize,
        tag_values: &[ValueRef<'_>],
    ) -> bool {
        self.normal_indices
            .iter()
            .zip(tag_values.iter())
            .all(|(idx, expected)| vectors[*idx].get_ref(row) == *expected)
    }

    fn push_output_row(&mut self, tag_values: &[ValueRef<'_>], result: f64) {
        debug_assert_eq!(self.normal_indices.len(), tag_values.len());
        for (idx, value) in self.normal_indices.iter().zip(tag_values.iter()) {
            self.output_buffer[*idx].push_value_ref(value);
        }
        self.output_buffer[self.field_column_index].push_value_ref(&ValueRef::from(result));
        self.output_buffered_rows += 1;
    }

    fn finalize_safe_group(&mut self) -> DataFusionResult<()> {
        if let Some(group) = self.safe_group.take() {
            if group.tag_values.is_empty() {
                return Ok(());
            }

            let has_inf = group
                .buckets
                .last()
                .map(|v| v.is_infinite() && v.is_sign_positive())
                .unwrap_or(false);
            let result = if group.buckets.len() < 2 || !has_inf {
                f64::NAN
            } else {
                Self::evaluate_row(self.quantile, &group.buckets, &group.counters)
                    .unwrap_or(f64::NAN)
            };
            let mut tag_value_refs = Vec::with_capacity(group.tag_values.len());
            tag_value_refs.extend(group.tag_values.iter().map(|v| v.as_value_ref()));
            self.push_output_row(&tag_value_refs, result);
        }
        Ok(())
    }

    fn process_safe_mode_buffer(&mut self) -> DataFusionResult<()> {
        if self.input_buffer.is_empty() {
            self.input_buffered_rows = 0;
            return Ok(());
        }

        let batch = concat_batches(&self.input_schema, self.input_buffer.drain(..).as_ref())?;
        self.input_buffered_rows = 0;
        let vectors = Helper::try_into_vectors(batch.columns())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let le_array = batch.column(self.le_column_index).as_string::<i32>();
        let field_array = batch
            .column(self.field_column_index)
            .as_primitive::<Float64Type>();
        let mut tag_values_buf = Vec::with_capacity(self.normal_indices.len());

        for row in 0..batch.num_rows() {
            self.collect_tag_values(&vectors, row, &mut tag_values_buf);
            let should_start_new_group = self
                .safe_group
                .as_ref()
                .is_none_or(|group| !Self::tag_values_equal(&group.tag_values, &tag_values_buf));
            if should_start_new_group {
                self.finalize_safe_group()?;
                self.safe_group = Some(SafeGroup {
                    tag_values: tag_values_buf.iter().cloned().map(Value::from).collect(),
                    buckets: Vec::new(),
                    counters: Vec::new(),
                });
            }

            let Some(group) = self.safe_group.as_mut() else {
                continue;
            };

            let bucket = if le_array.is_valid(row) {
                le_array.value(row).parse::<f64>().unwrap_or(f64::NAN)
            } else {
                f64::NAN
            };
            let counter = if field_array.is_valid(row) {
                field_array.value(row)
            } else {
                f64::NAN
            };

            group.buckets.push(bucket);
            group.counters.push(counter);
        }

        Ok(())
    }

    fn tag_values_equal(group_values: &[Value], current: &[ValueRef<'_>]) -> bool {
        group_values.len() == current.len()
            && group_values
                .iter()
                .zip(current.iter())
                .all(|(group, now)| group.as_value_ref() == *now)
    }

    /// Compute result from output buffer
    fn take_output_buf(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        if self.output_buffered_rows == 0 {
            if self.input_buffered_rows != 0 {
                warn!(
                    "input buffer is not empty, {} rows remaining",
                    self.input_buffered_rows
                );
            }
            return Ok(None);
        }

        let mut output_buf = Self::empty_output_buffer(&self.output_schema, self.le_column_index)?;
        std::mem::swap(&mut self.output_buffer, &mut output_buf);
        let mut columns = Vec::with_capacity(output_buf.len());
        for builder in output_buf.iter_mut() {
            columns.push(builder.to_vector().to_arrow_array());
        }
        // remove the placeholder column for `le`
        columns.remove(self.le_column_index);

        self.output_buffered_rows = 0;
        RecordBatch::try_new(self.output_schema.clone(), columns)
            .map(Some)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn flush_remaining(&mut self) -> DataFusionResult<()> {
        if self.mode == FoldMode::Optimistic && self.input_buffered_rows > 0 {
            let buffered_batches: Vec<_> = self.input_buffer.drain(..).collect();
            if !buffered_batches.is_empty() {
                let batch = concat_batches(&self.input_schema, buffered_batches.as_slice())?;
                self.switch_to_safe_mode(batch)?;
            } else {
                self.input_buffered_rows = 0;
            }
        }

        if self.mode == FoldMode::Safe {
            self.process_safe_mode_buffer()?;
            self.finalize_safe_group()?;
        }

        Ok(())
    }

    fn is_positive_infinity(le_array: &StringArray, index: usize) -> bool {
        le_array.is_valid(index)
            && matches!(
                le_array.value(index).parse::<f64>(),
                Ok(value) if value.is_infinite() && value.is_sign_positive()
            )
    }

    /// Evaluate the field column and return the result
    fn evaluate_row(quantile: f64, bucket: &[f64], counter: &[f64]) -> DataFusionResult<f64> {
        // check bucket
        if bucket.len() <= 1 {
            return Ok(f64::NAN);
        }
        if bucket.last().unwrap().is_finite() {
            return Err(DataFusionError::Execution(
                "last bucket should be +Inf".to_string(),
            ));
        }
        if bucket.len() != counter.len() {
            return Err(DataFusionError::Execution(
                "bucket and counter should have the same length".to_string(),
            ));
        }
        // check quantile
        if quantile < 0.0 {
            return Ok(f64::NEG_INFINITY);
        } else if quantile > 1.0 {
            return Ok(f64::INFINITY);
        } else if quantile.is_nan() {
            return Ok(f64::NAN);
        }

        // check input value
        if !bucket.windows(2).all(|w| w[0] <= w[1]) {
            return Ok(f64::NAN);
        }
        let counter = {
            let needs_fix =
                counter.iter().any(|v| !v.is_finite()) || !counter.windows(2).all(|w| w[0] <= w[1]);
            if !needs_fix {
                Cow::Borrowed(counter)
            } else {
                let mut fixed = Vec::with_capacity(counter.len());
                let mut prev = 0.0;
                for (idx, &v) in counter.iter().enumerate() {
                    let mut val = if v.is_finite() { v } else { prev };
                    if idx > 0 && val < prev {
                        val = prev;
                    }
                    fixed.push(val);
                    prev = val;
                }
                Cow::Owned(fixed)
            }
        };

        let total = *counter.last().unwrap();
        let expected_pos = total * quantile;
        let mut fit_bucket_pos = 0;
        while fit_bucket_pos < bucket.len() && counter[fit_bucket_pos] < expected_pos {
            fit_bucket_pos += 1;
        }
        if fit_bucket_pos >= bucket.len() - 1 {
            Ok(bucket[bucket.len() - 2])
        } else {
            let upper_bound = bucket[fit_bucket_pos];
            let upper_count = counter[fit_bucket_pos];
            let mut lower_bound = bucket[0].min(0.0);
            let mut lower_count = 0.0;
            if fit_bucket_pos > 0 {
                lower_bound = bucket[fit_bucket_pos - 1];
                lower_count = counter[fit_bucket_pos - 1];
            }
            if (upper_count - lower_count).abs() < 1e-10 {
                return Ok(f64::NAN);
            }
            Ok(lower_bound
                + (upper_bound - lower_bound) / (upper_count - lower_count)
                    * (expected_pos - lower_count))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
    use datafusion::arrow::datatypes::{Field, Schema, SchemaRef, TimeUnit};
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::EmptyRelation;
    use datafusion::prelude::SessionContext;
    use datatypes::arrow_array::StringArray;
    use futures::FutureExt;

    use super::*;

    fn project_batch(batch: &RecordBatch, indices: &[usize]) -> RecordBatch {
        let fields = indices
            .iter()
            .map(|&idx| batch.schema().field(idx).clone())
            .collect::<Vec<_>>();
        let columns = indices
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn prepare_test_data() -> DataSourceExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));

        // 12 items
        let host_column_1 = Arc::new(StringArray::from(vec![
            "host_1", "host_1", "host_1", "host_1", "host_1", "host_1", "host_1", "host_1",
            "host_1", "host_1", "host_1", "host_1",
        ])) as _;
        let le_column_1 = Arc::new(StringArray::from(vec![
            "0.001", "0.1", "10", "1000", "+Inf", "0.001", "0.1", "10", "1000", "+inf", "0.001",
            "0.1",
        ])) as _;
        let val_column_1 = Arc::new(Float64Array::from(vec![
            0_0.0, 1.0, 1.0, 5.0, 5.0, 0_0.0, 20.0, 60.0, 70.0, 100.0, 0_1.0, 1.0,
        ])) as _;

        // 2 items
        let host_column_2 = Arc::new(StringArray::from(vec!["host_1", "host_1"])) as _;
        let le_column_2 = Arc::new(StringArray::from(vec!["10", "1000"])) as _;
        let val_column_2 = Arc::new(Float64Array::from(vec![1.0, 1.0])) as _;

        // 11 items
        let host_column_3 = Arc::new(StringArray::from(vec![
            "host_1", "host_2", "host_2", "host_2", "host_2", "host_2", "host_2", "host_2",
            "host_2", "host_2", "host_2",
        ])) as _;
        let le_column_3 = Arc::new(StringArray::from(vec![
            "+INF", "0.001", "0.1", "10", "1000", "+iNf", "0.001", "0.1", "10", "1000", "+Inf",
        ])) as _;
        let val_column_3 = Arc::new(Float64Array::from(vec![
            1.0, 0_0.0, 0.0, 0.0, 0.0, 0.0, 0_0.0, 1.0, 2.0, 3.0, 4.0,
        ])) as _;

        let data_1 = RecordBatch::try_new(
            schema.clone(),
            vec![host_column_1, le_column_1, val_column_1],
        )
        .unwrap();
        let data_2 = RecordBatch::try_new(
            schema.clone(),
            vec![host_column_2, le_column_2, val_column_2],
        )
        .unwrap();
        let data_3 = RecordBatch::try_new(
            schema.clone(),
            vec![host_column_3, le_column_3, val_column_3],
        )
        .unwrap();

        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![data_1, data_2, data_3]], schema, None).unwrap(),
        ))
    }

    fn build_fold_exec_from_batches(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        quantile: f64,
        ts_column_index: usize,
    ) -> Arc<HistogramFoldExec> {
        let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[batches], schema.clone(), None).unwrap(),
        )));
        let output_schema: SchemaRef = Arc::new(
            HistogramFold::convert_schema(&Arc::new(input.schema().to_dfschema().unwrap()), "le")
                .unwrap()
                .as_arrow()
                .clone(),
        );

        let (tag_columns, partition_exprs, properties) =
            build_test_plan_properties(&input, output_schema.clone(), ts_column_index);

        Arc::new(HistogramFoldExec {
            le_column_index: 1,
            field_column_index: 2,
            quantile,
            ts_column_index,
            input,
            output_schema,
            tag_columns,
            partition_exprs,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    type PlanPropsResult = (
        Vec<Arc<dyn PhysicalExpr>>,
        Vec<Arc<dyn PhysicalExpr>>,
        Arc<PlanProperties>,
    );

    fn build_test_plan_properties(
        input: &Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        ts_column_index: usize,
    ) -> PlanPropsResult {
        let tag_columns = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if idx == 1 || idx == 2 || idx == ts_column_index {
                    None
                } else {
                    Some(Arc::new(PhyColumn::new(field.name(), idx)) as _)
                }
            })
            .collect::<Vec<_>>();

        let partition_exprs = if tag_columns.is_empty() {
            vec![Arc::new(PhyColumn::new(
                input.schema().field(ts_column_index).name(),
                ts_column_index,
            )) as _]
        } else {
            tag_columns.clone()
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::Hash(
                partition_exprs.clone(),
                input.output_partitioning().partition_count(),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        (tag_columns, partition_exprs, Arc::new(properties))
    }

    #[tokio::test]
    async fn fold_overall() {
        let memory_exec: Arc<dyn ExecutionPlan> = Arc::new(prepare_test_data());
        let output_schema: SchemaRef = Arc::new(
            HistogramFold::convert_schema(
                &Arc::new(memory_exec.schema().to_dfschema().unwrap()),
                "le",
            )
            .unwrap()
            .as_arrow()
            .clone(),
        );
        let (tag_columns, partition_exprs, properties) =
            build_test_plan_properties(&memory_exec, output_schema.clone(), 0);
        let fold_exec = Arc::new(HistogramFoldExec {
            le_column_index: 1,
            field_column_index: 2,
            quantile: 0.4,
            ts_column_index: 0,
            input: memory_exec,
            output_schema,
            tag_columns,
            partition_exprs,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        });

        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(fold_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        let expected = String::from(
            "+--------+-------------------+
| host   | val               |
+--------+-------------------+
| host_1 | 257.5             |
| host_1 | 5.05              |
| host_1 | 0.0004            |
| host_2 | NaN               |
| host_2 | 6.040000000000001 |
+--------+-------------------+",
        );
        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn pruning_should_keep_le_column_for_exec() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        let df_schema = schema.clone().to_dfschema_ref().unwrap();
        let input = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema,
        });
        let plan = HistogramFold::new(
            "le".to_string(),
            "val".to_string(),
            "ts".to_string(),
            0.5,
            input,
        )
        .unwrap();

        let output_columns = [0usize, 1usize];
        let required = plan.necessary_children_exprs(&output_columns).unwrap();
        let required = &required[0];
        assert_eq!(required.as_slice(), &[0, 1, 2]);

        let input_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["0.1", "+Inf"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();
        let projected = project_batch(&input_batch, required);
        let projected_schema = projected.schema();
        let memory_exec = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![projected]], projected_schema, None).unwrap(),
        )));

        let fold_exec = plan.to_execution_plan(memory_exec);
        let session_context = SessionContext::default();
        let output_batches =
            datafusion::physical_plan::collect(fold_exec, session_context.task_ctx())
                .await
                .unwrap();
        assert_eq!(output_batches.len(), 1);

        let output_batch = &output_batches[0];
        assert_eq!(output_batch.num_rows(), 1);

        let ts = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.values(), &[0i64]);

        let values = output_batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((values.value(0) - 0.1).abs() < 1e-12);

        // Simulate the pre-fix pruning behavior: omit the `le` column from the child input.
        let le_index = 1usize;
        let broken_required = output_columns
            .iter()
            .map(|&output_column| {
                if output_column < le_index {
                    output_column
                } else {
                    output_column + 1
                }
            })
            .collect::<Vec<_>>();

        let broken = project_batch(&input_batch, &broken_required);
        let broken_schema = broken.schema();
        let broken_exec = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![broken]], broken_schema, None).unwrap(),
        )));
        let broken_fold_exec = plan.to_execution_plan(broken_exec);
        let session_context = SessionContext::default();
        let broken_result = std::panic::AssertUnwindSafe(async {
            datafusion::physical_plan::collect(broken_fold_exec, session_context.task_ctx()).await
        })
        .catch_unwind()
        .await;
        assert!(broken_result.is_err());
    }

    #[test]
    fn confirm_schema() {
        let input_schema = Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ])
        .to_dfschema_ref()
        .unwrap();
        let expected_output_schema = Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ])
        .to_dfschema_ref()
        .unwrap();

        let actual = HistogramFold::convert_schema(&input_schema, "le").unwrap();
        assert_eq!(actual, expected_output_schema)
    }

    #[tokio::test]
    async fn fallback_to_safe_mode_on_missing_inf() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        let host_column = Arc::new(StringArray::from(vec!["a", "a", "a", "a", "b", "b"])) as _;
        let le_column = Arc::new(StringArray::from(vec![
            "0.1", "+Inf", "0.1", "1.0", "0.1", "+Inf",
        ])) as _;
        let val_column = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 3.0, 1.0, 5.0])) as _;
        let batch =
            RecordBatch::try_new(schema.clone(), vec![host_column, le_column, val_column]).unwrap();
        let fold_exec = build_fold_exec_from_batches(vec![batch], schema, 0.5, 0);
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(fold_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        let expected = String::from(
            "+------+-----+
| host | val |
+------+-----+
| a    | 0.1 |
| a    | NaN |
| b    | 0.1 |
+------+-----+",
        );
        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn emit_nan_when_no_inf_present() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        let host_column = Arc::new(StringArray::from(vec!["c", "c"])) as _;
        let le_column = Arc::new(StringArray::from(vec!["0.1", "1.0"])) as _;
        let val_column = Arc::new(Float64Array::from(vec![1.0, 2.0])) as _;
        let batch =
            RecordBatch::try_new(schema.clone(), vec![host_column, le_column, val_column]).unwrap();
        let fold_exec = build_fold_exec_from_batches(vec![batch], schema, 0.9, 0);
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(fold_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();

        let expected = String::from(
            "+------+-----+
| host | val |
+------+-----+
| c    | NaN |
+------+-----+",
        );
        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn safe_mode_handles_misaligned_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));

        let ts_column = Arc::new(TimestampMillisecondArray::from(vec![
            2900000, 2900000, 2900000, 3000000, 3000000, 3000000, 3000000, 3005000, 3005000,
            3010000, 3010000, 3010000, 3010000, 3010000,
        ])) as _;
        let le_column = Arc::new(StringArray::from(vec![
            "0.1", "1", "5", "0.1", "1", "5", "+Inf", "0.1", "+Inf", "0.1", "1", "3", "5", "+Inf",
        ])) as _;
        let val_column = Arc::new(Float64Array::from(vec![
            0.0, 0.0, 0.0, 50.0, 70.0, 110.0, 120.0, 10.0, 30.0, 10.0, 20.0, 30.0, 40.0, 50.0,
        ])) as _;
        let batch =
            RecordBatch::try_new(schema.clone(), vec![ts_column, le_column, val_column]).unwrap();
        let fold_exec = build_fold_exec_from_batches(vec![batch], schema, 0.5, 0);
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(fold_exec, session_context.task_ctx())
            .await
            .unwrap();

        let mut values = Vec::new();
        for batch in result {
            let array = batch.column(1).as_primitive::<Float64Type>();
            values.extend(array.iter().map(|v| v.unwrap()));
        }

        assert_eq!(values.len(), 4);
        assert!(values[0].is_nan());
        assert!((values[1] - 0.55).abs() < 1e-10);
        assert!((values[2] - 0.1).abs() < 1e-10);
        assert!((values[3] - 2.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn missing_buckets_at_first_timestamp() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));

        let ts_column = Arc::new(TimestampMillisecondArray::from(vec![
            2_900_000, 3_000_000, 3_000_000, 3_000_000, 3_000_000, 3_005_000, 3_005_000, 3_010_000,
            3_010_000, 3_010_000, 3_010_000, 3_010_000,
        ])) as _;
        let le_column = Arc::new(StringArray::from(vec![
            "0.1", "0.1", "1", "5", "+Inf", "0.1", "+Inf", "0.1", "1", "3", "5", "+Inf",
        ])) as _;
        let val_column = Arc::new(Float64Array::from(vec![
            0.0, 50.0, 70.0, 110.0, 120.0, 10.0, 30.0, 10.0, 20.0, 30.0, 40.0, 50.0,
        ])) as _;

        let batch =
            RecordBatch::try_new(schema.clone(), vec![ts_column, le_column, val_column]).unwrap();
        let fold_exec = build_fold_exec_from_batches(vec![batch], schema, 0.5, 0);
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(fold_exec, session_context.task_ctx())
            .await
            .unwrap();

        let mut values = Vec::new();
        for batch in result {
            let array = batch.column(1).as_primitive::<Float64Type>();
            values.extend(array.iter().map(|v| v.unwrap()));
        }

        assert_eq!(values.len(), 4);
        assert!(values[0].is_nan());
        assert!((values[1] - 0.55).abs() < 1e-10);
        assert!((values[2] - 0.1).abs() < 1e-10);
        assert!((values[3] - 2.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn missing_inf_in_first_group() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("le", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));

        let ts_column = Arc::new(TimestampMillisecondArray::from(vec![
            1000, 1000, 1000, 2000, 2000, 2000, 2000,
        ])) as _;
        let le_column = Arc::new(StringArray::from(vec![
            "0.1", "1", "5", "0.1", "1", "5", "+Inf",
        ])) as _;
        let val_column = Arc::new(Float64Array::from(vec![
            0.0, 0.0, 0.0, 10.0, 20.0, 30.0, 30.0,
        ])) as _;
        let batch =
            RecordBatch::try_new(schema.clone(), vec![ts_column, le_column, val_column]).unwrap();
        let fold_exec = build_fold_exec_from_batches(vec![batch], schema, 0.5, 0);
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(fold_exec, session_context.task_ctx())
            .await
            .unwrap();

        let mut values = Vec::new();
        for batch in result {
            let array = batch.column(1).as_primitive::<Float64Type>();
            values.extend(array.iter().map(|v| v.unwrap()));
        }

        assert_eq!(values.len(), 2);
        assert!(values[0].is_nan());
        assert!((values[1] - 0.55).abs() < 1e-10, "{values:?}");
    }

    #[test]
    fn evaluate_row_normal_case() {
        let bucket = [0.0, 1.0, 2.0, 3.0, 4.0, f64::INFINITY];

        #[derive(Debug)]
        struct Case {
            quantile: f64,
            counters: Vec<f64>,
            expected: f64,
        }

        let cases = [
            Case {
                quantile: 0.9,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: 4.0,
            },
            Case {
                quantile: 0.89,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: 4.0,
            },
            Case {
                quantile: 0.78,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: 3.9,
            },
            Case {
                quantile: 0.5,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: 2.5,
            },
            Case {
                quantile: 0.5,
                counters: vec![0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                expected: f64::NAN,
            },
            Case {
                quantile: 1.0,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: 4.0,
            },
            Case {
                quantile: 0.0,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: f64::NAN,
            },
            Case {
                quantile: 1.1,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: f64::INFINITY,
            },
            Case {
                quantile: -1.0,
                counters: vec![0.0, 10.0, 20.0, 30.0, 40.0, 50.0],
                expected: f64::NEG_INFINITY,
            },
        ];

        for case in cases {
            let actual =
                HistogramFoldStream::evaluate_row(case.quantile, &bucket, &case.counters).unwrap();
            assert_eq!(
                format!("{actual}"),
                format!("{}", case.expected),
                "{:?}",
                case
            );
        }
    }

    #[test]
    fn evaluate_out_of_order_input() {
        let bucket = [0.0, 1.0, 2.0, 3.0, 4.0, f64::INFINITY];
        let counters = [5.0, 4.0, 3.0, 2.0, 1.0, 0.0];
        let result = HistogramFoldStream::evaluate_row(0.5, &bucket, &counters).unwrap();
        assert_eq!(0.0, result);
    }

    #[test]
    fn evaluate_wrong_bucket() {
        let bucket = [0.0, 1.0, 2.0, 3.0, 4.0, f64::INFINITY, 5.0];
        let counters = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let result = HistogramFoldStream::evaluate_row(0.5, &bucket, &counters);
        assert!(result.is_err());
    }

    #[test]
    fn evaluate_small_fraction() {
        let bucket = [0.0, 2.0, 4.0, 6.0, f64::INFINITY];
        let counters = [0.0, 1.0 / 300.0, 2.0 / 300.0, 0.01, 0.01];
        let result = HistogramFoldStream::evaluate_row(0.5, &bucket, &counters).unwrap();
        assert_eq!(3.0, result);
    }

    #[test]
    fn evaluate_non_monotonic_counter() {
        let bucket = [0.0, 1.0, 2.0, 3.0, f64::INFINITY];
        let counters = [0.1, 0.2, 0.4, 0.17, 0.5];
        let result = HistogramFoldStream::evaluate_row(0.5, &bucket, &counters).unwrap();
        assert!((result - 1.25).abs() < 1e-10, "{result}");
    }

    #[test]
    fn evaluate_nan_counter() {
        let bucket = [0.0, 1.0, 2.0, 3.0, f64::INFINITY];
        let counters = [f64::NAN, 1.0, 2.0, 3.0, 3.0];
        let result = HistogramFoldStream::evaluate_row(0.5, &bucket, &counters).unwrap();
        assert!((result - 1.5).abs() < 1e-10, "{result}");
    }

    fn build_empty_relation(schema: &Arc<Schema>) -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: schema.clone().to_dfschema_ref().unwrap(),
        })
    }

    #[tokio::test]
    async fn encode_decode_histogram_fold() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("le", DataType::Utf8, false),
            Field::new("val", DataType::Float64, false),
        ]));
        let input_plan = build_empty_relation(&schema);
        let plan_node = HistogramFold::new(
            "le".to_string(),
            "val".to_string(),
            "ts".to_string(),
            0.8,
            input_plan.clone(),
        )
        .unwrap();

        let bytes = plan_node.serialize();

        let histogram_fold = HistogramFold::deserialize(&bytes).unwrap();
        // need fix
        let histogram_fold = histogram_fold
            .with_exprs_and_inputs(vec![], vec![input_plan])
            .unwrap();

        assert_eq!(histogram_fold.le_column, "le");
        assert_eq!(histogram_fold.ts_column, "ts");
        assert_eq!(histogram_fold.field_column, "val");
        assert_eq!(histogram_fold.quantile, OrderedF64::from(0.8));
        assert_eq!(histogram_fold.output_schema.fields().len(), 2);
        assert_eq!(histogram_fold.output_schema.field(0).name(), "ts");
        assert_eq!(histogram_fold.output_schema.field(1).name(), "val");
    }
}
