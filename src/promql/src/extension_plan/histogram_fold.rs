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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use common_recordbatch::RecordBatch as GtRecordBatch;
use common_telemetry::warn;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::{self, concat_batches, SortOptions};
use datafusion::arrow::datatypes::{DataType, Float64Type, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, DFSchema, DFSchemaRef, Statistics};
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
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::{Column, Expr};
use datatypes::prelude::{ConcreteDataType, DataType as GtDataType};
use datatypes::schema::Schema as GtSchema;
use datatypes::value::{OrderedF64, ValueRef};
use datatypes::vectors::MutableVector;
use futures::{ready, Stream, StreamExt};

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
        vec![]
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
        Ok(Self {
            le_column: self.le_column.clone(),
            ts_column: self.ts_column.clone(),
            input: inputs.into_iter().next().unwrap(),
            field_column: self.field_column.clone(),
            quantile: self.quantile,
            // This method cannot return error. Otherwise we should re-calculate
            // the output schema
            output_schema: self.output_schema.clone(),
        })
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

        let output_schema: SchemaRef = Arc::new(self.output_schema.as_ref().into());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Arc::new(HistogramFoldExec {
            le_column_index,
            field_column_index,
            ts_column_index,
            input: exec_input,
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
    quantile: f64,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl ExecutionPlan for HistogramFoldExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let mut cols = self
            .tag_col_exprs()
            .into_iter()
            .map(|expr| PhysicalSortRequirement {
                expr,
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
        self.input.required_input_distribution()
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
        Ok(Arc::new(Self {
            input: children[0].clone(),
            metric: self.metric.clone(),
            le_column_index: self.le_column_index,
            ts_column_index: self.ts_column_index,
            quantile: self.quantile,
            output_schema: self.output_schema.clone(),
            field_column_index: self.field_column_index,
            properties: self.properties.clone(),
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

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::new_unknown();
                // plus one more for the removed column by function `convert_schema`
                self.schema().flattened_fields().len() + 1
            ],
        })
    }

    fn name(&self) -> &str {
        "HistogramFoldExec"
    }
}

impl HistogramFoldExec {
    /// Return all the [PhysicalExpr] of tag columns in order.
    ///
    /// Tag columns are all columns except `le`, `field` and `ts` columns.
    pub fn tag_col_exprs(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if idx == self.le_column_index
                    || idx == self.field_column_index
                    || idx == self.ts_column_index
                {
                    None
                } else {
                    Some(Arc::new(PhyColumn::new(field.name(), idx)) as _)
                }
            })
            .collect()
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

    // buffers
    input_buffer: Vec<RecordBatch>,
    input_buffered_rows: usize,
    output_buffer: Vec<Box<dyn MutableVector>>,
    output_buffered_rows: usize,

    // runtime things
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
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
                None => break Poll::Ready(self.take_output_buf()?.map(Ok)),
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
        let Some(bucket_num) = self.calculate_bucket_num(&input)? else {
            return Ok(None);
        };

        if self.input_buffered_rows + input.num_rows() < bucket_num {
            // not enough rows to fold
            self.push_input_buf(input);
            return Ok(None);
        }

        self.fold_buf(bucket_num, input)?;
        if self.output_buffered_rows >= self.batch_size {
            return Ok(self.take_output_buf()?.map(Ok));
        }

        Ok(None)
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

    fn calculate_bucket_num(&mut self, batch: &RecordBatch) -> DataFusionResult<Option<usize>> {
        if let Some(size) = self.bucket_size {
            return Ok(Some(size));
        }

        let inf_pos = self.find_positive_inf(batch)?;
        if inf_pos == batch.num_rows() {
            // no positive inf found, append to buffer and wait for next batch
            self.push_input_buf(batch.clone());
            return Ok(None);
        }

        // else we found the positive inf.
        // calculate the bucket size
        let bucket_size = inf_pos + self.input_buffered_rows + 1;
        Ok(Some(bucket_size))
    }

    /// Fold record batches from input buffer and put to output buffer
    fn fold_buf(&mut self, bucket_num: usize, input: RecordBatch) -> DataFusionResult<()> {
        self.push_input_buf(input);
        // TODO(ruihang): this concat is avoidable.
        let batch = concat_batches(&self.input.schema(), self.input_buffer.drain(..).as_ref())?;
        let mut remaining_rows = self.input_buffered_rows;
        let mut cursor = 0;

        let gt_schema = GtSchema::try_from(self.input.schema()).unwrap();
        let batch = GtRecordBatch::try_from_df_record_batch(Arc::new(gt_schema), batch).unwrap();

        while remaining_rows >= bucket_num {
            // "sample" normal columns
            for normal_index in &self.normal_indices {
                let val = batch.column(*normal_index).get(cursor);
                self.output_buffer[*normal_index].push_value_ref(val.as_value_ref());
            }
            // "fold" `le` and field columns
            let le_array = batch.column(self.le_column_index);
            let field_array = batch.column(self.field_column_index);
            let mut bucket = vec![];
            let mut counters = vec![];
            for bias in 0..bucket_num {
                let le_str_val = le_array.get(cursor + bias);
                let le_str_val_ref = le_str_val.as_value_ref();
                let le_str = le_str_val_ref
                    .as_string()
                    .unwrap()
                    .expect("le column should not be nullable");
                let le = le_str.parse::<f64>().unwrap();
                bucket.push(le);

                let counter = field_array
                    .get(cursor + bias)
                    .as_value_ref()
                    .as_f64()
                    .unwrap()
                    .expect("field column should not be nullable");
                counters.push(counter);
            }
            // ignore invalid data
            let result = Self::evaluate_row(self.quantile, &bucket, &counters).unwrap_or(f64::NAN);
            self.output_buffer[self.field_column_index].push_value_ref(ValueRef::from(result));
            cursor += bucket_num;
            remaining_rows -= bucket_num;
            self.output_buffered_rows += 1;
        }

        let remaining_input_batch = batch.into_df_record_batch().slice(cursor, remaining_rows);
        self.input_buffered_rows = remaining_input_batch.num_rows();
        self.input_buffer.push(remaining_input_batch);

        Ok(())
    }

    fn push_input_buf(&mut self, batch: RecordBatch) {
        self.input_buffered_rows += batch.num_rows();
        self.input_buffer.push(batch);
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

    /// Find the first `+Inf` which indicates the end of the bucket group
    ///
    /// If the return value equals to batch's num_rows means the it's not found
    /// in this batch
    fn find_positive_inf(&self, batch: &RecordBatch) -> DataFusionResult<usize> {
        // fuse this function. It should not be called when the
        // bucket size is already know.
        if let Some(bucket_size) = self.bucket_size {
            return Ok(bucket_size);
        }
        let string_le_array = batch.column(self.le_column_index);
        let float_le_array = compute::cast(&string_le_array, &DataType::Float64).map_err(|e| {
            DataFusionError::Execution(format!(
                "cannot cast {} array to float64 array: {:?}",
                string_le_array.data_type(),
                e
            ))
        })?;
        let le_as_f64_array = float_le_array
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "expect a float64 array, but found {}",
                    float_le_array.data_type()
                ))
            })?;
        for (i, v) in le_as_f64_array.iter().enumerate() {
            if let Some(v) = v
                && v == f64::INFINITY
            {
                return Ok(i);
            }
        }

        Ok(batch.num_rows())
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
        debug_assert!(bucket.windows(2).all(|w| w[0] <= w[1]), "{bucket:?}");
        debug_assert!(counter.windows(2).all(|w| w[0] <= w[1]), "{counter:?}");

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
            Ok(lower_bound
                + (upper_bound - lower_bound) / (upper_count - lower_count)
                    * (expected_pos - lower_count))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::prelude::SessionContext;
    use datatypes::arrow_array::StringArray;

    use super::*;

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

    #[tokio::test]
    async fn fold_overall() {
        let memory_exec = Arc::new(prepare_test_data());
        let output_schema: SchemaRef = Arc::new(
            (*HistogramFold::convert_schema(
                &Arc::new(memory_exec.schema().to_dfschema().unwrap()),
                "le",
            )
            .unwrap()
            .as_ref())
            .clone()
            .into(),
        );
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let fold_exec = Arc::new(HistogramFoldExec {
            le_column_index: 1,
            field_column_index: 2,
            quantile: 0.4,
            ts_column_index: 9999, // not exist but doesn't matter
            input: memory_exec,
            output_schema,
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
    #[should_panic]
    fn evaluate_out_of_order_input() {
        let bucket = [0.0, 1.0, 2.0, 3.0, 4.0, f64::INFINITY];
        let counters = [5.0, 4.0, 3.0, 2.0, 1.0, 0.0];
        HistogramFoldStream::evaluate_row(0.5, &bucket, &counters).unwrap();
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
}
