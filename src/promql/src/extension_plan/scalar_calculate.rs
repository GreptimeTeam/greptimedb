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
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, DFSchemaRef, Result as DataFusionResult, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use datafusion::sql::TableReference;
use datatypes::arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use datatypes::arrow::compute::{cast_with_options, concat_batches, CastOptions};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datatypes::arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{ColumnNotFoundSnafu, DataFusionPlanningSnafu, DeserializeSnafu, Result};
use crate::extension_plan::Millisecond;

/// `ScalarCalculate` is the custom logical plan to calculate
/// [`scalar`](https://prometheus.io/docs/prometheus/latest/querying/functions/#scalar)
/// in PromQL, return NaN when have multiple time series.
///
/// Return the time series as scalar value when only have one time series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScalarCalculate {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,

    time_index: String,
    tag_columns: Vec<String>,
    field_column: String,
    input: LogicalPlan,
    output_schema: DFSchemaRef,
}

impl ScalarCalculate {
    /// create a new `ScalarCalculate` plan
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        input: LogicalPlan,
        time_index: &str,
        tag_colunms: &[String],
        field_column: &str,
        table_name: Option<&str>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let Ok(ts_field) = input_schema
            .field_with_unqualified_name(time_index)
            .cloned()
        else {
            return ColumnNotFoundSnafu { col: time_index }.fail();
        };
        let val_field = Field::new(format!("scalar({})", field_column), DataType::Float64, true);
        let qualifier = table_name.map(TableReference::bare);
        let schema = DFSchema::new_with_metadata(
            vec![
                (qualifier.clone(), Arc::new(ts_field)),
                (qualifier, Arc::new(val_field)),
            ],
            input_schema.metadata().clone(),
        )
        .context(DataFusionPlanningSnafu)?;

        Ok(Self {
            start,
            end,
            interval,
            time_index: time_index.to_string(),
            tag_columns: tag_colunms.to_vec(),
            field_column: field_column.to_string(),
            input,
            output_schema: Arc::new(schema),
        })
    }

    /// The name of this custom plan
    pub const fn name() -> &'static str {
        "ScalarCalculate"
    }

    /// Create a new execution plan from ScalarCalculate
    pub fn to_execution_plan(
        &self,
        exec_input: Arc<dyn ExecutionPlan>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let fields: Vec<_> = self
            .output_schema
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
            .collect();
        let input_schema = exec_input.schema();
        let ts_index = input_schema
            .index_of(&self.time_index)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let val_index = input_schema
            .index_of(&self.field_column)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let schema = Arc::new(Schema::new(fields));
        let properties = exec_input.properties();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            properties.emission_type,
            properties.boundedness,
        );
        Ok(Arc::new(ScalarCalculateExec {
            start: self.start,
            end: self.end,
            interval: self.interval,
            schema,
            input: exec_input,
            project_index: (ts_index, val_index),
            tag_columns: self.tag_columns.clone(),
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        }))
    }

    pub fn serialize(&self) -> Vec<u8> {
        pb::ScalarCalculate {
            start: self.start,
            end: self.end,
            interval: self.interval,
            time_index: self.time_index.clone(),
            tag_columns: self.tag_columns.clone(),
            field_column: self.field_column.clone(),
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_scalar_calculate = pb::ScalarCalculate::decode(bytes).context(DeserializeSnafu)?;
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        // TODO(Taylor-lagrange): Supports timestamps of different precisions
        let ts_field = Field::new(
            &pb_scalar_calculate.time_index,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        );
        let val_field = Field::new(
            format!("scalar({})", pb_scalar_calculate.field_column),
            DataType::Float64,
            true,
        );
        // TODO(Taylor-lagrange): missing tablename in pb
        let schema = DFSchema::new_with_metadata(
            vec![(None, Arc::new(ts_field)), (None, Arc::new(val_field))],
            HashMap::new(),
        )
        .context(DataFusionPlanningSnafu)?;

        Ok(Self {
            start: pb_scalar_calculate.start,
            end: pb_scalar_calculate.end,
            interval: pb_scalar_calculate.interval,
            time_index: pb_scalar_calculate.time_index,
            tag_columns: pb_scalar_calculate.tag_columns,
            field_column: pb_scalar_calculate.field_column,
            output_schema: Arc::new(schema),
            input: placeholder_plan,
        })
    }
}

impl PartialOrd for ScalarCalculate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare fields in order excluding output_schema
        match self.start.partial_cmp(&other.start) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.end.partial_cmp(&other.end) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.interval.partial_cmp(&other.interval) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.time_index.partial_cmp(&other.time_index) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.tag_columns.partial_cmp(&other.tag_columns) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.field_column.partial_cmp(&other.field_column) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for ScalarCalculate {
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

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ScalarCalculate: tags={:?}", self.tag_columns)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if !exprs.is_empty() {
            return Err(DataFusionError::Internal(
                "ScalarCalculate should not have any expressions".to_string(),
            ));
        }
        Ok(ScalarCalculate {
            start: self.start,
            end: self.end,
            interval: self.interval,
            time_index: self.time_index.clone(),
            tag_columns: self.tag_columns.clone(),
            field_column: self.field_column.clone(),
            input: inputs.into_iter().next().unwrap(),
            output_schema: self.output_schema.clone(),
        })
    }
}

#[derive(Debug, Clone)]
struct ScalarCalculateExec {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    schema: SchemaRef,
    project_index: (usize, usize),
    input: Arc<dyn ExecutionPlan>,
    tag_columns: Vec<String>,
    metric: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl ExecutionPlan for ScalarCalculateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ScalarCalculateExec {
            start: self.start,
            end: self.end,
            interval: self.interval,
            schema: self.schema.clone(),
            project_index: self.project_index,
            tag_columns: self.tag_columns.clone(),
            input: children[0].clone(),
            metric: self.metric.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let tag_indices = self
            .tag_columns
            .iter()
            .map(|tag| {
                schema
                    .column_with_name(tag)
                    .unwrap_or_else(|| panic!("tag column not found {tag}"))
                    .0
            })
            .collect();

        Ok(Box::pin(ScalarCalculateStream {
            start: self.start,
            end: self.end,
            interval: self.interval,
            schema: self.schema.clone(),
            project_index: self.project_index,
            metric: baseline_metric,
            tag_indices,
            input,
            have_multi_series: false,
            done: false,
            batch: None,
            tag_value: None,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        let input_stats = self.input.partition_statistics(partition)?;

        let estimated_row_num = (self.end - self.start) as f64 / self.interval as f64;
        let estimated_total_bytes = input_stats
            .total_byte_size
            .get_value()
            .zip(input_stats.num_rows.get_value())
            .map(|(size, rows)| {
                Precision::Inexact(((*size as f64 / *rows as f64) * estimated_row_num).floor() as _)
            })
            .unwrap_or_default();

        Ok(Statistics {
            num_rows: Precision::Inexact(estimated_row_num as _),
            total_byte_size: estimated_total_bytes,
            // TODO(ruihang): support this column statistics
            column_statistics: Statistics::unknown_column(&self.schema()),
        })
    }

    fn name(&self) -> &str {
        "ScalarCalculateExec"
    }
}

impl DisplayAs for ScalarCalculateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "ScalarCalculateExec: tags={:?}", self.tag_columns)
            }
        }
    }
}

struct ScalarCalculateStream {
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    metric: BaselineMetrics,
    tag_indices: Vec<usize>,
    /// with format `(ts_index, field_index)`
    project_index: (usize, usize),
    have_multi_series: bool,
    done: bool,
    batch: Option<RecordBatch>,
    tag_value: Option<Vec<String>>,
}

impl RecordBatchStream for ScalarCalculateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ScalarCalculateStream {
    fn update_batch(&mut self, batch: RecordBatch) -> DataFusionResult<()> {
        let _timer = self.metric.elapsed_compute();
        // if have multi time series or empty batch, scalar will return NaN
        if self.have_multi_series || batch.num_rows() == 0 {
            return Ok(());
        }
        // fast path: no tag columns means all data belongs to the same series.
        if self.tag_indices.is_empty() {
            self.append_batch(batch)?;
            return Ok(());
        }
        let all_same = |val: Option<&str>, array: &StringArray| -> bool {
            if let Some(v) = val {
                array.iter().all(|s| s == Some(v))
            } else {
                array.is_empty() || array.iter().skip(1).all(|s| s == Some(array.value(0)))
            }
        };
        // assert the entire batch belong to the same series
        let all_tag_columns_same = if let Some(tags) = &self.tag_value {
            tags.iter()
                .zip(self.tag_indices.iter())
                .all(|(value, index)| {
                    let array = batch.column(*index);
                    let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    all_same(Some(value), string_array)
                })
        } else {
            let mut tag_values = Vec::with_capacity(self.tag_indices.len());
            let is_same = self.tag_indices.iter().all(|index| {
                let array = batch.column(*index);
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                tag_values.push(string_array.value(0).to_string());
                all_same(None, string_array)
            });
            self.tag_value = Some(tag_values);
            is_same
        };
        if all_tag_columns_same {
            self.append_batch(batch)?;
        } else {
            self.have_multi_series = true;
        }
        Ok(())
    }

    fn append_batch(&mut self, input_batch: RecordBatch) -> DataFusionResult<()> {
        let ts_column = input_batch.column(self.project_index.0).clone();
        let val_column = cast_with_options(
            input_batch.column(self.project_index.1),
            &DataType::Float64,
            &CastOptions::default(),
        )?;
        let input_batch = RecordBatch::try_new(self.schema.clone(), vec![ts_column, val_column])?;
        if let Some(batch) = &self.batch {
            self.batch = Some(concat_batches(&self.schema, vec![batch, &input_batch])?);
        } else {
            self.batch = Some(input_batch);
        }
        Ok(())
    }
}

impl Stream for ScalarCalculateStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.done {
                return Poll::Ready(None);
            }
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.update_batch(batch)?;
                }
                // inner had error, return to caller
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                // inner is done, producing output
                None => {
                    self.done = true;
                    return match self.batch.take() {
                        Some(batch) if !self.have_multi_series => {
                            self.metric.record_output(batch.num_rows());
                            Poll::Ready(Some(Ok(batch)))
                        }
                        _ => {
                            let time_array = (self.start..=self.end)
                                .step_by(self.interval as _)
                                .collect::<Vec<_>>();
                            let nums = time_array.len();
                            let nan_batch = RecordBatch::try_new(
                                self.schema.clone(),
                                vec![
                                    Arc::new(TimestampMillisecondArray::from(time_array)),
                                    Arc::new(Float64Array::from(vec![f64::NAN; nums])),
                                ],
                            )?;
                            self.metric.record_output(nan_batch.num_rows());
                            Poll::Ready(Some(Ok(nan_batch)))
                        }
                    };
                }
            };
        }
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::prelude::SessionContext;
    use datatypes::arrow::array::{Float64Array, TimestampMillisecondArray};
    use datatypes::arrow::datatypes::TimeUnit;

    use super::*;

    fn prepare_test_data(series: Vec<RecordBatch>) -> DataSourceExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[series], schema, None).unwrap(),
        ))
    }

    async fn run_test(series: Vec<RecordBatch>, expected: &str) {
        let memory_exec = Arc::new(prepare_test_data(series));
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("val", DataType::Float64, true),
        ]));
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let scalar_exec = Arc::new(ScalarCalculateExec {
            start: 0,
            end: 15_000,
            interval: 5000,
            tag_columns: vec!["tag1".to_string(), "tag2".to_string()],
            input: memory_exec,
            schema,
            project_index: (0, 3),
            metric: ExecutionPlanMetricsSet::new(),
            properties,
        });
        let session_context = SessionContext::default();
        let result = datafusion::physical_plan::collect(scalar_exec, session_context.task_ctx())
            .await
            .unwrap();
        let result_literal = datatypes::arrow::util::pretty::pretty_format_batches(&result)
            .unwrap()
            .to_string();
        assert_eq!(result_literal, expected);
    }

    #[tokio::test]
    async fn same_series() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        run_test(
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(vec![0, 5_000])),
                        Arc::new(StringArray::from(vec!["foo", "foo"])),
                        Arc::new(StringArray::from(vec!["🥺", "🥺"])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampMillisecondArray::from(vec![10_000, 15_000])),
                        Arc::new(StringArray::from(vec!["foo", "foo"])),
                        Arc::new(StringArray::from(vec!["🥺", "🥺"])),
                        Arc::new(Float64Array::from(vec![3.0, 4.0])),
                    ],
                )
                .unwrap(),
            ],
            "+---------------------+-----+\
            \n| ts                  | val |\
            \n+---------------------+-----+\
            \n| 1970-01-01T00:00:00 | 1.0 |\
            \n| 1970-01-01T00:00:05 | 2.0 |\
            \n| 1970-01-01T00:00:10 | 3.0 |\
            \n| 1970-01-01T00:00:15 | 4.0 |\
            \n+---------------------+-----+",
        )
        .await
    }

    #[tokio::test]
    async fn diff_series() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        run_test(
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(vec![0, 5_000])),
                        Arc::new(StringArray::from(vec!["foo", "foo"])),
                        Arc::new(StringArray::from(vec!["🥺", "🥺"])),
                        Arc::new(Float64Array::from(vec![1.0, 2.0])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(TimestampMillisecondArray::from(vec![10_000, 15_000])),
                        Arc::new(StringArray::from(vec!["foo", "foo"])),
                        Arc::new(StringArray::from(vec!["🥺", "😝"])),
                        Arc::new(Float64Array::from(vec![3.0, 4.0])),
                    ],
                )
                .unwrap(),
            ],
            "+---------------------+-----+\
            \n| ts                  | val |\
            \n+---------------------+-----+\
            \n| 1970-01-01T00:00:00 | NaN |\
            \n| 1970-01-01T00:00:05 | NaN |\
            \n| 1970-01-01T00:00:10 | NaN |\
            \n| 1970-01-01T00:00:15 | NaN |\
            \n+---------------------+-----+",
        )
        .await
    }

    #[tokio::test]
    async fn empty_series() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("val", DataType::Float64, true),
        ]));
        run_test(
            vec![RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(TimestampMillisecondArray::new_null(0)),
                    Arc::new(StringArray::new_null(0)),
                    Arc::new(StringArray::new_null(0)),
                    Arc::new(Float64Array::new_null(0)),
                ],
            )
            .unwrap()],
            "+---------------------+-----+\
            \n| ts                  | val |\
            \n+---------------------+-----+\
            \n| 1970-01-01T00:00:00 | NaN |\
            \n| 1970-01-01T00:00:05 | NaN |\
            \n| 1970-01-01T00:00:10 | NaN |\
            \n| 1970-01-01T00:00:15 | NaN |\
            \n+---------------------+-----+",
        )
        .await
    }
}
