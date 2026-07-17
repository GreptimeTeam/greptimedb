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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use ahash::HashSet;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::hash_utils::RandomState as FixedState;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, hash_utils,
};
use datafusion_expr::col;
use datatypes::arrow::compute;
use futures::{Stream, StreamExt, ready};
use greptime_proto::substrait_extension as pb;
use prost::Message;
use snafu::ResultExt;

use crate::error::{DataFusionPlanningSnafu, DeserializeSnafu, Result};

/// A special kind of `UNION`(`OR` in PromQL) operator, for PromQL specific use case.
///
/// This operator is similar to `UNION` from SQL, but it only accepts two inputs. The
/// most different part is that it treat left child and right child differently:
/// - All columns from left child will be outputted.
/// - Only check collisions (when not distinct) on the columns specified by `compare_keys`.
/// - Rows from the right child with the same comparison signature are all preserved.
/// - If a signature occurs in the left child, all matching rows from the right child are discarded.
/// - Output preserves each input's batch and row order, with all left rows before right rows.
/// - The output schema is based on the left child schema, with nullability widened from both inputs.
///
/// Execution streams the left child first while retaining its comparison signatures. The
/// right child is polled only after the left child completes, then rows whose signatures were
/// observed on the left are omitted.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct UnionDistinctOn {
    left: LogicalPlan,
    right: LogicalPlan,
    /// The columns to compare for equality.
    /// TIME INDEX is included.
    compare_key_indices: Vec<usize>,
    ts_col_idx: usize,
    output_schema: DFSchemaRef,
}

impl UnionDistinctOn {
    pub fn name() -> &'static str {
        "UnionDistinctOn"
    }

    pub fn try_new(
        left: LogicalPlan,
        right: LogicalPlan,
        compare_key_indices: Vec<usize>,
        ts_col_idx: usize,
    ) -> DataFusionResult<Self> {
        let output_schema =
            Self::validate_children(&left, &right, &compare_key_indices, ts_col_idx)?;
        Ok(Self {
            left,
            right,
            compare_key_indices,
            ts_col_idx,
            output_schema,
        })
    }

    fn validate_children(
        left: &LogicalPlan,
        right: &LogicalPlan,
        compare_key_indices: &[usize],
        ts_col_idx: usize,
    ) -> DataFusionResult<DFSchemaRef> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let left_fields = left_schema.fields();
        let right_fields = right_schema.fields();

        if left_fields.len() != right_fields.len() {
            return Err(DataFusionError::Plan(format!(
                "UnionDistinctOn inputs have different field counts: left={}, right={}",
                left_fields.len(),
                right_fields.len()
            )));
        }

        for (column_type, index) in compare_key_indices
            .iter()
            .map(|index| ("compare key", *index))
            .chain(std::iter::once(("timestamp", ts_col_idx)))
        {
            if index >= left_fields.len() || index >= right_fields.len() {
                return Err(DataFusionError::Plan(format!(
                    "UnionDistinctOn {column_type} index {index} is out of bounds for inputs with {} fields",
                    left_fields.len()
                )));
            }
        }

        for (index, (left_field, right_field)) in left_fields.iter().zip(right_fields).enumerate() {
            if left_field.data_type() != right_field.data_type() {
                return Err(DataFusionError::Plan(format!(
                    "UnionDistinctOn input field at index {index} has incompatible data types: left={:?}, right={:?}",
                    left_field.data_type(),
                    right_field.data_type()
                )));
            }
        }

        let output_fields = left_fields
            .iter()
            .zip(right_fields)
            .enumerate()
            .map(|(index, (left_field, right_field))| {
                let (qualifier, _) = left_schema.qualified_field(index);
                (
                    qualifier.cloned(),
                    Arc::new(
                        left_field
                            .as_ref()
                            .clone()
                            .with_nullable(left_field.is_nullable() || right_field.is_nullable()),
                    ),
                )
            })
            .collect();
        let output_schema =
            DFSchema::new_with_metadata(output_fields, left_schema.metadata().clone()).map_err(
                |error| {
                    DataFusionError::Plan(format!(
                        "Failed to construct UnionDistinctOn output schema: {error}"
                    ))
                },
            )?;

        Ok(Arc::new(output_schema))
    }

    pub fn to_execution_plan(
        &self,
        left_exec: Arc<dyn ExecutionPlan>,
        right_exec: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let output_schema: SchemaRef = self.output_schema.inner().clone();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Arc::new(UnionDistinctOnExec {
            left: left_exec,
            right: right_exec,
            compare_key_indices: self.compare_key_indices.clone(),
            ts_col_idx: self.ts_col_idx,
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            random_state: FixedState::with_seed(0),
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let compare_key_indices = self
            .compare_key_indices
            .iter()
            .map(|index| u64::try_from(*index).expect("usize always fits in u64"))
            .collect();
        let ts_col_idx = u64::try_from(self.ts_col_idx).expect("usize always fits in u64");

        pb::UnionDistinctOn {
            compare_key_indices,
            ts_col_idx,
        }
        .encode_to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        let pb_union = pb::UnionDistinctOn::decode(bytes).context(DeserializeSnafu)?;
        let placeholder_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });

        let compare_key_indices = pb_union
            .compare_key_indices
            .into_iter()
            .map(|index| {
                usize::try_from(index).map_err(|_| {
                    DataFusionError::Plan(format!(
                        "UnionDistinctOn compare key index {index} does not fit in usize"
                    ))
                })
            })
            .collect::<DataFusionResult<Vec<_>>>()
            .context(DataFusionPlanningSnafu)?;
        let ts_col_idx = usize::try_from(pb_union.ts_col_idx)
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "UnionDistinctOn timestamp index {} does not fit in usize",
                    pb_union.ts_col_idx
                ))
            })
            .context(DataFusionPlanningSnafu)?;

        Ok(Self {
            left: placeholder_plan.clone(),
            right: placeholder_plan,
            compare_key_indices,
            ts_col_idx,
            output_schema: Arc::new(DFSchema::empty()),
        })
    }
}

impl PartialOrd for UnionDistinctOn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Compare fields in order excluding output_schema
        match self.left.partial_cmp(&other.left) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.right.partial_cmp(&other.right) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self
            .compare_key_indices
            .partial_cmp(&other.compare_key_indices)
        {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.ts_col_idx.partial_cmp(&other.ts_col_idx)
    }
}

impl UserDefinedLogicalNodeCore for UnionDistinctOn {
    fn name(&self) -> &str {
        Self::name()
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left, &self.right]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.output_schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let fields = self.left.schema().fields();
        let mut exprs = self
            .compare_key_indices
            .iter()
            .filter_map(|index| fields.get(*index).map(|field| col(field.name())))
            .collect::<Vec<_>>();
        if !self.compare_key_indices.contains(&self.ts_col_idx)
            && let Some(field) = fields.get(self.ts_col_idx)
        {
            exprs.push(col(field.name()));
        }
        exprs
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        let left_len = self.left.schema().fields().len();
        let right_len = self.right.schema().fields().len();
        Some(vec![
            (0..left_len).collect::<Vec<_>>(),
            (0..right_len).collect::<Vec<_>>(),
        ])
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields = self.left.schema().fields();
        let display_column = |index: usize| match fields.get(index) {
            Some(field) => format!("{}@{index}", field.name()),
            None => format!("@{index}"),
        };
        let compare_keys = self
            .compare_key_indices
            .iter()
            .map(|index| display_column(*index))
            .collect::<Vec<_>>();
        write!(
            f,
            "UnionDistinctOn: on col={compare_keys:?}, ts_col={}",
            display_column(self.ts_col_idx)
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if inputs.len() != 2 {
            return Err(DataFusionError::Internal(
                "UnionDistinctOn must have exactly 2 inputs".to_string(),
            ));
        }

        let mut inputs = inputs.into_iter();
        let left = inputs.next().unwrap();
        let right = inputs.next().unwrap();

        Self::try_new(
            left,
            right,
            self.compare_key_indices.clone(),
            self.ts_col_idx,
        )
    }
}

#[derive(Debug)]
pub struct UnionDistinctOnExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    compare_key_indices: Vec<usize>,
    ts_col_idx: usize,
    output_schema: SchemaRef,
    metric: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,

    /// Shared deterministic hash state for the hashing algorithm.
    random_state: FixedState,
}

impl ExecutionPlan for UnionDistinctOnExec {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition, Distribution::SinglePartition]
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 2);

        let left = children[0].clone();
        let right = children[1].clone();
        Ok(Arc::new(UnionDistinctOnExec {
            left,
            right,
            compare_key_indices: self.compare_key_indices.clone(),
            ts_col_idx: self.ts_col_idx,
            output_schema: self.output_schema.clone(),
            metric: self.metric.clone(),
            properties: self.properties.clone(),
            random_state: self.random_state.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let left_stream = self.left.execute(partition, context.clone())?;

        let mut key_indices = self.compare_key_indices.clone();
        key_indices.push(self.ts_col_idx);

        Ok(Box::pin(UnionDistinctOnStream {
            left: left_stream,
            right_plan: self.right.clone(),
            right_partition: partition,
            right_context: context,
            right: None,
            compare_keys: key_indices,
            output_schema: self.output_schema.clone(),
            random_state: self.random_state.clone(),
            lhs_signatures: HashSet::default(),
            hashes: Vec::new(),
            phase: StreamPhase::Left,
            metric: BaselineMetrics::new(&self.metric, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn name(&self) -> &str {
        "UnionDistinctOnExec"
    }
}

impl DisplayAs for UnionDistinctOnExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "UnionDistinctOnExec: on col={:?}, ts_col={}",
                    self.compare_key_indices, self.ts_col_idx
                )
            }
        }
    }
}

pub struct UnionDistinctOnStream {
    left: SendableRecordBatchStream,
    right_plan: Arc<dyn ExecutionPlan>,
    right_partition: usize,
    right_context: Arc<TaskContext>,
    right: Option<SendableRecordBatchStream>,
    /// Include time index
    compare_keys: Vec<usize>,
    output_schema: SchemaRef,
    random_state: FixedState,
    lhs_signatures: HashSet<u64>,
    hashes: Vec<u64>,
    phase: StreamPhase,
    metric: BaselineMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamPhase {
    Left,
    Right,
    Done,
}

impl UnionDistinctOnStream {
    fn hash_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        let arrays = self
            .compare_keys
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect::<Vec<_>>();
        self.hashes.clear();
        self.hashes.resize(batch.num_rows(), 0);
        hash_utils::create_hashes(&arrays, &self.random_state, &mut self.hashes)?;
        Ok(())
    }

    fn filter_rhs_batch(&mut self, batch: RecordBatch) -> DataFusionResult<Option<RecordBatch>> {
        self.hash_batch(&batch)?;
        let mut survivor_indices: Option<Vec<usize>> = None;
        for (index, hash) in self.hashes.iter().enumerate() {
            if self.lhs_signatures.contains(hash) {
                survivor_indices.get_or_insert_with(|| (0..index).collect());
            } else if let Some(indices) = &mut survivor_indices {
                indices.push(index);
            }
        }

        match survivor_indices {
            None => Ok(Some(with_schema(batch, self.output_schema.clone())?)),
            Some(indices) if indices.is_empty() => Ok(None),
            Some(indices) => Ok(Some(with_schema(
                take_batch(&batch, &indices)?,
                self.output_schema.clone(),
            )?)),
        }
    }

    fn terminal_error(&mut self, error: DataFusionError) -> Poll<Option<<Self as Stream>::Item>> {
        self.phase = StreamPhase::Done;
        Poll::Ready(Some(Err(error)))
    }

    fn poll_right(&mut self, cx: &mut Context<'_>) -> Poll<Option<<Self as Stream>::Item>> {
        if self.right.is_none() {
            let right = match self
                .right_plan
                .execute(self.right_partition, self.right_context.clone())
            {
                Ok(right) => right,
                Err(error) => return self.terminal_error(error),
            };
            self.right = Some(right);
        }

        let right = self.right.as_mut().expect("right stream is initialized");
        match ready!(right.poll_next_unpin(cx)) {
            Some(Ok(batch)) if self.lhs_signatures.is_empty() => {
                match with_schema(batch, self.output_schema.clone()) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(error) => self.terminal_error(error),
                }
            }
            Some(Ok(batch)) => match self.filter_rhs_batch(batch) {
                Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                Ok(None) => {
                    // One fully filtered input batch has been consumed. Yield so a sequence
                    // of filtered batches cannot monopolize a single downstream poll.
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(error) => self.terminal_error(error),
            },
            Some(Err(error)) => self.terminal_error(error),
            None => {
                self.phase = StreamPhase::Done;
                Poll::Ready(None)
            }
        }
    }

    fn poll_impl(&mut self, cx: &mut Context<'_>) -> Poll<Option<<Self as Stream>::Item>> {
        match self.phase {
            StreamPhase::Left => match ready!(self.left.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        if let Err(error) = self.hash_batch(&batch) {
                            return self.terminal_error(error);
                        }
                        self.lhs_signatures.extend(self.hashes.iter().copied());
                    }
                    match with_schema(batch, self.output_schema.clone()) {
                        Ok(batch) => Poll::Ready(Some(Ok(batch))),
                        Err(error) => self.terminal_error(error),
                    }
                }
                Some(Err(error)) => self.terminal_error(error),
                None => {
                    self.phase = StreamPhase::Right;
                    self.poll_right(cx)
                }
            },
            StreamPhase::Right => self.poll_right(cx),
            StreamPhase::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for UnionDistinctOnStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for UnionDistinctOnStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_impl(cx);
        self.metric.record_poll(poll)
    }
}

fn with_schema(batch: RecordBatch, schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Utility function to take ordered rows from a record batch.
fn take_batch(batch: &RecordBatch, indices: &[usize]) -> DataFusionResult<RecordBatch> {
    if batch.num_rows() == indices.len() {
        return Ok(batch.clone());
    }

    let indices_array = UInt64Array::from_iter(indices.iter().map(|index| *index as u64));
    let arrays = batch
        .columns()
        .iter()
        .map(|array| compute::take(array, &indices_array, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;

    RecordBatch::try_new(batch.schema(), arrays)
        .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    use super::*;

    #[test]
    fn pruning_should_keep_all_columns_for_exec() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int32, false),
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let df_schema = schema.to_dfschema_ref().unwrap();
        let left = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema.clone(),
        });
        let right = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema.clone(),
        });
        let plan = UnionDistinctOn::try_new(left, right, vec![1], 0).unwrap();

        // Simulate a parent projection requesting only one output column.
        let output_columns = [2usize];
        let required = plan.necessary_children_exprs(&output_columns).unwrap();
        assert_eq!(required.len(), 2);
        assert_eq!(required[0].as_slice(), &[0, 1, 2]);
        assert_eq!(required[1].as_slice(), &[0, 1, 2]);
    }

    #[test]
    fn test_take_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )
        .unwrap();

        let indices = vec![0, 2];
        let result = take_batch(&batch, &indices).unwrap();

        let expected = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 3])),
                Arc::new(Int32Array::from(vec![4, 6])),
            ],
        )
        .unwrap();

        assert_eq!(result, expected);
    }

    fn empty_plan(schema: datafusion::common::DFSchemaRef) -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema,
        })
    }

    fn schemas(left: SchemaRef, right: SchemaRef) -> (LogicalPlan, LogicalPlan) {
        (
            empty_plan(left.to_dfschema_ref().unwrap()),
            empty_plan(right.to_dfschema_ref().unwrap()),
        )
    }

    fn schema(prefix: &str, key: &str, nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}_ts"), DataType::Int64, false),
            Field::new(format!("{prefix}_{key}"), DataType::Utf8, nullable),
            Field::new(format!("{prefix}_value"), DataType::Float64, nullable),
        ]))
    }

    fn batch(schema: SchemaRef, ts: i64, key: Option<&str>, value: Option<f64>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![ts])),
                Arc::new(StringArray::from(vec![key])),
                Arc::new(Float64Array::from(vec![value])),
            ],
        )
        .unwrap()
    }

    fn source_exec(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        source_exec_batches(batch.schema(), vec![batch])
    }

    async fn execute(
        plan: &UnionDistinctOn,
        left: RecordBatch,
        right: RecordBatch,
    ) -> Vec<RecordBatch> {
        datafusion::physical_plan::collect(
            plan.to_execution_plan(source_exec(left), source_exec(right)),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap()
    }

    fn source_exec_batches(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[batches], schema, None).unwrap(),
        )))
    }

    async fn execute_batches(
        plan: &UnionDistinctOn,
        left_schema: SchemaRef,
        left: Vec<RecordBatch>,
        right_schema: SchemaRef,
        right: Vec<RecordBatch>,
    ) -> Vec<RecordBatch> {
        datafusion::physical_plan::collect(
            plan.to_execution_plan(
                source_exec_batches(left_schema, left),
                source_exec_batches(right_schema, right),
            ),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap()
    }

    fn simple_schema(prefix: &str, nullable: bool) -> SchemaRef {
        schema(prefix, "label", nullable)
    }

    fn simple_batch(schema: SchemaRef, rows: &[(i64, &str, f64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(Float64Array::from_iter_values(rows.iter().map(|row| row.2))),
            ],
        )
        .unwrap()
    }

    fn simple_rows(batches: &[RecordBatch]) -> Vec<(i64, String, f64)> {
        batches
            .iter()
            .flat_map(|batch| {
                let timestamps = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let labels = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..batch.num_rows()).map(move |row| {
                    (
                        timestamps.value(row),
                        labels.value(row).to_string(),
                        values.value(row),
                    )
                })
            })
            .collect()
    }

    fn simple_plan(left_schema: SchemaRef, right_schema: SchemaRef) -> UnionDistinctOn {
        let (left, right) = schemas(left_schema, right_schema);
        UnionDistinctOn::try_new(left, right, vec![1], 0).unwrap()
    }

    #[derive(Clone, Debug)]
    enum TestEvent {
        Batch(RecordBatch),
        Error(&'static str),
    }

    struct TestStream {
        schema: SchemaRef,
        events: VecDeque<TestEvent>,
        polls: Arc<AtomicUsize>,
    }

    impl Stream for TestStream {
        type Item = DataFusionResult<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            match self.events.pop_front() {
                Some(TestEvent::Batch(batch)) => Poll::Ready(Some(Ok(batch))),
                Some(TestEvent::Error(message)) => {
                    Poll::Ready(Some(Err(DataFusionError::Internal(message.to_string()))))
                }
                None => Poll::Ready(None),
            }
        }
    }

    impl RecordBatchStream for TestStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    struct CountingWake(AtomicUsize);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn test_stream(
        schema: SchemaRef,
        events: Vec<TestEvent>,
        polls: Arc<AtomicUsize>,
    ) -> SendableRecordBatchStream {
        Box::pin(TestStream {
            schema,
            events: events.into(),
            polls,
        })
    }

    #[derive(Debug)]
    struct TestExec {
        schema: SchemaRef,
        events: Vec<TestEvent>,
        polls: Arc<AtomicUsize>,
        executions: Arc<AtomicUsize>,
        execute_error: Option<&'static str>,
        properties: Arc<PlanProperties>,
    }

    impl TestExec {
        fn new(
            schema: SchemaRef,
            events: Vec<TestEvent>,
            polls: Arc<AtomicUsize>,
            executions: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                schema,
                events,
                polls,
                executions,
                execute_error: None,
            }
        }

        fn with_execute_error(mut self, error: &'static str) -> Self {
            self.execute_error = Some(error);
            self
        }
    }

    impl DisplayAs for TestExec {
        fn fmt_as(&self, _t: DisplayFormatType, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
            Ok(())
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            "TestExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> DataFusionResult<SendableRecordBatchStream> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.execute_error {
                return Err(DataFusionError::Internal(error.to_string()));
            }
            Ok(test_stream(
                self.schema.clone(),
                self.events.clone(),
                self.polls.clone(),
            ))
        }
    }

    fn test_union_stream(
        left: SendableRecordBatchStream,
        right: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> UnionDistinctOnStream {
        let metrics = ExecutionPlanMetricsSet::new();
        UnionDistinctOnStream {
            left,
            right_plan: right,
            right_partition: 0,
            right_context: SessionContext::default().task_ctx(),
            right: None,
            compare_keys: vec![1, 0],
            output_schema,
            random_state: FixedState::with_seed(0),
            lhs_signatures: HashSet::default(),
            hashes: Vec::new(),
            phase: StreamPhase::Left,
            metric: BaselineMetrics::new(&metrics, 0),
        }
    }

    fn series_rows(batches: &[RecordBatch]) -> Vec<(i64, &str, f64, &str)> {
        batches
            .iter()
            .flat_map(|batch| {
                let timestamps = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let series = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let keys = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows()).map(move |row| {
                    (
                        timestamps.value(row),
                        series.value(row),
                        values.value(row),
                        keys.value(row),
                    )
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn serialize_deserialize_and_execute_with_different_input_names() {
        let (left_schema, right_schema) =
            (schema("left", "job", false), schema("right", "job", false));
        let (left_plan, right_plan) = schemas(left_schema.clone(), right_schema.clone());
        let decoded = UnionDistinctOn::deserialize(
            &UnionDistinctOn::try_new(left_plan.clone(), right_plan.clone(), vec![1], 0)
                .unwrap()
                .serialize(),
        )
        .unwrap()
        .with_exprs_and_inputs(vec![], vec![left_plan, right_plan])
        .unwrap();
        assert_eq!(
            (decoded.compare_key_indices.as_slice(), decoded.ts_col_idx),
            (&[1usize][..], 0)
        );
        assert_eq!(
            decoded.output_schema,
            left_schema.clone().to_dfschema_ref().unwrap()
        );
        let result = execute(
            &decoded,
            batch(left_schema.clone(), 1, Some("left"), Some(10.0)),
            batch(right_schema, 2, Some("right"), Some(20.0)),
        )
        .await;
        assert!(result.iter().all(|batch| batch.schema() == left_schema));
    }

    #[tokio::test]
    async fn execute_widens_nullable_fields_and_emits_rhs_nulls() {
        let (left_schema, right_schema) = (
            schema("left", "label", false),
            schema("right", "label", true),
        );
        let (left_plan, right_plan) = schemas(left_schema.clone(), right_schema.clone());
        let plan = UnionDistinctOn::try_new(left_plan, right_plan, vec![1, 2], 0).unwrap();
        let declared_schema = plan.output_schema.inner().clone();
        assert!(
            declared_schema.fields()[1..]
                .iter()
                .all(|field| field.is_nullable())
        );
        let result = execute(
            &plan,
            batch(left_schema, 1, Some("present"), Some(10.0)),
            batch(right_schema, 2, None, None),
        )
        .await;
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|batch| batch.schema() == declared_schema));
        assert!(result[1].column(1).is_null(0));
        assert!(result[1].column(2).is_null(0));
    }

    #[tokio::test]
    async fn empty_lhs_preserves_distinct_rhs_series_with_same_normalized_key() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("series", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("__normalized_absent_label", DataType::Utf8, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("rhs_ts", DataType::Int64, false),
            Field::new("rhs_series", DataType::Utf8, false),
            Field::new("rhs_value", DataType::Float64, false),
            Field::new("rhs_normalized_absent_label", DataType::Utf8, false),
        ]));
        let (left, right) = schemas(left_schema.clone(), right_schema.clone());
        let plan = UnionDistinctOn::try_new(left, right, vec![3], 0).unwrap();
        let declared_schema = plan.output_schema.inner().clone();
        let rhs = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_000, 1_000])),
                Arc::new(StringArray::from(vec!["series_a", "series_b"])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
                Arc::new(StringArray::from(vec!["", ""])),
            ],
        )
        .unwrap();

        let result = execute(&plan, RecordBatch::new_empty(left_schema), rhs).await;
        assert!(result.iter().all(|batch| batch.schema() == declared_schema));
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

        let mut rows = series_rows(&result);
        rows.sort_by_key(|row| row.1);
        assert_eq!(
            rows,
            vec![(1_000, "series_a", 10.0, ""), (1_000, "series_b", 20.0, "")]
        );
    }

    #[tokio::test]
    async fn lhs_signature_suppresses_all_matching_rhs_rows_and_preserves_lhs() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("series", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("__normalized_absent_label", DataType::Utf8, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("rhs_ts", DataType::Int64, false),
            Field::new("rhs_series", DataType::Utf8, false),
            Field::new("rhs_value", DataType::Float64, false),
            Field::new("rhs_normalized_absent_label", DataType::Utf8, false),
        ]));
        let (left, right) = schemas(left_schema.clone(), right_schema.clone());
        let plan = UnionDistinctOn::try_new(left, right, vec![3], 0).unwrap();
        let declared_schema = plan.output_schema.inner().clone();
        let lhs = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_000, 2_000])),
                Arc::new(StringArray::from(vec!["lhs_match", "lhs_only"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
                Arc::new(StringArray::from(vec!["", "left-only"])),
            ],
        )
        .unwrap();
        let rhs = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int64Array::from(vec![1_000, 1_000, 3_000])),
                Arc::new(StringArray::from(vec![
                    "rhs_match_a",
                    "rhs_match_b",
                    "rhs_unmatched",
                ])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
                Arc::new(StringArray::from(vec!["", "", ""])),
            ],
        )
        .unwrap();

        let result = execute(&plan, lhs, rhs).await;
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(result.iter().all(|batch| batch.schema() == declared_schema));
        let mut rows = series_rows(&result);
        rows.sort_by_key(|row| row.1);
        assert_eq!(
            rows,
            vec![
                (1_000, "lhs_match", 1.0, ""),
                (2_000, "lhs_only", 2.0, "left-only"),
                (3_000, "rhs_unmatched", 30.0, ""),
            ]
        );
    }

    #[tokio::test]
    async fn empty_lhs_preserves_duplicate_rhs_rows_across_batches() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());
        let result = execute_batches(
            &plan,
            left_schema,
            vec![],
            right_schema.clone(),
            vec![
                simple_batch(
                    right_schema.clone(),
                    &[(1, "same", 10.0), (1, "same", 11.0)],
                ),
                simple_batch(right_schema, &[(1, "same", 12.0)]),
            ],
        )
        .await;
        assert_eq!(
            simple_rows(&result),
            vec![
                (1, "same".to_string(), 10.0),
                (1, "same".to_string(), 11.0),
                (1, "same".to_string(), 12.0),
            ]
        );
    }

    #[tokio::test]
    async fn lhs_signature_suppresses_rhs_duplicates_across_batches() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());
        let result = execute_batches(
            &plan,
            left_schema.clone(),
            vec![simple_batch(left_schema, &[(1, "match", 1.0)])],
            right_schema.clone(),
            vec![
                simple_batch(right_schema.clone(), &[(1, "match", 10.0)]),
                simple_batch(right_schema, &[(1, "match", 11.0)]),
            ],
        )
        .await;
        assert_eq!(simple_rows(&result), vec![(1, "match".to_string(), 1.0)]);
    }

    #[tokio::test]
    async fn hash_scratch_reset_suppresses_null_label_after_same_sized_lhs_batches() {
        let left_schema = simple_schema("left", true);
        let right_schema = simple_schema("right", true);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());
        let lhs_poison = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("poison")])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let lhs_null = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        let rhs_null = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Float64Array::from(vec![999.0])),
            ],
        )
        .unwrap();

        let result = execute_batches(
            &plan,
            left_schema,
            vec![lhs_poison, lhs_null],
            right_schema,
            vec![rhs_null],
        )
        .await;
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let values = result
            .iter()
            .flat_map(|batch| {
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![1.0, 2.0]);
    }

    #[tokio::test]
    async fn mixed_rhs_duplicates_retain_unmatched_rows_in_order() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());
        let result = execute_batches(
            &plan,
            left_schema.clone(),
            vec![simple_batch(left_schema, &[(1, "match", 1.0)])],
            right_schema.clone(),
            vec![simple_batch(
                right_schema,
                &[
                    (1, "match", 10.0),
                    (1, "keep", 11.0),
                    (1, "keep", 12.0),
                    (1, "match", 13.0),
                    (1, "later", 14.0),
                ],
            )],
        )
        .await;
        assert_eq!(
            simple_rows(&result),
            vec![
                (1, "match".to_string(), 1.0),
                (1, "keep".to_string(), 11.0),
                (1, "keep".to_string(), 12.0),
                (1, "later".to_string(), 14.0),
            ]
        );
    }

    #[tokio::test]
    async fn same_label_at_different_timestamps_is_unmatched() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());
        let result = execute_batches(
            &plan,
            left_schema.clone(),
            vec![simple_batch(left_schema, &[(1, "label", 1.0)])],
            right_schema.clone(),
            vec![simple_batch(right_schema, &[(2, "label", 2.0)])],
        )
        .await;
        assert_eq!(
            simple_rows(&result),
            vec![(1, "label".to_string(), 1.0), (2, "label".to_string(), 2.0)]
        );
    }

    #[tokio::test]
    async fn empty_input_combinations_complete_cleanly() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());

        let empty_left = execute_batches(
            &plan,
            left_schema.clone(),
            vec![],
            right_schema.clone(),
            vec![simple_batch(right_schema.clone(), &[(1, "rhs", 1.0)])],
        )
        .await;
        assert_eq!(simple_rows(&empty_left), vec![(1, "rhs".to_string(), 1.0)]);

        let empty_right = execute_batches(
            &plan,
            left_schema.clone(),
            vec![simple_batch(left_schema.clone(), &[(1, "lhs", 1.0)])],
            right_schema.clone(),
            vec![],
        )
        .await;
        assert_eq!(simple_rows(&empty_right), vec![(1, "lhs".to_string(), 1.0)]);

        let both_empty = execute_batches(&plan, left_schema, vec![], right_schema, vec![]).await;
        assert!(both_empty.is_empty());
    }

    #[tokio::test]
    async fn zero_row_batches_on_either_side_are_handled() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());

        let zero_row_left = execute_batches(
            &plan,
            left_schema.clone(),
            vec![simple_batch(left_schema.clone(), &[])],
            right_schema.clone(),
            vec![simple_batch(right_schema.clone(), &[(1, "rhs", 1.0)])],
        )
        .await;
        assert_eq!(
            simple_rows(&zero_row_left),
            vec![(1, "rhs".to_string(), 1.0)]
        );

        let zero_row_right = execute_batches(
            &plan,
            left_schema.clone(),
            vec![simple_batch(left_schema, &[(1, "lhs", 1.0)])],
            right_schema.clone(),
            vec![simple_batch(right_schema, &[])],
        )
        .await;
        assert_eq!(
            simple_rows(&zero_row_right),
            vec![(1, "lhs".to_string(), 1.0)]
        );
    }

    #[tokio::test]
    async fn metrics_record_output_rows_and_completion() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());
        let exec = plan.to_execution_plan(
            source_exec(simple_batch(left_schema, &[(1, "lhs", 1.0)])),
            source_exec(simple_batch(right_schema, &[(2, "rhs", 2.0)])),
        );

        let output =
            datafusion::physical_plan::collect(exec.clone(), SessionContext::default().task_ctx())
                .await
                .unwrap();
        assert_eq!(simple_rows(&output).len(), 2);

        let metrics = exec.metrics().unwrap();
        assert_eq!(metrics.output_rows(), Some(2));
        assert!(metrics.iter().any(|metric| {
            metric.value().name() == "end_timestamp" && metric.value().as_usize() > 0
        }));
    }

    #[tokio::test]
    async fn output_keeps_lhs_before_rhs_and_input_order() {
        let left_schema = simple_schema("left", false);
        let right_schema = simple_schema("right", false);
        let plan = simple_plan(left_schema.clone(), right_schema.clone());
        let result = execute_batches(
            &plan,
            left_schema.clone(),
            vec![
                simple_batch(left_schema.clone(), &[(1, "lhs-a", 1.0)]),
                simple_batch(left_schema, &[(2, "lhs-b", 2.0)]),
            ],
            right_schema.clone(),
            vec![
                simple_batch(right_schema.clone(), &[(3, "rhs-a", 3.0)]),
                simple_batch(right_schema, &[(4, "rhs-b", 4.0)]),
            ],
        )
        .await;
        assert_eq!(
            simple_rows(&result),
            vec![
                (1, "lhs-a".to_string(), 1.0),
                (2, "lhs-b".to_string(), 2.0),
                (3, "rhs-a".to_string(), 3.0),
                (4, "rhs-b".to_string(), 4.0),
            ]
        );
    }

    #[test]
    fn fully_filtered_rhs_batch_wakes_before_later_unmatched_batch() {
        let schema = simple_schema("stream", false);
        let left_polls = Arc::new(AtomicUsize::new(0));
        let right_polls = Arc::new(AtomicUsize::new(0));
        let right_executions = Arc::new(AtomicUsize::new(0));
        let left = test_stream(
            schema.clone(),
            vec![TestEvent::Batch(simple_batch(
                schema.clone(),
                &[(1, "match", 1.0)],
            ))],
            left_polls,
        );
        let right = Arc::new(TestExec::new(
            schema.clone(),
            vec![
                TestEvent::Batch(simple_batch(schema.clone(), &[(1, "match", 2.0)])),
                TestEvent::Batch(simple_batch(schema.clone(), &[(2, "keep", 3.0)])),
            ],
            right_polls.clone(),
            right_executions.clone(),
        ));
        let mut stream = Box::pin(test_union_stream(left, right, schema));
        let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            stream.as_mut().poll_next(&mut context),
            Poll::Ready(Some(Ok(_)))
        ));
        assert!(matches!(
            stream.as_mut().poll_next(&mut context),
            Poll::Pending
        ));
        assert_eq!(right_executions.load(Ordering::SeqCst), 1);
        assert_eq!(right_polls.load(Ordering::SeqCst), 1);
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            stream.as_mut().poll_next(&mut context),
            Poll::Ready(Some(Ok(batch)))
                if simple_rows(std::slice::from_ref(&batch))
                    == vec![(2, "keep".to_string(), 3.0)]
        ));
    }

    #[tokio::test]
    async fn rhs_is_not_polled_before_lhs_eof_and_drop_preserves_backpressure() {
        let schema = simple_schema("stream", false);
        let left_polls = Arc::new(AtomicUsize::new(0));
        let right_polls = Arc::new(AtomicUsize::new(0));
        let left = test_stream(
            schema.clone(),
            vec![TestEvent::Batch(simple_batch(
                schema.clone(),
                &[(1, "lhs", 1.0)],
            ))],
            left_polls.clone(),
        );
        let right_executions = Arc::new(AtomicUsize::new(0));
        let right = Arc::new(TestExec::new(
            schema.clone(),
            vec![TestEvent::Batch(simple_batch(
                schema.clone(),
                &[(2, "rhs", 2.0)],
            ))],
            right_polls.clone(),
            right_executions.clone(),
        ));
        let mut stream = Box::pin(test_union_stream(left, right, schema));

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(simple_rows(&[first]), vec![(1, "lhs".to_string(), 1.0)]);
        assert_eq!(left_polls.load(Ordering::SeqCst), 1);
        assert_eq!(right_polls.load(Ordering::SeqCst), 0);
        assert_eq!(right_executions.load(Ordering::SeqCst), 0);
        drop(stream);
        assert_eq!(right_polls.load(Ordering::SeqCst), 0);
        assert_eq!(right_executions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn lhs_and_delayed_rhs_errors_propagate() {
        let schema = simple_schema("stream", false);
        let polls = Arc::new(AtomicUsize::new(0));
        let mut left_error = Box::pin(test_union_stream(
            test_stream(
                schema.clone(),
                vec![TestEvent::Error("left failed")],
                polls.clone(),
            ),
            Arc::new(TestExec::new(
                schema.clone(),
                vec![],
                polls.clone(),
                Arc::new(AtomicUsize::new(0)),
            )),
            schema.clone(),
        ));
        assert!(left_error.next().await.unwrap().is_err());
        assert!(left_error.next().await.is_none());

        let mut right_error = Box::pin(test_union_stream(
            test_stream(
                schema.clone(),
                vec![TestEvent::Batch(simple_batch(
                    schema.clone(),
                    &[(1, "lhs", 1.0)],
                ))],
                polls.clone(),
            ),
            Arc::new(TestExec::new(
                schema.clone(),
                vec![TestEvent::Error("right failed")],
                polls,
                Arc::new(AtomicUsize::new(0)),
            )),
            schema.clone(),
        ));
        assert!(right_error.next().await.unwrap().is_ok());
        assert!(right_error.next().await.unwrap().is_err());
        assert!(right_error.next().await.is_none());

        let right_execute_error = Arc::new(
            TestExec::new(
                schema.clone(),
                vec![],
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            )
            .with_execute_error("right execute failed"),
        );
        let mut right_execute_error = Box::pin(test_union_stream(
            test_stream(schema.clone(), vec![], Arc::new(AtomicUsize::new(0))),
            right_execute_error,
            schema,
        ));
        assert!(right_execute_error.next().await.unwrap().is_err());
        assert!(right_execute_error.next().await.is_none());
    }

    #[tokio::test]
    async fn rhs_partial_and_full_batches_rebind_to_declared_left_schema() {
        let left_schema = schema("left", "label", false);
        let right_schema = schema("right", "label", true);
        let (left, right) = schemas(left_schema.clone(), right_schema.clone());
        let plan = UnionDistinctOn::try_new(left, right, vec![1], 0).unwrap();
        let declared_schema = plan.output_schema.inner().clone();
        let partial_rhs = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("match"), None])),
                Arc::new(Float64Array::from(vec![Some(10.0), None])),
            ],
        )
        .unwrap();
        let full_rhs = batch(right_schema.clone(), 3, Some("full"), Some(30.0));
        let result = execute_batches(
            &plan,
            left_schema.clone(),
            vec![batch(left_schema, 1, Some("match"), Some(1.0))],
            right_schema,
            vec![partial_rhs, full_rhs],
        )
        .await;
        assert!(result.iter().all(|batch| batch.schema() == declared_schema));
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(result[1].column(1).is_null(0));
    }

    #[test]
    fn malformed_indices_and_incompatible_inputs_fail_before_execution() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("job", DataType::Utf8, false),
        ]));
        let invalid = |compare_key_indices, ts_col_idx| {
            let decoded = UnionDistinctOn::deserialize(
                &pb::UnionDistinctOn {
                    compare_key_indices,
                    ts_col_idx,
                }
                .encode_to_vec(),
            )
            .unwrap();
            let (left, right) = schemas(schema.clone(), schema.clone());
            decoded
                .with_exprs_and_inputs(vec![], vec![left, right])
                .is_err()
        };
        assert!(invalid(vec![2], 0));
        assert!(invalid(vec![1], 2));
        let incompatible_schema = Arc::new(Schema::new(vec![
            Field::new("other_ts", DataType::Int64, false),
            Field::new("other_job", DataType::Int64, false),
        ]));
        let (left, right) = schemas(schema, incompatible_schema);
        assert!(UnionDistinctOn::try_new(left, right, vec![1], 0).is_err());
    }
}
