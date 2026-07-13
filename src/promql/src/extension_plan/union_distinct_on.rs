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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use ahash::{HashMap, RandomState};
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
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
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, TryStreamExt, ready};
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
/// - When there is a collision:
///   - If the collision is from right child itself, only the first observed row will be
///     preserved. All others are discarded.
///   - If the collision is from left child, the row in right child will be discarded.
/// - The output order is not maintained. This plan will output left child first, then right child.
/// - The output schema is based on the left child schema, with nullability widened from both inputs.
///
/// From the implementation perspective, this operator is similar to `HashJoin`, but the
/// probe side is the right child, and the build side is the left child. Another difference
/// is that the probe is opting-out.
///
/// This plan will exhaust the right child first to build probe hash table, then streaming
/// on left side, and use the left side to "mask" the hash table.
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
            random_state: RandomState::new(),
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

    /// Shared the `RandomState` for the hashing algorithm
    random_state: RandomState,
}

impl ExecutionPlan for UnionDistinctOnExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        let right_stream = self.right.execute(partition, context.clone())?;

        let mut key_indices = self.compare_key_indices.clone();
        key_indices.push(self.ts_col_idx);

        // Build right hash table future.
        let hashed_data_future = HashedDataFut::Pending(Box::pin(HashedData::new(
            right_stream,
            self.random_state.clone(),
            key_indices.clone(),
            self.output_schema.clone(),
        )));

        let baseline_metric = BaselineMetrics::new(&self.metric, partition);
        Ok(Box::pin(UnionDistinctOnStream {
            left: left_stream,
            right: hashed_data_future,
            compare_keys: key_indices,
            output_schema: self.output_schema.clone(),
            metric: baseline_metric,
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

// TODO(ruihang): some unused fields are for metrics, which will be implemented later.
#[allow(dead_code)]
pub struct UnionDistinctOnStream {
    left: SendableRecordBatchStream,
    right: HashedDataFut,
    /// Include time index
    compare_keys: Vec<usize>,
    output_schema: SchemaRef,
    metric: BaselineMetrics,
}

impl UnionDistinctOnStream {
    fn poll_impl(&mut self, cx: &mut Context<'_>) -> Poll<Option<<Self as Stream>::Item>> {
        // resolve the right stream
        let right = match self.right {
            HashedDataFut::Pending(ref mut fut) => {
                let right = ready!(fut.as_mut().poll(cx))?;
                self.right = HashedDataFut::Ready(right);
                let HashedDataFut::Ready(right_ref) = &mut self.right else {
                    unreachable!()
                };
                right_ref
            }
            HashedDataFut::Ready(ref mut right) => right,
            HashedDataFut::Empty => return Poll::Ready(None),
        };

        // poll left and probe with right
        let next_left = ready!(self.left.poll_next_unpin(cx));
        match next_left {
            Some(Ok(left)) => {
                // observe left batch and return it
                right.update_map(&left)?;
                Poll::Ready(Some(Ok(with_schema(left, self.output_schema.clone())?)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => {
                // left stream is exhausted, so we can send the right part
                let right = std::mem::replace(&mut self.right, HashedDataFut::Empty);
                let HashedDataFut::Ready(data) = right else {
                    unreachable!()
                };
                Poll::Ready(Some(data.finish()))
            }
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
        self.poll_impl(cx)
    }
}

/// Simple future state for [HashedData]
enum HashedDataFut {
    /// The result is not ready
    Pending(BoxFuture<'static, DataFusionResult<HashedData>>),
    /// The result is ready
    Ready(HashedData),
    /// The result is taken
    Empty,
}

/// ALL input batches and its hash table
struct HashedData {
    // TODO(ruihang): use `JoinHashMap` instead after upgrading to DF 34.0
    /// Hash table for all input batches. The key is hash value, and the value
    /// is the index of `bathc`.
    hash_map: HashMap<u64, usize>,
    /// Output batch.
    batch: RecordBatch,
    /// The indices of the columns to be hashed.
    hash_key_indices: Vec<usize>,
    random_state: RandomState,
}

impl HashedData {
    pub async fn new(
        input: SendableRecordBatchStream,
        random_state: RandomState,
        hash_key_indices: Vec<usize>,
        output_schema: SchemaRef,
    ) -> DataFusionResult<Self> {
        // Collect all batches from the input stream
        let initial = (Vec::new(), 0);
        let (batches, _num_rows) = input
            .try_fold(initial, |mut acc, batch| async {
                // Update rowcount
                acc.1 += batch.num_rows();
                // Push batch to output
                acc.0.push(batch);
                Ok(acc)
            })
            .await?;

        // Create hash for each batch
        let mut hash_map = HashMap::default();
        let mut hashes_buffer = Vec::new();
        let mut interleave_indices = Vec::new();
        for (batch_number, batch) in batches.iter().enumerate() {
            hashes_buffer.resize(batch.num_rows(), 0);
            // get columns for hashing
            let arrays = hash_key_indices
                .iter()
                .map(|i| batch.column(*i).clone())
                .collect::<Vec<_>>();

            // compute hash
            let hash_values =
                hash_utils::create_hashes(&arrays, &random_state, &mut hashes_buffer)?;
            for (row_number, hash_value) in hash_values.iter().enumerate() {
                // Only keeps the first observed row for each hash value
                if hash_map
                    .try_insert(*hash_value, interleave_indices.len())
                    .is_ok()
                {
                    interleave_indices.push((batch_number, row_number));
                }
            }
        }

        // Finalize the hash map
        let batch = interleave_batches(output_schema, batches, interleave_indices)?;

        Ok(Self {
            hash_map,
            batch,
            hash_key_indices,
            random_state,
        })
    }

    /// Remove rows that hash value present in the input
    /// record batch from the hash map.
    pub fn update_map(&mut self, input: &RecordBatch) -> DataFusionResult<()> {
        // get columns for hashing
        let mut hashes_buffer = Vec::new();
        let arrays = self
            .hash_key_indices
            .iter()
            .map(|i| input.column(*i).clone())
            .collect::<Vec<_>>();

        // compute hash
        hashes_buffer.resize(input.num_rows(), 0);
        let hash_values =
            hash_utils::create_hashes(&arrays, &self.random_state, &mut hashes_buffer)?;

        // remove those hashes
        for hash in hash_values {
            self.hash_map.remove(hash);
        }

        Ok(())
    }

    pub fn finish(self) -> DataFusionResult<RecordBatch> {
        let valid_indices = self.hash_map.values().copied().collect::<Vec<_>>();
        let result = take_batch(&self.batch, &valid_indices)?;
        Ok(result)
    }
}

fn with_schema(batch: RecordBatch, schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Utility function to interleave batches. Based on [interleave](datafusion::arrow::compute::interleave)
fn interleave_batches(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    indices: Vec<(usize, usize)>,
) -> DataFusionResult<RecordBatch> {
    if batches.is_empty() {
        if indices.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        } else {
            return Err(DataFusionError::Internal(
                "Cannot interleave empty batches with non-empty indices".to_string(),
            ));
        }
    }

    // transform batches into arrays
    let mut arrays = vec![vec![]; schema.fields().len()];
    for batch in &batches {
        for (i, array) in batch.columns().iter().enumerate() {
            arrays[i].push(array.as_ref());
        }
    }

    // interleave arrays
    let interleaved_arrays: Vec<_> = arrays
        .into_iter()
        .map(|array| compute::interleave(&array, &indices))
        .collect::<std::result::Result<_, _>>()?;

    // assemble new record batch
    RecordBatch::try_new(schema, interleaved_arrays)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Utility function to take rows from a record batch. Based on [take](datafusion::arrow::compute::take)
fn take_batch(batch: &RecordBatch, indices: &[usize]) -> DataFusionResult<RecordBatch> {
    // fast path
    if batch.num_rows() == indices.len() {
        return Ok(batch.clone());
    }

    let schema = batch.schema();

    let indices_array = UInt64Array::from_iter(indices.iter().map(|i| *i as u64));
    let arrays = batch
        .columns()
        .iter()
        .map(|array| compute::take(array, &indices_array, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    let result = RecordBatch::try_new(schema, arrays)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok(result)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::ToDFSchema;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::prelude::SessionContext;

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
    fn test_interleave_batches() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![10, 11, 12])),
            ],
        )
        .unwrap();

        let batch3 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![13, 14, 15])),
                Arc::new(Int32Array::from(vec![16, 17, 18])),
            ],
        )
        .unwrap();

        let batches = vec![batch1, batch2, batch3];
        let indices = vec![(0, 0), (1, 0), (2, 0), (0, 1), (1, 1), (2, 1)];
        let result = interleave_batches(Arc::new(schema.clone()), batches, indices).unwrap();

        let expected = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 7, 13, 2, 8, 14])),
                Arc::new(Int32Array::from(vec![4, 10, 16, 5, 11, 17])),
            ],
        )
        .unwrap();

        assert_eq!(result, expected);
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

    fn source_exec(schema: SchemaRef, batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap(),
        )))
    }

    #[tokio::test]
    async fn serialize_deserialize_and_execute_with_different_input_names() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("left_ts", DataType::Int64, false),
            Field::new("left_job", DataType::Utf8, false),
            Field::new("left_value", DataType::Float64, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("right_ts", DataType::Int64, false),
            Field::new("right_job", DataType::Utf8, false),
            Field::new("right_value", DataType::Float64, false),
        ]));
        let left_plan = empty_plan(left_schema.clone().to_dfschema_ref().unwrap());
        let right_plan = empty_plan(right_schema.clone().to_dfschema_ref().unwrap());
        let plan =
            UnionDistinctOn::try_new(left_plan.clone(), right_plan.clone(), vec![1], 0).unwrap();

        let decoded = UnionDistinctOn::deserialize(&plan.serialize()).unwrap();
        let decoded = decoded
            .with_exprs_and_inputs(vec![], vec![left_plan, right_plan])
            .unwrap();
        assert_eq!(decoded.compare_key_indices, vec![1]);
        assert_eq!(decoded.ts_col_idx, 0);
        assert_eq!(
            decoded.output_schema,
            left_schema.clone().to_dfschema_ref().unwrap()
        );

        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["left"])),
                Arc::new(Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["right"])),
                Arc::new(Float64Array::from(vec![20.0])),
            ],
        )
        .unwrap();
        let exec = decoded.to_execution_plan(
            source_exec(left_schema.clone(), left_batch),
            source_exec(right_schema, right_batch),
        );
        let result = datafusion::physical_plan::collect(exec, SessionContext::default().task_ctx())
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|batch| batch.schema() == left_schema));
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[1].num_rows(), 1);
    }

    #[tokio::test]
    async fn execute_widens_nullable_label_and_emits_rhs_null() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("left_ts", DataType::Int64, false),
            Field::new("left_label", DataType::Utf8, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("right_ts", DataType::Int64, false),
            Field::new("right_label", DataType::Utf8, true),
        ]));
        let plan = UnionDistinctOn::try_new(
            empty_plan(left_schema.clone().to_dfschema_ref().unwrap()),
            empty_plan(right_schema.clone().to_dfschema_ref().unwrap()),
            vec![1],
            0,
        )
        .unwrap();
        let declared_schema = plan.output_schema.inner().clone();
        assert!(declared_schema.field(1).is_nullable());

        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["present"])),
            ],
        )
        .unwrap();
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();
        let result = datafusion::physical_plan::collect(
            plan.to_execution_plan(
                source_exec(left_schema, left_batch),
                source_exec(right_schema, right_batch),
            ),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|batch| batch.schema() == declared_schema));
        assert!(result[1].column(1).is_null(0));
    }

    #[tokio::test]
    async fn execute_widens_nullable_value_for_both_inputs() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("left_ts", DataType::Int64, false),
            Field::new("left_value", DataType::Float64, false),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("right_ts", DataType::Int64, false),
            Field::new("right_value", DataType::Float64, true),
        ]));
        let plan = UnionDistinctOn::try_new(
            empty_plan(left_schema.clone().to_dfschema_ref().unwrap()),
            empty_plan(right_schema.clone().to_dfschema_ref().unwrap()),
            vec![1],
            0,
        )
        .unwrap();
        let declared_schema = plan.output_schema.inner().clone();
        assert!(declared_schema.field(1).is_nullable());

        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Float64Array::from(vec![20.0])),
            ],
        )
        .unwrap();
        let result = datafusion::physical_plan::collect(
            plan.to_execution_plan(
                source_exec(left_schema, left_batch),
                source_exec(right_schema, right_batch),
            ),
            SessionContext::default().task_ctx(),
        )
        .await
        .unwrap();

        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|batch| batch.schema() == declared_schema));
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[1].num_rows(), 1);
    }

    #[test]
    fn malformed_indices_and_incompatible_inputs_fail_before_execution() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("job", DataType::Utf8, false),
        ]));
        let df_schema = schema.to_dfschema_ref().unwrap();

        for protobuf in [
            pb::UnionDistinctOn {
                compare_key_indices: vec![2],
                ts_col_idx: 0,
            },
            pb::UnionDistinctOn {
                compare_key_indices: vec![1],
                ts_col_idx: 2,
            },
        ] {
            let decoded = UnionDistinctOn::deserialize(&protobuf.encode_to_vec()).unwrap();
            assert!(
                decoded
                    .with_exprs_and_inputs(
                        vec![],
                        vec![empty_plan(df_schema.clone()), empty_plan(df_schema.clone())],
                    )
                    .is_err()
            );
        }

        let incompatible_schema = Arc::new(Schema::new(vec![
            Field::new("other_ts", DataType::Int64, false),
            Field::new("other_job", DataType::Int64, false),
        ]));
        assert!(
            UnionDistinctOn::try_new(
                empty_plan(df_schema),
                empty_plan(incompatible_schema.to_dfschema_ref().unwrap()),
                vec![1],
                0,
            )
            .is_err()
        );
    }
}
