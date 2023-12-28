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
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    hash_utils, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datatypes::arrow::compute;
use futures::future::BoxFuture;
use futures::{ready, Stream, StreamExt, TryStreamExt};

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
/// - The output schema contains all columns from left or right child plans.
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
    compare_keys: Vec<String>,
    ts_col: String,
    output_schema: DFSchemaRef,
}

impl UnionDistinctOn {
    pub fn name() -> &'static str {
        "UnionDistinctOn"
    }

    pub fn new(
        left: LogicalPlan,
        right: LogicalPlan,
        compare_keys: Vec<String>,
        ts_col: String,
        output_schema: DFSchemaRef,
    ) -> Self {
        Self {
            left,
            right,
            compare_keys,
            ts_col,
            output_schema,
        }
    }

    pub fn to_execution_plan(
        &self,
        left_exec: Arc<dyn ExecutionPlan>,
        right_exec: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(UnionDistinctOnExec {
            left: left_exec,
            right: right_exec,
            compare_keys: self.compare_keys.clone(),
            ts_col: self.ts_col.clone(),
            output_schema: Arc::new(self.output_schema.as_ref().into()),
            metric: ExecutionPlanMetricsSet::new(),
            random_state: RandomState::new(),
        })
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
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnionDistinctOn: on col=[{:?}], ts_col=[{}]",
            self.compare_keys, self.ts_col
        )
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        assert_eq!(inputs.len(), 2);

        let left = inputs[0].clone();
        let right = inputs[1].clone();
        Self {
            left,
            right,
            compare_keys: self.compare_keys.clone(),
            ts_col: self.ts_col.clone(),
            output_schema: self.output_schema.clone(),
        }
    }
}

#[derive(Debug)]
pub struct UnionDistinctOnExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    compare_keys: Vec<String>,
    ts_col: String,
    output_schema: SchemaRef,
    metric: ExecutionPlanMetricsSet,

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

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    /// [UnionDistinctOnExec] will output left first, then right.
    /// So the order of the output is not maintained.
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
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
            compare_keys: self.compare_keys.clone(),
            ts_col: self.ts_col.clone(),
            output_schema: self.output_schema.clone(),
            metric: self.metric.clone(),
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

        // Convert column name to column index. Add one for the time column.
        let mut key_indices = Vec::with_capacity(self.compare_keys.len() + 1);
        for key in &self.compare_keys {
            let index = self
                .output_schema
                .column_with_name(key)
                .map(|(i, _)| i)
                .ok_or_else(|| DataFusionError::Internal(format!("Column {} not found", key)))?;
            key_indices.push(index);
        }
        let ts_index = self
            .output_schema
            .column_with_name(&self.ts_col)
            .map(|(i, _)| i)
            .ok_or_else(|| {
                DataFusionError::Internal(format!("Column {} not found", self.ts_col))
            })?;
        key_indices.push(ts_index);

        // Build right hash table future.
        let hashed_data_future = HashedDataFut::Pending(Box::pin(HashedData::new(
            right_stream,
            self.random_state.clone(),
            key_indices.clone(),
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

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl DisplayAs for UnionDistinctOnExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "UnionDistinctOnExec: on col=[{:?}], ts_col=[{}]",
                    self.compare_keys, self.ts_col
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
                Poll::Ready(Some(Ok(left)))
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

        // Finilize the hash map
        let batch = interleave_batches(batches, interleave_indices)?;

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

/// Utility function to interleave batches. Based on [interleave](datafusion::arrow::compute::interleave)
fn interleave_batches(
    batches: Vec<RecordBatch>,
    indices: Vec<(usize, usize)>,
) -> DataFusionResult<RecordBatch> {
    let schema = batches[0].schema();

    // transform batches into arrays
    let mut arrays = vec![vec![]; schema.fields().len()];
    for batch in &batches {
        for (i, array) in batch.columns().iter().enumerate() {
            arrays[i].push(array.as_ref());
        }
    }

    // interleave arrays
    let mut interleaved_arrays = Vec::with_capacity(arrays.len());
    for array in arrays {
        interleaved_arrays.push(compute::interleave(&array, &indices)?);
    }

    // assemble new record batch
    RecordBatch::try_new(schema.clone(), interleaved_arrays).map_err(DataFusionError::ArrowError)
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
        .map_err(DataFusionError::ArrowError)?;

    let result = RecordBatch::try_new(schema, arrays).map_err(DataFusionError::ArrowError)?;
    Ok(result)
}

#[cfg(test)]
mod test {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

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
        let result = interleave_batches(batches, indices).unwrap();

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
}
