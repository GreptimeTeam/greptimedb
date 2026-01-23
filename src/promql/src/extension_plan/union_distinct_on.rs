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

use crate::error::{DeserializeSnafu, Result};
use crate::extension_plan::{resolve_column_name, serialize_column_index};

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
    unfix: Option<UnfixIndices>,
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
struct UnfixIndices {
    pub compare_key_indices: Vec<u64>,
    pub ts_col_idx: u64,
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
            unfix: None,
        }
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
            compare_keys: self.compare_keys.clone(),
            ts_col: self.ts_col.clone(),
            output_schema,
            metric: ExecutionPlanMetricsSet::new(),
            properties,
            random_state: RandomState::new(),
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let compare_key_indices = self
            .compare_keys
            .iter()
            .map(|name| serialize_column_index(&self.output_schema, name))
            .collect::<Vec<u64>>();

        let ts_col_idx = serialize_column_index(&self.output_schema, &self.ts_col);

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

        let unfix = UnfixIndices {
            compare_key_indices: pb_union.compare_key_indices.clone(),
            ts_col_idx: pb_union.ts_col_idx,
        };

        Ok(Self {
            left: placeholder_plan.clone(),
            right: placeholder_plan,
            compare_keys: Vec::new(),
            ts_col: String::new(),
            output_schema: Arc::new(DFSchema::empty()),
            unfix: Some(unfix),
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
        match self.compare_keys.partial_cmp(&other.compare_keys) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.ts_col.partial_cmp(&other.ts_col)
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
        if self.unfix.is_some() {
            return vec![];
        }

        let mut exprs: Vec<Expr> = self.compare_keys.iter().map(col).collect();
        if !self.compare_keys.iter().any(|key| key == &self.ts_col) {
            exprs.push(col(&self.ts_col));
        }
        exprs
    }

    fn necessary_children_exprs(&self, _output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        if self.unfix.is_some() {
            return None;
        }

        let left_len = self.left.schema().fields().len();
        let right_len = self.right.schema().fields().len();
        Some(vec![
            (0..left_len).collect::<Vec<_>>(),
            (0..right_len).collect::<Vec<_>>(),
        ])
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UnionDistinctOn: on col=[{:?}], ts_col=[{}]",
            self.compare_keys, self.ts_col
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

        if let Some(unfix) = &self.unfix {
            let output_schema = left.schema().clone();

            let compare_keys = unfix
                .compare_key_indices
                .iter()
                .map(|idx| {
                    resolve_column_name(*idx, &output_schema, "UnionDistinctOn", "compare key")
                })
                .collect::<DataFusionResult<Vec<String>>>()?;

            let ts_col =
                resolve_column_name(unfix.ts_col_idx, &output_schema, "UnionDistinctOn", "ts")?;

            Ok(Self {
                left,
                right,
                compare_keys,
                ts_col,
                output_schema,
                unfix: None,
            })
        } else {
            Ok(Self {
                left,
                right,
                compare_keys: self.compare_keys.clone(),
                ts_col: self.ts_col.clone(),
                output_schema: self.output_schema.clone(),
                unfix: None,
            })
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

    fn properties(&self) -> &PlanProperties {
        self.properties.as_ref()
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
            compare_keys: self.compare_keys.clone(),
            ts_col: self.ts_col.clone(),
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
        let schema = input.schema();
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
        let batch = interleave_batches(schema, batches, interleave_indices)?;

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

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ToDFSchema;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};

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
        let plan = UnionDistinctOn::new(
            left,
            right,
            vec!["k".to_string()],
            "ts".to_string(),
            df_schema,
        );

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

    #[tokio::test]
    async fn encode_decode_union_distinct_on() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("job", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let df_schema = schema.clone().to_dfschema_ref().unwrap();
        let left_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema.clone(),
        });
        let right_plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: df_schema.clone(),
        });
        let plan_node = UnionDistinctOn::new(
            left_plan.clone(),
            right_plan.clone(),
            vec!["job".to_string()],
            "ts".to_string(),
            df_schema.clone(),
        );

        let bytes = plan_node.serialize();

        let union_distinct_on = UnionDistinctOn::deserialize(&bytes).unwrap();
        let union_distinct_on = union_distinct_on
            .with_exprs_and_inputs(vec![], vec![left_plan, right_plan])
            .unwrap();

        assert_eq!(union_distinct_on.compare_keys, vec!["job".to_string()]);
        assert_eq!(union_distinct_on.ts_col, "ts");
        assert_eq!(union_distinct_on.output_schema, df_schema);
    }
}
