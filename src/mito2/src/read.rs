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

//! Common structs and utilities for reading data.

pub mod compat;
pub mod dedup;
pub mod last_row;
pub mod merge;
pub mod projection;
pub(crate) mod prune;
pub(crate) mod scan_region;
pub(crate) mod seq_scan;
pub(crate) mod unordered_scan;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use api::v1::OpType;
use async_trait::async_trait;
use common_time::Timestamp;
use datafusion_common::arrow::array::UInt8Array;
use datatypes::arrow;
use datatypes::arrow::array::{Array, ArrayRef};
use datatypes::arrow::compute::SortOptions;
use datatypes::arrow::row::{RowConverter, SortField};
use datatypes::prelude::{ConcreteDataType, DataType, ScalarVector};
use datatypes::types::TimestampType;
use datatypes::value::{Value, ValueRef};
use datatypes::vectors::{
    BooleanVector, Helper, TimestampMicrosecondVector, TimestampMillisecondVector,
    TimestampNanosecondVector, TimestampSecondVector, UInt32Vector, UInt64Vector, UInt8Vector,
    Vector, VectorRef,
};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{
    ComputeArrowSnafu, ComputeVectorSnafu, ConvertVectorSnafu, InvalidBatchSnafu, Result,
};
use crate::memtable::BoxedBatchIterator;
use crate::metrics::{READ_BATCHES_RETURN, READ_ROWS_RETURN, READ_STAGE_ELAPSED};
use crate::read::prune::PruneReader;

/// Storage internal representation of a batch of rows for a primary key (time series).
///
/// Rows are sorted by primary key, timestamp, sequence desc, op_type desc. Fields
/// always keep the same relative order as fields in [RegionMetadata](store_api::metadata::RegionMetadata).
#[derive(Debug, PartialEq, Clone)]
pub struct Batch {
    /// Primary key encoded in a comparable form.
    primary_key: Vec<u8>,
    /// Possibly decoded `primary_key` values. Some places would decode it in advance.
    pk_values: Option<Vec<Value>>,
    /// Timestamps of rows, should be sorted and not null.
    timestamps: VectorRef,
    /// Sequences of rows
    ///
    /// UInt64 type, not null.
    sequences: Arc<UInt64Vector>,
    /// Op types of rows
    ///
    /// UInt8 type, not null.
    op_types: Arc<UInt8Vector>,
    /// Fields organized in columnar format.
    fields: Vec<BatchColumn>,
}

impl Batch {
    /// Creates a new batch.
    pub fn new(
        primary_key: Vec<u8>,
        timestamps: VectorRef,
        sequences: Arc<UInt64Vector>,
        op_types: Arc<UInt8Vector>,
        fields: Vec<BatchColumn>,
    ) -> Result<Batch> {
        BatchBuilder::with_required_columns(primary_key, timestamps, sequences, op_types)
            .with_fields(fields)
            .build()
    }

    /// Tries to set fields for the batch.
    pub fn with_fields(self, fields: Vec<BatchColumn>) -> Result<Batch> {
        Batch::new(
            self.primary_key,
            self.timestamps,
            self.sequences,
            self.op_types,
            fields,
        )
    }

    /// Returns primary key of the batch.
    pub fn primary_key(&self) -> &[u8] {
        &self.primary_key
    }

    /// Returns possibly decoded primary-key values.
    pub fn pk_values(&self) -> Option<&[Value]> {
        self.pk_values.as_deref()
    }

    /// Sets possibly decoded primary-key values.
    pub fn set_pk_values(&mut self, pk_values: Vec<Value>) {
        self.pk_values = Some(pk_values);
    }

    /// Removes possibly decoded primary-key values. For testing only.
    #[cfg(any(test, feature = "test"))]
    pub fn remove_pk_values(&mut self) {
        self.pk_values = None;
    }

    /// Returns fields in the batch.
    pub fn fields(&self) -> &[BatchColumn] {
        &self.fields
    }

    /// Returns timestamps of the batch.
    pub fn timestamps(&self) -> &VectorRef {
        &self.timestamps
    }

    /// Returns sequences of the batch.
    pub fn sequences(&self) -> &Arc<UInt64Vector> {
        &self.sequences
    }

    /// Returns op types of the batch.
    pub fn op_types(&self) -> &Arc<UInt8Vector> {
        &self.op_types
    }

    /// Returns the number of rows in the batch.
    pub fn num_rows(&self) -> usize {
        // All vectors have the same length. We use the length of sequences vector
        // since it has static type.
        self.sequences.len()
    }

    /// Returns true if the number of rows in the batch is 0.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Returns the first timestamp in the batch or `None` if the batch is empty.
    pub fn first_timestamp(&self) -> Option<Timestamp> {
        if self.timestamps.is_empty() {
            return None;
        }

        Some(self.get_timestamp(0))
    }

    /// Returns the last timestamp in the batch or `None` if the batch is empty.
    pub fn last_timestamp(&self) -> Option<Timestamp> {
        if self.timestamps.is_empty() {
            return None;
        }

        Some(self.get_timestamp(self.timestamps.len() - 1))
    }

    /// Returns the first sequence in the batch or `None` if the batch is empty.
    pub fn first_sequence(&self) -> Option<SequenceNumber> {
        if self.sequences.is_empty() {
            return None;
        }

        Some(self.get_sequence(0))
    }

    /// Returns the last sequence in the batch or `None` if the batch is empty.
    pub fn last_sequence(&self) -> Option<SequenceNumber> {
        if self.sequences.is_empty() {
            return None;
        }

        Some(self.get_sequence(self.sequences.len() - 1))
    }

    /// Replaces the primary key of the batch.
    ///
    /// Notice that this [Batch] also contains a maybe-exist `pk_values`.
    /// Be sure to update that field as well.
    pub fn set_primary_key(&mut self, primary_key: Vec<u8>) {
        self.primary_key = primary_key;
    }

    /// Slice the batch, returning a new batch.
    ///
    /// # Panics
    /// Panics if `offset + length > self.num_rows()`.
    pub fn slice(&self, offset: usize, length: usize) -> Batch {
        let fields = self
            .fields
            .iter()
            .map(|column| BatchColumn {
                column_id: column.column_id,
                data: column.data.slice(offset, length),
            })
            .collect();
        // We skip using the builder to avoid validating the batch again.
        Batch {
            // Now we need to clone the primary key. We could try `Bytes` if
            // this becomes a bottleneck.
            primary_key: self.primary_key.clone(),
            pk_values: self.pk_values.clone(),
            timestamps: self.timestamps.slice(offset, length),
            sequences: Arc::new(self.sequences.get_slice(offset, length)),
            op_types: Arc::new(self.op_types.get_slice(offset, length)),
            fields,
        }
    }

    /// Takes `batches` and concat them into one batch.
    ///
    /// All `batches` must have the same primary key.
    pub fn concat(mut batches: Vec<Batch>) -> Result<Batch> {
        ensure!(
            !batches.is_empty(),
            InvalidBatchSnafu {
                reason: "empty batches",
            }
        );
        if batches.len() == 1 {
            // Now we own the `batches` so we could pop it directly.
            return Ok(batches.pop().unwrap());
        }

        let primary_key = std::mem::take(&mut batches[0].primary_key);
        let first = &batches[0];
        // We took the primary key from the first batch so we don't use `first.primary_key()`.
        ensure!(
            batches
                .iter()
                .skip(1)
                .all(|b| b.primary_key() == primary_key),
            InvalidBatchSnafu {
                reason: "batches have different primary key",
            }
        );
        for b in batches.iter().skip(1) {
            ensure!(
                b.fields.len() == first.fields.len(),
                InvalidBatchSnafu {
                    reason: "batches have different field num",
                }
            );
            for (l, r) in b.fields.iter().zip(&first.fields) {
                ensure!(
                    l.column_id == r.column_id,
                    InvalidBatchSnafu {
                        reason: "batches have different fields",
                    }
                );
            }
        }

        // We take the primary key from the first batch.
        let mut builder = BatchBuilder::new(primary_key);
        // Concat timestamps, sequences, op_types, fields.
        let array = concat_arrays(batches.iter().map(|b| b.timestamps().to_arrow_array()))?;
        builder.timestamps_array(array)?;
        let array = concat_arrays(batches.iter().map(|b| b.sequences().to_arrow_array()))?;
        builder.sequences_array(array)?;
        let array = concat_arrays(batches.iter().map(|b| b.op_types().to_arrow_array()))?;
        builder.op_types_array(array)?;
        for (i, batch_column) in first.fields.iter().enumerate() {
            let array = concat_arrays(batches.iter().map(|b| b.fields()[i].data.to_arrow_array()))?;
            builder.push_field_array(batch_column.column_id, array)?;
        }

        builder.build()
    }

    /// Removes rows whose op type is delete.
    pub fn filter_deleted(&mut self) -> Result<()> {
        // Safety: op type column is not null.
        let array = self.op_types.as_arrow();
        // Find rows with non-delete op type.
        let rhs = UInt8Array::new_scalar(OpType::Delete as u8);
        let predicate =
            arrow::compute::kernels::cmp::neq(array, &rhs).context(ComputeArrowSnafu)?;
        self.filter(&BooleanVector::from(predicate))
    }

    // Applies the `predicate` to the batch.
    // Safety: We know the array type so we unwrap on casting.
    pub fn filter(&mut self, predicate: &BooleanVector) -> Result<()> {
        self.timestamps = self
            .timestamps
            .filter(predicate)
            .context(ComputeVectorSnafu)?;
        self.sequences = Arc::new(
            UInt64Vector::try_from_arrow_array(
                arrow::compute::filter(self.sequences.as_arrow(), predicate.as_boolean_array())
                    .context(ComputeArrowSnafu)?,
            )
            .unwrap(),
        );
        self.op_types = Arc::new(
            UInt8Vector::try_from_arrow_array(
                arrow::compute::filter(self.op_types.as_arrow(), predicate.as_boolean_array())
                    .context(ComputeArrowSnafu)?,
            )
            .unwrap(),
        );
        for batch_column in &mut self.fields {
            batch_column.data = batch_column
                .data
                .filter(predicate)
                .context(ComputeVectorSnafu)?;
        }

        Ok(())
    }

    /// Sorts rows in the batch. If `dedup` is true, it also removes
    /// duplicated rows according to primary keys.
    ///
    /// It orders rows by timestamp, sequence desc and only keep the latest
    /// row for the same timestamp. It doesn't consider op type as sequence
    /// should already provide uniqueness for a row.
    pub fn sort(&mut self, dedup: bool) -> Result<()> {
        // If building a converter each time is costly, we may allow passing a
        // converter.
        let converter = RowConverter::new(vec![
            SortField::new(self.timestamps.data_type().as_arrow_type()),
            SortField::new_with_options(
                self.sequences.data_type().as_arrow_type(),
                SortOptions {
                    descending: true,
                    ..Default::default()
                },
            ),
        ])
        .context(ComputeArrowSnafu)?;
        // Columns to sort.
        let columns = [
            self.timestamps.to_arrow_array(),
            self.sequences.to_arrow_array(),
        ];
        let rows = converter.convert_columns(&columns).unwrap();
        let mut to_sort: Vec<_> = rows.iter().enumerate().collect();
        to_sort.sort_unstable_by(|left, right| left.1.cmp(&right.1));

        if dedup {
            // Dedup by timestamps.
            to_sort.dedup_by(|left, right| {
                debug_assert_eq!(18, left.1.as_ref().len());
                debug_assert_eq!(18, right.1.as_ref().len());
                let (left_key, right_key) = (left.1.as_ref(), right.1.as_ref());
                // We only compare the timestamp part and ignore sequence.
                left_key[..TIMESTAMP_KEY_LEN] == right_key[..TIMESTAMP_KEY_LEN]
            });
        }

        let indices = UInt32Vector::from_iter_values(to_sort.iter().map(|v| v.0 as u32));
        self.take_in_place(&indices)
    }

    /// Returns the estimated memory size of the batch.
    pub fn memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        size += self.primary_key.len();
        size += self.timestamps.memory_size();
        size += self.sequences.memory_size();
        size += self.op_types.memory_size();
        for batch_column in &self.fields {
            size += batch_column.data.memory_size();
        }
        size
    }

    /// Returns ids and datatypes of fields in the [Batch] after applying the `projection`.
    pub(crate) fn projected_fields(
        metadata: &RegionMetadata,
        projection: &[ColumnId],
    ) -> Vec<(ColumnId, ConcreteDataType)> {
        let projected_ids: HashSet<_> = projection.iter().copied().collect();
        metadata
            .field_columns()
            .filter_map(|column| {
                if projected_ids.contains(&column.column_id) {
                    Some((column.column_id, column.column_schema.data_type.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns timestamps in a native slice or `None` if the batch is empty.
    pub(crate) fn timestamps_native(&self) -> Option<&[i64]> {
        if self.timestamps.is_empty() {
            return None;
        }

        let values = match self.timestamps.data_type() {
            ConcreteDataType::Timestamp(TimestampType::Second(_)) => self
                .timestamps
                .as_any()
                .downcast_ref::<TimestampSecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
            ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => self
                .timestamps
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
            ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => self
                .timestamps
                .as_any()
                .downcast_ref::<TimestampMicrosecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
            ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => self
                .timestamps
                .as_any()
                .downcast_ref::<TimestampNanosecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
            other => panic!("timestamps in a Batch has other type {:?}", other),
        };

        Some(values)
    }

    /// Takes the batch in place.
    fn take_in_place(&mut self, indices: &UInt32Vector) -> Result<()> {
        self.timestamps = self.timestamps.take(indices).context(ComputeVectorSnafu)?;
        let array = arrow::compute::take(self.sequences.as_arrow(), indices.as_arrow(), None)
            .context(ComputeArrowSnafu)?;
        // Safety: we know the array and vector type.
        self.sequences = Arc::new(UInt64Vector::try_from_arrow_array(array).unwrap());
        let array = arrow::compute::take(self.op_types.as_arrow(), indices.as_arrow(), None)
            .context(ComputeArrowSnafu)?;
        self.op_types = Arc::new(UInt8Vector::try_from_arrow_array(array).unwrap());
        for batch_column in &mut self.fields {
            batch_column.data = batch_column
                .data
                .take(indices)
                .context(ComputeVectorSnafu)?;
        }

        Ok(())
    }

    /// Gets a timestamp at given `index`.
    ///
    /// # Panics
    /// Panics if `index` is out-of-bound or the timestamp vector returns null.
    fn get_timestamp(&self, index: usize) -> Timestamp {
        match self.timestamps.get_ref(index) {
            ValueRef::Timestamp(timestamp) => timestamp,

            // We have check the data type is timestamp compatible in the [BatchBuilder] so it's safe to panic.
            value => panic!("{:?} is not a timestamp", value),
        }
    }

    /// Gets a sequence at given `index`.
    ///
    /// # Panics
    /// Panics if `index` is out-of-bound or the sequence vector returns null.
    pub(crate) fn get_sequence(&self, index: usize) -> SequenceNumber {
        // Safety: sequences is not null so it actually returns Some.
        self.sequences.get_data(index).unwrap()
    }
}

/// Len of timestamp in arrow row format.
const TIMESTAMP_KEY_LEN: usize = 9;

/// Helper function to concat arrays from `iter`.
fn concat_arrays(iter: impl Iterator<Item = ArrayRef>) -> Result<ArrayRef> {
    let arrays: Vec<_> = iter.collect();
    let dyn_arrays: Vec<_> = arrays.iter().map(|array| array.as_ref()).collect();
    arrow::compute::concat(&dyn_arrays).context(ComputeArrowSnafu)
}

/// A column in a [Batch].
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BatchColumn {
    /// Id of the column.
    pub column_id: ColumnId,
    /// Data of the column.
    pub data: VectorRef,
}

/// Builder to build [Batch].
pub struct BatchBuilder {
    primary_key: Vec<u8>,
    timestamps: Option<VectorRef>,
    sequences: Option<Arc<UInt64Vector>>,
    op_types: Option<Arc<UInt8Vector>>,
    fields: Vec<BatchColumn>,
}

impl BatchBuilder {
    /// Creates a new [BatchBuilder] with primary key.
    pub fn new(primary_key: Vec<u8>) -> BatchBuilder {
        BatchBuilder {
            primary_key,
            timestamps: None,
            sequences: None,
            op_types: None,
            fields: Vec::new(),
        }
    }

    /// Creates a new [BatchBuilder] with all required columns.
    pub fn with_required_columns(
        primary_key: Vec<u8>,
        timestamps: VectorRef,
        sequences: Arc<UInt64Vector>,
        op_types: Arc<UInt8Vector>,
    ) -> BatchBuilder {
        BatchBuilder {
            primary_key,
            timestamps: Some(timestamps),
            sequences: Some(sequences),
            op_types: Some(op_types),
            fields: Vec::new(),
        }
    }

    /// Set all field columns.
    pub fn with_fields(mut self, fields: Vec<BatchColumn>) -> Self {
        self.fields = fields;
        self
    }

    /// Push a field column.
    pub fn push_field(&mut self, column: BatchColumn) -> &mut Self {
        self.fields.push(column);
        self
    }

    /// Push an array as a field.
    pub fn push_field_array(&mut self, column_id: ColumnId, array: ArrayRef) -> Result<&mut Self> {
        let vector = Helper::try_into_vector(array).context(ConvertVectorSnafu)?;
        self.fields.push(BatchColumn {
            column_id,
            data: vector,
        });

        Ok(self)
    }

    /// Try to set an array as timestamps.
    pub fn timestamps_array(&mut self, array: ArrayRef) -> Result<&mut Self> {
        let vector = Helper::try_into_vector(array).context(ConvertVectorSnafu)?;
        ensure!(
            vector.data_type().is_timestamp(),
            InvalidBatchSnafu {
                reason: format!("{:?} is not a timestamp type", vector.data_type()),
            }
        );

        self.timestamps = Some(vector);
        Ok(self)
    }

    /// Try to set an array as sequences.
    pub fn sequences_array(&mut self, array: ArrayRef) -> Result<&mut Self> {
        ensure!(
            *array.data_type() == arrow::datatypes::DataType::UInt64,
            InvalidBatchSnafu {
                reason: "sequence array is not UInt64 type",
            }
        );
        // Safety: The cast must success as we have ensured it is uint64 type.
        let vector = Arc::new(UInt64Vector::try_from_arrow_array(array).unwrap());
        self.sequences = Some(vector);

        Ok(self)
    }

    /// Try to set an array as op types.
    pub fn op_types_array(&mut self, array: ArrayRef) -> Result<&mut Self> {
        ensure!(
            *array.data_type() == arrow::datatypes::DataType::UInt8,
            InvalidBatchSnafu {
                reason: "sequence array is not UInt8 type",
            }
        );
        // Safety: The cast must success as we have ensured it is uint64 type.
        let vector = Arc::new(UInt8Vector::try_from_arrow_array(array).unwrap());
        self.op_types = Some(vector);

        Ok(self)
    }

    /// Builds the [Batch].
    pub fn build(self) -> Result<Batch> {
        let timestamps = self.timestamps.context(InvalidBatchSnafu {
            reason: "missing timestamps",
        })?;
        let sequences = self.sequences.context(InvalidBatchSnafu {
            reason: "missing sequences",
        })?;
        let op_types = self.op_types.context(InvalidBatchSnafu {
            reason: "missing op_types",
        })?;
        // Our storage format ensure these columns are not nullable so
        // we use assert here.
        assert_eq!(0, timestamps.null_count());
        assert_eq!(0, sequences.null_count());
        assert_eq!(0, op_types.null_count());

        let ts_len = timestamps.len();
        ensure!(
            sequences.len() == ts_len,
            InvalidBatchSnafu {
                reason: format!(
                    "sequence have different len {} != {}",
                    sequences.len(),
                    ts_len
                ),
            }
        );
        ensure!(
            op_types.len() == ts_len,
            InvalidBatchSnafu {
                reason: format!(
                    "op type have different len {} != {}",
                    op_types.len(),
                    ts_len
                ),
            }
        );
        for column in &self.fields {
            ensure!(
                column.data.len() == ts_len,
                InvalidBatchSnafu {
                    reason: format!(
                        "column {} has different len {} != {}",
                        column.column_id,
                        column.data.len(),
                        ts_len
                    ),
                }
            );
        }

        Ok(Batch {
            primary_key: self.primary_key,
            pk_values: None,
            timestamps,
            sequences,
            op_types,
            fields: self.fields,
        })
    }
}

/// Async [Batch] reader and iterator wrapper.
///
/// This is the data source for SST writers or internal readers.
pub enum Source {
    /// Source from a [BoxedBatchReader].
    Reader(BoxedBatchReader),
    /// Source from a [BoxedBatchIterator].
    Iter(BoxedBatchIterator),
    /// Source from a [BoxedBatchStream].
    Stream(BoxedBatchStream),
    /// Source from a [PruneReader].
    PruneReader(PruneReader),
}

impl Source {
    /// Returns next [Batch] from this data source.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<Batch>> {
        match self {
            Source::Reader(reader) => reader.next_batch().await,
            Source::Iter(iter) => iter.next().transpose(),
            Source::Stream(stream) => stream.try_next().await,
            Source::PruneReader(reader) => reader.next_batch().await,
        }
    }
}

/// Async batch reader.
///
/// The reader must guarantee [Batch]es returned by it have the same schema.
#[async_trait]
pub trait BatchReader: Send {
    /// Fetch next [Batch].
    ///
    /// Returns `Ok(None)` when the reader has reached its end and calling `next_batch()`
    /// again won't return batch again.
    ///
    /// If `Err` is returned, caller should not call this method again, the implementor
    /// may or may not panic in such case.
    async fn next_batch(&mut self) -> Result<Option<Batch>>;
}

/// Pointer to [BatchReader].
pub type BoxedBatchReader = Box<dyn BatchReader>;

/// Pointer to a stream that yields [Batch].
pub type BoxedBatchStream = BoxStream<'static, Result<Batch>>;

#[async_trait::async_trait]
impl<T: BatchReader + ?Sized> BatchReader for Box<T> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        (**self).next_batch().await
    }
}

/// Metrics for scanners.
#[derive(Debug, Default)]
pub(crate) struct ScannerMetrics {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Duration,
    /// Duration to build parts.
    build_parts_cost: Duration,
    /// Duration to build the (merge) reader.
    build_reader_cost: Duration,
    /// Duration to scan data.
    scan_cost: Duration,
    /// Duration to convert batches.
    convert_cost: Duration,
    /// Duration while waiting for `yield`.
    yield_cost: Duration,
    /// Duration of the scan.
    total_cost: Duration,
    /// Number of batches returned.
    num_batches: usize,
    /// Number of rows returned.
    num_rows: usize,
}

impl ScannerMetrics {
    /// Sets and observes metrics on initializing parts.
    fn observe_init_part(&mut self, build_parts_cost: Duration) {
        self.build_parts_cost = build_parts_cost;

        // Observes metrics.
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(self.prepare_scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["build_parts"])
            .observe(self.build_parts_cost.as_secs_f64());
    }

    /// Observes metrics on scanner finish.
    fn observe_metrics_on_finish(&self) {
        READ_STAGE_ELAPSED
            .with_label_values(&["build_reader"])
            .observe(self.build_reader_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["convert_rb"])
            .observe(self.convert_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan"])
            .observe(self.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["yield"])
            .observe(self.yield_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["total"])
            .observe(self.total_cost.as_secs_f64());
        READ_ROWS_RETURN.observe(self.num_rows as f64);
        READ_BATCHES_RETURN.observe(self.num_batches as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::test_util::new_batch_builder;

    fn new_batch(
        timestamps: &[i64],
        sequences: &[u64],
        op_types: &[OpType],
        field: &[u64],
    ) -> Batch {
        new_batch_builder(b"test", timestamps, sequences, op_types, 1, field)
            .build()
            .unwrap()
    }

    #[test]
    fn test_empty_batch() {
        let batch = new_batch(&[], &[], &[], &[]);
        assert_eq!(None, batch.first_timestamp());
        assert_eq!(None, batch.last_timestamp());
        assert_eq!(None, batch.first_sequence());
        assert_eq!(None, batch.last_sequence());
        assert!(batch.timestamps_native().is_none());
    }

    #[test]
    fn test_first_last_one() {
        let batch = new_batch(&[1], &[2], &[OpType::Put], &[4]);
        assert_eq!(
            Timestamp::new_millisecond(1),
            batch.first_timestamp().unwrap()
        );
        assert_eq!(
            Timestamp::new_millisecond(1),
            batch.last_timestamp().unwrap()
        );
        assert_eq!(2, batch.first_sequence().unwrap());
        assert_eq!(2, batch.last_sequence().unwrap());
    }

    #[test]
    fn test_first_last_multiple() {
        let batch = new_batch(
            &[1, 2, 3],
            &[11, 12, 13],
            &[OpType::Put, OpType::Put, OpType::Put],
            &[21, 22, 23],
        );
        assert_eq!(
            Timestamp::new_millisecond(1),
            batch.first_timestamp().unwrap()
        );
        assert_eq!(
            Timestamp::new_millisecond(3),
            batch.last_timestamp().unwrap()
        );
        assert_eq!(11, batch.first_sequence().unwrap());
        assert_eq!(13, batch.last_sequence().unwrap());
    }

    #[test]
    fn test_slice() {
        let batch = new_batch(
            &[1, 2, 3, 4],
            &[11, 12, 13, 14],
            &[OpType::Put, OpType::Delete, OpType::Put, OpType::Put],
            &[21, 22, 23, 24],
        );
        let batch = batch.slice(1, 2);
        let expect = new_batch(
            &[2, 3],
            &[12, 13],
            &[OpType::Delete, OpType::Put],
            &[22, 23],
        );
        assert_eq!(expect, batch);
    }

    #[test]
    fn test_timestamps_native() {
        let batch = new_batch(
            &[1, 2, 3, 4],
            &[11, 12, 13, 14],
            &[OpType::Put, OpType::Delete, OpType::Put, OpType::Put],
            &[21, 22, 23, 24],
        );
        assert_eq!(&[1, 2, 3, 4], batch.timestamps_native().unwrap());
    }

    #[test]
    fn test_concat_empty() {
        let err = Batch::concat(vec![]).unwrap_err();
        assert!(
            matches!(err, Error::InvalidBatch { .. }),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn test_concat_one() {
        let batch = new_batch(&[], &[], &[], &[]);
        let actual = Batch::concat(vec![batch.clone()]).unwrap();
        assert_eq!(batch, actual);

        let batch = new_batch(&[1, 2], &[11, 12], &[OpType::Put, OpType::Put], &[21, 22]);
        let actual = Batch::concat(vec![batch.clone()]).unwrap();
        assert_eq!(batch, actual);
    }

    #[test]
    fn test_concat_multiple() {
        let batches = vec![
            new_batch(&[1, 2], &[11, 12], &[OpType::Put, OpType::Put], &[21, 22]),
            new_batch(
                &[3, 4, 5],
                &[13, 14, 15],
                &[OpType::Put, OpType::Delete, OpType::Put],
                &[23, 24, 25],
            ),
            new_batch(&[], &[], &[], &[]),
            new_batch(&[6], &[16], &[OpType::Put], &[26]),
        ];
        let batch = Batch::concat(batches).unwrap();
        let expect = new_batch(
            &[1, 2, 3, 4, 5, 6],
            &[11, 12, 13, 14, 15, 16],
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Delete,
                OpType::Put,
                OpType::Put,
            ],
            &[21, 22, 23, 24, 25, 26],
        );
        assert_eq!(expect, batch);
    }

    #[test]
    fn test_concat_different() {
        let batch1 = new_batch(&[1], &[1], &[OpType::Put], &[1]);
        let mut batch2 = new_batch(&[2], &[2], &[OpType::Put], &[2]);
        batch2.primary_key = b"hello".to_vec();
        let err = Batch::concat(vec![batch1, batch2]).unwrap_err();
        assert!(
            matches!(err, Error::InvalidBatch { .. }),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn test_concat_different_fields() {
        let batch1 = new_batch(&[1], &[1], &[OpType::Put], &[1]);
        let fields = vec![
            batch1.fields()[0].clone(),
            BatchColumn {
                column_id: 2,
                data: Arc::new(UInt64Vector::from_slice([2])),
            },
        ];
        // Batch 2 has more fields.
        let batch2 = batch1.clone().with_fields(fields).unwrap();
        let err = Batch::concat(vec![batch1.clone(), batch2]).unwrap_err();
        assert!(
            matches!(err, Error::InvalidBatch { .. }),
            "unexpected err: {err}"
        );

        // Batch 2 has different field.
        let fields = vec![BatchColumn {
            column_id: 2,
            data: Arc::new(UInt64Vector::from_slice([2])),
        }];
        let batch2 = batch1.clone().with_fields(fields).unwrap();
        let err = Batch::concat(vec![batch1, batch2]).unwrap_err();
        assert!(
            matches!(err, Error::InvalidBatch { .. }),
            "unexpected err: {err}"
        );
    }

    #[test]
    fn test_filter_deleted_empty() {
        let mut batch = new_batch(&[], &[], &[], &[]);
        batch.filter_deleted().unwrap();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_filter_deleted() {
        let mut batch = new_batch(
            &[1, 2, 3, 4],
            &[11, 12, 13, 14],
            &[OpType::Delete, OpType::Put, OpType::Delete, OpType::Put],
            &[21, 22, 23, 24],
        );
        batch.filter_deleted().unwrap();
        let expect = new_batch(&[2, 4], &[12, 14], &[OpType::Put, OpType::Put], &[22, 24]);
        assert_eq!(expect, batch);

        let mut batch = new_batch(
            &[1, 2, 3, 4],
            &[11, 12, 13, 14],
            &[OpType::Put, OpType::Put, OpType::Put, OpType::Put],
            &[21, 22, 23, 24],
        );
        let expect = batch.clone();
        batch.filter_deleted().unwrap();
        assert_eq!(expect, batch);
    }

    #[test]
    fn test_filter() {
        // Filters put only.
        let mut batch = new_batch(
            &[1, 2, 3, 4],
            &[11, 12, 13, 14],
            &[OpType::Put, OpType::Put, OpType::Put, OpType::Put],
            &[21, 22, 23, 24],
        );
        let predicate = BooleanVector::from_vec(vec![false, false, true, true]);
        batch.filter(&predicate).unwrap();
        let expect = new_batch(&[3, 4], &[13, 14], &[OpType::Put, OpType::Put], &[23, 24]);
        assert_eq!(expect, batch);

        // Filters deletion.
        let mut batch = new_batch(
            &[1, 2, 3, 4],
            &[11, 12, 13, 14],
            &[OpType::Put, OpType::Delete, OpType::Put, OpType::Put],
            &[21, 22, 23, 24],
        );
        let predicate = BooleanVector::from_vec(vec![false, false, true, true]);
        batch.filter(&predicate).unwrap();
        let expect = new_batch(&[3, 4], &[13, 14], &[OpType::Put, OpType::Put], &[23, 24]);
        assert_eq!(expect, batch);

        // Filters to empty.
        let predicate = BooleanVector::from_vec(vec![false, false]);
        batch.filter(&predicate).unwrap();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_sort_and_dedup() {
        let original = new_batch(
            &[2, 3, 1, 4, 5, 2],
            &[1, 2, 3, 4, 5, 6],
            &[
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
                OpType::Put,
            ],
            &[21, 22, 23, 24, 25, 26],
        );

        let mut batch = original.clone();
        batch.sort(true).unwrap();
        // It should only keep one timestamp 2.
        assert_eq!(
            new_batch(
                &[1, 2, 3, 4, 5],
                &[3, 6, 2, 4, 5],
                &[
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                ],
                &[23, 26, 22, 24, 25],
            ),
            batch
        );

        let mut batch = original.clone();
        batch.sort(false).unwrap();

        // It should only keep one timestamp 2.
        assert_eq!(
            new_batch(
                &[1, 2, 2, 3, 4, 5],
                &[3, 6, 1, 2, 4, 5],
                &[
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                    OpType::Put,
                ],
                &[23, 26, 21, 22, 24, 25],
            ),
            batch
        );

        let original = new_batch(
            &[2, 2, 1],
            &[1, 6, 1],
            &[OpType::Delete, OpType::Put, OpType::Put],
            &[21, 22, 23],
        );

        let mut batch = original.clone();
        batch.sort(true).unwrap();
        let expect = new_batch(&[1, 2], &[1, 6], &[OpType::Put, OpType::Put], &[23, 22]);
        assert_eq!(expect, batch);

        let mut batch = original.clone();
        batch.sort(false).unwrap();
        let expect = new_batch(
            &[1, 2, 2],
            &[1, 6, 1],
            &[OpType::Put, OpType::Put, OpType::Delete],
            &[23, 22, 21],
        );
        assert_eq!(expect, batch);
    }
}
