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

use std::sync::Arc;

use api::v1::OpType;
use async_trait::async_trait;
use common_time::Timestamp;
use datatypes::arrow;
use datatypes::arrow::array::{Array, ArrayRef};
use datatypes::arrow::compute::SortOptions;
use datatypes::arrow::row::{RowConverter, SortField};
use datatypes::prelude::{DataType, ScalarVector};
use datatypes::value::ValueRef;
use datatypes::vectors::{
    BooleanVector, Helper, UInt32Vector, UInt64Vector, UInt8Vector, Vector, VectorRef,
};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};

use crate::error::{
    ComputeArrowSnafu, ComputeVectorSnafu, ConvertVectorSnafu, InvalidBatchSnafu, Result,
};

/// Storage internal representation of a batch of rows
/// for a primary key (time series).
///
/// Rows are sorted by primary key, timestamp, sequence desc, op_type desc.
#[derive(Debug, PartialEq, Clone)]
pub struct Batch {
    /// Primary key encoded in a comparable form.
    primary_key: Vec<u8>,
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

    /// Returns primary key of the batch.
    pub fn primary_key(&self) -> &[u8] {
        &self.primary_key
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

    /// Returns the first timestamp in the batch.
    pub fn first_timestamp(&self) -> Option<Timestamp> {
        self.get_timestamp(0)
    }

    /// Returns the last timestamp in the batch.
    pub fn last_timestamp(&self) -> Option<Timestamp> {
        self.get_timestamp(self.timestamps.len() - 1)
    }

    /// Returns the first sequence in the batch or `None` if the batch is empty.
    pub fn first_sequence(&self) -> Option<SequenceNumber> {
        self.get_sequence(0)
    }

    /// Returns the last sequence in the batch or `None` if the batch is empty.
    pub fn last_sequence(&self) -> Option<SequenceNumber> {
        self.get_sequence(self.sequences.len() - 1)
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
                .all(|b| b.primary_key() == &primary_key),
            InvalidBatchSnafu {
                reason: "batches have different primary key",
            }
        );
        ensure!(
            batches
                .iter()
                .skip(1)
                .all(|b| b.fields().len() == first.fields().len()),
            InvalidBatchSnafu {
                reason: "batches have different field num",
            }
        );

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
        let predicate =
            arrow::compute::neq_scalar(array, OpType::Delete as u8).context(ComputeArrowSnafu)?;
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

    /// Sorts rows in the batch.
    ///
    /// It orders rows by timestamp, sequence desc. It doesn't consider op type as sequence
    /// should already provide uniqueness for a row.
    pub fn sort(&mut self) -> Result<()> {
        // If building a converter each time is costly, we may allow passing a
        // converter.
        let mut converter = RowConverter::new(vec![
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

        let indices = UInt32Vector::from_iter_values(to_sort.iter().map(|v| v.0 as u32));
        self.take_in_place(&indices)
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
    fn get_timestamp(&self, index: usize) -> Option<Timestamp> {
        if self.timestamps.is_empty() {
            return None;
        }

        match self.timestamps.get_ref(index) {
            ValueRef::Timestamp(timestamp) => Some(timestamp),
            // Int64 is always millisecond.
            // TODO(yingwen): Don't allow using int64 as time index.
            ValueRef::Int64(v) => Some(Timestamp::new_millisecond(v)),
            // We have check the data type is timestamp compatible in the [BatchBuilder] so it's safe to panic.
            value => panic!("{:?} is not a timestamp", value),
        }
    }

    /// Gets a sequence at given `index`.
    fn get_sequence(&self, index: usize) -> Option<SequenceNumber> {
        if self.sequences.is_empty() {
            return None;
        }

        // Sequences is not null so it actually returns Some.
        self.sequences.get_data(index)
    }
}

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
            vector.data_type().is_timestamp_compatible(),
            InvalidBatchSnafu {
                reason: format!("{:?} is a timestamp type", vector.data_type()),
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
            timestamps,
            sequences,
            op_types,
            fields: self.fields,
        })
    }
}

/// Collected [Source] statistics.
#[derive(Debug, Clone)]
pub struct SourceStats {
    /// Number of rows fetched.
    pub num_rows: usize,
    /// Min timestamp from fetched batches.
    ///
    /// If no rows fetched, the value of the timestamp is i64::MIN.
    pub min_timestamp: Timestamp,
    /// Max timestamp from fetched batches.
    ///
    /// If no rows fetched, the value of the timestamp is i64::MAX.
    pub max_timestamp: Timestamp,
}

/// Async [Batch] reader and iterator wrapper.
///
/// This is the data source for SST writers or internal readers.
pub enum Source {}

impl Source {
    /// Returns next [Batch] from this data source.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<Batch>> {
        unimplemented!()
    }

    /// Returns the metadata of the source region.
    pub(crate) fn metadata(&self) -> RegionMetadataRef {
        unimplemented!()
    }

    // TODO(yingwen): Maybe remove this method.
    /// Returns statisics of fetched batches.
    pub(crate) fn stats(&self) -> SourceStats {
        unimplemented!()
    }
}

/// Async batch reader.
///
/// The reader must guarantee [Batch]es returned by it have the same schema.
#[async_trait]
pub trait BatchReader: Send {
    // TODO(yingwen): fields of the batch returned.

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

#[async_trait::async_trait]
impl<T: BatchReader + ?Sized> BatchReader for Box<T> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        (**self).next_batch().await
    }
}
