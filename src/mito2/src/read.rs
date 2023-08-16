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

use async_trait::async_trait;
use common_time::Timestamp;
use datatypes::arrow;
use datatypes::arrow::array::ArrayRef;
use datatypes::prelude::DataType;
use datatypes::value::ValueRef;
use datatypes::vectors::{Helper, UInt64Vector, UInt8Vector, Vector, VectorRef};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::error::{ConvertVectorSnafu, InvalidBatchSnafu, Result};

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
        // All vectors have the same length so we use
        // the length of timestamps vector.
        self.timestamps.len()
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

    /// Get a timestamp at given index.
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
            value => panic!("{:?} is not a timestmap", value),
        }
    }
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
