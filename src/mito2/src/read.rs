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

use async_trait::async_trait;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::VectorRef;
use snafu::ensure;
use store_api::storage::ColumnId;

use crate::error::{InvalidBatchSnafu, Result};
use crate::metadata::RegionMetadataRef;

/// Storage internal representation of a batch of rows
/// for a primary key (time series).
///
/// Rows are sorted by primary key, timestamp, sequence desc, op_type desc.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Batch {
    /// Primary key encoded in a comparable form.
    primary_key: Vec<u8>,
    /// Timestamps of rows, should be sorted and not null.
    timestamps: VectorRef,
    /// Sequences of rows
    ///
    /// UInt64 type, not null.
    sequences: VectorRef,
    /// Op types of rows
    ///
    /// UInt8 type, not null.
    op_types: VectorRef,
    /// Fields organized in columnar format.
    fields: Vec<BatchColumn>,
}

impl Batch {
    /// Creates a new batch.
    pub fn new(
        primary_key: Vec<u8>,
        timestamps: VectorRef,
        sequences: VectorRef,
        op_types: VectorRef,
        fields: Vec<BatchColumn>,
    ) -> Result<Batch> {
        BatchBuilder::new(primary_key, timestamps, sequences, op_types)
            .fields(fields)
            .build()
    }

    /// Returns fields in the batch.
    pub fn fields(&self) -> &[BatchColumn] {
        &self.fields
    }

    /// Returns sequences of the batch.
    pub fn sequences(&self) -> &VectorRef {
        &self.sequences
    }

    /// Returns op types of the batch.
    pub fn op_types(&self) -> &VectorRef {
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
    timestamps: VectorRef,
    sequences: VectorRef,
    op_types: VectorRef,
    fields: Vec<BatchColumn>,
}

impl BatchBuilder {
    /// Creates a new [BatchBuilder].
    pub fn new(
        primary_key: Vec<u8>,
        timestamps: VectorRef,
        sequences: VectorRef,
        op_types: VectorRef,
    ) -> BatchBuilder {
        BatchBuilder {
            primary_key,
            timestamps,
            sequences,
            op_types,
            fields: Vec::new(),
        }
    }

    /// Set all field columns.
    pub fn fields(&mut self, fields: Vec<BatchColumn>) -> &mut Self {
        self.fields = fields;
        self
    }

    /// Push a field column.
    pub fn push_field(&mut self, column: BatchColumn) -> &mut Self {
        self.fields.push(column);
        self
    }

    /// Builds the [Batch].
    pub fn build(self) -> Result<Batch> {
        let ts_len = self.timestamps.len();
        ensure!(
            self.sequences.len() == ts_len,
            InvalidBatchSnafu {
                reason: format!(
                    "sequence have different len {} != {}",
                    self.sequences.len(),
                    ts_len
                ),
            }
        );
        ensure!(
            self.sequences.data_type() == ConcreteDataType::uint64_datatype(),
            InvalidBatchSnafu {
                reason: format!(
                    "sequence must has uint64 type, given: {:?}",
                    self.sequences.data_type()
                ),
            }
        );
        ensure!(
            self.op_types.len() == ts_len,
            InvalidBatchSnafu {
                reason: format!(
                    "op type have different len {} != {}",
                    self.op_types.len(),
                    ts_len
                ),
            }
        );
        ensure!(
            self.sequences.data_type() == ConcreteDataType::uint8_datatype(),
            InvalidBatchSnafu {
                reason: format!(
                    "sequence must has uint8 type, given: {:?}",
                    self.op_types.data_type()
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
            timestamps: self.timestamps,
            sequences: self.sequences,
            op_types: self.op_types,
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

#[async_trait::async_trait]
impl<T: BatchReader + ?Sized> BatchReader for Box<T> {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        (**self).next_batch().await
    }
}
