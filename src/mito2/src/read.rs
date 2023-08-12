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
use datatypes::vectors::VectorRef;
use store_api::storage::{ColumnId, Tsid};

use crate::error::Result;
use crate::metadata::RegionMetadataRef;

/// Storage internal representation of a batch of rows
/// for a primary key (time series).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Batch {
    /// Tsid of the batch.
    tsid: Tsid,
    /// Primary key encoded in a comparable form.
    // TODO(yingwen): Maybe use `Bytes`.
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
    /// Rows organized in columnar format.
    columns: Vec<BatchColumn>,
    /// Has delete op.
    has_delete_op: bool,
    /// Has duplicate keys to dedup.
    has_duplication: bool,
}

impl Batch {
    /// Returns columns in the batch.
    pub fn columns(&self) -> &[BatchColumn] {
        &self.columns
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

    /// Returns the tsid of the batch.
    ///
    /// It's used to identify a time series inside a SST. So different
    /// time series might have the same tsid.
    pub(crate) fn tsid(&self) -> Tsid {
        self.tsid
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
