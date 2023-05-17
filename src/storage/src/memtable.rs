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

mod btree;
mod inserter;
#[cfg(test)]
pub mod tests;
mod version;

use std::sync::atomic::{AtomicI64, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

use datatypes::vectors::VectorRef;
use store_api::storage::{consts, OpType, SequenceNumber};

use crate::error::Result;
use crate::memtable::btree::BTreeMemtable;
pub use crate::memtable::inserter::Inserter;
pub use crate::memtable::version::MemtableVersion;
use crate::read::Batch;
use crate::schema::{ProjectedSchemaRef, RegionSchemaRef};

/// Unique id for memtables under same region.
pub type MemtableId = u32;

#[derive(Debug)]
pub struct MemtableStats {
    /// The  estimated bytes allocated by this memtable from heap. Result
    /// of this method may be larger than the estimated based on [`num_rows`] because
    /// of the implementor's pre-alloc behavior.
    estimated_bytes: AtomicUsize,
    /// The max timestamp that this memtable contains.
    max_timestamp: AtomicI64,
    /// The min timestamp that this memtable contains.
    min_timestamp: AtomicI64,
}

impl MemtableStats {
    pub fn bytes_allocated(&self) -> usize {
        self.estimated_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for MemtableStats {
    fn default() -> Self {
        Self {
            estimated_bytes: AtomicUsize::default(),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
        }
    }
}

/// In memory storage.
pub trait Memtable: Send + Sync + std::fmt::Debug {
    /// Returns id of this memtable.
    fn id(&self) -> MemtableId;

    /// Returns schema of the memtable.
    fn schema(&self) -> RegionSchemaRef;

    /// Write key/values to the memtable.
    ///
    /// # Panics
    /// Panics if the schema of key/value differs from memtable's schema.
    fn write(&self, kvs: &KeyValues) -> Result<()>;

    /// Iterates the memtable.
    fn iter(&self, ctx: &IterContext) -> Result<BoxedBatchIterator>;

    /// Returns the estimated bytes allocated by this memtable from heap. Result
    /// of this method may be larger than the estimated based on [`num_rows`] because
    /// of the implementor's pre-alloc behavior.
    fn num_rows(&self) -> usize;

    /// Returns stats of this memtable.
    fn stats(&self) -> &MemtableStats;
}

pub type MemtableRef = Arc<dyn Memtable>;

/// Context for iterating memtable.
///
/// Should be cheap to clone.
#[derive(Debug, Clone)]
pub struct IterContext {
    /// The suggested batch size of the iterator.
    pub batch_size: usize,
    /// Max visible sequence (inclusive).
    pub visible_sequence: SequenceNumber,

    // TODO(yingwen): [flush] Maybe delay deduping and visiblility handling, just returns all rows
    // in memtable.
    /// Returns all rows, ignores sequence visibility and key duplication.
    pub for_flush: bool,

    /// Schema the reader expect to read.
    ///
    /// Set to `None` to read all columns.
    pub projected_schema: Option<ProjectedSchemaRef>,
}

impl Default for IterContext {
    fn default() -> Self {
        Self {
            batch_size: consts::READ_BATCH_SIZE,
            // All data in memory is visible by default.
            visible_sequence: SequenceNumber::MAX,
            for_flush: false,
            projected_schema: None,
        }
    }
}

/// The ordering of the iterator output.
#[derive(Debug, PartialEq, Eq)]
pub enum RowOrdering {
    /// The output rows are unordered.
    Unordered,

    /// The output rows are ordered by key.
    Key,
}

/// Iterator of memtable.
///
/// Since data of memtable are stored in memory, so avoid defining this trait
/// as an async trait.
pub trait BatchIterator: Iterator<Item = Result<Batch>> + Send + Sync {
    /// Returns the schema of this iterator.
    fn schema(&self) -> ProjectedSchemaRef;

    /// Returns the ordering of the output rows from this iterator.
    fn ordering(&self) -> RowOrdering;
}

pub type BoxedBatchIterator = Box<dyn BatchIterator>;

pub trait MemtableBuilder: Send + Sync + std::fmt::Debug {
    fn build(&self, schema: RegionSchemaRef) -> MemtableRef;
}

pub type MemtableBuilderRef = Arc<dyn MemtableBuilder>;

/// Key-value pairs in columnar format.
pub struct KeyValues {
    pub sequence: SequenceNumber,
    pub op_type: OpType,
    /// Start index of these key-value paris in batch. Each row in the same batch has
    /// a unique index to identify it.
    pub start_index_in_batch: usize,
    pub keys: Vec<VectorRef>,
    pub values: Vec<VectorRef>,
    pub timestamp: Option<VectorRef>,
}

impl KeyValues {
    // Note that `sequence` is not reset.
    fn reset(&mut self, op_type: OpType, index_in_batch: usize) {
        self.op_type = op_type;
        self.start_index_in_batch = index_in_batch;
        self.keys.clear();
        self.values.clear();
        self.timestamp = None;
    }

    pub fn len(&self) -> usize {
        self.timestamp.as_ref().map(|v| v.len()).unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn estimated_memory_size(&self) -> usize {
        self.keys.iter().fold(0, |acc, v| acc + v.memory_size())
            + self.values.iter().fold(0, |acc, v| acc + v.memory_size())
            + self
                .timestamp
                .as_ref()
                .map(|t| t.memory_size())
                .unwrap_or_default()
    }
}

#[derive(Debug, Default)]
pub struct DefaultMemtableBuilder {
    memtable_id: AtomicU32,
}

impl MemtableBuilder for DefaultMemtableBuilder {
    fn build(&self, schema: RegionSchemaRef) -> MemtableRef {
        let id = self.memtable_id.fetch_add(1, Ordering::Relaxed);
        Arc::new(BTreeMemtable::new(id, schema))
    }
}
