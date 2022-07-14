mod btree;
mod inserter;
#[cfg(test)]
pub mod tests;
mod version;

use std::sync::Arc;

use datatypes::vectors::VectorRef;
use store_api::storage::{consts, OpType, SequenceNumber};

use crate::error::Result;
use crate::memtable::btree::BTreeMemtable;
pub use crate::memtable::inserter::Inserter;
pub use crate::memtable::version::{MemtableSet, MemtableVersion};
use crate::read::Batch;
use crate::schema::RegionSchemaRef;

/// Unique id for memtables under same region.
pub type MemtableId = u32;

/// In memory storage.
pub trait Memtable: Send + Sync + std::fmt::Debug {
    fn id(&self) -> MemtableId;

    fn schema(&self) -> RegionSchemaRef;

    /// Write key/values to the memtable.
    ///
    /// # Panics
    /// Panics if the schema of key/value differs from memtable's schema.
    fn write(&self, kvs: &KeyValues) -> Result<()>;

    /// Iterates the memtable.
    // TODO(yingwen): 1. Use reference of IterContext? 2. Consider passing a projector (does column projection).
    fn iter(&self, ctx: IterContext) -> Result<BoxedBatchIterator>;

    /// Returns the estimated bytes allocated by this memtable from heap.
    fn bytes_allocated(&self) -> usize;
}

pub type MemtableRef = Arc<dyn Memtable>;

/// Context for iterating memtable.
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
}

impl Default for IterContext {
    fn default() -> Self {
        Self {
            batch_size: consts::READ_BATCH_SIZE,
            // All data in memory is visible by default.
            visible_sequence: SequenceNumber::MAX,
            for_flush: false,
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
    fn schema(&self) -> RegionSchemaRef;

    /// Returns the ordering of the output rows from this iterator.
    fn ordering(&self) -> RowOrdering;
}

pub type BoxedBatchIterator = Box<dyn BatchIterator>;

pub trait MemtableBuilder: Send + Sync + std::fmt::Debug {
    fn build(&self, id: MemtableId, schema: RegionSchemaRef) -> MemtableRef;
}

pub type MemtableBuilderRef = Arc<dyn MemtableBuilder>;

// TODO(yingwen): Maybe use individual vector for timestamp and version.
/// Key-value pairs in columnar format.
pub struct KeyValues {
    pub sequence: SequenceNumber,
    pub op_type: OpType,
    /// Start index of these key-value paris in batch. Each row in the same batch has
    /// a unique index to identify it.
    pub start_index_in_batch: usize,
    pub keys: Vec<VectorRef>,
    pub values: Vec<VectorRef>,
}

impl KeyValues {
    // Note that `sequence` is not reset.
    fn reset(&mut self, op_type: OpType, index_in_batch: usize) {
        self.op_type = op_type;
        self.start_index_in_batch = index_in_batch;
        self.keys.clear();
        self.values.clear();
    }

    pub fn len(&self) -> usize {
        self.keys.first().map(|v| v.len()).unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn estimated_memory_size(&self) -> usize {
        self.keys.iter().fold(0, |acc, v| acc + v.memory_size())
            + self.values.iter().fold(0, |acc, v| acc + v.memory_size())
    }
}

#[derive(Debug)]
pub struct DefaultMemtableBuilder;

impl MemtableBuilder for DefaultMemtableBuilder {
    fn build(&self, id: MemtableId, schema: RegionSchemaRef) -> MemtableRef {
        Arc::new(BTreeMemtable::new(id, schema))
    }
}
