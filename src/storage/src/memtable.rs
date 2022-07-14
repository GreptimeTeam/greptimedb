mod btree;
mod inserter;
mod schema;
#[cfg(test)]
pub mod tests;
mod version;

use std::sync::Arc;

use datatypes::vectors::{UInt64Vector, UInt8Vector, VectorRef};
use store_api::storage::{consts, SequenceNumber, ValueType};

use crate::error::Result;
use crate::memtable::btree::BTreeMemtable;
pub use crate::memtable::inserter::Inserter;
pub use crate::memtable::schema::MemtableSchema;
pub use crate::memtable::version::{FreezeError, MemtableSet, MemtableVersion};

/// Unique id for memtables under same region.
pub type MemtableId = u32;

/// In memory storage.
pub trait Memtable: Send + Sync + std::fmt::Debug {
    fn id(&self) -> MemtableId;

    fn schema(&self) -> &MemtableSchema;

    /// Write key/values to the memtable.
    ///
    /// # Panics
    /// Panics if the schema of key/value differs from memtable's schema.
    fn write(&self, kvs: &KeyValues) -> Result<()>;

    /// Iterates the memtable.
    // TODO(yingwen): Consider passing a projector (does column projection).
    fn iter(&self, ctx: IterContext) -> Result<BatchIteratorPtr>;

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
#[derive(Debug, PartialEq)]
pub enum RowOrdering {
    /// The output rows are unordered.
    Unordered,

    /// The output rows are ordered by key.
    Key,
}

// TODO(yingwen): Maybe pack value_type with sequence (reserve 8bits in u64 for value type) like RocksDB.
pub struct Batch {
    pub keys: Vec<VectorRef>,
    pub sequences: UInt64Vector,
    pub value_types: UInt8Vector,
    pub values: Vec<VectorRef>,
}

/// Iterator of memtable.
pub trait BatchIterator: Iterator<Item = Result<Batch>> + Send + Sync {
    /// Returns the schema of this iterator.
    fn schema(&self) -> &MemtableSchema;

    /// Returns the ordering of the output rows from this iterator.
    fn ordering(&self) -> RowOrdering;
}

pub type BatchIteratorPtr = Box<dyn BatchIterator>;

pub trait MemtableBuilder: Send + Sync {
    fn build(&self, id: MemtableId, schema: MemtableSchema) -> MemtableRef;
}

pub type MemtableBuilderRef = Arc<dyn MemtableBuilder>;

// TODO(yingwen): Maybe use individual vector for timestamp and version.
/// Key-value pairs in columnar format.
pub struct KeyValues {
    pub sequence: SequenceNumber,
    pub value_type: ValueType,
    /// Start index of these key-value paris in batch.
    pub start_index_in_batch: usize,
    pub keys: Vec<VectorRef>,
    pub values: Vec<VectorRef>,
}

impl KeyValues {
    // Note that `sequence` is not reset.
    fn reset(&mut self, value_type: ValueType, index_in_batch: usize) {
        self.value_type = value_type;
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

pub struct DefaultMemtableBuilder {}

impl MemtableBuilder for DefaultMemtableBuilder {
    fn build(&self, id: MemtableId, schema: MemtableSchema) -> MemtableRef {
        Arc::new(BTreeMemtable::new(id, schema))
    }
}
