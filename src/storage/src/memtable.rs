mod btree;
mod inserter;
mod schema;
#[cfg(test)]
mod tests;

use std::mem;
use std::sync::Arc;

use datatypes::vectors::{UInt64Vector, UInt8Vector, VectorRef};
use snafu::Snafu;
use store_api::storage::{SequenceNumber, ValueType};

use crate::error::Result;
use crate::memtable::btree::BTreeMemtable;
pub use crate::memtable::inserter::Inserter;
pub use crate::memtable::schema::MemtableSchema;

/// In memory storage.
pub trait Memtable: Send + Sync {
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
    pub sequence: SequenceNumber,
}

impl Default for IterContext {
    fn default() -> Self {
        Self {
            batch_size: 256,
            // All data in memory is visible.
            sequence: SequenceNumber::MAX,
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

pub struct Batch {
    pub keys: Vec<VectorRef>,
    pub sequences: UInt64Vector,
    pub value_types: UInt8Vector,
    pub values: Vec<VectorRef>,
}

/// Iterator of memtable.
pub trait BatchIterator: Send {
    /// Returns the schema of this iterator.
    fn schema(&self) -> &MemtableSchema;

    /// Returns the ordering of the output rows from this iterator.
    fn ordering(&self) -> RowOrdering;

    /// Fetch next batch from the memtable.
    ///
    /// # Panics
    /// Panics if the iterator has already been exhausted.
    fn next(&mut self) -> Result<Option<Batch>>;
}

pub type BatchIteratorPtr = Box<dyn BatchIterator>;

pub trait MemtableBuilder: Send + Sync {
    fn build(&self, schema: MemtableSchema) -> MemtableRef;
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
}

pub struct DefaultMemtableBuilder {}

impl MemtableBuilder for DefaultMemtableBuilder {
    fn build(&self, schema: MemtableSchema) -> MemtableRef {
        Arc::new(BTreeMemtable::new(schema))
    }
}

#[derive(Debug, Snafu)]
#[snafu(display("Fail to switch memtable"))]
pub struct SwitchError;

pub struct MemtableSet {
    mem: MemtableRef,
    // TODO(yingwen): Support multiple immutable memtables.
    _immem: Option<MemtableRef>,
}

impl MemtableSet {
    pub fn new(mem: MemtableRef) -> MemtableSet {
        MemtableSet { mem, _immem: None }
    }

    pub fn mutable_memtable(&self) -> &MemtableRef {
        &self.mem
    }

    /// Switch mutable memtable to immutable memtable, returns the old mutable memtable if success.
    pub fn _switch_memtable(
        &mut self,
        mem: &MemtableRef,
    ) -> std::result::Result<MemtableRef, SwitchError> {
        match &self._immem {
            Some(_) => SwitchSnafu {}.fail(),
            None => {
                let old_mem = mem::replace(&mut self.mem, mem.clone());
                self._immem = Some(old_mem.clone());
                Ok(old_mem)
            }
        }
    }
}
