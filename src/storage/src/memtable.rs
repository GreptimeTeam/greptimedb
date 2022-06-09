mod btree;
mod inserter;
mod schema;

use std::mem;
use std::sync::Arc;

use datatypes::vectors::VectorRef;
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
    /// Panic if the schema of key/value differs from memtable's schema.
    fn write(&self, kvs: &KeyValues) -> Result<()>;

    /// Returns the estimated bytes allocated by this memtable from heap.
    fn bytes_allocated(&self) -> usize;
}

pub type MemtableRef = Arc<dyn Memtable>;

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
}

impl KeyValues {
    pub fn len(&self) -> usize {
        self.keys.first().map(|v| v.len()).unwrap_or_default()
    }
}

pub struct DefaultMemtableBuilder {}

impl MemtableBuilder for DefaultMemtableBuilder {
    fn build(&self, schema: MemtableSchema) -> MemtableRef {
        Arc::new(BTreeMemtable::new(schema))
    }
}

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
    pub fn _switch_memtable(&mut self, mem: &MemtableRef) -> std::result::Result<MemtableRef, ()> {
        match &self._immem {
            Some(_) => Err(()),
            None => {
                let old_mem = mem::replace(&mut self.mem, mem.clone());
                self._immem = Some(old_mem.clone());
                Ok(old_mem)
            }
        }
    }
}
