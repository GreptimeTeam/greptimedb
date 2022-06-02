mod btree;
mod inserter;
mod schema;

use std::mem;
use std::sync::Arc;

use datatypes::vectors::VectorRef;
use store_api::storage::{SequenceNumber, ValueType};

use crate::error::Result;
use crate::memtable::btree::BTreeMemTable;
pub use crate::memtable::inserter::Inserter;
pub use crate::memtable::schema::MemTableSchema;

/// In memory storage.
pub trait MemTable: Send + Sync {
    fn schema(&self) -> &MemTableSchema;

    /// Write key/values to the memtable.
    ///
    /// # Panics
    /// Panic if the schema of key/value differs from memtable's schema.
    fn write(&self, key_values: &KeyValues) -> Result<()>;

    fn bytes_allocated(&self) -> usize;
}

pub type MemTableRef = Arc<dyn MemTable>;

pub trait MemTableBuilder: Send + Sync {
    fn build(&self, schema: MemTableSchema) -> MemTableRef;
}

pub type MemTableBuilderRef = Arc<dyn MemTableBuilder>;

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
    pub fn len(&self) -> usize {
        self.keys.first().map(|v| v.len()).unwrap_or_default()
    }
}

pub struct DefaultMemTableBuilder {}

impl MemTableBuilder for DefaultMemTableBuilder {
    fn build(&self, schema: MemTableSchema) -> MemTableRef {
        Arc::new(BTreeMemTable::new(schema))
    }
}

pub struct MemTableSet {
    mem: MemTableRef,
    // TODO(yingwen): Support multiple immutable memtables.
    _immem: Option<MemTableRef>,
}

impl MemTableSet {
    pub fn new(mem: MemTableRef) -> MemTableSet {
        MemTableSet { mem, _immem: None }
    }

    pub fn mutable_memtable(&self) -> &MemTableRef {
        &self.mem
    }

    /// Switch mutable memtable to immutable memtable, returns the old mutable memtable if success.
    pub fn _switch_memtable(&mut self, mem: &MemTableRef) -> std::result::Result<MemTableRef, ()> {
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
