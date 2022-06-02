mod inserter;
mod schema;

use std::mem;
use std::sync::Arc;

use datatypes::vectors::VectorRef;
use store_api::storage::{SequenceNumber, ValueType};

use crate::error::Result;
pub use crate::memtable::inserter::Inserter;
pub use crate::memtable::schema::MemTableSchema;

/// Key-value pairs in columnar format.
pub struct KeyValues {
    pub sequence: SequenceNumber,
    pub value_type: ValueType,
    /// Start index of these key-value paris in batch.
    pub start_index_in_batch: usize,
    pub keys: Vec<VectorRef>,
    pub values: Vec<VectorRef>,
}

/// In memory storage.
pub trait MemTable: Send + Sync {
    fn schema(&self) -> &MemTableSchema;

    /// Write key/values in request to the memtable.
    ///
    /// # Panics
    /// Panic if the schema of key/value differs from memtable's schema.
    fn write(&self, request: &KeyValues) -> Result<()>;

    fn bytes_allocated(&self) -> usize;
}

pub type MemTableRef = Arc<dyn MemTable>;

pub trait MemTableBuilder: Send + Sync {
    fn build(&self, schema: MemTableSchema) -> MemTableRef;
}

pub type MemTableBuilderRef = Arc<dyn MemTableBuilder>;

pub struct DefaultMemTableBuilder {}

impl MemTableBuilder for DefaultMemTableBuilder {
    fn build(&self, _schema: MemTableSchema) -> MemTableRef {
        unimplemented!()
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
