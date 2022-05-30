use std::mem;
use std::sync::Arc;

use store_api::storage::SchemaRef;

use crate::error::Result;
use crate::write_batch::WriteBatch;

/// In memory storage.
pub trait MemTable: Send + Sync {
    fn schema(&self) -> &SchemaRef;

    fn write(&self, batch: &WriteBatch) -> Result<()>;

    fn bytes_allocated(&self) -> usize;
}

pub type MemTableRef = Arc<dyn MemTable>;

pub trait MemTableBuilder: Send + Sync {
    fn build(&self) -> MemTableRef;
}

pub type MemTableBuilderRef = Arc<dyn MemTableBuilder>;

pub struct DefaultMemTableBuilder {}

impl MemTableBuilder for DefaultMemTableBuilder {
    fn build(&self) -> MemTableRef {
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
