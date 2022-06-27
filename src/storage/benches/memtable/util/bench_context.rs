use storage::memtable::{IterContext, KeyValues, MemtableRef};
use store_api::storage::SequenceNumber;

use crate::memtable::util::new_memtable;

pub struct BenchContext {
    memtable: MemtableRef,
}
impl Default for BenchContext {
    fn default() -> Self {
        BenchContext::new()
    }
}
impl BenchContext {
    pub fn new() -> BenchContext {
        BenchContext {
            memtable: new_memtable(),
        }
    }

    pub fn write(&self, kvs: &KeyValues) {
        self.memtable.write(kvs).unwrap();
    }

    pub fn read(&self, batch_size: usize) {
        let iter_ctx = IterContext {
            batch_size,
            visible_sequence: SequenceNumber::MAX,
        };
        let mut iter = self.memtable.iter(iter_ctx).unwrap();
        while iter.next().unwrap().is_some() {}
    }
}
