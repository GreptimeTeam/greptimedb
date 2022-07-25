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

    pub fn read(&self, batch_size: usize) -> usize {
        let mut read_count = 0;
        let iter_ctx = IterContext {
            batch_size,
            visible_sequence: SequenceNumber::MAX,
            for_flush: false,
        };
        let iter = self.memtable.iter(iter_ctx).unwrap();
        for batch in iter {
            batch.unwrap();
            read_count += batch_size;
        }
        read_count
    }
}
