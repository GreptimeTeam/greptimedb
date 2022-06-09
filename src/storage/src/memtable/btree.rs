use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::RwLock;

use datatypes::value::Value;
use store_api::storage::{SequenceNumber, ValueType};

use crate::error::Result;
use crate::memtable::{KeyValues, Memtable, MemtableSchema};

/// A simple memtable implementation based on std's [`BTreeMap`].
///
/// Mainly for test purpose.
pub struct BTreeMemtable {
    schema: MemtableSchema,
    map: RwLock<BTreeMap<RowKey, RowValue>>,
}

impl BTreeMemtable {
    pub fn new(schema: MemtableSchema) -> BTreeMemtable {
        BTreeMemtable {
            schema,
            map: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Memtable for BTreeMemtable {
    fn schema(&self) -> &MemtableSchema {
        &self.schema
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        let mut map = self.map.write().unwrap();

        let iter_row = IterRow::new(kvs);
        for (row_key, row_value) in iter_row {
            map.insert(row_key, row_value);
        }

        Ok(())
    }

    fn bytes_allocated(&self) -> usize {
        unimplemented!()
    }
}

struct IterRow<'a> {
    kvs: &'a KeyValues,
    index: usize,
    len: usize,
}

impl<'a> IterRow<'a> {
    fn new(kvs: &KeyValues) -> IterRow {
        IterRow {
            kvs,
            index: 0,
            len: kvs.len(),
        }
    }

    fn fetch_row(&mut self) -> (RowKey, RowValue) {
        let keys = self
            .kvs
            .keys
            .iter()
            .map(|vector| vector.get(self.index))
            .collect();
        let row_key = RowKey {
            keys,
            sequence: self.kvs.sequence,
            index_in_batch: self.kvs.start_index_in_batch + self.index,
            value_type: self.kvs.value_type,
        };

        let row_value = RowValue {
            _values: self
                .kvs
                .values
                .iter()
                .map(|vector| vector.get(self.index))
                .collect(),
        };

        (row_key, row_value)
    }
}

impl<'a> Iterator for IterRow<'a> {
    type Item = (RowKey, RowValue);

    fn next(&mut self) -> Option<(RowKey, RowValue)> {
        if self.index >= self.len {
            return None;
        }

        Some(self.fetch_row())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.kvs.keys.len(), Some(self.kvs.keys.len()))
    }
}

// TODO(yingwen): Actually the version and timestamp may order desc.
#[derive(PartialEq, Eq)]
struct RowKey {
    keys: Vec<Value>,
    sequence: SequenceNumber,
    index_in_batch: usize,
    value_type: ValueType,
}

impl Ord for RowKey {
    fn cmp(&self, other: &RowKey) -> Ordering {
        // Order by (keys asc, sequence desc, index_in_batch desc, value type desc), though (key,
        // sequence, index_in_batch) should be enough to disambiguate.
        self.keys
            .cmp(&other.keys)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| other.index_in_batch.cmp(&self.index_in_batch))
            .then_with(|| other.value_type.cmp(&self.value_type))
    }
}

impl PartialOrd for RowKey {
    fn partial_cmp(&self, other: &RowKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct RowValue {
    _values: Vec<Value>,
}
