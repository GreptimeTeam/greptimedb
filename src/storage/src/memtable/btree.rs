use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::RwLock;

use datatypes::value::Value;
use store_api::storage::{SequenceNumber, ValueType};

use crate::error::Result;
use crate::memtable::{KeyValues, MemTable, MemTableSchema};

/// A simple memtable implementation based on std's [`BTreeMap`].
///
/// Mainly for test purpose.
pub struct BTreeMemTable {
    schema: MemTableSchema,
    map: RwLock<BTreeMap<RowKey, RowValue>>,
}

impl BTreeMemTable {
    pub fn new(schema: MemTableSchema) -> BTreeMemTable {
        BTreeMemTable {
            schema,
            map: RwLock::new(BTreeMap::new()),
        }
    }
}

impl MemTable for BTreeMemTable {
    fn schema(&self) -> &MemTableSchema {
        &self.schema
    }

    fn write(&self, key_values: &KeyValues) -> Result<()> {
        let mut map = self.map.write().unwrap();

        let iter_row = IterRow::new(key_values);
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
    key_values: &'a KeyValues,
    index: usize,
    len: usize,
}

impl<'a> IterRow<'a> {
    fn new(key_values: &KeyValues) -> IterRow {
        IterRow {
            key_values,
            index: 0,
            len: key_values.len(),
        }
    }

    fn fetch_row(&mut self) -> (RowKey, RowValue) {
        let keys = self
            .key_values
            .keys
            .iter()
            .map(|vector| vector.get_value(self.index))
            .collect();
        let row_key = RowKey {
            keys,
            sequence: self.key_values.sequence,
            index_in_batch: self.key_values.start_index_in_batch + self.index,
            value_type: self.key_values.value_type,
        };

        let row_value = RowValue {
            _values: self
                .key_values
                .values
                .iter()
                .map(|vector| vector.get_value(self.index))
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
