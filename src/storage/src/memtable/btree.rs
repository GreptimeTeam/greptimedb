use std::collections::BTreeMap;

use datatypes::value::Value;
use store_api::storage::{SequenceNumber, ValueType};

use crate::error::Result;
use crate::memtable::{KeyValues, MemTable, MemTableSchema};

// TODO(yingwen): Actually the version and timestamp may order desc.
struct RowKey {
    keys: Vec<Value>,
    sequence: SequenceNumber,
    value_type: ValueType,
    index_in_batch: usize,
}

struct RowValue(Vec<Value>);

/// A simple memtable implementation based on std's [`BTreeMap`].
///
/// Mainly for test purpose.
pub struct BTreeMemTable {
    schema: MemTableSchema,
    map: BTreeMap<RowKey, RowValue>,
}

impl BTreeMemTable {
    pub fn new(schema: MemTableSchema) -> BTreeMemTable {
        BTreeMemTable {
            schema,
            map: BTreeMap::new(),
        }
    }
}

impl MemTable for BTreeMemTable {
    fn schema(&self) -> &MemTableSchema {
        &self.schema
    }

    fn write(&self, key_values: &KeyValues) -> Result<()> {
        //
        unimplemented!()
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
        //

        unimplemented!()
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
