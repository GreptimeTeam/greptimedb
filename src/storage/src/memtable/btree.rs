use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap};
use std::ops::Bound;
use std::sync::{Arc, RwLock};

use datatypes::prelude::*;
use datatypes::value::Value;
use datatypes::vectors::{UInt64VectorBuilder, UInt8VectorBuilder, VectorBuilder};
use store_api::storage::{SequenceNumber, ValueType};

use crate::error::Result;
use crate::memtable::{
    Batch, BatchIterator, BatchIteratorPtr, IterContext, KeyValues, Memtable, MemtableSchema,
    RowOrdering,
};

type RwLockMap = RwLock<BTreeMap<InnerKey, RowValue>>;

/// A simple memtable implementation based on std's [`BTreeMap`].
///
/// Mainly for test purpose, don't use in production.
pub struct BTreeMemtable {
    schema: MemtableSchema,
    map: Arc<RwLockMap>,
}

impl BTreeMemtable {
    pub fn new(schema: MemtableSchema) -> BTreeMemtable {
        BTreeMemtable {
            schema,
            map: Arc::new(RwLock::new(BTreeMap::new())),
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
        for (inner_key, row_value) in iter_row {
            map.insert(inner_key, row_value);
        }

        Ok(())
    }

    fn iter(&self, ctx: IterContext) -> Result<BatchIteratorPtr> {
        assert!(ctx.batch_size > 0);

        let iter = BTreeIterator::new(ctx, self.schema.clone(), self.map.clone());

        Ok(Box::new(iter))
    }

    fn bytes_allocated(&self) -> usize {
        unimplemented!()
    }
}

struct BTreeIterator {
    ctx: IterContext,
    schema: MemtableSchema,
    map: Arc<RwLockMap>,
    last_key: Option<InnerKey>,
}

impl BatchIterator for BTreeIterator {
    fn schema(&self) -> &MemtableSchema {
        &self.schema
    }

    fn ordering(&self) -> RowOrdering {
        RowOrdering::Key
    }

    fn next(&mut self) -> Result<Option<Batch>> {
        Ok(self.next_batch())
    }
}

impl BTreeIterator {
    fn new(ctx: IterContext, schema: MemtableSchema, map: Arc<RwLockMap>) -> BTreeIterator {
        BTreeIterator {
            ctx,
            schema,
            map,
            last_key: None,
        }
    }

    fn next_batch(&mut self) -> Option<Batch> {
        let map = self.map.read().unwrap();
        let iter = if let Some(last_key) = &self.last_key {
            map.range((Bound::Excluded(last_key), Bound::Unbounded))
        } else {
            map.range(..)
        };
        let iter = MapIterWrapper::new(iter);

        let mut keys = Vec::with_capacity(self.ctx.batch_size);
        let mut sequences = UInt64VectorBuilder::with_capacity(self.ctx.batch_size);
        let mut value_types = UInt8VectorBuilder::with_capacity(self.ctx.batch_size);
        let mut values = Vec::with_capacity(self.ctx.batch_size);
        for (inner_key, row_value) in iter.take(self.ctx.batch_size) {
            keys.push(inner_key);
            sequences.push(Some(inner_key.sequence));
            value_types.push(Some(inner_key.value_type.as_u8()));
            values.push(row_value);
        }

        if keys.is_empty() {
            return None;
        }
        self.last_key = keys.last().map(|k| (*k).clone());

        Some(Batch {
            keys: Self::keys_to_vectors(&keys),
            sequences: sequences.finish(),
            value_types: value_types.finish(),
            values: Self::values_to_vectors(&values),
        })
    }

    // Assumes column num of all row key is equal.
    fn keys_to_vectors(keys: &[&InnerKey]) -> Vec<VectorRef> {
        if keys.is_empty() {
            return Vec::new();
        }

        let column_num = keys[0].row_key.len();
        let row_num = keys.len();
        let mut builders = Vec::with_capacity(column_num);
        for v in &keys[0].row_key {
            builders.push(VectorBuilder::with_capacity(v.data_type(), row_num));
        }

        let mut vectors = Vec::with_capacity(column_num);
        for (col_idx, builder) in builders.iter_mut().enumerate() {
            for row_key in keys {
                let value = &row_key.row_key[col_idx];
                builder.push(value);
            }

            vectors.push(builder.finish());
        }

        vectors
    }

    // Assumes column num of all row value is equal.
    fn values_to_vectors(values: &[&RowValue]) -> Vec<VectorRef> {
        if values.is_empty() {
            return Vec::new();
        }

        let column_num = values[0].values.len();
        let row_num = values.len();
        let mut builders = Vec::with_capacity(column_num);
        for v in &values[0].values {
            builders.push(VectorBuilder::with_capacity(v.data_type(), row_num));
        }

        let mut vectors = Vec::with_capacity(column_num);
        for (col_idx, builder) in builders.iter_mut().enumerate() {
            for row_value in values {
                let value = &row_value.values[col_idx];
                builder.push(value);
            }

            vectors.push(builder.finish());
        }

        vectors
    }
}

/// `MapIterWrapper` removes same user key with elder sequence.
struct MapIterWrapper<'a, InnerKey, RowValue> {
    iter: btree_map::Range<'a, InnerKey, RowValue>,
    prev_key: Option<InnerKey>,
}

impl<'a> MapIterWrapper<'a, InnerKey, RowValue> {
    fn new(
        iter: btree_map::Range<'a, InnerKey, RowValue>,
    ) -> MapIterWrapper<'a, InnerKey, RowValue> {
        MapIterWrapper {
            iter,
            prev_key: None,
        }
    }
}

impl<'a> Iterator for MapIterWrapper<'a, InnerKey, RowValue> {
    type Item = (&'a InnerKey, &'a RowValue);

    fn next(&mut self) -> Option<(&'a InnerKey, &'a RowValue)> {
        let (mut current_key, mut current_value) = self.iter.next()?;
        if self.prev_key.is_none() {
            self.prev_key = Some(current_key.clone());
            return Some((current_key, current_value));
        }

        let prev_key = self.prev_key.take().unwrap();
        while prev_key.is_row_key_equal(current_key) {
            if let Some((next_key, next_value)) = self.iter.next() {
                (current_key, current_value) = (next_key, next_value);
            } else {
                return None;
            }
        }

        self.prev_key = Some(current_key.clone());

        Some((current_key, current_value))
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

    fn fetch_row(&mut self) -> (InnerKey, RowValue) {
        let row_key = self
            .kvs
            .keys
            .iter()
            .map(|vector| vector.get(self.index))
            .collect();
        let inner_key = InnerKey {
            row_key,
            sequence: self.kvs.sequence,
            index_in_batch: self.kvs.start_index_in_batch + self.index,
            value_type: self.kvs.value_type,
        };

        let row_value = RowValue {
            values: self
                .kvs
                .values
                .iter()
                .map(|vector| vector.get(self.index))
                .collect(),
        };

        self.index += 1;

        (inner_key, row_value)
    }
}

impl<'a> Iterator for IterRow<'a> {
    type Item = (InnerKey, RowValue);

    fn next(&mut self) -> Option<(InnerKey, RowValue)> {
        if self.index >= self.len {
            return None;
        }

        Some(self.fetch_row())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.kvs.keys.len(), Some(self.kvs.keys.len()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct InnerKey {
    row_key: Vec<Value>,
    sequence: SequenceNumber,
    index_in_batch: usize,
    value_type: ValueType,
}

impl Ord for InnerKey {
    fn cmp(&self, other: &InnerKey) -> Ordering {
        // Order by (row_key asc, sequence desc, index_in_batch desc, value type desc), though (key,
        // sequence, index_in_batch) should be enough to disambiguate.
        self.row_key
            .cmp(&other.row_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| other.index_in_batch.cmp(&self.index_in_batch))
            .then_with(|| other.value_type.cmp(&self.value_type))
    }
}

impl PartialOrd for InnerKey {
    fn partial_cmp(&self, other: &InnerKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl InnerKey {
    fn is_row_key_equal(&self, other: &InnerKey) -> bool {
        self.row_key == other.row_key
    }
}

#[derive(Clone, Debug)]
struct RowValue {
    values: Vec<Value>,
}
