use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap};
use std::ops::Bound;
use std::sync::{
    atomic::{AtomicUsize, Ordering as AtomicOrdering},
    Arc, RwLock,
};

use datatypes::prelude::*;
use datatypes::value::Value;
use datatypes::vectors::{
    UInt64Vector, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder, VectorBuilder,
};
use store_api::storage::{OpType, SequenceNumber};

use crate::error::Result;
use crate::memtable::{
    BatchIterator, BoxedBatchIterator, IterContext, KeyValues, Memtable, MemtableId, RowOrdering,
};
use crate::read::Batch;
use crate::schema::RegionSchemaRef;

type RwLockMap = RwLock<BTreeMap<InnerKey, RowValue>>;

/// A simple memtable implementation based on std's [`BTreeMap`].
///
/// Mainly for test purpose, don't use in production.
#[derive(Debug)]
pub struct BTreeMemtable {
    id: MemtableId,
    schema: RegionSchemaRef,
    map: Arc<RwLockMap>,
    estimated_bytes: AtomicUsize,
}

impl BTreeMemtable {
    pub fn new(id: MemtableId, schema: RegionSchemaRef) -> BTreeMemtable {
        BTreeMemtable {
            id,
            schema,
            map: Arc::new(RwLock::new(BTreeMap::new())),
            estimated_bytes: AtomicUsize::new(0),
        }
    }
}

impl Memtable for BTreeMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn schema(&self) -> RegionSchemaRef {
        self.schema.clone()
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        self.estimated_bytes
            .fetch_add(kvs.estimated_memory_size(), AtomicOrdering::Relaxed);

        let mut map = self.map.write().unwrap();
        let iter_row = IterRow::new(kvs);
        for (inner_key, row_value) in iter_row {
            map.insert(inner_key, row_value);
        }

        Ok(())
    }

    fn iter(&self, ctx: IterContext) -> Result<BoxedBatchIterator> {
        assert!(ctx.batch_size > 0);

        let iter = BTreeIterator::new(ctx, self.schema.clone(), self.map.clone());

        Ok(Box::new(iter))
    }

    fn bytes_allocated(&self) -> usize {
        self.estimated_bytes.load(AtomicOrdering::Relaxed)
    }
}

struct BTreeIterator {
    ctx: IterContext,
    schema: RegionSchemaRef,
    map: Arc<RwLockMap>,
    last_key: Option<InnerKey>,
}

impl BatchIterator for BTreeIterator {
    fn schema(&self) -> RegionSchemaRef {
        self.schema.clone()
    }

    fn ordering(&self) -> RowOrdering {
        RowOrdering::Key
    }
}

impl Iterator for BTreeIterator {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        self.next_batch().map(Ok)
    }
}

impl BTreeIterator {
    fn new(ctx: IterContext, schema: RegionSchemaRef, map: Arc<RwLockMap>) -> BTreeIterator {
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

        let (keys, sequences, op_types, values) = if self.ctx.for_flush {
            collect_iter(iter, self.ctx.batch_size)
        } else {
            let iter = MapIterWrapper::new(iter, self.ctx.visible_sequence);
            collect_iter(iter, self.ctx.batch_size)
        };

        if keys.is_empty() {
            return None;
        }
        self.last_key = keys.last().map(|k| {
            let mut last_key = (*k).clone();
            last_key.reset_for_seek();
            last_key
        });

        let key_data_types = self
            .schema
            .row_key_columns()
            .map(|column_meta| column_meta.desc.data_type.clone());
        let value_data_types = self
            .schema
            .value_columns()
            .map(|column_meta| column_meta.desc.data_type.clone());

        Some(Batch {
            keys: rows_to_vectors(key_data_types, keys.as_slice()),
            sequences,
            op_types,
            values: rows_to_vectors(value_data_types, values.as_slice()),
        })
    }
}

fn collect_iter<'a, I: Iterator<Item = (&'a InnerKey, &'a RowValue)>>(
    iter: I,
    batch_size: usize,
) -> (
    Vec<&'a InnerKey>,
    UInt64Vector,
    UInt8Vector,
    Vec<&'a RowValue>,
) {
    let mut keys = Vec::with_capacity(batch_size);
    let mut sequences = UInt64VectorBuilder::with_capacity(batch_size);
    let mut op_types = UInt8VectorBuilder::with_capacity(batch_size);
    let mut values = Vec::with_capacity(batch_size);
    for (inner_key, row_value) in iter.take(batch_size) {
        keys.push(inner_key);
        sequences.push(Some(inner_key.sequence));
        op_types.push(Some(inner_key.op_type.as_u8()));
        values.push(row_value);
    }

    (keys, sequences.finish(), op_types.finish(), values)
}

/// `MapIterWrapper` removes same user key with invisible sequence.
struct MapIterWrapper<'a, InnerKey, RowValue> {
    iter: btree_map::Range<'a, InnerKey, RowValue>,
    prev_key: Option<InnerKey>,
    visible_sequence: SequenceNumber,
}

impl<'a> MapIterWrapper<'a, InnerKey, RowValue> {
    fn new(
        iter: btree_map::Range<'a, InnerKey, RowValue>,
        visible_sequence: SequenceNumber,
    ) -> MapIterWrapper<'a, InnerKey, RowValue> {
        MapIterWrapper {
            iter,
            prev_key: None,
            visible_sequence,
        }
    }

    fn next_visible_entry(&mut self) -> Option<(&'a InnerKey, &'a RowValue)> {
        for (k, v) in self.iter.by_ref() {
            if k.is_visible(self.visible_sequence) {
                return Some((k, v));
            }
        }

        None
    }
}

impl<'a> Iterator for MapIterWrapper<'a, InnerKey, RowValue> {
    type Item = (&'a InnerKey, &'a RowValue);

    fn next(&mut self) -> Option<(&'a InnerKey, &'a RowValue)> {
        let (mut current_key, mut current_value) = self.next_visible_entry()?;
        if self.prev_key.is_none() {
            self.prev_key = Some(current_key.clone());
            return Some((current_key, current_value));
        }

        let prev_key = self.prev_key.take().unwrap();
        while prev_key.is_row_key_equal(current_key) {
            if let Some((next_key, next_value)) = self.next_visible_entry() {
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
            op_type: self.kvs.op_type,
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
    op_type: OpType,
}

impl Ord for InnerKey {
    fn cmp(&self, other: &InnerKey) -> Ordering {
        // Order by (row_key asc, sequence desc, index_in_batch desc, op_type desc), though (key,
        // sequence, index_in_batch) should be enough to disambiguate.
        self.row_key
            .cmp(&other.row_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
            .then_with(|| other.index_in_batch.cmp(&self.index_in_batch))
            .then_with(|| other.op_type.cmp(&self.op_type))
    }
}

impl PartialOrd for InnerKey {
    fn partial_cmp(&self, other: &InnerKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl InnerKey {
    #[inline]
    fn is_row_key_equal(&self, other: &InnerKey) -> bool {
        self.row_key == other.row_key
    }

    #[inline]
    fn is_visible(&self, sequence: SequenceNumber) -> bool {
        self.sequence <= sequence
    }

    /// Reset the `InnerKey` so that we can use it to seek next key that
    /// has different row key.
    fn reset_for_seek(&mut self) {
        // sequence, index_in_batch, op_type are ordered in desc order, so
        // we can represent the last inner key with same row key by setting them
        // to zero (Minimum value).
        self.sequence = 0;
        self.index_in_batch = 0;
        self.op_type = OpType::min_type();
    }
}

#[derive(Clone, Debug)]
struct RowValue {
    values: Vec<Value>,
}

trait RowsProvider {
    fn row_num(&self) -> usize;

    fn column_num(&self) -> usize {
        self.row_by_index(0).len()
    }

    fn is_empty(&self) -> bool {
        self.row_num() == 0
    }

    fn row_by_index(&self, idx: usize) -> &Vec<Value>;
}

impl<'a> RowsProvider for &'a [&InnerKey] {
    fn row_num(&self) -> usize {
        self.len()
    }

    fn row_by_index(&self, idx: usize) -> &Vec<Value> {
        &self[idx].row_key
    }
}

impl<'a> RowsProvider for &'a [&RowValue] {
    fn row_num(&self) -> usize {
        self.len()
    }

    fn row_by_index(&self, idx: usize) -> &Vec<Value> {
        &self[idx].values
    }
}

fn rows_to_vectors<I: Iterator<Item = ConcreteDataType>, T: RowsProvider>(
    data_types: I,
    provider: T,
) -> Vec<VectorRef> {
    if provider.is_empty() {
        return Vec::new();
    }

    let column_num = provider.column_num();
    let row_num = provider.row_num();
    let mut builders = Vec::with_capacity(column_num);
    for data_type in data_types {
        builders.push(VectorBuilder::with_capacity(data_type, row_num));
    }

    let mut vectors = Vec::with_capacity(column_num);
    for (col_idx, builder) in builders.iter_mut().enumerate() {
        for row_idx in 0..row_num {
            let row = provider.row_by_index(row_idx);
            let value = &row[col_idx];
            builder.push(value);
        }

        vectors.push(builder.finish());
    }

    vectors
}
