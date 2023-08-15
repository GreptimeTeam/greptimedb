// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::{btree_map, BTreeMap};
use std::fmt;
use std::ops::Bound;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};

use api::v1::OpType;
use common_time::range::TimestampRange;
use datatypes::data_type::DataType;
use datatypes::prelude::*;
use datatypes::value::Value;
use datatypes::vectors::{UInt64Vector, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder};
use store_api::storage::{SequenceNumber, MIN_OP_TYPE};

use crate::error::Result;
use crate::flush::FlushStrategyRef;
use crate::memtable::{
    AllocTracker, BatchIterator, BoxedBatchIterator, IterContext, KeyValues, Memtable, MemtableId,
    MemtableStats, RowOrdering,
};
use crate::read::Batch;
use crate::schema::compat::ReadAdapter;
use crate::schema::{ProjectedSchema, ProjectedSchemaRef, RegionSchemaRef};

type RwLockMap = RwLock<BTreeMap<InnerKey, RowValue>>;

/// A simple memtable implementation based on std's [`BTreeMap`].
///
/// Mainly for test purpose, don't use in production.
pub struct BTreeMemtable {
    id: MemtableId,
    schema: RegionSchemaRef,
    map: Arc<RwLockMap>,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
}

impl BTreeMemtable {
    pub fn new(
        id: MemtableId,
        schema: RegionSchemaRef,
        flush_strategy: Option<FlushStrategyRef>,
    ) -> BTreeMemtable {
        BTreeMemtable {
            id,
            schema,
            map: Arc::new(RwLock::new(BTreeMap::new())),
            alloc_tracker: AllocTracker::new(flush_strategy),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
        }
    }

    /// Updates memtable stats.
    /// This function is guarded by `BTreeMemtable::map` so that store-after-load is safe.
    fn update_stats(&self, request_size: usize, min: Option<Value>, max: Option<Value>) {
        self.alloc_tracker.on_allocate(request_size);

        if let Some(min) = min {
            let min_val = min
                .as_timestamp()
                .expect("Min timestamp must be a valid timestamp value")
                .value();
            let cur_min = self.min_timestamp.load(AtomicOrdering::Relaxed);
            if min_val < cur_min {
                self.min_timestamp.store(min_val, AtomicOrdering::Relaxed);
            }
        }

        if let Some(max) = max {
            let cur_max = self.max_timestamp.load(AtomicOrdering::Relaxed);
            let max_val = max
                .as_timestamp()
                .expect("Max timestamp must be a valid timestamp value")
                .value();
            if max_val > cur_max {
                self.max_timestamp.store(max_val, AtomicOrdering::Relaxed);
            }
        }
    }
}

impl fmt::Debug for BTreeMemtable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let len = self.map.read().unwrap().len();

        f.debug_struct("BTreeMemtable")
            .field("id", &self.id)
            // Only show StoreSchema
            .field("schema", &self.schema)
            .field("rows", &len)
            .field("alloc_tracker", &self.alloc_tracker)
            .field("max_timestamp", &self.max_timestamp)
            .field("min_timestamp", &self.min_timestamp)
            .finish()
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
        debug_assert!(kvs.timestamp.is_some());
        let iter_row = IterRow::new(kvs);
        let mut map = self.map.write().unwrap();

        let mut min_ts = None;
        let mut max_ts = None;
        for (inner_key, row_value) in iter_row {
            let ts = inner_key.timestamp();
            let min_ts = min_ts.get_or_insert_with(|| ts.clone());
            let max_ts = max_ts.get_or_insert_with(|| ts.clone());
            if ts < min_ts {
                *min_ts = ts.clone();
            }
            if ts > max_ts {
                *max_ts = ts.clone();
            }
            let _ = map.insert(inner_key, row_value);
        }

        self.update_stats(kvs.estimated_memory_size(), min_ts, max_ts);

        Ok(())
    }

    fn iter(&self, ctx: IterContext) -> Result<BoxedBatchIterator> {
        assert!(ctx.batch_size > 0);

        let iter = BTreeIterator::new(ctx, self.schema.clone(), self.map.clone())?;

        Ok(Box::new(iter))
    }

    fn num_rows(&self) -> usize {
        self.map.read().unwrap().len()
    }

    fn stats(&self) -> MemtableStats {
        let ts_meta = self.schema.column_metadata(self.schema.timestamp_index());

        let Some(timestamp_type) = ts_meta.desc.data_type.as_timestamp() else {
            // safety: timestamp column always has timestamp type, otherwise it's a bug.
            panic!(
                "Timestamp column is not a valid timestamp type: {:?}",
                self.schema
            );
        };

        MemtableStats {
            estimated_bytes: self.alloc_tracker.bytes_allocated(),
            max_timestamp: timestamp_type
                .create_timestamp(self.max_timestamp.load(AtomicOrdering::Relaxed)),
            min_timestamp: timestamp_type
                .create_timestamp(self.min_timestamp.load(AtomicOrdering::Relaxed)),
        }
    }

    fn mark_immutable(&self) {
        self.alloc_tracker.done_allocating();
    }
}

struct BTreeIterator {
    ctx: IterContext,
    /// Schema of this memtable.
    schema: RegionSchemaRef,
    /// Projected schema that user expect to read.
    projected_schema: ProjectedSchemaRef,
    adapter: ReadAdapter,
    map: Arc<RwLockMap>,
    last_key: Option<InnerKey>,
}

impl BatchIterator for BTreeIterator {
    fn schema(&self) -> ProjectedSchemaRef {
        self.projected_schema.clone()
    }

    fn ordering(&self) -> RowOrdering {
        RowOrdering::Key
    }
}

impl Iterator for BTreeIterator {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        self.next_batch().transpose()
    }
}

impl BTreeIterator {
    fn new(
        ctx: IterContext,
        schema: RegionSchemaRef,
        map: Arc<RwLockMap>,
    ) -> Result<BTreeIterator> {
        let projected_schema = ctx
            .projected_schema
            .clone()
            .unwrap_or_else(|| Arc::new(ProjectedSchema::no_projection(schema.clone())));
        let adapter = ReadAdapter::new(schema.store_schema().clone(), projected_schema.clone())?;

        Ok(BTreeIterator {
            ctx,
            schema,
            projected_schema,
            adapter,
            map,
            last_key: None,
        })
    }

    fn next_batch(&mut self) -> Result<Option<Batch>> {
        let map = self.map.read().unwrap();
        let iter = if let Some(last_key) = &self.last_key {
            map.range((Bound::Excluded(last_key), Bound::Unbounded))
        } else {
            map.range(..)
        };

        let iter = MapIterWrapper::new(iter, self.ctx.visible_sequence, self.ctx.time_range);
        let (keys, sequences, op_types, values) = collect_iter(iter, self.ctx.batch_size);

        if keys.is_empty() {
            return Ok(None);
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
            .field_columns()
            .map(|column_meta| column_meta.desc.data_type.clone());

        let key_columns = rows_to_vectors(
            key_data_types,
            self.adapter.source_key_needed(),
            keys.as_slice(),
        );
        let field_columns = rows_to_vectors(
            value_data_types,
            self.adapter.source_value_needed(),
            values.as_slice(),
        );

        let batch = self.adapter.batch_from_parts(
            key_columns,
            field_columns,
            Arc::new(sequences),
            Arc::new(op_types),
        )?;

        Ok(Some(batch))
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
        op_types.push(Some(inner_key.op_type as u8));
        values.push(row_value);
    }

    (keys, sequences.finish(), op_types.finish(), values)
}

/// `MapIterWrapper` removes same user key with invisible sequence.
struct MapIterWrapper<'a, InnerKey, RowValue> {
    iter: btree_map::Range<'a, InnerKey, RowValue>,
    prev_key: Option<InnerKey>,
    visible_sequence: SequenceNumber,
    time_range: Option<TimestampRange>,
}

impl<'a> MapIterWrapper<'a, InnerKey, RowValue> {
    fn new(
        iter: btree_map::Range<'a, InnerKey, RowValue>,
        visible_sequence: SequenceNumber,
        time_range: Option<TimestampRange>,
    ) -> MapIterWrapper<'a, InnerKey, RowValue> {
        MapIterWrapper {
            iter,
            prev_key: None,
            visible_sequence,
            time_range,
        }
    }

    fn next_visible_entry(&mut self) -> Option<(&'a InnerKey, &'a RowValue)> {
        for (k, v) in self.iter.by_ref() {
            if k.is_visible(self.visible_sequence) && k.is_in_time_range(&self.time_range) {
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
        let mut row_key: Vec<_> = self
            .kvs
            .keys
            .iter()
            .map(|vector| vector.get(self.index))
            .collect();

        // unwrap safety: KeyValues always contains a timestamp as guaranteed in [Inserter::write_one_mutation]
        row_key.push(self.kvs.timestamp.as_ref().unwrap().get(self.index));
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
    /// User defined primary keys
    row_key: Vec<Value>,
    /// Sequence number of row
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
    fn timestamp(&self) -> &Value {
        // safety: row key shall at least contain a timestamp column
        self.row_key.last().unwrap()
    }

    #[inline]
    fn is_row_key_equal(&self, other: &InnerKey) -> bool {
        self.row_key == other.row_key
    }

    #[inline]
    fn is_visible(&self, sequence: SequenceNumber) -> bool {
        self.sequence <= sequence
    }

    #[inline]
    fn is_in_time_range(&self, range: &Option<TimestampRange>) -> bool {
        let Some(range) = range else {
            return true;
        };
        range.contains(
            &self
                .timestamp()
                .as_timestamp()
                .expect("Timestamp field must be a valid timestamp value"),
        )
    }

    /// Reset the `InnerKey` so that we can use it to seek next key that
    /// has different row key.
    fn reset_for_seek(&mut self) {
        // sequence, index_in_batch, op_type are ordered in desc order, so
        // we can represent the last inner key with same row key by setting them
        // to zero (Minimum value).
        self.sequence = 0;
        self.index_in_batch = 0;
        self.op_type = MIN_OP_TYPE;
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
    column_needed: &[bool],
    provider: T,
) -> Vec<VectorRef> {
    if provider.is_empty() {
        return Vec::new();
    }

    let column_num = provider.column_num();
    let row_num = provider.row_num();
    let mut builders = Vec::with_capacity(column_num);
    for data_type in data_types {
        builders.push(data_type.create_mutable_vector(row_num));
    }

    let mut vectors = Vec::with_capacity(column_num);
    for (col_idx, builder) in builders.iter_mut().enumerate() {
        if !column_needed[col_idx] {
            continue;
        }

        for row_idx in 0..row_num {
            let row = provider.row_by_index(row_idx);
            let value = &row[col_idx];
            builder.as_mut().push_value_ref(value.as_value_ref());
        }

        vectors.push(builder.to_vector());
    }

    vectors
}
