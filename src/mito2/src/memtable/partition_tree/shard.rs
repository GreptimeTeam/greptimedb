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

//! Shard in a partition.

use std::cmp::Ordering;
use std::time::{Duration, Instant};

use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::partition_tree::data::{
    DataBatch, DataParts, DataPartsReader, DataPartsReaderBuilder, DATA_INIT_CAP,
};
use crate::memtable::partition_tree::dict::KeyDictRef;
use crate::memtable::partition_tree::merger::{Merger, Node};
use crate::memtable::partition_tree::partition::PrimaryKeyFilter;
use crate::memtable::partition_tree::shard_builder::ShardBuilderReader;
use crate::memtable::partition_tree::{PkId, PkIndex, ShardId};
use crate::metrics::PARTITION_TREE_READ_STAGE_ELAPSED;

/// Shard stores data related to the same key dictionary.
pub struct Shard {
    pub(crate) shard_id: ShardId,
    /// Key dictionary of the shard. `None` if the schema of the tree doesn't have a primary key.
    key_dict: Option<KeyDictRef>,
    /// Data in the shard.
    data_parts: DataParts,
    dedup: bool,
    /// Number of rows to freeze a data part.
    data_freeze_threshold: usize,
}

impl Shard {
    /// Returns a new shard.
    pub fn new(
        shard_id: ShardId,
        key_dict: Option<KeyDictRef>,
        data_parts: DataParts,
        dedup: bool,
        data_freeze_threshold: usize,
    ) -> Shard {
        Shard {
            shard_id,
            key_dict,
            data_parts,
            dedup,
            data_freeze_threshold,
        }
    }

    /// Writes a key value into the shard.
    ///
    /// It will freezes the active buffer if it is full.
    pub fn write_with_pk_id(&mut self, pk_id: PkId, key_value: &KeyValue) -> Result<()> {
        debug_assert_eq!(self.shard_id, pk_id.shard_id);

        if self.data_parts.num_active_rows() >= self.data_freeze_threshold {
            self.data_parts.freeze()?;
        }

        self.data_parts.write_row(pk_id.pk_index, key_value);
        Ok(())
    }

    /// Scans the shard.
    // TODO(yingwen): Push down projection to data parts.
    pub fn read(&self) -> Result<ShardReaderBuilder> {
        let parts_reader = self.data_parts.read()?;

        Ok(ShardReaderBuilder {
            shard_id: self.shard_id,
            key_dict: self.key_dict.clone(),
            inner: parts_reader,
        })
    }

    /// Forks a shard.
    pub fn fork(&self, metadata: RegionMetadataRef) -> Shard {
        Shard {
            shard_id: self.shard_id,
            key_dict: self.key_dict.clone(),
            data_parts: DataParts::new(metadata, DATA_INIT_CAP, self.dedup),
            dedup: self.dedup,
            data_freeze_threshold: self.data_freeze_threshold,
        }
    }

    /// Returns true if the shard is empty (No data).
    pub fn is_empty(&self) -> bool {
        self.data_parts.is_empty()
    }

    /// Returns the memory size of the shard part.
    pub(crate) fn shared_memory_size(&self) -> usize {
        self.key_dict
            .as_ref()
            .map(|dict| dict.shared_memory_size())
            .unwrap_or(0)
    }
}

/// Source that returns [DataBatch].
pub trait DataBatchSource {
    /// Returns whether current source is still valid.
    fn is_valid(&self) -> bool;

    /// Advances source to next data batch.
    fn next(&mut self) -> Result<()>;

    /// Returns current pk id.
    /// # Panics
    /// If source is not valid.
    fn current_pk_id(&self) -> PkId;

    /// Returns the current primary key bytes or None if it doesn't have primary key.
    ///
    /// # Panics
    /// If source is not valid.
    fn current_key(&self) -> Option<&[u8]>;

    /// Returns the data part.
    /// # Panics
    /// If source is not valid.
    fn current_data_batch(&self) -> DataBatch;
}

pub type BoxedDataBatchSource = Box<dyn DataBatchSource + Send>;

pub struct ShardReaderBuilder {
    shard_id: ShardId,
    key_dict: Option<KeyDictRef>,
    inner: DataPartsReaderBuilder,
}

impl ShardReaderBuilder {
    pub(crate) fn build(self, key_filter: Option<PrimaryKeyFilter>) -> Result<ShardReader> {
        let ShardReaderBuilder {
            shard_id,
            key_dict,
            inner,
        } = self;
        let now = Instant::now();
        let parts_reader = inner.build()?;
        ShardReader::new(shard_id, key_dict, parts_reader, key_filter, now.elapsed())
    }
}

/// Reader to read rows in a shard.
pub struct ShardReader {
    shard_id: ShardId,
    key_dict: Option<KeyDictRef>,
    parts_reader: DataPartsReader,
    key_filter: Option<PrimaryKeyFilter>,
    last_yield_pk_index: Option<PkIndex>,
    keys_before_pruning: usize,
    keys_after_pruning: usize,
    prune_pk_cost: Duration,
    data_build_cost: Duration,
}

impl ShardReader {
    fn new(
        shard_id: ShardId,
        key_dict: Option<KeyDictRef>,
        parts_reader: DataPartsReader,
        key_filter: Option<PrimaryKeyFilter>,
        data_build_cost: Duration,
    ) -> Result<Self> {
        let has_pk = key_dict.is_some();
        let mut reader = Self {
            shard_id,
            key_dict,
            parts_reader,
            key_filter: if has_pk { key_filter } else { None },
            last_yield_pk_index: None,
            keys_before_pruning: 0,
            keys_after_pruning: 0,
            prune_pk_cost: Duration::default(),
            data_build_cost,
        };
        reader.prune_batch_by_key()?;

        Ok(reader)
    }

    fn is_valid(&self) -> bool {
        self.parts_reader.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.parts_reader.next()?;
        self.prune_batch_by_key()
    }

    fn current_key(&self) -> Option<&[u8]> {
        let pk_index = self.parts_reader.current_data_batch().pk_index();
        self.key_dict
            .as_ref()
            .map(|dict| dict.key_by_pk_index(pk_index))
    }

    fn current_pk_id(&self) -> PkId {
        let pk_index = self.parts_reader.current_data_batch().pk_index();
        PkId {
            shard_id: self.shard_id,
            pk_index,
        }
    }

    fn current_data_batch(&self) -> DataBatch {
        self.parts_reader.current_data_batch()
    }

    fn prune_batch_by_key(&mut self) -> Result<()> {
        let Some(key_filter) = &mut self.key_filter else {
            return Ok(());
        };

        while self.parts_reader.is_valid() {
            let pk_index = self.parts_reader.current_data_batch().pk_index();
            if let Some(yield_pk_index) = self.last_yield_pk_index {
                if pk_index == yield_pk_index {
                    break;
                }
            }
            self.keys_before_pruning += 1;
            // Safety: `key_filter` is some so the shard has primary keys.
            let key = self.key_dict.as_ref().unwrap().key_by_pk_index(pk_index);
            let now = Instant::now();
            if key_filter.prune_primary_key(key) {
                self.prune_pk_cost += now.elapsed();
                self.last_yield_pk_index = Some(pk_index);
                self.keys_after_pruning += 1;
                break;
            }
            self.prune_pk_cost += now.elapsed();
            self.parts_reader.next()?;
        }

        Ok(())
    }
}

impl Drop for ShardReader {
    fn drop(&mut self) {
        let shard_prune_pk = self.prune_pk_cost.as_secs_f64();
        PARTITION_TREE_READ_STAGE_ELAPSED
            .with_label_values(&["shard_prune_pk"])
            .observe(shard_prune_pk);
        if self.keys_before_pruning > 0 {
            common_telemetry::debug!(
                "ShardReader metrics, data parts: {}, before pruning: {}, after pruning: {}, prune cost: {}s, build cost: {}s",
                self.parts_reader.num_parts(),
                self.keys_before_pruning,
                self.keys_after_pruning,
                shard_prune_pk,
                self.data_build_cost.as_secs_f64(),
            );
        }
    }
}

/// A merger that merges batches from multiple shards.
pub(crate) struct ShardMerger {
    merger: Merger<ShardNode>,
}

impl ShardMerger {
    pub(crate) fn try_new(nodes: Vec<ShardNode>) -> Result<Self> {
        let merger = Merger::try_new(nodes)?;
        Ok(ShardMerger { merger })
    }
}

impl DataBatchSource for ShardMerger {
    fn is_valid(&self) -> bool {
        self.merger.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.merger.next()
    }

    fn current_pk_id(&self) -> PkId {
        self.merger.current_node().current_pk_id()
    }

    fn current_key(&self) -> Option<&[u8]> {
        self.merger.current_node().current_key()
    }

    fn current_data_batch(&self) -> DataBatch {
        let batch = self.merger.current_node().current_data_batch();
        batch.slice(0, self.merger.current_rows())
    }
}

pub(crate) enum ShardSource {
    Builder(ShardBuilderReader),
    Shard(ShardReader),
}

impl ShardSource {
    fn is_valid(&self) -> bool {
        match self {
            ShardSource::Builder(r) => r.is_valid(),
            ShardSource::Shard(r) => r.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            ShardSource::Builder(r) => r.next(),
            ShardSource::Shard(r) => r.next(),
        }
    }

    fn current_pk_id(&self) -> PkId {
        match self {
            ShardSource::Builder(r) => r.current_pk_id(),
            ShardSource::Shard(r) => r.current_pk_id(),
        }
    }

    fn current_key(&self) -> Option<&[u8]> {
        match self {
            ShardSource::Builder(r) => r.current_key(),
            ShardSource::Shard(r) => r.current_key(),
        }
    }

    fn current_data_batch(&self) -> DataBatch {
        match self {
            ShardSource::Builder(r) => r.current_data_batch(),
            ShardSource::Shard(r) => r.current_data_batch(),
        }
    }
}

/// Node for the merger to get items.
pub(crate) struct ShardNode {
    source: ShardSource,
}

impl ShardNode {
    pub(crate) fn new(source: ShardSource) -> Self {
        Self { source }
    }

    fn current_pk_id(&self) -> PkId {
        self.source.current_pk_id()
    }

    fn current_key(&self) -> Option<&[u8]> {
        self.source.current_key()
    }

    fn current_data_batch(&self) -> DataBatch {
        self.source.current_data_batch()
    }
}

impl PartialEq for ShardNode {
    fn eq(&self, other: &Self) -> bool {
        self.source.current_key() == other.source.current_key()
    }
}

impl Eq for ShardNode {}

impl Ord for ShardNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source
            .current_key()
            .cmp(&other.source.current_key())
            .reverse()
    }
}

impl PartialOrd for ShardNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Node for ShardNode {
    fn is_valid(&self) -> bool {
        self.source.is_valid()
    }

    fn is_behind(&self, other: &Self) -> bool {
        // We expect a key only belongs to one shard.
        debug_assert_ne!(self.source.current_key(), other.source.current_key());
        self.source.current_key() < other.source.current_key()
    }

    fn advance(&mut self, len: usize) -> Result<()> {
        debug_assert_eq!(self.source.current_data_batch().num_rows(), len);
        self.source.next()
    }

    fn current_item_len(&self) -> usize {
        self.current_data_batch().num_rows()
    }

    fn search_key_in_current_item(&self, _other: &Self) -> Result<usize, usize> {
        Err(self.source.current_data_batch().num_rows())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::memtable::partition_tree::data::timestamp_array_to_i64_slice;
    use crate::memtable::partition_tree::dict::KeyDictBuilder;
    use crate::memtable::partition_tree::PkIndex;
    use crate::memtable::stats::WriteMetrics;
    use crate::memtable::KeyValues;
    use crate::test_util::memtable_util::{
        build_key_values_with_ts_seq_values, encode_keys, metadata_for_test,
    };

    /// Returns key values and expect pk index.
    fn input_with_key(metadata: &RegionMetadataRef) -> Vec<(KeyValues, PkIndex)> {
        vec![
            (
                build_key_values_with_ts_seq_values(
                    metadata,
                    "shard".to_string(),
                    2,
                    [20, 21].into_iter(),
                    [Some(0.0), Some(1.0)].into_iter(),
                    0,
                ),
                2,
            ),
            (
                build_key_values_with_ts_seq_values(
                    metadata,
                    "shard".to_string(),
                    0,
                    [0, 1].into_iter(),
                    [Some(0.0), Some(1.0)].into_iter(),
                    1,
                ),
                0,
            ),
            (
                build_key_values_with_ts_seq_values(
                    metadata,
                    "shard".to_string(),
                    1,
                    [10, 11].into_iter(),
                    [Some(0.0), Some(1.0)].into_iter(),
                    2,
                ),
                1,
            ),
        ]
    }

    fn new_shard_with_dict(
        shard_id: ShardId,
        metadata: RegionMetadataRef,
        input: &[(KeyValues, PkIndex)],
        data_freeze_threshold: usize,
    ) -> Shard {
        let mut dict_builder = KeyDictBuilder::new(1024);
        let mut metrics = WriteMetrics::default();
        let mut keys = Vec::with_capacity(input.len());
        for (kvs, _) in input {
            encode_keys(&metadata, kvs, &mut keys);
        }
        for key in &keys {
            dict_builder.insert_key(key, None, &mut metrics);
        }

        let (dict, _) = dict_builder.finish().unwrap();
        let data_parts = DataParts::new(metadata, DATA_INIT_CAP, true);

        Shard::new(
            shard_id,
            Some(Arc::new(dict)),
            data_parts,
            true,
            data_freeze_threshold,
        )
    }

    fn collect_timestamps(shard: &Shard) -> Vec<i64> {
        let mut reader = shard.read().unwrap().build(None).unwrap();
        let mut timestamps = Vec::new();
        while reader.is_valid() {
            let rb = reader.current_data_batch().slice_record_batch();
            let ts_array = rb.column(1);
            let ts_slice = timestamp_array_to_i64_slice(ts_array);
            timestamps.extend_from_slice(ts_slice);

            reader.next().unwrap();
        }
        timestamps
    }

    #[test]
    fn test_write_read_shard() {
        let metadata = metadata_for_test();
        let input = input_with_key(&metadata);
        let mut shard = new_shard_with_dict(8, metadata, &input, 100);
        assert!(shard.is_empty());
        for (key_values, pk_index) in &input {
            for kv in key_values.iter() {
                let pk_id = PkId {
                    shard_id: shard.shard_id,
                    pk_index: *pk_index,
                };
                shard.write_with_pk_id(pk_id, &kv).unwrap();
            }
        }
        assert!(!shard.is_empty());

        let timestamps = collect_timestamps(&shard);
        assert_eq!(vec![0, 1, 10, 11, 20, 21], timestamps);
    }

    #[test]
    fn test_shard_freeze() {
        let metadata = metadata_for_test();
        let kvs = build_key_values_with_ts_seq_values(
            &metadata,
            "shard".to_string(),
            0,
            [0].into_iter(),
            [Some(0.0)].into_iter(),
            0,
        );
        let mut shard = new_shard_with_dict(8, metadata.clone(), &[(kvs, 0)], 50);
        let expected: Vec<_> = (0..200).collect();
        for i in &expected {
            let kvs = build_key_values_with_ts_seq_values(
                &metadata,
                "shard".to_string(),
                0,
                [*i].into_iter(),
                [Some(0.0)].into_iter(),
                *i as u64,
            );
            let pk_id = PkId {
                shard_id: shard.shard_id,
                pk_index: *i as PkIndex,
            };
            for kv in kvs.iter() {
                shard.write_with_pk_id(pk_id, &kv).unwrap();
            }
        }
        assert!(!shard.is_empty());
        assert_eq!(3, shard.data_parts.frozen_len());

        let timestamps = collect_timestamps(&shard);
        assert_eq!(expected, timestamps);
    }
}
