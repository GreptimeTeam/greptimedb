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

//! Builder of a shard.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use store_api::metadata::RegionMetadataRef;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::partition_tree::data::{
    DataBatch, DataBuffer, DataBufferReader, DataBufferReaderBuilder, DataParts, DATA_INIT_CAP,
};
use crate::memtable::partition_tree::dict::{DictBuilderReader, KeyDictBuilder};
use crate::memtable::partition_tree::shard::Shard;
use crate::memtable::partition_tree::{PartitionTreeConfig, PkId, PkIndex, ShardId};
use crate::memtable::stats::WriteMetrics;
use crate::metrics::PARTITION_TREE_READ_STAGE_ELAPSED;
use crate::row_converter::PrimaryKeyFilter;

/// Builder to write keys and data to a shard that the key dictionary
/// is still active.
pub struct ShardBuilder {
    /// Id of the current shard to build.
    current_shard_id: ShardId,
    /// Builder for the key dictionary.
    dict_builder: KeyDictBuilder,
    /// Buffer to store data.
    data_buffer: DataBuffer,
    /// Number of rows to freeze a data part.
    data_freeze_threshold: usize,
    dedup: bool,
}

impl ShardBuilder {
    /// Returns a new builder.
    pub fn new(
        metadata: RegionMetadataRef,
        config: &PartitionTreeConfig,
        shard_id: ShardId,
    ) -> ShardBuilder {
        ShardBuilder {
            current_shard_id: shard_id,
            dict_builder: KeyDictBuilder::new(config.index_max_keys_per_shard),
            data_buffer: DataBuffer::with_capacity(metadata, DATA_INIT_CAP, config.dedup),
            data_freeze_threshold: config.data_freeze_threshold,
            dedup: config.dedup,
        }
    }

    /// Write a key value with given pk_index (caller must ensure the pk_index exist in dict_builder)
    pub fn write_with_pk_id(&mut self, pk_id: PkId, key_value: &KeyValue) {
        assert_eq!(self.current_shard_id, pk_id.shard_id);
        self.data_buffer.write_row(pk_id.pk_index, key_value);
    }

    /// Write a key value with its encoded primary key.
    pub fn write_with_key(
        &mut self,
        full_primary_key: &[u8],
        sparse_key: Option<&[u8]>,
        key_value: &KeyValue,
        metrics: &mut WriteMetrics,
    ) -> PkId {
        // Safety: we check whether the builder need to freeze before.
        let pk_index = self
            .dict_builder
            .insert_key(full_primary_key, sparse_key, metrics);
        self.data_buffer.write_row(pk_index, key_value);
        PkId {
            shard_id: self.current_shard_id,
            pk_index,
        }
    }

    /// Returns true if the builder need to freeze.
    pub fn should_freeze(&self) -> bool {
        self.dict_builder.is_full() || self.data_buffer.num_rows() == self.data_freeze_threshold
    }

    /// Returns the current shard id of the builder.
    pub fn current_shard_id(&self) -> ShardId {
        self.current_shard_id
    }

    /// Builds a new shard and resets the builder.
    ///
    /// Returns `None` if the builder is empty.
    pub fn finish(
        &mut self,
        metadata: RegionMetadataRef,
        pk_to_pk_id: &mut HashMap<Vec<u8>, PkId>,
    ) -> Result<Option<Shard>> {
        if self.is_empty() {
            return Ok(None);
        }

        let (data_part, key_dict) = match self.dict_builder.finish() {
            Some((dict, pk_to_index)) => {
                // Adds mapping to the map.
                pk_to_pk_id.reserve(pk_to_index.len());
                for (k, pk_index) in pk_to_index {
                    pk_to_pk_id.insert(
                        k,
                        PkId {
                            shard_id: self.current_shard_id,
                            pk_index,
                        },
                    );
                }

                let pk_weights = dict.pk_weights_to_sort_data();
                let part = self.data_buffer.freeze(Some(&pk_weights), true)?;
                (part, Some(dict))
            }
            None => {
                let pk_weights = [0];
                (self.data_buffer.freeze(Some(&pk_weights), true)?, None)
            }
        };

        // build data parts.
        let data_parts =
            DataParts::new(metadata, DATA_INIT_CAP, self.dedup).with_frozen(vec![data_part]);
        let key_dict = key_dict.map(Arc::new);
        let shard_id = self.current_shard_id;
        self.current_shard_id += 1;

        Ok(Some(Shard::new(
            shard_id,
            key_dict,
            data_parts,
            self.dedup,
            self.data_freeze_threshold,
        )))
    }

    /// Scans the shard builder.
    pub fn read(&self, pk_weights_buffer: &mut Vec<u16>) -> Result<ShardBuilderReaderBuilder> {
        let dict_reader = {
            let _timer = PARTITION_TREE_READ_STAGE_ELAPSED
                .with_label_values(&["shard_builder_read_pk"])
                .start_timer();
            self.dict_builder.read()
        };

        {
            let _timer = PARTITION_TREE_READ_STAGE_ELAPSED
                .with_label_values(&["sort_pk"])
                .start_timer();
            dict_reader.pk_weights_to_sort_data(pk_weights_buffer);
        }

        let data_reader = self.data_buffer.read()?;
        Ok(ShardBuilderReaderBuilder {
            shard_id: self.current_shard_id,
            dict_reader,
            data_reader,
        })
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.data_buffer.is_empty()
    }
}

pub(crate) struct ShardBuilderReaderBuilder {
    shard_id: ShardId,
    dict_reader: DictBuilderReader,
    data_reader: DataBufferReaderBuilder,
}

impl ShardBuilderReaderBuilder {
    pub(crate) fn build(
        self,
        pk_weights: Option<&[u16]>,
        key_filter: Option<Box<dyn PrimaryKeyFilter>>,
    ) -> Result<ShardBuilderReader> {
        let now = Instant::now();
        let data_reader = self.data_reader.build(pk_weights)?;
        ShardBuilderReader::new(
            self.shard_id,
            self.dict_reader,
            data_reader,
            key_filter,
            now.elapsed(),
        )
    }
}

/// Reader to scan a shard builder.
pub struct ShardBuilderReader {
    shard_id: ShardId,
    dict_reader: DictBuilderReader,
    data_reader: DataBufferReader,
    key_filter: Option<Box<dyn PrimaryKeyFilter>>,
    last_yield_pk_index: Option<PkIndex>,
    keys_before_pruning: usize,
    keys_after_pruning: usize,
    prune_pk_cost: Duration,
    data_build_cost: Duration,
}

impl ShardBuilderReader {
    fn new(
        shard_id: ShardId,
        dict_reader: DictBuilderReader,
        data_reader: DataBufferReader,
        key_filter: Option<Box<dyn PrimaryKeyFilter>>,
        data_build_cost: Duration,
    ) -> Result<Self> {
        let mut reader = ShardBuilderReader {
            shard_id,
            dict_reader,
            data_reader,
            key_filter,
            last_yield_pk_index: None,
            keys_before_pruning: 0,
            keys_after_pruning: 0,
            prune_pk_cost: Duration::default(),
            data_build_cost,
        };
        reader.prune_batch_by_key()?;

        Ok(reader)
    }

    pub fn is_valid(&self) -> bool {
        self.data_reader.is_valid()
    }

    pub fn next(&mut self) -> Result<()> {
        self.data_reader.next()?;
        self.prune_batch_by_key()
    }

    pub fn current_key(&self) -> Option<&[u8]> {
        let pk_index = self.data_reader.current_data_batch().pk_index();
        Some(self.dict_reader.key_by_pk_index(pk_index))
    }

    pub fn current_pk_id(&self) -> PkId {
        let pk_index = self.data_reader.current_data_batch().pk_index();
        PkId {
            shard_id: self.shard_id,
            pk_index,
        }
    }

    pub fn current_data_batch(&self) -> DataBatch {
        self.data_reader.current_data_batch()
    }

    fn prune_batch_by_key(&mut self) -> Result<()> {
        let Some(key_filter) = &mut self.key_filter else {
            return Ok(());
        };

        while self.data_reader.is_valid() {
            let pk_index = self.data_reader.current_data_batch().pk_index();
            if let Some(yield_pk_index) = self.last_yield_pk_index {
                if pk_index == yield_pk_index {
                    break;
                }
            }
            self.keys_before_pruning += 1;
            let key = self.dict_reader.key_by_pk_index(pk_index);
            let now = Instant::now();
            if key_filter.prune_primary_key(key) {
                self.prune_pk_cost += now.elapsed();
                self.last_yield_pk_index = Some(pk_index);
                self.keys_after_pruning += 1;
                break;
            }
            self.prune_pk_cost += now.elapsed();
            self.data_reader.next()?;
        }

        Ok(())
    }
}

impl Drop for ShardBuilderReader {
    fn drop(&mut self) {
        let shard_builder_prune_pk = self.prune_pk_cost.as_secs_f64();
        PARTITION_TREE_READ_STAGE_ELAPSED
            .with_label_values(&["shard_builder_prune_pk"])
            .observe(shard_builder_prune_pk);
        if self.keys_before_pruning > 0 {
            common_telemetry::debug!(
                "ShardBuilderReader metrics, before pruning: {}, after pruning: {}, prune cost: {}s, build cost: {}s",
                self.keys_before_pruning,
                self.keys_after_pruning,
                shard_builder_prune_pk,
                self.data_build_cost.as_secs_f64(),
            );
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::memtable::partition_tree::data::timestamp_array_to_i64_slice;
    use crate::memtable::KeyValues;
    use crate::test_util::memtable_util::{
        build_key_values_with_ts_seq_values, encode_key_by_kv, metadata_for_test,
    };

    fn input_with_key(metadata: &RegionMetadataRef) -> Vec<KeyValues> {
        vec![
            build_key_values_with_ts_seq_values(
                metadata,
                "shard_builder".to_string(),
                2,
                [20, 21].into_iter(),
                [Some(0.0), Some(1.0)].into_iter(),
                0,
            ),
            build_key_values_with_ts_seq_values(
                metadata,
                "shard_builder".to_string(),
                0,
                [0, 1].into_iter(),
                [Some(0.0), Some(1.0)].into_iter(),
                1,
            ),
            build_key_values_with_ts_seq_values(
                metadata,
                "shard_builder".to_string(),
                1,
                [10, 11].into_iter(),
                [Some(0.0), Some(1.0)].into_iter(),
                2,
            ),
        ]
    }

    #[test]
    fn test_write_shard_builder() {
        let metadata = metadata_for_test();
        let input = input_with_key(&metadata);
        let config = PartitionTreeConfig::default();
        let mut shard_builder = ShardBuilder::new(metadata.clone(), &config, 1);
        let mut metrics = WriteMetrics::default();
        assert!(shard_builder
            .finish(metadata.clone(), &mut HashMap::new())
            .unwrap()
            .is_none());
        assert_eq!(1, shard_builder.current_shard_id);

        for key_values in &input {
            for kv in key_values.iter() {
                let key = encode_key_by_kv(&kv);
                shard_builder.write_with_key(&key, None, &kv, &mut metrics);
            }
        }
        let shard = shard_builder
            .finish(metadata, &mut HashMap::new())
            .unwrap()
            .unwrap();
        assert_eq!(1, shard.shard_id);
        assert_eq!(2, shard_builder.current_shard_id);
    }

    #[test]
    fn test_write_read_shard_builder() {
        let metadata = metadata_for_test();
        let input = input_with_key(&metadata);
        let config = PartitionTreeConfig::default();
        let mut shard_builder = ShardBuilder::new(metadata.clone(), &config, 1);
        let mut metrics = WriteMetrics::default();

        for key_values in &input {
            for kv in key_values.iter() {
                let key = encode_key_by_kv(&kv);
                shard_builder.write_with_key(&key, None, &kv, &mut metrics);
            }
        }

        let mut pk_weights = Vec::new();
        let mut reader = shard_builder
            .read(&mut pk_weights)
            .unwrap()
            .build(Some(&pk_weights), None)
            .unwrap();
        let mut timestamps = Vec::new();
        while reader.is_valid() {
            let rb = reader.current_data_batch().slice_record_batch();
            let ts_array = rb.column(1);
            let ts_slice = timestamp_array_to_i64_slice(ts_array);
            timestamps.extend_from_slice(ts_slice);

            reader.next().unwrap();
        }
        assert_eq!(vec![0, 1, 10, 11, 20, 21], timestamps);
        assert_eq!(vec![2, 0, 1], pk_weights);
    }
}
