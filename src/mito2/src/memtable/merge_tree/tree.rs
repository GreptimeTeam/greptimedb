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

//! Implementation of the memtable merge tree.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use api::v1::OpType;
use common_time::Timestamp;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::{PrimaryKeyLengthMismatchSnafu, Result};
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::{self, DataBatch, DataParts};
use crate::memtable::merge_tree::index::{
    compute_pk_weights, IndexConfig, IndexReader, KeyIndex, KeyIndexRef, ShardReader,
};
use crate::memtable::merge_tree::mutable::WriteMetrics;
use crate::memtable::merge_tree::{MergeTreeConfig, PkId, PkIndex};
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::read::{Batch, BatchBuilder};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

/// Initial capacity for the data buffer.
const DATA_INIT_CAP: usize = 8;

/// The merge tree.
pub(crate) struct MergeTree {
    /// Config of the tree.
    config: MergeTreeConfig,
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Primary key codec.
    row_codec: Arc<McmpRowCodec>,
    pub(crate) parts: RwLock<TreeParts>,
}

pub(crate) type MergeTreeRef = Arc<MergeTree>;

impl MergeTree {
    /// Creates a new merge tree.
    pub(crate) fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> MergeTree {
        let row_codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );

        let index = (!metadata.primary_key.is_empty()).then(|| {
            Arc::new(KeyIndex::new(IndexConfig {
                max_keys_per_shard: config.index_max_keys_per_shard,
            }))
        });
        let data =
            DataParts::with_capacity(metadata.clone(), DATA_INIT_CAP, config.freeze_threshold);
        let parts = TreeParts {
            immutable: false,
            index,
            data,
        };

        MergeTree {
            config: config.clone(),
            metadata,
            row_codec: Arc::new(row_codec),
            parts: RwLock::new(parts),
        }
    }

    // TODO(yingwen): The size computed from values is inaccurate.
    /// Write key-values into the tree.
    ///
    /// # Panics
    /// Panics if the tree is immutable (frozen).
    pub(crate) fn write(&self, kvs: &KeyValues, metrics: &mut WriteMetrics) -> Result<()> {
        let mut primary_key = Vec::new();
        let has_pk = !self.metadata.primary_key.is_empty();

        for kv in kvs.iter() {
            ensure!(
                kv.num_primary_keys() == self.row_codec.num_fields(),
                PrimaryKeyLengthMismatchSnafu {
                    expect: self.row_codec.num_fields(),
                    actual: kv.num_primary_keys(),
                }
            );
            // Safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
            metrics.min_ts = metrics.min_ts.min(ts);
            metrics.max_ts = metrics.max_ts.max(ts);
            metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

            if !has_pk {
                // No primary key.
                // Now we always assign the first shard and the first pk index to the id.
                let pk_id = PkId {
                    shard_id: 0,
                    pk_index: 0,
                };
                self.write_with_id(pk_id, kv)?;
                continue;
            }

            // Encode primary key.
            primary_key.clear();
            self.row_codec
                .encode_to_vec(kv.primary_keys(), &mut primary_key)?;

            // Write rows with primary keys.
            self.write_with_key(&primary_key, kv, metrics)?;
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Scans the tree.
    pub(crate) fn scan(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        assert!(predicate.is_none(), "Predicate is unsupported");
        // Creates the projection set.
        let projection: HashSet<_> = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.metadata.field_columns().map(|c| c.column_id).collect()
        };

        let index = {
            let parts = self.parts.read().unwrap();
            parts.index.clone()
        };
        let index_reader = index
            .as_ref()
            .map(|index| index.scan_shard(0))
            .transpose()?;
        // Compute pk weights.
        let mut pk_weights = Vec::new();
        if let Some(reader) = &index_reader {
            compute_pk_weights(reader.sorted_pk_index(), &mut pk_weights);
        } else {
            // Push weight for the only key.
            // TODO(yingwen): Allow passing empty weights if there is no primary key.
            pk_weights.push(0);
        }

        let data_iter = {
            let mut parts = self.parts.write().unwrap();
            parts.data.iter(pk_weights)?
        };

        let iter = ShardIter {
            metadata: self.metadata.clone(),
            projection,
            index_reader,
            data_reader: DataReader::new(data_iter)?,
        };

        Ok(Box::new(iter))
    }

    /// Returns true if the tree is empty.
    pub(crate) fn is_empty(&self) -> bool {
        let parts = self.parts.read().unwrap();
        // Gets whether the memtable is empty from the data part.
        parts.data.is_empty()
        // TODO(yingwen): Also consider other parts if we freeze the data buffer.
    }

    /// Marks the tree as immutable.
    ///
    /// Once the tree becomes immutable, callers should not write to it again.
    pub(crate) fn freeze(&self) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        parts.immutable = true;
        // Freezes the index.
        if let Some(index) = &parts.index {
            index.freeze()?;
        }

        Ok(())
    }

    /// Forks an immutable tree. Returns a mutable tree that inherits the index
    /// of this tree.
    pub(crate) fn fork(&self, metadata: RegionMetadataRef) -> MergeTree {
        if metadata.primary_key != self.metadata.primary_key {
            // The priamry key is changed. We can't reuse fields.
            return MergeTree::new(metadata, &self.config);
        }

        let current_parts = self.parts.read().unwrap();
        let index = current_parts
            .index
            .as_ref()
            .map(|index| Arc::new(index.fork()));
        // New parts.
        let parts = TreeParts {
            immutable: false,
            index,
            data: DataParts::new(
                metadata.clone(),
                DATA_INIT_CAP,
                self.config.freeze_threshold,
            ),
        };

        MergeTree {
            config: self.config.clone(),
            metadata,
            // We can reuse row codec.
            row_codec: self.row_codec.clone(),
            parts: RwLock::new(parts),
        }
    }

    /// Returns the memory size of shared parts.
    pub(crate) fn shared_memory_size(&self) -> usize {
        let parts = self.parts.read().unwrap();
        parts
            .index
            .as_ref()
            .map(|index| index.memory_size())
            .unwrap_or(0)
    }

    fn write_with_key(
        &self,
        primary_key: &[u8],
        kv: KeyValue,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        // Write the pk to the index.
        let pk_id = self.write_primary_key(primary_key, metrics)?;
        // Writes data.
        self.write_with_id(pk_id, kv)
    }

    fn write_with_id(&self, pk_id: PkId, kv: KeyValue) -> Result<()> {
        let mut parts = self.parts.write().unwrap();
        assert!(!parts.immutable);
        if parts.data.write_row(pk_id, kv) {
            // should trigger freeze
            let weights = if let Some(index) = parts.index.as_ref() {
                let pk_indices = index.sorted_pk_indices();
                let mut weights = Vec::with_capacity(pk_indices.len());
                compute_pk_weights(&pk_indices, &mut weights);
                weights
            } else {
                vec![0]
            };
            parts.data.freeze(&weights)
        } else {
            Ok(())
        }
    }

    fn write_primary_key(&self, key: &[u8], metrics: &mut WriteMetrics) -> Result<PkId> {
        let index = {
            let parts = self.parts.read().unwrap();
            assert!(!parts.immutable);
            // Safety: The region has primary keys.
            parts.index.clone().unwrap()
        };

        index.write_primary_key(key, metrics)
    }
}

pub(crate) struct TreeParts {
    /// Whether the tree is immutable.
    immutable: bool,
    /// Index part of the tree. If the region doesn't have a primary key, this field
    /// is `None`.
    index: Option<KeyIndexRef>,
    /// Data part of the tree.
    pub(crate) data: DataParts,
}

struct ShardIter {
    metadata: RegionMetadataRef,
    projection: HashSet<ColumnId>,
    index_reader: Option<ShardReader>,
    data_reader: DataReader,
}

impl Iterator for ShardIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.data_reader.is_valid() {
            return None;
        }

        self.next_batch().transpose()
    }
}

impl ShardIter {
    /// Fetches next batch and advances the iter.
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        let Some(index_reader) = &mut self.index_reader else {
            // No primary key to read.
            // Safety: `next()` ensures the data reader is valid.
            let batch = self.data_reader.convert_current_record_batch(
                &self.metadata,
                &self.projection,
                &[],
            )?;
            // Advances the data reader.
            self.data_reader.next()?;
            return Ok(Some(batch));
        };

        // Iterate the index reader until we see the same pk index of the data batch.
        while index_reader.is_valid()
            && index_reader.current_pk_index() != self.data_reader.current_pk_index()
        {
            index_reader.next();
        }
        assert!(
            index_reader.is_valid(),
            "Data contains pk_index {} not in the index",
            self.data_reader.current_pk_index()
        );

        let batch = self.data_reader.convert_current_record_batch(
            &self.metadata,
            &self.projection,
            index_reader.current_key(),
        )?;
        // Advances the data reader.
        self.data_reader.next()?;
        Ok(Some(batch))
    }
}

struct DataReader {
    current: Option<DataBatch>,
    iter: data::Iter,
}

impl DataReader {
    fn new(mut iter: data::Iter) -> Result<Self> {
        let current = iter.next().transpose()?;

        Ok(Self { current, iter })
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn current_pk_index(&self) -> PkIndex {
        self.current.as_ref().unwrap().pk_index()
    }

    /// Converts current [RecordBatch] to [Batch].
    fn convert_current_record_batch(
        &self,
        metadata: &RegionMetadataRef,
        projection: &HashSet<ColumnId>,
        primary_key: &[u8],
    ) -> Result<Batch> {
        let data_batch = self.current.as_ref().unwrap();
        let offset = data_batch.range().start;
        let length = data_batch.range().len();
        let record_batch = data_batch.record_batch();

        let mut builder = BatchBuilder::new(primary_key.to_vec());
        builder
            .timestamps_array(record_batch.column(1).slice(offset, length))?
            .sequences_array(record_batch.column(2).slice(offset, length))?
            .op_types_array(record_batch.column(3).slice(offset, length))?;

        // TODO(yingwen): Pushdown projection to data parts.
        if record_batch.num_columns() <= 4 {
            // No fields.
            return builder.build();
        }

        // Iterate all field columns.
        for (array, field) in record_batch
            .columns()
            .iter()
            .zip(record_batch.schema().fields().iter())
            .skip(4)
        {
            // Safety: metadata should contain all fields.
            let column_id = metadata.column_by_name(field.name()).unwrap().column_id;
            if !projection.contains(&column_id) {
                continue;
            }
            builder.push_field_array(column_id, array.slice(offset, length))?;
        }

        builder.build()
    }

    fn next(&mut self) -> Result<()> {
        self.current = self.iter.next().transpose()?;
        Ok(())
    }
}
