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

use std::collections::HashMap;

use futures::stream::BoxStream;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::error::Result;
use crate::key::{TableMetaKey, REGION_META_KEY_PREFIX};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

// TODO(niebayes): to be removed when `Kafka Remote Wal` is merged.
pub type KafkaTopic = String;

/// A region's unique metadata.
pub struct RegionMeta {
    region_id: RegionId,
    topic: Option<KafkaTopic>,
}

pub struct RegionMetaKey {
    table_id: TableId,
    region_number: RegionNumber,
}

impl RegionMetaKey {
    pub fn new(table_id: TableId, region_number: RegionNumber) -> Self {
        Self {
            table_id,
            region_number,
        }
    }
}

impl TableMetaKey for RegionMetaKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!(
            "{}/{}/{}",
            REGION_META_KEY_PREFIX, self.table_id, self.region_number
        )
        .into_bytes()
    }
}

pub struct RegionMetaValue {
    topic: Option<KafkaTopic>,
}

pub struct RegionMetaManager {
    kv_backend: KvBackendRef,
}

impl RegionMetaManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Gets the region meta value associated with the given region meta key.
    /// Returns None if the key does not exist.
    pub fn get(&self, key: &RegionMetaKey) -> Result<Option<RegionMetaValue>> {
        todo!()
    }

    /// Gets metadata of all regions in a table with the given table id.
    /// Returns a stream of region meta values.
    pub fn region_metas(&self, table_id: TableId) -> BoxStream<'static, Result<RegionMetaValue>> {
        todo!()
    }

    /// Builds a txn to create a sequence of region metadata.
    pub(crate) fn build_create_txn(&self, region_metas: Vec<RegionMeta>) -> Result<Txn> {
        todo!()
    }

    /// Builds a txn to delete region metadata of the regions identified by the given region ids.
    pub(crate) fn build_delete_txn(&self, region_ids: Vec<RegionId>) {
        todo!()
    }
}
