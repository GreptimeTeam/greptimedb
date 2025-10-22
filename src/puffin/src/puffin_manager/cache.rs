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

use std::sync::Arc;

use prometheus::IntGaugeVec;

use crate::file_metadata::FileMetadata;
/// Metrics for index metadata.
const PUFFIN_METADATA_TYPE: &str = "puffin_metadata";

pub type PuffinMetadataCacheRef = Arc<PuffinMetadataCache>;

/// A cache for storing the metadata of the index files.
pub struct PuffinMetadataCache {
    cache: moka::sync::Cache<String, Arc<FileMetadata>>,
}

fn puffin_metadata_weight(k: &str, v: &Arc<FileMetadata>) -> u32 {
    (k.len() + v.memory_usage()) as u32
}

impl PuffinMetadataCache {
    pub fn new(capacity: u64, cache_bytes: &'static IntGaugeVec) -> Self {
        common_telemetry::debug!("Building PuffinMetadataCache with capacity: {capacity}");
        Self {
            cache: moka::sync::CacheBuilder::new(capacity)
                .name("puffin_metadata")
                .weigher(|k: &String, v| puffin_metadata_weight(k, v))
                .eviction_listener(|k, v, _cause| {
                    let size = puffin_metadata_weight(&k, &v);
                    cache_bytes
                        .with_label_values(&[PUFFIN_METADATA_TYPE])
                        .sub(size.into());
                })
                .build(),
        }
    }

    /// Gets the metadata from the cache.
    pub fn get_metadata(&self, file_id: &str) -> Option<Arc<FileMetadata>> {
        self.cache.get(file_id)
    }

    /// Puts the metadata into the cache.
    pub fn put_metadata(&self, file_id: String, metadata: Arc<FileMetadata>) {
        self.cache.insert(file_id, metadata);
    }

    /// Removes the metadata of the given file from the cache, if present.
    pub fn remove(&self, file_id: &str) {
        self.cache.invalidate(file_id);
    }
}
