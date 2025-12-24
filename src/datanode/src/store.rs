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

//! object storage utilities

use common_telemetry::{info, warn};
use object_store::factory::new_raw_object_store;
use object_store::util::{clean_temp_dir, join_dir, with_instrument_layers, with_retry_layers};
use object_store::{ATOMIC_WRITE_DIR, ObjectStore};
use snafu::prelude::*;

use crate::config::ObjectStoreConfig;
use crate::error::{self, Result};

pub(crate) async fn new_object_store_without_cache(
    store: &ObjectStoreConfig,
    data_home: &str,
) -> Result<ObjectStore> {
    let object_store = new_raw_object_store(store, data_home)
        .await
        .context(error::ObjectStoreSnafu)?;
    // Enable retry layer and cache layer for non-fs object storages
    let object_store = if store.is_object_storage() {
        // Adds retry layer
        with_retry_layers(object_store)
    } else {
        object_store
    };

    let object_store = with_instrument_layers(object_store, true);
    Ok(object_store)
}

/// Cleans up old LRU read cache directories that were removed.
fn clean_old_read_cache(store: &ObjectStoreConfig, data_home: &str) {
    if !store.is_object_storage() {
        return;
    }

    let Some(cache_config) = store.cache_config() else {
        return;
    };

    // Only cleans if read cache was enabled
    if !cache_config.enable_read_cache {
        return;
    }

    let cache_base_dir = if cache_config.cache_path.is_empty() {
        data_home
    } else {
        &cache_config.cache_path
    };

    // Cleans up the old read cache directory
    let old_read_cache_dir = join_dir(cache_base_dir, "cache/object/read");
    info!(
        "Cleaning up old read cache directory: {}",
        old_read_cache_dir
    );
    if let Err(e) = clean_temp_dir(&old_read_cache_dir) {
        warn!(e; "Failed to clean old read cache directory {}", old_read_cache_dir);
    }

    // Cleans up the atomic temp dir used by the cache layer
    let cache_atomic_temp_dir = join_dir(cache_base_dir, ATOMIC_WRITE_DIR);
    info!(
        "Cleaning up old cache atomic temp directory: {}",
        cache_atomic_temp_dir
    );
    if let Err(e) = clean_temp_dir(&cache_atomic_temp_dir) {
        warn!(e; "Failed to clean old cache atomic temp directory {}", cache_atomic_temp_dir);
    }
}

pub async fn new_object_store(store: ObjectStoreConfig, data_home: &str) -> Result<ObjectStore> {
    // Cleans up old LRU read cache directories.
    // TODO: Remove this line after the 1.0 release.
    clean_old_read_cache(&store, data_home);

    let object_store = new_raw_object_store(&store, data_home)
        .await
        .context(error::ObjectStoreSnafu)?;
    // Enables retry layer for non-fs object storages
    let object_store = if store.is_object_storage() {
        // Adds retry layer
        with_retry_layers(object_store)
    } else {
        object_store
    };

    let object_store = with_instrument_layers(object_store, true);
    Ok(object_store)
}
