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

use std::path::Path;
use std::sync::Arc;

use common_telemetry::info;
use object_store::factory::new_raw_object_store;
use object_store::layers::LruCacheLayer;
use object_store::services::Fs;
use object_store::util::{clean_temp_dir, join_dir, with_instrument_layers, with_retry_layers};
use object_store::{
    ATOMIC_WRITE_DIR, Access, OLD_ATOMIC_WRITE_DIR, ObjectStore, ObjectStoreBuilder,
};
use snafu::prelude::*;

use crate::config::ObjectStoreConfig;
use crate::error::{self, CreateDirSnafu, Result};

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

pub(crate) async fn new_object_store(
    store: ObjectStoreConfig,
    data_home: &str,
) -> Result<ObjectStore> {
    let object_store = new_raw_object_store(&store, data_home)
        .await
        .context(error::ObjectStoreSnafu)?;
    // Enable retry layer and cache layer for non-fs object storages
    let object_store = if store.is_object_storage() {
        let object_store = if let Some(cache_layer) = build_cache_layer(&store, data_home).await? {
            // Adds cache layer
            object_store.layer(cache_layer)
        } else {
            object_store
        };

        // Adds retry layer
        with_retry_layers(object_store)
    } else {
        object_store
    };

    let object_store = with_instrument_layers(object_store, true);
    Ok(object_store)
}

async fn build_cache_layer(
    store_config: &ObjectStoreConfig,
    data_home: &str,
) -> Result<Option<LruCacheLayer<impl Access>>> {
    // No need to build cache layer if read cache is disabled.
    if !store_config.enable_read_cache() {
        return Ok(None);
    }

    let (name, mut cache_path, cache_capacity) = {
        // It's safe to unwrap here because we already checked above.
        let cache_config = store_config.cache_config().unwrap();
        (
            store_config.config_name(),
            cache_config.cache_path.clone(),
            cache_config.cache_capacity,
        )
    };

    // If `cache_path` is unset, default to use `${data_home}` as the local read cache directory.
    if cache_path.is_empty() {
        let read_cache_path = data_home.to_string();
        tokio::fs::create_dir_all(Path::new(&read_cache_path))
            .await
            .context(CreateDirSnafu {
                dir: &read_cache_path,
            })?;

        info!(
            "The object storage cache path is not set for '{}', using the default path: '{}'",
            name, &read_cache_path
        );

        cache_path = read_cache_path;
    }

    if !cache_path.trim().is_empty() {
        let atomic_temp_dir = join_dir(&cache_path, ATOMIC_WRITE_DIR);
        clean_temp_dir(&atomic_temp_dir).context(error::ObjectStoreSnafu)?;

        // Compatible code. Remove this after a major release.
        let old_atomic_temp_dir = join_dir(&cache_path, OLD_ATOMIC_WRITE_DIR);
        clean_temp_dir(&old_atomic_temp_dir).context(error::ObjectStoreSnafu)?;

        let cache_store = Fs::default()
            .root(&cache_path)
            .atomic_write_dir(&atomic_temp_dir)
            .build()
            .context(error::BuildCacheStoreSnafu)?;

        let cache_layer = LruCacheLayer::new(Arc::new(cache_store), cache_capacity.0 as usize)
            .context(error::BuildCacheStoreSnafu)?;
        cache_layer.recover_cache(false).await;
        info!(
            "Enabled local object storage cache, path: {}, capacity: {}.",
            cache_path, cache_capacity
        );

        Ok(Some(cache_layer))
    } else {
        Ok(None)
    }
}
