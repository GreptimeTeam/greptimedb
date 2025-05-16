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

mod azblob;
pub mod fs;
mod gcs;
mod oss;
mod s3;
use std::path;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common_telemetry::{info, warn};
use mito2::access_layer::{ATOMIC_WRITE_DIR, OLD_ATOMIC_WRITE_DIR};
use object_store::layers::{LruCacheLayer, RetryInterceptor, RetryLayer};
use object_store::services::Fs;
use object_store::util::{join_dir, normalize_dir, with_instrument_layers};
use object_store::{Access, Error, HttpClient, ObjectStore, ObjectStoreBuilder};
use snafu::prelude::*;

use crate::config::{HttpClientConfig, ObjectStoreConfig, DEFAULT_OBJECT_STORE_CACHE_SIZE};
use crate::error::{self, BuildHttpClientSnafu, CreateDirSnafu, Result};

pub(crate) async fn new_raw_object_store(
    store: &ObjectStoreConfig,
    data_home: &str,
) -> Result<ObjectStore> {
    let data_home = normalize_dir(data_home);
    let object_store = match store {
        ObjectStoreConfig::File(file_config) => {
            fs::new_fs_object_store(&data_home, file_config).await
        }
        ObjectStoreConfig::S3(s3_config) => s3::new_s3_object_store(s3_config).await,
        ObjectStoreConfig::Oss(oss_config) => oss::new_oss_object_store(oss_config).await,
        ObjectStoreConfig::Azblob(azblob_config) => {
            azblob::new_azblob_object_store(azblob_config).await
        }
        ObjectStoreConfig::Gcs(gcs_config) => gcs::new_gcs_object_store(gcs_config).await,
    }?;
    Ok(object_store)
}

fn with_retry_layers(object_store: ObjectStore) -> ObjectStore {
    object_store.layer(
        RetryLayer::new()
            .with_jitter()
            .with_notify(PrintDetailedError),
    )
}

pub(crate) async fn new_object_store_without_cache(
    store: &ObjectStoreConfig,
    data_home: &str,
) -> Result<ObjectStore> {
    let object_store = new_raw_object_store(store, data_home).await?;
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
    let object_store = new_raw_object_store(&store, data_home).await?;
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
    let (name, mut cache_path, cache_capacity) = match store_config {
        ObjectStoreConfig::S3(s3_config) => {
            let path = s3_config.cache.cache_path.clone();
            let name = &s3_config.name;
            let capacity = s3_config
                .cache
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (name, path, capacity)
        }
        ObjectStoreConfig::Oss(oss_config) => {
            let path = oss_config.cache.cache_path.clone();
            let name = &oss_config.name;
            let capacity = oss_config
                .cache
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (name, path, capacity)
        }
        ObjectStoreConfig::Azblob(azblob_config) => {
            let path = azblob_config.cache.cache_path.clone();
            let name = &azblob_config.name;
            let capacity = azblob_config
                .cache
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (name, path, capacity)
        }
        ObjectStoreConfig::Gcs(gcs_config) => {
            let path = gcs_config.cache.cache_path.clone();
            let name = &gcs_config.name;
            let capacity = gcs_config
                .cache
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (name, path, capacity)
        }
        _ => unreachable!("Already checked above"),
    };

    // Enable object cache by default
    // Set the cache_path to be `${data_home}` by default
    // if it's not present
    if cache_path.is_none() {
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

        cache_path = Some(read_cache_path);
    }

    if let Some(path) = cache_path.as_ref()
        && !path.trim().is_empty()
    {
        let atomic_temp_dir = join_dir(path, ATOMIC_WRITE_DIR);
        clean_temp_dir(&atomic_temp_dir)?;

        // Compatible code. Remove this after a major release.
        let old_atomic_temp_dir = join_dir(path, OLD_ATOMIC_WRITE_DIR);
        clean_temp_dir(&old_atomic_temp_dir)?;

        let cache_store = Fs::default()
            .root(path)
            .atomic_write_dir(&atomic_temp_dir)
            .build()
            .context(error::InitBackendSnafu)?;

        let cache_layer = LruCacheLayer::new(Arc::new(cache_store), cache_capacity.0 as usize)
            .context(error::InitBackendSnafu)?;
        cache_layer.recover_cache(false).await;
        info!(
            "Enabled local object storage cache, path: {}, capacity: {}.",
            path, cache_capacity
        );

        Ok(Some(cache_layer))
    } else {
        Ok(None)
    }
}

pub(crate) fn clean_temp_dir(dir: &str) -> Result<()> {
    if path::Path::new(&dir).exists() {
        info!("Begin to clean temp storage directory: {}", dir);
        std::fs::remove_dir_all(dir).context(error::RemoveDirSnafu { dir })?;
        info!("Cleaned temp storage directory: {}", dir);
    }

    Ok(())
}

pub(crate) fn build_http_client(config: &HttpClientConfig) -> Result<HttpClient> {
    let client = reqwest::ClientBuilder::new()
        .pool_max_idle_per_host(config.pool_max_idle_per_host as usize)
        .connect_timeout(config.connect_timeout)
        .pool_idle_timeout(config.pool_idle_timeout)
        .timeout(config.timeout)
        .build()
        .context(BuildHttpClientSnafu)?;
    Ok(HttpClient::with(client))
}
struct PrintDetailedError;

// PrintDetailedError is a retry interceptor that prints error in Debug format in retrying.
impl RetryInterceptor for PrintDetailedError {
    fn intercept(&self, err: &Error, dur: Duration) {
        warn!("Retry after {}s, error: {:#?}", dur.as_secs_f64(), err);
    }
}
