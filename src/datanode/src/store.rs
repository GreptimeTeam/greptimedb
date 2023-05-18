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
use std::sync::Arc;
use std::{fs, path};

use common_base::readable_size::ReadableSize;
use common_telemetry::logging::info;
use object_store::layers::{LoggingLayer, LruCacheLayer, MetricsLayer, RetryLayer, TracingLayer};
use object_store::services::{Fs as FsBuilder, Oss as OSSBuilder, S3 as S3Builder};
use object_store::{util, ObjectStore, ObjectStoreBuilder};
use secrecy::ExposeSecret;
use snafu::prelude::*;

use crate::datanode::{ObjectStoreConfig, DEFAULT_OBJECT_STORE_CACHE_SIZE};
use crate::error::{self, Result};

pub(crate) async fn new_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let object_store = match store_config {
        ObjectStoreConfig::File { .. } => new_fs_object_store(store_config).await,
        ObjectStoreConfig::S3 { .. } => new_s3_object_store(store_config).await,
        ObjectStoreConfig::Oss { .. } => new_oss_object_store(store_config).await,
    };

    // Don't enable retry layer when using local file backend.
    let object_store = if !matches!(store_config, ObjectStoreConfig::File(..)) {
        object_store.map(|object_store| object_store.layer(RetryLayer::new().with_jitter()))
    } else {
        object_store
    };

    object_store.map(|object_store| {
        object_store
            .layer(MetricsLayer)
            .layer(
                LoggingLayer::default()
                    // Print the expected error only in DEBUG level.
                    // See https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html#method.with_error_level
                    .with_error_level(Some(log::Level::Debug)),
            )
            .layer(TracingLayer)
    })
}

pub(crate) async fn new_oss_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let oss_config = match store_config {
        ObjectStoreConfig::Oss(config) => config,
        _ => unreachable!(),
    };

    let root = util::normalize_dir(&oss_config.root);
    info!(
        "The oss storage bucket is: {}, root is: {}",
        oss_config.bucket, &root
    );

    let mut builder = OSSBuilder::default();
    builder
        .root(&root)
        .bucket(&oss_config.bucket)
        .endpoint(&oss_config.endpoint)
        .access_key_id(oss_config.access_key_id.expose_secret())
        .access_key_secret(oss_config.access_key_secret.expose_secret());

    let object_store = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();

    create_object_store_with_cache(object_store, store_config).await
}

async fn create_object_store_with_cache(
    object_store: ObjectStore,
    store_config: &ObjectStoreConfig,
) -> Result<ObjectStore> {
    let (cache_path, cache_capacity) = match store_config {
        ObjectStoreConfig::S3(s3_config) => {
            let path = s3_config.cache_path.as_ref();
            let capacity = s3_config
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (path, capacity)
        }
        ObjectStoreConfig::Oss(oss_config) => {
            let path = oss_config.cache_path.as_ref();
            let capacity = oss_config
                .cache_capacity
                .unwrap_or(DEFAULT_OBJECT_STORE_CACHE_SIZE);
            (path, capacity)
        }
        _ => (None, ReadableSize(0)),
    };

    if let Some(path) = cache_path {
        let atomic_temp_dir = format!("{path}/.tmp/");
        clean_temp_dir(&atomic_temp_dir)?;
        let cache_store = FsBuilder::default()
            .root(path)
            .atomic_write_dir(&atomic_temp_dir)
            .build()
            .context(error::InitBackendSnafu)?;

        let cache_layer = LruCacheLayer::new(Arc::new(cache_store), cache_capacity.0 as usize)
            .await
            .context(error::InitBackendSnafu)?;
        Ok(object_store.layer(cache_layer))
    } else {
        Ok(object_store)
    }
}

pub(crate) async fn new_s3_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let s3_config = match store_config {
        ObjectStoreConfig::S3(config) => config,
        _ => unreachable!(),
    };

    let root = util::normalize_dir(&s3_config.root);
    info!(
        "The s3 storage bucket is: {}, root is: {}",
        s3_config.bucket, &root
    );

    let mut builder = S3Builder::default();
    builder
        .root(&root)
        .bucket(&s3_config.bucket)
        .access_key_id(s3_config.access_key_id.expose_secret())
        .secret_access_key(s3_config.secret_access_key.expose_secret());

    if s3_config.endpoint.is_some() {
        builder.endpoint(s3_config.endpoint.as_ref().unwrap());
    }
    if s3_config.region.is_some() {
        builder.region(s3_config.region.as_ref().unwrap());
    }

    create_object_store_with_cache(
        ObjectStore::new(builder)
            .context(error::InitBackendSnafu)?
            .finish(),
        store_config,
    )
    .await
}

fn clean_temp_dir(dir: &str) -> Result<()> {
    if path::Path::new(&dir).exists() {
        info!("Begin to clean temp storage directory: {}", dir);
        fs::remove_dir_all(dir).context(error::RemoveDirSnafu { dir })?;
        info!("Cleaned temp storage directory: {}", dir);
    }

    Ok(())
}

pub(crate) async fn new_fs_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    let file_config = match store_config {
        ObjectStoreConfig::File(config) => config,
        _ => unreachable!(),
    };
    let data_home = util::normalize_dir(&file_config.data_home);
    fs::create_dir_all(path::Path::new(&data_home))
        .context(error::CreateDirSnafu { dir: &data_home })?;
    info!("The file storage home is: {}", &data_home);

    let atomic_write_dir = format!("{data_home}/.tmp/");
    clean_temp_dir(&atomic_write_dir)?;

    let mut builder = FsBuilder::default();
    builder.root(&data_home).atomic_write_dir(&atomic_write_dir);

    let object_store = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();

    Ok(object_store)
}
