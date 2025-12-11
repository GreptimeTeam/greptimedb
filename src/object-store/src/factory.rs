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

use std::{fs, path};

use common_telemetry::info;
use opendal::layers::HttpClientLayer;
use opendal::services::{Fs, Gcs, Oss, S3};
use snafu::prelude::*;

use crate::config::{AzblobConfig, FileConfig, GcsConfig, ObjectStoreConfig, OssConfig, S3Config};
use crate::error::{self, Result};
use crate::services::Azblob;
use crate::util::{build_http_client, clean_temp_dir, join_dir, normalize_dir};
use crate::{ATOMIC_WRITE_DIR, OLD_ATOMIC_WRITE_DIR, ObjectStore, util};

pub async fn new_raw_object_store(
    store: &ObjectStoreConfig,
    data_home: &str,
) -> Result<ObjectStore> {
    let data_home = normalize_dir(data_home);
    match store {
        ObjectStoreConfig::File(file_config) => new_fs_object_store(&data_home, file_config),
        ObjectStoreConfig::S3(s3_config) => new_s3_object_store(s3_config).await,
        ObjectStoreConfig::Oss(oss_config) => new_oss_object_store(oss_config).await,
        ObjectStoreConfig::Azblob(azblob_config) => new_azblob_object_store(azblob_config).await,
        ObjectStoreConfig::Gcs(gcs_config) => new_gcs_object_store(gcs_config).await,
    }
}

/// A helper function to create a file system object store.
pub fn new_fs_object_store(data_home: &str, _file_config: &FileConfig) -> Result<ObjectStore> {
    fs::create_dir_all(path::Path::new(&data_home))
        .context(error::CreateDirSnafu { dir: data_home })?;
    info!("The file storage home is: {}", data_home);

    let atomic_write_dir = join_dir(data_home, ATOMIC_WRITE_DIR);
    clean_temp_dir(&atomic_write_dir)?;

    // Compatible code. Remove this after a major release.
    let old_atomic_temp_dir = join_dir(data_home, OLD_ATOMIC_WRITE_DIR);
    clean_temp_dir(&old_atomic_temp_dir)?;

    let builder = Fs::default()
        .root(data_home)
        .atomic_write_dir(&atomic_write_dir);

    let object_store = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();

    Ok(object_store)
}

pub async fn new_azblob_object_store(azblob_config: &AzblobConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&azblob_config.connection.root);
    info!(
        "The azure storage container is: {}, root is: {}",
        azblob_config.connection.container, &root
    );

    let client = build_http_client(&azblob_config.http_client)?;
    let builder = Azblob::from(&azblob_config.connection);
    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .layer(HttpClientLayer::new(client))
        .finish();

    Ok(operator)
}

pub async fn new_gcs_object_store(gcs_config: &GcsConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&gcs_config.connection.root);
    info!(
        "The gcs storage bucket is: {}, root is: {}",
        gcs_config.connection.bucket, &root
    );

    let client = build_http_client(&gcs_config.http_client)?;
    let builder = Gcs::from(&gcs_config.connection);
    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .layer(HttpClientLayer::new(client))
        .finish();

    Ok(operator)
}

pub async fn new_oss_object_store(oss_config: &OssConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&oss_config.connection.root);
    info!(
        "The oss storage bucket is: {}, root is: {}",
        oss_config.connection.bucket, &root
    );

    let client = build_http_client(&oss_config.http_client)?;
    let builder = Oss::from(&oss_config.connection);
    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .layer(HttpClientLayer::new(client))
        .finish();

    Ok(operator)
}

pub async fn new_s3_object_store(s3_config: &S3Config) -> Result<ObjectStore> {
    let root = util::normalize_dir(&s3_config.connection.root);
    info!(
        "The s3 storage bucket is: {}, root is: {}, endpoint: {:?}",
        s3_config.connection.bucket, &root, s3_config.connection.endpoint
    );

    let client = build_http_client(&s3_config.http_client)?;
    let builder = S3::from(&s3_config.connection);
    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .layer(HttpClientLayer::new(client))
        .finish();

    Ok(operator)
}
