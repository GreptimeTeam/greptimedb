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

use common_base::secrets::ExposeSecret;
use common_telemetry::info;
use opendal::services::{Fs, Gcs, Oss, S3};
use snafu::prelude::*;

use crate::config::{AzblobConfig, FileConfig, GcsConfig, ObjectStoreConfig, OssConfig, S3Config};
use crate::error::{self, Result};
use crate::services::Azblob;
use crate::util::{build_http_client, clean_temp_dir, join_dir, normalize_dir};
use crate::{util, ObjectStore, ATOMIC_WRITE_DIR, OLD_ATOMIC_WRITE_DIR};

pub async fn new_raw_object_store(
    store: &ObjectStoreConfig,
    data_home: &str,
) -> Result<ObjectStore> {
    let data_home = normalize_dir(data_home);
    match store {
        ObjectStoreConfig::File(file_config) => new_fs_object_store(&data_home, file_config).await,
        ObjectStoreConfig::S3(s3_config) => new_s3_object_store(s3_config).await,
        ObjectStoreConfig::Oss(oss_config) => new_oss_object_store(oss_config).await,
        ObjectStoreConfig::Azblob(azblob_config) => new_azblob_object_store(azblob_config).await,
        ObjectStoreConfig::Gcs(gcs_config) => new_gcs_object_store(gcs_config).await,
    }
}

/// A helper function to create a file system object store.
pub async fn new_fs_object_store(
    data_home: &str,
    _file_config: &FileConfig,
) -> Result<ObjectStore> {
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
    let root = util::normalize_dir(&azblob_config.root);

    info!(
        "The azure storage container is: {}, root is: {}",
        azblob_config.container, &root
    );

    let client = build_http_client(&azblob_config.http_client)?;

    let mut builder = Azblob::default()
        .root(&root)
        .container(&azblob_config.container)
        .endpoint(&azblob_config.endpoint)
        .account_name(azblob_config.account_name.expose_secret())
        .account_key(azblob_config.account_key.expose_secret());

    if let Some(token) = &azblob_config.sas_token {
        builder = builder.sas_token(token);
    };

    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();
    operator.update_http_client(|_| client);
    Ok(operator)
}

pub async fn new_gcs_object_store(gcs_config: &GcsConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&gcs_config.root);
    info!(
        "The gcs storage bucket is: {}, root is: {}",
        gcs_config.bucket, &root
    );

    let client = build_http_client(&gcs_config.http_client)?;

    let builder = Gcs::default()
        .root(&root)
        .bucket(&gcs_config.bucket)
        .scope(&gcs_config.scope)
        .credential_path(gcs_config.credential_path.expose_secret())
        .credential(gcs_config.credential.expose_secret())
        .endpoint(&gcs_config.endpoint);

    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();
    operator.update_http_client(|_| client);
    Ok(operator)
}

pub async fn new_oss_object_store(oss_config: &OssConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&oss_config.root);
    info!(
        "The oss storage bucket is: {}, root is: {}",
        oss_config.bucket, &root
    );

    let client = build_http_client(&oss_config.http_client)?;

    let builder = Oss::default()
        .root(&root)
        .bucket(&oss_config.bucket)
        .endpoint(&oss_config.endpoint)
        .access_key_id(oss_config.access_key_id.expose_secret())
        .access_key_secret(oss_config.access_key_secret.expose_secret());

    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();
    operator.update_http_client(|_| client);
    Ok(operator)
}

pub async fn new_s3_object_store(s3_config: &S3Config) -> Result<ObjectStore> {
    let root = util::normalize_dir(&s3_config.root);

    info!(
        "The s3 storage bucket is: {}, root is: {}",
        s3_config.bucket, &root
    );

    let client = build_http_client(&s3_config.http_client)?;

    let mut builder = S3::default()
        .root(&root)
        .bucket(&s3_config.bucket)
        .access_key_id(s3_config.access_key_id.expose_secret())
        .secret_access_key(s3_config.secret_access_key.expose_secret());

    if s3_config.endpoint.is_some() {
        builder = builder.endpoint(s3_config.endpoint.as_ref().unwrap());
    }
    if s3_config.region.is_some() {
        builder = builder.region(s3_config.region.as_ref().unwrap());
    }
    if s3_config.enable_virtual_host_style {
        builder = builder.enable_virtual_host_style();
    }

    let operator = ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish();
    operator.update_http_client(|_| client);
    Ok(operator)
}
