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

use common_base::secrets::ExposeSecret;
use common_telemetry::info;
use object_store::services::S3;
use object_store::{util, ObjectStore};
use snafu::prelude::*;

use crate::config::S3Config;
use crate::error::{self, Result};
use crate::store::build_http_client;

pub(crate) async fn new_s3_object_store(s3_config: &S3Config) -> Result<ObjectStore> {
    let root = util::normalize_dir(&s3_config.root);

    info!(
        "The s3 storage bucket is: {}, root is: {}",
        s3_config.bucket, &root
    );

    let mut builder = S3::default()
        .root(&root)
        .bucket(&s3_config.bucket)
        .access_key_id(s3_config.access_key_id.expose_secret())
        .secret_access_key(s3_config.secret_access_key.expose_secret())
        .http_client(build_http_client()?);

    if s3_config.endpoint.is_some() {
        builder = builder.endpoint(s3_config.endpoint.as_ref().unwrap());
    };
    if s3_config.region.is_some() {
        builder = builder.region(s3_config.region.as_ref().unwrap());
    };

    Ok(ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish())
}
