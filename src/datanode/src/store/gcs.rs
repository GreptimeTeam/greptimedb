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
use object_store::services::Gcs;
use object_store::{util, ObjectStore};
use snafu::prelude::*;

use crate::config::GcsConfig;
use crate::error::{self, Result};
use crate::store::build_http_client;

pub(crate) async fn new_gcs_object_store(gcs_config: &GcsConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&gcs_config.root);
    info!(
        "The gcs storage bucket is: {}, root is: {}",
        gcs_config.bucket, &root
    );

    let builder = Gcs::default()
        .root(&root)
        .bucket(&gcs_config.bucket)
        .scope(&gcs_config.scope)
        .credential_path(gcs_config.credential_path.expose_secret())
        .endpoint(&gcs_config.endpoint)
        .http_client(build_http_client()?);

    Ok(ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish())
}
