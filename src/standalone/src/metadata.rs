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

use common_config::KvBackendConfig;
use common_error::ext::BoxedError;
use common_meta::kv_backend::KvBackendRef;
use common_telemetry::info;
use log_store::raft_engine::RaftEngineBackend;
use snafu::{ResultExt, ensure};
use url::Url;

use crate::error::{InvalidUrlSchemeSnafu, OpenMetadataKvBackendSnafu, ParseUrlSnafu, Result};

/// Builds the metadata kvbackend.
pub fn build_metadata_kvbackend(dir: String, config: KvBackendConfig) -> Result<KvBackendRef> {
    info!(
        "Creating metadata kvbackend with dir: {}, config: {:?}",
        dir, config
    );
    let kv_backend = RaftEngineBackend::try_open_with_cfg(dir, &config)
        .map_err(BoxedError::new)
        .context(OpenMetadataKvBackendSnafu)?;

    Ok(Arc::new(kv_backend))
}

/// Builds the metadata kvbackend from a list of URLs.
pub fn build_metadata_kv_from_url(url: &str) -> Result<KvBackendRef> {
    let url = Url::parse(url).context(ParseUrlSnafu { url })?;
    ensure!(
        url.scheme() == "raftengine",
        InvalidUrlSchemeSnafu {
            scheme: url.scheme(),
        }
    );

    build_metadata_kvbackend(url.path().to_string(), Default::default())
}
