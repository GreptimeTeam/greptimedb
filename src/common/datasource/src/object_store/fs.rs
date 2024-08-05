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

use object_store::services::Fs;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{BuildBackendSnafu, Result};

pub fn build_fs_backend(root: &str) -> Result<ObjectStore> {
    let builder = Fs::default();
    let object_store = ObjectStore::new(builder.root(root))
        .context(BuildBackendSnafu)?
        .layer(
            object_store::layers::LoggingLayer::default()
                // Print the expected error only in DEBUG level.
                // See https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html#method.with_error_level
                .with_error_level(Some("debug"))
                .expect("input error level must be valid"),
        )
        .layer(object_store::layers::TracingLayer)
        .layer(object_store::layers::PrometheusMetricsLayer::new(true))
        .finish();
    Ok(object_store)
}
