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

use std::collections::HashMap;

use object_store::ObjectStore;
use object_store::services::Gcs;
use object_store::util::DefaultLoggingInterceptor;
use snafu::ResultExt;

use crate::error::{self, Result};

const ENDPOINT: &str = "endpoint";
const CREDENTIAL: &str = "credential";
const SCOPE: &str = "scope";

pub fn is_supported_in_gcs(key: &str) -> bool {
    [ENDPOINT, CREDENTIAL, SCOPE].contains(&key)
}

pub fn build_gcs_backend(
    bucket: &str,
    root: &str,
    connection: &HashMap<String, String>,
) -> Result<ObjectStore> {
    let mut builder = Gcs::default().root(root).bucket(bucket);

    if let Some(scope) = connection.get(SCOPE) {
        builder = builder.scope(scope);
    }

    if let Some(credential) = connection.get(CREDENTIAL) {
        builder = builder.credential(credential);
    }

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    Ok(ObjectStore::new(builder)
        .context(error::BuildBackendSnafu)?
        .layer(
            object_store::layers::RetryLayer::new()
                .with_jitter()
                .with_notify(object_store::util::PrintDetailedError),
        )
        .layer(object_store::layers::LoggingLayer::new(
            DefaultLoggingInterceptor,
        ))
        .layer(object_store::layers::TracingLayer)
        .layer(object_store::layers::build_prometheus_metrics_layer(true))
        .finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_in_gcs() {
        assert!(is_supported_in_gcs(ENDPOINT));
        assert!(is_supported_in_gcs(CREDENTIAL));
        assert!(is_supported_in_gcs(SCOPE));
        assert!(!is_supported_in_gcs("foo"));
    }
}
