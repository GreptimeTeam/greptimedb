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
use snafu::ResultExt;

use crate::error::{self, Result};

const ROOT: &str = "root";
const BUCKET: &str = "bucket";
const ENDPOINT: &str = "endpoint";
const CREDENTIAL: &str = "credential";
const PREDEFINED_ACL: &str = "predefined_acl";
const DEFAULT_STORAGE_CLASS: &str = "default_storage_class";

pub fn is_supported_in_gcs(key: &str) -> bool {
    [
        ENDPOINT,
        ROOT,
        BUCKET,
        CREDENTIAL,
        PREDEFINED_ACL,
        DEFAULT_STORAGE_CLASS,
    ]
    .contains(&key)
}

/// Build a gcs backend using the provided bucket, root, and connection parameters.
pub fn build_gcs_backend(
    bucket: &str,
    root: &str,
    connection: &HashMap<String, String>,
) -> Result<ObjectStore> {
    let mut builder = Gcs::default().bucket(bucket).root(root);

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    if let Some(credential) = connection.get(CREDENTIAL) {
        builder = builder.credential(credential);
    }

    if let Some(predefined_acl) = connection.get(PREDEFINED_ACL) {
        builder = builder.predefined_acl(predefined_acl);
    }

    if let Some(default_storage_class) = connection.get(DEFAULT_STORAGE_CLASS) {
        builder = builder.default_storage_class(default_storage_class);
    }

    let op = ObjectStore::new(builder)
        .context(error::BuildBackendSnafu)?
        .layer(
            object_store::layers::RetryLayer::new()
                .with_jitter()
                .with_notify(object_store::util::PrintDetailedError),
        )
        .layer(object_store::layers::LoggingLayer::default())
        .layer(object_store::layers::TracingLayer)
        .layer(object_store::layers::build_prometheus_metrics_layer(true))
        .finish();

    Ok(op)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_in_gcs() {
        assert!(is_supported_in_gcs(ENDPOINT));
        assert!(is_supported_in_gcs(ROOT));
        assert!(is_supported_in_gcs(BUCKET));
        assert!(is_supported_in_gcs(CREDENTIAL));
        assert!(is_supported_in_gcs(PREDEFINED_ACL));
        assert!(is_supported_in_gcs(DEFAULT_STORAGE_CLASS));
        assert!(!is_supported_in_gcs("hahahhah"))
    }

    #[test]
    fn test_build_gcs_backend() {
        let mut connection = HashMap::new();

        connection.insert(
            ENDPOINT.to_string(),
            "https://storage.googleapis.com".to_string(),
        );
        connection.insert(CREDENTIAL.to_string(), "authentication token".to_string());
        connection.insert(PREDEFINED_ACL.to_string(), "publicRead".to_string());
        connection.insert(DEFAULT_STORAGE_CLASS.to_string(), "STANDARD".to_string());

        let result = build_gcs_backend("my-bucket", "my-root", &connection);
        assert!(result.is_ok());
    }
}
