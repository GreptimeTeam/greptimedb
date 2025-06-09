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

use object_store::services::Oss;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{self, Result};

const BUCKET: &str = "bucket";
const ENDPOINT: &str = "endpoint";
const ACCESS_KEY_ID: &str = "access_key_id";
const ACCESS_KEY_SECRET: &str = "access_key_secret";
const ROOT: &str = "root";
const ALLOW_ANONYMOUS: &str = "allow_anonymous";

/// Check if the key is supported in OSS configuration.
pub fn is_supported_in_oss(key: &str) -> bool {
    [
        ROOT,
        ALLOW_ANONYMOUS,
        BUCKET,
        ENDPOINT,
        ACCESS_KEY_ID,
        ACCESS_KEY_SECRET,
    ]
    .contains(&key)
}

/// Build an OSS backend using the provided bucket, root, and connection parameters.
pub fn build_oss_backend(
    bucket: &str,
    root: &str,
    connection: &HashMap<String, String>,
) -> Result<ObjectStore> {
    let mut builder = Oss::default().bucket(bucket).root(root);

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    if let Some(access_key_id) = connection.get(ACCESS_KEY_ID) {
        builder = builder.access_key_id(access_key_id);
    }

    if let Some(access_key_secret) = connection.get(ACCESS_KEY_SECRET) {
        builder = builder.access_key_secret(access_key_secret);
    }

    if let Some(allow_anonymous) = connection.get(ALLOW_ANONYMOUS) {
        let allow = allow_anonymous.as_str().parse::<bool>().map_err(|e| {
            error::InvalidConnectionSnafu {
                msg: format!(
                    "failed to parse the option {}={}, {}",
                    ALLOW_ANONYMOUS, allow_anonymous, e
                ),
            }
            .build()
        })?;
        if allow {
            builder = builder.allow_anonymous();
        }
    }

    let op = ObjectStore::new(builder)
        .context(error::BuildBackendSnafu)?
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
    fn test_is_supported_in_oss() {
        assert!(is_supported_in_oss(ROOT));
        assert!(is_supported_in_oss(ALLOW_ANONYMOUS));
        assert!(is_supported_in_oss(BUCKET));
        assert!(is_supported_in_oss(ENDPOINT));
        assert!(is_supported_in_oss(ACCESS_KEY_ID));
        assert!(is_supported_in_oss(ACCESS_KEY_SECRET));
        assert!(!is_supported_in_oss("foo"));
        assert!(!is_supported_in_oss("BAR"));
    }

    #[test]
    fn test_build_oss_backend_all_fields_valid() {
        let mut connection = HashMap::new();
        connection.insert(
            ENDPOINT.to_string(),
            "http://oss-ap-southeast-1.aliyuncs.com".to_string(),
        );
        connection.insert(ACCESS_KEY_ID.to_string(), "key_id".to_string());
        connection.insert(ACCESS_KEY_SECRET.to_string(), "key_secret".to_string());
        connection.insert(ALLOW_ANONYMOUS.to_string(), "true".to_string());

        let result = build_oss_backend("my-bucket", "my-root", &connection);
        assert!(result.is_ok());
    }
}
