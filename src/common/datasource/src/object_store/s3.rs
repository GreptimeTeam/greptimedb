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

use object_store::services::S3;
use object_store::ObjectStore;
use snafu::ResultExt;

use crate::error::{self, Result};

const ENDPOINT: &str = "endpoint";
const ACCESS_KEY_ID: &str = "access_key_id";
const SECRET_ACCESS_KEY: &str = "secret_access_key";
const SESSION_TOKEN: &str = "session_token";
const REGION: &str = "region";
const ENABLE_VIRTUAL_HOST_STYLE: &str = "enable_virtual_host_style";

pub fn is_supported_in_s3(key: &str) -> bool {
    [
        ENDPOINT,
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        REGION,
        ENABLE_VIRTUAL_HOST_STYLE,
    ]
    .contains(&key)
}

pub fn build_s3_backend(
    host: &str,
    path: &str,
    connection: &HashMap<String, String>,
) -> Result<ObjectStore> {
    let mut builder = S3::default().root(path).bucket(host);

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    if let Some(region) = connection.get(REGION) {
        builder = builder.region(region);
    }

    if let Some(key_id) = connection.get(ACCESS_KEY_ID) {
        builder = builder.access_key_id(key_id);
    }

    if let Some(key) = connection.get(SECRET_ACCESS_KEY) {
        builder = builder.secret_access_key(key);
    }

    if let Some(session_token) = connection.get(SESSION_TOKEN) {
        builder = builder.session_token(session_token);
    }

    if let Some(enable_str) = connection.get(ENABLE_VIRTUAL_HOST_STYLE) {
        let enable = enable_str.as_str().parse::<bool>().map_err(|e| {
            error::InvalidConnectionSnafu {
                msg: format!(
                    "failed to parse the option {}={}, {}",
                    ENABLE_VIRTUAL_HOST_STYLE, enable_str, e
                ),
            }
            .build()
        })?;
        if enable {
            builder = builder.enable_virtual_host_style();
        }
    }

    // TODO(weny): Consider finding a better way to eliminate duplicate code.
    Ok(ObjectStore::new(builder)
        .context(error::BuildBackendSnafu)?
        .layer(
            object_store::layers::LoggingLayer::default()
                // Print the expected error only in DEBUG level.
                // See https://docs.rs/opendal/latest/opendal/layers/struct.LoggingLayer.html#method.with_error_level
                .with_error_level(Some("debug"))
                .expect("input error level must be valid"),
        )
        .layer(object_store::layers::TracingLayer)
        .layer(object_store::layers::PrometheusMetricsLayer::new(true))
        .finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_is_supported_in_s3() {
        assert!(is_supported_in_s3(ENDPOINT));
        assert!(is_supported_in_s3(ACCESS_KEY_ID));
        assert!(is_supported_in_s3(SECRET_ACCESS_KEY));
        assert!(is_supported_in_s3(SESSION_TOKEN));
        assert!(is_supported_in_s3(REGION));
        assert!(is_supported_in_s3(ENABLE_VIRTUAL_HOST_STYLE));
        assert!(!is_supported_in_s3("foo"))
    }
}
