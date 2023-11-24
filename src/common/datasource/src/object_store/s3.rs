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

use object_store::services::{Oss, S3};
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
    key == ENDPOINT
        || key == ACCESS_KEY_ID
        || key == SECRET_ACCESS_KEY
        || key == SESSION_TOKEN
        || key == REGION
        || key == ENABLE_VIRTUAL_HOST_STYLE
}

fn build_aws_backend(
    host: &str,
    path: &str,
    connection: &HashMap<String, String>,
) -> Result<ObjectStore> {
    let mut builder = S3::default();

    let _ = builder.root(path).bucket(host);

    if let Some(endpoint) = connection.get(ENDPOINT) {
        let _ = builder.endpoint(endpoint);
    }

    if let Some(region) = connection.get(REGION) {
        let _ = builder.region(region);
    }

    if let Some(key_id) = connection.get(ACCESS_KEY_ID) {
        let _ = builder.access_key_id(key_id);
    }

    if let Some(key) = connection.get(SECRET_ACCESS_KEY) {
        let _ = builder.secret_access_key(key);
    }

    if let Some(session_token) = connection.get(SESSION_TOKEN) {
        let _ = builder.security_token(session_token);
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
            let _ = builder.enable_virtual_host_style();
        }
    }

    Ok(ObjectStore::new(builder)
        .context(error::BuildBackendSnafu)?
        .finish())
}

fn build_aliyun_backend(
    host: &str,
    path: &str,
    connection: &HashMap<String, String>,
) -> Result<ObjectStore> {
    let mut builder = Oss::default();
    let _ = builder.root(path).bucket(host);

    if let Some(endpoint) = connection.get(ENDPOINT) {
        let _ = builder.endpoint(endpoint);
    }

    if let Some(key_id) = connection.get(ACCESS_KEY_ID) {
        let _ = builder.access_key_id(key_id);
    }

    if let Some(key) = connection.get(SECRET_ACCESS_KEY) {
        builder.access_key_secret(key);
    }

    Ok(ObjectStore::new(builder)
        .context(error::BuildBackendSnafu)?
        .finish())
}

pub fn build_s3_backend(
    host: &str,
    path: &str,
    connection: &HashMap<String, String>,
) -> Result<ObjectStore> {
    if let Some(endpoint) = connection.get(ENDPOINT) {
        if endpoint.ends_with("aliyuncs.com") {
            return build_aliyun_backend(host, path, connection);
        } else {
            return build_aws_backend(host, path, connection);
        }
    } else {
        return build_aws_backend(host, path, connection);
    }
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
