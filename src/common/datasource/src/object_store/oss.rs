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
use object_store::remote_access::RemoteStorageAccess;
use object_store::services::Oss;

use crate::error::{self, Result};
use crate::object_store::{
    connection_value, ensure_server_credentials_allowed, finish_remote_backend,
};

const BUCKET: &str = "bucket";
const ENDPOINT: &str = "endpoint";
const ACCESS_KEY_ID: &str = "access_key_id";
const ACCESS_KEY_SECRET: &str = "access_key_secret";
const ROOT: &str = "root";
const ALLOW_ANONYMOUS: &str = "allow_anonymous";
const SKIP_SIGNATURE: &str = "skip_signature";

/// Check if the key is supported in OSS configuration.
pub fn is_supported_in_oss(key: &str) -> bool {
    [
        ROOT,
        ALLOW_ANONYMOUS,
        SKIP_SIGNATURE,
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
    remote_storage_access: &RemoteStorageAccess,
) -> Result<ObjectStore> {
    let mut builder = Oss::default().bucket(bucket).root(root);

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    let mut skip_signature = false;
    if let Some((key, value)) = connection
        .get(SKIP_SIGNATURE)
        .map(|value| (SKIP_SIGNATURE, value))
        .or_else(|| {
            connection
                .get(ALLOW_ANONYMOUS)
                .map(|value| (ALLOW_ANONYMOUS, value))
        })
    {
        skip_signature = value.as_str().parse::<bool>().map_err(|e| {
            error::InvalidConnectionSnafu {
                msg: format!("failed to parse the option {}={}, {}", key, value, e),
            }
            .build()
        })?;
        if skip_signature {
            builder = builder.skip_signature();
        }
    }

    if !skip_signature {
        match (
            connection_value(connection, ACCESS_KEY_ID),
            connection_value(connection, ACCESS_KEY_SECRET),
        ) {
            (Some(access_key_id), Some(access_key_secret)) => {
                builder = builder
                    .access_key_id(access_key_id)
                    .access_key_secret(access_key_secret);
            }
            (None, None) => ensure_server_credentials_allowed(remote_storage_access, "OSS")?,
            _ => {
                return error::InvalidConnectionSnafu {
                    msg: format!("{ACCESS_KEY_ID} and {ACCESS_KEY_SECRET} must be set together"),
                }
                .fail();
            }
        }
    }

    finish_remote_backend(builder, remote_storage_access)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_in_oss() {
        assert!(is_supported_in_oss(ROOT));
        assert!(is_supported_in_oss(ALLOW_ANONYMOUS));
        assert!(is_supported_in_oss(SKIP_SIGNATURE));
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

        let result = build_oss_backend(
            "my-bucket",
            "my-root",
            &connection,
            &RemoteStorageAccess::unrestricted(),
        );
        assert!(result.is_ok());
    }
}
