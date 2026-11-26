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
use object_store::services::Azblob;
use opendal_service_azure_common::azure_account_name_from_endpoint;

use crate::error::{self, Result};
use crate::object_store::{
    connection_value, ensure_server_credentials_allowed, finish_remote_backend,
};

const ENDPOINT: &str = "endpoint";
const ACCOUNT_NAME: &str = "account_name";
const ACCOUNT_KEY: &str = "account_key";
const SAS_TOKEN: &str = "sas_token";

pub fn is_supported_in_azblob(key: &str) -> bool {
    [ENDPOINT, ACCOUNT_NAME, ACCOUNT_KEY, SAS_TOKEN].contains(&key)
}

pub fn build_azblob_backend(
    container: &str,
    root: &str,
    connection: &HashMap<String, String>,
    remote_storage_access: &RemoteStorageAccess,
) -> Result<ObjectStore> {
    let mut builder = Azblob::default().root(root).container(container);

    if let Some(account_name) = connection.get(ACCOUNT_NAME) {
        builder = builder.account_name(account_name);
    }

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    let sas_token = connection_value(connection, SAS_TOKEN);
    let account_key = connection_value(connection, ACCOUNT_KEY);
    if let Some(account_key) = account_key {
        // The shared key is only used when the account name is known, the same
        // way the backend resolves it; otherwise the key would be ignored.
        let has_account_name = connection_value(connection, ACCOUNT_NAME).is_some()
            || connection_value(connection, ENDPOINT)
                .and_then(azure_account_name_from_endpoint)
                .is_some();
        if !has_account_name {
            return error::InvalidConnectionSnafu {
                msg: format!(
                    "{ACCOUNT_KEY} requires {ACCOUNT_NAME}, or an {ENDPOINT} that contains the account name"
                ),
            }
            .fail();
        }
        builder = builder.account_key(account_key);
    }
    if let Some(sas_token) = sas_token {
        builder = builder.sas_token(sas_token);
    }
    if account_key.is_none() && sas_token.is_none() {
        ensure_server_credentials_allowed(remote_storage_access, "AzBlob")?;
    }

    finish_remote_backend(builder, remote_storage_access)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_supported_in_azblob() {
        assert!(is_supported_in_azblob(ENDPOINT));
        assert!(is_supported_in_azblob(ACCOUNT_NAME));
        assert!(is_supported_in_azblob(ACCOUNT_KEY));
        assert!(is_supported_in_azblob(SAS_TOKEN));
        assert!(!is_supported_in_azblob("foo"));
    }
}
