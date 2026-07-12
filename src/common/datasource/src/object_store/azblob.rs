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
use object_store::services::Azblob;
use object_store::util::{with_instrument_layers, with_retry_layers};
use snafu::ResultExt;

use crate::error::{self, Result};

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
) -> Result<ObjectStore> {
    let mut builder = Azblob::default().root(root).container(container);

    if let Some(account_name) = connection.get(ACCOUNT_NAME) {
        builder = builder.account_name(account_name);
    }

    if let Some(account_key) = connection.get(ACCOUNT_KEY) {
        builder = builder.account_key(account_key);
    }

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    if let Some(sas_token) = connection.get(SAS_TOKEN) {
        builder = builder.sas_token(sas_token);
    }

    let object_store = ObjectStore::new(builder)
        .context(error::BuildBackendSnafu)?
        .finish();
    Ok(with_instrument_layers(
        with_retry_layers(object_store),
        true,
    ))
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
