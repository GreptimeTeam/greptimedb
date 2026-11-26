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

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use object_store::ObjectStore;
use object_store::remote_access::RemoteStorageAccess;
use object_store::services::Gcs;
use serde::Deserialize;

use crate::error::{self, Result};
use crate::object_store::{
    connection_value, ensure_server_credentials_allowed, finish_remote_backend,
};

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
    remote_storage_access: &RemoteStorageAccess,
) -> Result<ObjectStore> {
    let mut builder = Gcs::default().root(root).bucket(bucket);

    if let Some(scope) = connection.get(SCOPE) {
        builder = builder.scope(scope);
    }

    if let Some(credential) = connection_value(connection, CREDENTIAL) {
        let credential = service_account_credential(credential)?;
        // The rebuilt credential always yields a provider. Disabling config
        // load and VM metadata also empties the default chain behind it, in
        // case reqsign ever parses the credential differently.
        builder = builder
            .credential(&credential)
            .disable_config_load()
            .disable_vm_metadata();
    } else {
        ensure_server_credentials_allowed(remote_storage_access, "GCS")?;
    }

    if let Some(endpoint) = connection.get(ENDPOINT) {
        builder = builder.endpoint(endpoint);
    }

    finish_remote_backend(builder, remote_storage_access)
}

/// The credential file shape reqsign parses, limited to service account keys.
///
/// Other credential types make the server read local files, expand environment
/// variables or call URLs taken from the credential.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CredentialFile {
    ServiceAccount(ServiceAccountKey),
}

#[derive(Deserialize)]
struct ServiceAccountKey {
    private_key: String,
    client_email: String,
}

/// Validates a service account key and rebuilds it with only the fields used
/// for signing, so other fields such as `token_uri` never reach reqsign.
fn service_account_credential(credential: &str) -> Result<String> {
    // Errors must not include the decoded content, which holds a private key.
    let key = BASE64_STANDARD
        .decode(credential)
        .ok()
        .and_then(|decoded| serde_json::from_slice::<CredentialFile>(&decoded).ok())
        .map(|CredentialFile::ServiceAccount(key)| key)
        .filter(|key| !key.private_key.is_empty() && !key.client_email.is_empty());
    if let Some(key) = key {
        let rebuilt = serde_json::json!({
            "type": "service_account",
            "private_key": key.private_key,
            "client_email": key.client_email,
        });
        Ok(BASE64_STANDARD.encode(rebuilt.to_string()))
    } else {
        error::InvalidConnectionSnafu {
            msg: format!(
                "{CREDENTIAL} must be a base64-encoded service_account key with private_key and client_email"
            ),
        }
        .fail()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_account_credential() {
        let encode = |json: &str| BASE64_STANDARD.encode(json);
        let rebuilt = service_account_credential(&encode(
            r#"{"type":"service_account","private_key":"key","client_email":"a@b.iam.gserviceaccount.com","token_uri":"https://example.com"}"#,
        ))
        .unwrap();
        let rebuilt: serde_json::Value =
            serde_json::from_slice(&BASE64_STANDARD.decode(rebuilt).unwrap()).unwrap();
        assert_eq!(
            rebuilt,
            serde_json::json!({
                "type": "service_account",
                "private_key": "key",
                "client_email": "a@b.iam.gserviceaccount.com",
            })
        );
        for invalid in [
            "not base64".to_string(),
            encode("not json"),
            encode(r#"{"private_key":"key","client_email":"a@b"}"#),
            encode(r#"{"type":"service_account","private_key":"key"}"#),
            encode(r#"{"type":"service_account","private_key":"","client_email":"a@b"}"#),
            encode(
                r#"{"type":"external_account","audience":"a","subject_token_type":"t","token_url":"http://127.0.0.1/","credential_source":{"file":"/etc/passwd"}}"#,
            ),
            encode(
                r#"{"type":"authorized_user","client_id":"a","client_secret":"b","refresh_token":"c"}"#,
            ),
        ] {
            let err = service_account_credential(&invalid).unwrap_err();
            assert!(!err.to_string().contains("/etc/passwd"));
        }
    }

    #[test]
    fn test_is_supported_in_gcs() {
        assert!(is_supported_in_gcs(ENDPOINT));
        assert!(is_supported_in_gcs(CREDENTIAL));
        assert!(is_supported_in_gcs(SCOPE));
        assert!(!is_supported_in_gcs("foo"));
    }
}
