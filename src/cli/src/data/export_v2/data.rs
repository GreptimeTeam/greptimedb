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

use common_base::secrets::{ExposeSecret, SecretString};
use common_telemetry::info;
use object_store::util::{join_path, normalize_path};
use snafu::ResultExt;
use url::Url;

use crate::common::ObjectStoreConfig;
use crate::data::export_v2::error::{DatabaseSnafu, InvalidUriSnafu, Result, UrlParseSnafu};
use crate::data::export_v2::manifest::{DataFormat, TimeRange};
use crate::data::path::data_dir_for_schema_chunk;
use crate::data::snapshot_storage::StorageScheme;
use crate::data::sql::{escape_sql_identifier, escape_sql_literal};
use crate::database::DatabaseClient;

pub(super) struct CopyOptions {
    pub(super) format: DataFormat,
    pub(super) time_range: TimeRange,
    pub(super) parallelism: usize,
}

pub(super) struct CopyTarget {
    pub(super) location: String,
    pub(super) connection: String,
    secrets: Vec<Option<String>>,
}

pub(crate) struct CopySource {
    pub(crate) location: String,
    pub(crate) connection: String,
    secrets: Vec<Option<String>>,
}

impl CopyTarget {
    fn mask_sql(&self, sql: &str) -> String {
        mask_secrets(sql, &self.secrets)
    }
}

impl CopySource {
    fn mask_sql(&self, sql: &str) -> String {
        mask_secrets(sql, &self.secrets)
    }
}

pub(super) fn build_copy_target(
    snapshot_uri: &str,
    storage: &ObjectStoreConfig,
    schema: &str,
    chunk_id: u32,
) -> Result<CopyTarget> {
    let location = build_copy_location(snapshot_uri, storage, schema, chunk_id)?;
    Ok(CopyTarget {
        location: location.location,
        connection: location.connection,
        secrets: location.secrets,
    })
}

pub(crate) fn build_copy_source(
    snapshot_uri: &str,
    storage: &ObjectStoreConfig,
    schema: &str,
    chunk_id: u32,
) -> Result<CopySource> {
    let location = build_copy_location(snapshot_uri, storage, schema, chunk_id)?;
    Ok(CopySource {
        location: location.location,
        connection: location.connection,
        secrets: location.secrets,
    })
}

struct CopyLocation {
    location: String,
    connection: String,
    secrets: Vec<Option<String>>,
}

fn build_copy_location(
    snapshot_uri: &str,
    storage: &ObjectStoreConfig,
    schema: &str,
    chunk_id: u32,
) -> Result<CopyLocation> {
    let url = Url::parse(snapshot_uri).context(UrlParseSnafu)?;
    let scheme = StorageScheme::from_uri(snapshot_uri)?;
    let suffix = data_dir_for_schema_chunk(schema, chunk_id);

    match scheme {
        StorageScheme::File => {
            let root = url.to_file_path().map_err(|_| {
                InvalidUriSnafu {
                    uri: snapshot_uri,
                    reason: "file:// URI must use an absolute path like file:///tmp/backup",
                }
                .build()
            })?;
            let location = normalize_path(&format!("{}/{}", root.to_string_lossy(), suffix));
            Ok(CopyLocation {
                location,
                connection: String::new(),
                secrets: Vec::new(),
            })
        }
        StorageScheme::S3 => {
            let (bucket, root) = extract_bucket_root(&url, snapshot_uri)?;
            let location = format!("s3://{}/{}", bucket, join_root(&root, &suffix));
            let (connection, secrets) = build_s3_connection(storage);
            Ok(CopyLocation {
                location,
                connection,
                secrets,
            })
        }
        StorageScheme::Oss => {
            let (bucket, root) = extract_bucket_root(&url, snapshot_uri)?;
            let location = format!("oss://{}/{}", bucket, join_root(&root, &suffix));
            let (connection, secrets) = build_oss_connection(storage);
            Ok(CopyLocation {
                location,
                connection,
                secrets,
            })
        }
        StorageScheme::Gcs => {
            let (bucket, root) = extract_bucket_root(&url, snapshot_uri)?;
            let location = format!("gcs://{}/{}", bucket, join_root(&root, &suffix));
            let (connection, secrets) = build_gcs_connection(storage, snapshot_uri)?;
            Ok(CopyLocation {
                location,
                connection,
                secrets,
            })
        }
        StorageScheme::Azblob => {
            let (bucket, root) = extract_bucket_root(&url, snapshot_uri)?;
            let location = format!("azblob://{}/{}", bucket, join_root(&root, &suffix));
            let (connection, secrets) = build_azblob_connection(storage);
            Ok(CopyLocation {
                location,
                connection,
                secrets,
            })
        }
    }
}

pub(super) async fn execute_copy_database(
    database_client: &DatabaseClient,
    catalog: &str,
    schema: &str,
    target: &CopyTarget,
    options: &CopyOptions,
) -> Result<()> {
    let with_options = build_with_options(options);
    let sql = format!(
        r#"COPY DATABASE "{}"."{}" TO '{}' WITH ({}){};"#,
        escape_sql_identifier(catalog),
        escape_sql_identifier(schema),
        escape_sql_literal(&target.location),
        with_options,
        target.connection
    );
    let safe_sql = target.mask_sql(&sql);
    info!("Executing sql: {}", safe_sql);
    database_client
        .sql_in_public(&sql)
        .await
        .context(DatabaseSnafu)?;
    Ok(())
}

pub(crate) async fn execute_copy_database_from(
    database_client: &DatabaseClient,
    catalog: &str,
    schema: &str,
    source: &CopySource,
    format: DataFormat,
) -> Result<()> {
    let sql = format!(
        r#"COPY DATABASE "{}"."{}" FROM '{}' WITH (FORMAT='{}'){};"#,
        escape_sql_identifier(catalog),
        escape_sql_identifier(schema),
        escape_sql_literal(&source.location),
        format,
        source.connection
    );
    let safe_sql = source.mask_sql(&sql);
    info!("Executing sql: {}", safe_sql);
    database_client
        .sql_in_public(&sql)
        .await
        .context(DatabaseSnafu)?;
    Ok(())
}

fn build_with_options(options: &CopyOptions) -> String {
    let mut parts = vec![format!("FORMAT='{}'", options.format)];
    if let Some(start) = options.time_range.start {
        parts.push(format!(
            "START_TIME='{}'",
            escape_sql_literal(&start.to_rfc3339())
        ));
    }
    if let Some(end) = options.time_range.end {
        parts.push(format!(
            "END_TIME='{}'",
            escape_sql_literal(&end.to_rfc3339())
        ));
    }
    parts.push(format!("PARALLELISM={}", options.parallelism));
    parts.join(", ")
}

fn extract_bucket_root(url: &Url, snapshot_uri: &str) -> Result<(String, String)> {
    let bucket = url.host_str().unwrap_or("").to_string();
    if bucket.is_empty() {
        return InvalidUriSnafu {
            uri: snapshot_uri,
            reason: "URI must include bucket/container in host",
        }
        .fail();
    }
    let root = url
        .path()
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();
    Ok((bucket, root))
}

fn join_root(root: &str, suffix: &str) -> String {
    join_path(root, suffix).trim_start_matches('/').to_string()
}

fn build_s3_connection(storage: &ObjectStoreConfig) -> (String, Vec<Option<String>>) {
    let access_key_id = expose_optional_secret(&storage.s3.s3_access_key_id);
    let secret_access_key = expose_optional_secret(&storage.s3.s3_secret_access_key);

    let mut options = Vec::new();
    if let Some(access_key_id) = &access_key_id {
        options.push(format!(
            "ACCESS_KEY_ID='{}'",
            escape_sql_literal(access_key_id)
        ));
    }
    if let Some(secret_access_key) = &secret_access_key {
        options.push(format!(
            "SECRET_ACCESS_KEY='{}'",
            escape_sql_literal(secret_access_key)
        ));
    }
    if let Some(region) = &storage.s3.s3_region {
        options.push(format!("REGION='{}'", escape_sql_literal(region)));
    }
    if let Some(endpoint) = &storage.s3.s3_endpoint {
        options.push(format!("ENDPOINT='{}'", escape_sql_literal(endpoint)));
    }

    let secrets = vec![access_key_id, secret_access_key];
    let connection = if options.is_empty() {
        String::new()
    } else {
        format!(" CONNECTION ({})", options.join(", "))
    };
    (connection, secrets)
}

fn build_oss_connection(storage: &ObjectStoreConfig) -> (String, Vec<Option<String>>) {
    let access_key_id = expose_optional_secret(&storage.oss.oss_access_key_id);
    let access_key_secret = expose_optional_secret(&storage.oss.oss_access_key_secret);

    let mut options = Vec::new();
    if let Some(access_key_id) = &access_key_id {
        options.push(format!(
            "ACCESS_KEY_ID='{}'",
            escape_sql_literal(access_key_id)
        ));
    }
    if let Some(access_key_secret) = &access_key_secret {
        options.push(format!(
            "ACCESS_KEY_SECRET='{}'",
            escape_sql_literal(access_key_secret)
        ));
    }
    if !storage.oss.oss_endpoint.is_empty() {
        options.push(format!(
            "ENDPOINT='{}'",
            escape_sql_literal(&storage.oss.oss_endpoint)
        ));
    }

    let secrets = vec![access_key_id, access_key_secret];
    let connection = if options.is_empty() {
        String::new()
    } else {
        format!(" CONNECTION ({})", options.join(", "))
    };
    (connection, secrets)
}

fn build_gcs_connection(
    storage: &ObjectStoreConfig,
    snapshot_uri: &str,
) -> Result<(String, Vec<Option<String>>)> {
    let credential_path = expose_optional_secret(&storage.gcs.gcs_credential_path);
    let credential = expose_optional_secret(&storage.gcs.gcs_credential);

    if credential.is_none() && credential_path.is_some() {
        return InvalidUriSnafu {
            uri: snapshot_uri,
            reason: "gcs_credential_path is not supported for server-side COPY; provide gcs_credential or rely on server-side ADC",
        }
        .fail();
    }

    let mut options = Vec::new();
    if let Some(credential) = &credential {
        options.push(format!("CREDENTIAL='{}'", escape_sql_literal(credential)));
    }
    if !storage.gcs.gcs_scope.is_empty() {
        options.push(format!(
            "SCOPE='{}'",
            escape_sql_literal(&storage.gcs.gcs_scope)
        ));
    }
    if !storage.gcs.gcs_endpoint.is_empty() {
        options.push(format!(
            "ENDPOINT='{}'",
            escape_sql_literal(&storage.gcs.gcs_endpoint)
        ));
    }

    let connection = if options.is_empty() {
        String::new()
    } else {
        format!(" CONNECTION ({})", options.join(", "))
    };
    let secrets = vec![credential_path, credential];
    Ok((connection, secrets))
}

fn build_azblob_connection(storage: &ObjectStoreConfig) -> (String, Vec<Option<String>>) {
    let account_name = expose_optional_secret(&storage.azblob.azblob_account_name);
    let account_key = expose_optional_secret(&storage.azblob.azblob_account_key);
    let sas_token = storage.azblob.azblob_sas_token.clone();

    let mut options = Vec::new();
    if let Some(account_name) = &account_name {
        options.push(format!(
            "ACCOUNT_NAME='{}'",
            escape_sql_literal(account_name)
        ));
    }
    if let Some(account_key) = &account_key {
        options.push(format!("ACCOUNT_KEY='{}'", escape_sql_literal(account_key)));
    }
    if let Some(sas_token) = &sas_token {
        options.push(format!("SAS_TOKEN='{}'", escape_sql_literal(sas_token)));
    }
    if !storage.azblob.azblob_endpoint.is_empty() {
        options.push(format!(
            "ENDPOINT='{}'",
            escape_sql_literal(&storage.azblob.azblob_endpoint)
        ));
    }

    let secrets = vec![account_name, account_key, sas_token];
    let connection = if options.is_empty() {
        String::new()
    } else {
        format!(" CONNECTION ({})", options.join(", "))
    };
    (connection, secrets)
}

fn expose_optional_secret(secret: &Option<SecretString>) -> Option<String> {
    secret.as_ref().map(|s| s.expose_secret().to_owned())
}

fn mask_secrets(sql: &str, secrets: &[Option<String>]) -> String {
    let mut masked = sql.to_string();
    for secret in secrets {
        if let Some(secret) = secret
            && !secret.is_empty()
        {
            masked = masked.replace(secret, "[REDACTED]");
        }
    }
    masked
}

#[cfg(test)]
mod tests {
    use common_base::secrets::SecretString;

    use super::*;
    use crate::common::{PrefixedAzblobConnection, PrefixedGcsConnection, PrefixedOssConnection};

    #[test]
    fn test_build_oss_connection_includes_endpoint() {
        let storage = ObjectStoreConfig {
            oss: PrefixedOssConnection {
                oss_endpoint: "https://oss.example.com".to_string(),
                oss_access_key_id: Some(SecretString::from("key_id".to_string())),
                oss_access_key_secret: Some(SecretString::from("key_secret".to_string())),
                ..Default::default()
            },
            ..Default::default()
        };

        let (connection, _) = build_oss_connection(&storage);
        assert!(connection.contains("ENDPOINT='https://oss.example.com'"));
    }

    #[test]
    fn test_build_gcs_connection_uses_scope_and_inline_credential() {
        let storage = ObjectStoreConfig {
            gcs: PrefixedGcsConnection {
                gcs_scope: "scope-a".to_string(),
                gcs_endpoint: "https://storage.googleapis.com".to_string(),
                gcs_credential: Some(SecretString::from("credential-json".to_string())),
                ..Default::default()
            },
            ..Default::default()
        };

        let (connection, _) = build_gcs_connection(&storage, "gcs://bucket/root").unwrap();
        assert!(connection.contains("CREDENTIAL='credential-json'"));
        assert!(connection.contains("SCOPE='scope-a'"));
        assert!(connection.contains("ENDPOINT='https://storage.googleapis.com'"));
        assert!(!connection.contains("CREDENTIAL_PATH"));
    }

    #[test]
    fn test_build_gcs_connection_rejects_credential_path_only() {
        let storage = ObjectStoreConfig {
            gcs: PrefixedGcsConnection {
                gcs_scope: "scope-a".to_string(),
                gcs_credential_path: Some(SecretString::from("/tmp/creds.json".to_string())),
                ..Default::default()
            },
            ..Default::default()
        };

        let error = build_gcs_connection(&storage, "gcs://bucket/root")
            .expect_err("credential_path-only should be rejected")
            .to_string();
        assert!(error.contains("gcs_credential_path is not supported"));
    }

    #[test]
    fn test_build_azblob_connection_includes_endpoint() {
        let storage = ObjectStoreConfig {
            azblob: PrefixedAzblobConnection {
                azblob_account_name: Some(SecretString::from("account".to_string())),
                azblob_account_key: Some(SecretString::from("key".to_string())),
                azblob_endpoint: "https://blob.example.com".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        let (connection, _) = build_azblob_connection(&storage);
        assert!(connection.contains("ENDPOINT='https://blob.example.com'"));
    }

    #[test]
    fn test_build_azblob_connection_redacts_sas_token() {
        let storage = ObjectStoreConfig {
            azblob: PrefixedAzblobConnection {
                azblob_account_name: Some(SecretString::from("account".to_string())),
                azblob_account_key: Some(SecretString::from("key".to_string())),
                azblob_sas_token: Some("sig=secret-token".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        let (connection, secrets) = build_azblob_connection(&storage);
        let masked = mask_secrets(&connection, &secrets);

        assert!(connection.contains("SAS_TOKEN='sig=secret-token'"));
        assert!(masked.contains("SAS_TOKEN='[REDACTED]'"));
        assert!(!masked.contains("sig=secret-token"));
    }

    #[test]
    fn test_build_copy_target_decodes_file_uri_path() {
        let storage = ObjectStoreConfig::default();
        let target = build_copy_target("file:///tmp/my%20backup", &storage, "public", 7)
            .expect("file:// copy target should be built");

        assert_eq!(target.location, "/tmp/my backup/data/public/7/");
    }
}
