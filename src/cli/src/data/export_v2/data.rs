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
use snafu::ResultExt;
use url::Url;

use super::error::{DatabaseSnafu, InvalidUriSnafu, Result, UrlParseSnafu};
use super::manifest::{DataFormat, TimeRange};
use super::storage::StorageScheme;
use crate::common::ObjectStoreConfig;
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
    let suffix = format!("data/{schema}/{chunk_id}/");

    match scheme {
        StorageScheme::File => {
            let root = url.path().trim_end_matches('/');
            let location = format!("{}/{}", root, suffix);
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
            let (connection, secrets) = build_gcs_connection(storage);
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
        catalog, schema, target.location, with_options, target.connection
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
        catalog, schema, source.location, format, source.connection
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
        parts.push(format!("START_TIME='{}'", start.to_rfc3339()));
    }
    if let Some(end) = options.time_range.end {
        parts.push(format!("END_TIME='{}'", end.to_rfc3339()));
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
    if root.is_empty() {
        suffix.to_string()
    } else {
        format!(
            "{}/{}",
            root.trim_end_matches('/'),
            suffix.trim_start_matches('/')
        )
    }
}

fn build_s3_connection(storage: &ObjectStoreConfig) -> (String, Vec<Option<String>>) {
    let access_key_id = expose_optional_secret(&storage.s3.s3_access_key_id);
    let secret_access_key = expose_optional_secret(&storage.s3.s3_secret_access_key);

    let mut options = Vec::new();
    if let Some(access_key_id) = &access_key_id
        && !access_key_id.is_empty()
    {
        options.push(format!("ACCESS_KEY_ID='{}'", access_key_id));
    }
    if let Some(secret_access_key) = &secret_access_key
        && !secret_access_key.is_empty()
    {
        options.push(format!("SECRET_ACCESS_KEY='{}'", secret_access_key));
    }
    if let Some(region) = &storage.s3.s3_region {
        options.push(format!("REGION='{}'", region));
    }
    if let Some(endpoint) = &storage.s3.s3_endpoint {
        options.push(format!("ENDPOINT='{}'", endpoint));
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
    if let Some(access_key_id) = &access_key_id
        && !access_key_id.is_empty()
    {
        options.push(format!("ACCESS_KEY_ID='{}'", access_key_id));
    }
    if let Some(access_key_secret) = &access_key_secret
        && !access_key_secret.is_empty()
    {
        options.push(format!("ACCESS_KEY_SECRET='{}'", access_key_secret));
    }

    let secrets = vec![access_key_id, access_key_secret];
    let connection = if options.is_empty() {
        String::new()
    } else {
        format!(" CONNECTION ({})", options.join(", "))
    };
    (connection, secrets)
}

fn build_gcs_connection(storage: &ObjectStoreConfig) -> (String, Vec<Option<String>>) {
    let credential_path = expose_optional_secret(&storage.gcs.gcs_credential_path);
    let credential = expose_optional_secret(&storage.gcs.gcs_credential);

    let mut options = Vec::new();
    if let Some(credential_path) = &credential_path
        && !credential_path.is_empty()
    {
        options.push(format!("CREDENTIAL_PATH='{}'", credential_path));
    }
    if let Some(credential) = &credential
        && !credential.is_empty()
    {
        options.push(format!("CREDENTIAL='{}'", credential));
    }
    if !storage.gcs.gcs_endpoint.is_empty() {
        options.push(format!("ENDPOINT='{}'", storage.gcs.gcs_endpoint));
    }

    let connection = if options.is_empty() {
        String::new()
    } else {
        format!(" CONNECTION ({})", options.join(", "))
    };
    let secrets = vec![credential_path, credential];
    (connection, secrets)
}

fn build_azblob_connection(storage: &ObjectStoreConfig) -> (String, Vec<Option<String>>) {
    let account_name = expose_optional_secret(&storage.azblob.azblob_account_name);
    let account_key = expose_optional_secret(&storage.azblob.azblob_account_key);

    let mut options = Vec::new();
    if let Some(account_name) = &account_name
        && !account_name.is_empty()
    {
        options.push(format!("ACCOUNT_NAME='{}'", account_name));
    }
    if let Some(account_key) = &account_key
        && !account_key.is_empty()
    {
        options.push(format!("ACCOUNT_KEY='{}'", account_key));
    }
    if let Some(sas_token) = &storage.azblob.azblob_sas_token {
        options.push(format!("SAS_TOKEN='{}'", sas_token));
    }

    let secrets = vec![account_name, account_key];
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
