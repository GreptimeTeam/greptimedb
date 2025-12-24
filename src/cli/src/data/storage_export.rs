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

use std::path::PathBuf;

use common_base::secrets::{ExposeSecret, SecretString};
use common_error::ext::BoxedError;

use crate::common::{
    PrefixedAzblobConnection, PrefixedGcsConnection, PrefixedOssConnection, PrefixedS3Connection,
};

/// Helper function to extract secret string from Option<SecretString>.
/// Returns empty string if None.
fn expose_optional_secret(secret: &Option<SecretString>) -> &str {
    secret
        .as_ref()
        .map(|s| s.expose_secret().as_str())
        .unwrap_or("")
}

/// Helper function to format root path with leading slash if non-empty.
fn format_root_path(root: &str) -> String {
    if root.is_empty() {
        String::new()
    } else {
        format!("/{}", root)
    }
}

/// Helper function to mask multiple secrets in a string.
fn mask_secrets(mut sql: String, secrets: &[&str]) -> String {
    for secret in secrets {
        if !secret.is_empty() {
            sql = sql.replace(secret, "[REDACTED]");
        }
    }
    sql
}

/// Helper function to format storage URI.
fn format_uri(scheme: &str, bucket: &str, root: &str, path: &str) -> String {
    let root = format_root_path(root);
    format!("{}://{}{}/{}", scheme, bucket, root, path)
}

/// Trait for storage backends that can be used for data export.
pub trait StorageExport: Send + Sync {
    /// Generate the storage path for COPY DATABASE command.
    /// Returns (path, connection_string) where connection_string includes CONNECTION clause.
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String);

    /// Format the output path for logging purposes.
    fn format_output_path(&self, file_path: &str) -> String;

    /// Mask sensitive information in SQL commands for safe logging.
    fn mask_sensitive_info(&self, sql: &str) -> String;
}

macro_rules! define_backend {
    ($name:ident, $config:ty) => {
        #[derive(Clone)]
        pub struct $name {
            config: $config,
        }

        impl $name {
            pub fn new(config: $config) -> Result<Self, BoxedError> {
                config.validate()?;
                Ok(Self { config })
            }
        }
    };
}

/// Local file system storage backend.
#[derive(Clone)]
pub struct FsBackend {
    output_dir: String,
}

impl FsBackend {
    pub fn new(output_dir: String) -> Self {
        Self { output_dir }
    }
}

impl StorageExport for FsBackend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        if self.output_dir.is_empty() {
            unreachable!("output_dir must be set when not using remote storage")
        }
        let path = PathBuf::from(&self.output_dir)
            .join(catalog)
            .join(format!("{schema}/"))
            .to_string_lossy()
            .to_string();
        (path, String::new())
    }

    fn format_output_path(&self, file_path: &str) -> String {
        format!("{}/{}", self.output_dir, file_path)
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        sql.to_string()
    }
}

define_backend!(S3Backend, PrefixedS3Connection);

impl StorageExport for S3Backend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let s3_path = format_uri(
            "s3",
            &self.config.s3_bucket,
            &self.config.s3_root,
            &format!("{}/{}/", catalog, schema),
        );

        let mut connection_options = vec![
            format!(
                "ACCESS_KEY_ID='{}'",
                expose_optional_secret(&self.config.s3_access_key_id)
            ),
            format!(
                "SECRET_ACCESS_KEY='{}'",
                expose_optional_secret(&self.config.s3_secret_access_key)
            ),
        ];

        if let Some(region) = &self.config.s3_region {
            connection_options.push(format!("REGION='{}'", region));
        }

        if let Some(endpoint) = &self.config.s3_endpoint {
            connection_options.push(format!("ENDPOINT='{}'", endpoint));
        }

        let connection_str = format!(" CONNECTION ({})", connection_options.join(", "));
        (s3_path, connection_str)
    }

    fn format_output_path(&self, file_path: &str) -> String {
        format_uri(
            "s3",
            &self.config.s3_bucket,
            &self.config.s3_root,
            file_path,
        )
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        mask_secrets(
            sql.to_string(),
            &[
                expose_optional_secret(&self.config.s3_access_key_id),
                expose_optional_secret(&self.config.s3_secret_access_key),
            ],
        )
    }
}

define_backend!(OssBackend, PrefixedOssConnection);

impl StorageExport for OssBackend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let oss_path = format_uri(
            "oss",
            &self.config.oss_bucket,
            &self.config.oss_root,
            &format!("{}/{}/", catalog, schema),
        );

        let connection_options = [
            format!(
                "ACCESS_KEY_ID='{}'",
                expose_optional_secret(&self.config.oss_access_key_id)
            ),
            format!(
                "ACCESS_KEY_SECRET='{}'",
                expose_optional_secret(&self.config.oss_access_key_secret)
            ),
        ];

        let connection_str = format!(" CONNECTION ({})", connection_options.join(", "));
        (oss_path, connection_str)
    }

    fn format_output_path(&self, file_path: &str) -> String {
        format_uri(
            "oss",
            &self.config.oss_bucket,
            &self.config.oss_root,
            file_path,
        )
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        mask_secrets(
            sql.to_string(),
            &[
                expose_optional_secret(&self.config.oss_access_key_id),
                expose_optional_secret(&self.config.oss_access_key_secret),
            ],
        )
    }
}

define_backend!(GcsBackend, PrefixedGcsConnection);

impl StorageExport for GcsBackend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let gcs_path = format_uri(
            "gcs",
            &self.config.gcs_bucket,
            &self.config.gcs_root,
            &format!("{}/{}/", catalog, schema),
        );

        let mut connection_options = Vec::new();

        let credential_path = expose_optional_secret(&self.config.gcs_credential_path);
        if !credential_path.is_empty() {
            connection_options.push(format!("CREDENTIAL_PATH='{}'", credential_path));
        }

        let credential = expose_optional_secret(&self.config.gcs_credential);
        if !credential.is_empty() {
            connection_options.push(format!("CREDENTIAL='{}'", credential));
        }

        if !self.config.gcs_endpoint.is_empty() {
            connection_options.push(format!("ENDPOINT='{}'", self.config.gcs_endpoint));
        }

        let connection_str = if connection_options.is_empty() {
            String::new()
        } else {
            format!(" CONNECTION ({})", connection_options.join(", "))
        };

        (gcs_path, connection_str)
    }

    fn format_output_path(&self, file_path: &str) -> String {
        format_uri(
            "gcs",
            &self.config.gcs_bucket,
            &self.config.gcs_root,
            file_path,
        )
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        mask_secrets(
            sql.to_string(),
            &[
                expose_optional_secret(&self.config.gcs_credential_path),
                expose_optional_secret(&self.config.gcs_credential),
            ],
        )
    }
}

define_backend!(AzblobBackend, PrefixedAzblobConnection);

impl StorageExport for AzblobBackend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let azblob_path = format_uri(
            "azblob",
            &self.config.azblob_container,
            &self.config.azblob_root,
            &format!("{}/{}/", catalog, schema),
        );

        let mut connection_options = vec![
            format!(
                "ACCOUNT_NAME='{}'",
                expose_optional_secret(&self.config.azblob_account_name)
            ),
            format!(
                "ACCOUNT_KEY='{}'",
                expose_optional_secret(&self.config.azblob_account_key)
            ),
        ];

        if let Some(sas_token) = &self.config.azblob_sas_token {
            connection_options.push(format!("SAS_TOKEN='{}'", sas_token));
        }

        let connection_str = format!(" CONNECTION ({})", connection_options.join(", "));
        (azblob_path, connection_str)
    }

    fn format_output_path(&self, file_path: &str) -> String {
        format_uri(
            "azblob",
            &self.config.azblob_container,
            &self.config.azblob_root,
            file_path,
        )
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        mask_secrets(
            sql.to_string(),
            &[
                expose_optional_secret(&self.config.azblob_account_name),
                expose_optional_secret(&self.config.azblob_account_key),
            ],
        )
    }
}

#[derive(Clone)]
pub enum StorageType {
    Fs(FsBackend),
    S3(S3Backend),
    Oss(OssBackend),
    Gcs(GcsBackend),
    Azblob(AzblobBackend),
}

impl StorageExport for StorageType {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        match self {
            StorageType::Fs(backend) => backend.get_storage_path(catalog, schema),
            StorageType::S3(backend) => backend.get_storage_path(catalog, schema),
            StorageType::Oss(backend) => backend.get_storage_path(catalog, schema),
            StorageType::Gcs(backend) => backend.get_storage_path(catalog, schema),
            StorageType::Azblob(backend) => backend.get_storage_path(catalog, schema),
        }
    }

    fn format_output_path(&self, file_path: &str) -> String {
        match self {
            StorageType::Fs(backend) => backend.format_output_path(file_path),
            StorageType::S3(backend) => backend.format_output_path(file_path),
            StorageType::Oss(backend) => backend.format_output_path(file_path),
            StorageType::Gcs(backend) => backend.format_output_path(file_path),
            StorageType::Azblob(backend) => backend.format_output_path(file_path),
        }
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        match self {
            StorageType::Fs(backend) => backend.mask_sensitive_info(sql),
            StorageType::S3(backend) => backend.mask_sensitive_info(sql),
            StorageType::Oss(backend) => backend.mask_sensitive_info(sql),
            StorageType::Gcs(backend) => backend.mask_sensitive_info(sql),
            StorageType::Azblob(backend) => backend.mask_sensitive_info(sql),
        }
    }
}

impl StorageType {
    /// Returns true if the storage backend is remote (not local filesystem).
    pub fn is_remote_storage(&self) -> bool {
        !matches!(self, StorageType::Fs(_))
    }
}
