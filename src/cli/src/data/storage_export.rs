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

use async_trait::async_trait;
use common_base::secrets::ExposeSecret;
use common_error::ext::BoxedError;

use crate::common::{
    PrefixedAzblobConnection, PrefixedGcsConnection, PrefixedOssConnection, PrefixedS3Connection,
};
use crate::error;

/// Trait for storage backends that can be used for data export.
#[async_trait]
pub trait StorageExport: Send + Sync {
    /// Generate the storage path for COPY DATABASE command.
    /// Returns (path, connection_string) where connection_string includes CONNECTION clause.
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String);

    /// Format the output path for logging purposes.
    fn format_output_path(&self, catalog: &str, file_path: &str) -> String;

    /// Mask sensitive information in SQL commands for safe logging.
    fn mask_sensitive_info(&self, sql: &str) -> String;
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

#[async_trait]
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

    fn format_output_path(&self, _catalog: &str, file_path: &str) -> String {
        format!("{}/{}", self.output_dir, file_path)
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        sql.to_string()
    }
}

/// S3 storage backend.
#[derive(Clone)]
pub struct S3Backend {
    config: PrefixedS3Connection,
}

impl S3Backend {
    pub fn new(config: PrefixedS3Connection) -> Result<Self, BoxedError> {
        // Validate required fields
        if config.bucket().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "S3 bucket must be set when --s3 is enabled",
                }
                .build(),
            ));
        }
        if config.access_key_id().expose_secret().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "S3 access key ID must be set when --s3 is enabled",
                }
                .build(),
            ));
        }
        if config.secret_access_key().expose_secret().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "S3 secret access key must be set when --s3 is enabled",
                }
                .build(),
            ));
        }
        Ok(Self { config })
    }
}

#[async_trait]
impl StorageExport for S3Backend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let bucket = self.config.bucket();
        let root = if self.config.root().is_empty() {
            String::new()
        } else {
            format!("/{}", self.config.root())
        };

        let s3_path = format!("s3://{}{}/{}/{}/", bucket, root, catalog, schema);

        let mut connection_options = vec![
            format!(
                "ACCESS_KEY_ID='{}'",
                self.config.access_key_id().expose_secret()
            ),
            format!(
                "SECRET_ACCESS_KEY='{}'",
                self.config.secret_access_key().expose_secret()
            ),
        ];

        if let Some(region) = self.config.region() {
            connection_options.push(format!("REGION='{}'", region));
        }

        if let Some(endpoint) = self.config.endpoint() {
            connection_options.push(format!("ENDPOINT='{}'", endpoint));
        }

        let connection_str = format!(" CONNECTION ({})", connection_options.join(", "));
        (s3_path, connection_str)
    }

    fn format_output_path(&self, _catalog: &str, file_path: &str) -> String {
        let bucket = self.config.bucket();
        let root = if self.config.root().is_empty() {
            String::new()
        } else {
            format!("/{}", self.config.root())
        };
        format!("s3://{}{}/{}", bucket, root, file_path)
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        let mut masked = sql.to_string();
        masked = masked.replace(self.config.access_key_id().expose_secret(), "[REDACTED]");
        masked = masked.replace(
            self.config.secret_access_key().expose_secret(),
            "[REDACTED]",
        );
        masked
    }
}

/// OSS storage backend.
#[derive(Clone)]
pub struct OssBackend {
    config: PrefixedOssConnection,
}

impl OssBackend {
    pub fn new(config: PrefixedOssConnection) -> Result<Self, BoxedError> {
        // Validate required fields
        if config.bucket().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "OSS bucket must be set when --oss is enabled",
                }
                .build(),
            ));
        }
        if config.endpoint().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "OSS endpoint must be set when --oss is enabled",
                }
                .build(),
            ));
        }
        if config.access_key_id().expose_secret().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "OSS access key ID must be set when --oss is enabled",
                }
                .build(),
            ));
        }
        if config.access_key_secret().expose_secret().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "OSS access key secret must be set when --oss is enabled",
                }
                .build(),
            ));
        }
        Ok(Self { config })
    }
}

#[async_trait]
impl StorageExport for OssBackend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let bucket = self.config.bucket();
        let oss_path = format!("oss://{}/{}/{}/", bucket, catalog, schema);

        let mut connection_options = vec![
            format!(
                "ACCESS_KEY_ID='{}'",
                self.config.access_key_id().expose_secret()
            ),
            format!(
                "ACCESS_KEY_SECRET='{}'",
                self.config.access_key_secret().expose_secret()
            ),
        ];

        if !self.config.endpoint().is_empty() {
            connection_options.push(format!("ENDPOINT='{}'", self.config.endpoint()));
        }

        let connection_str = format!(" CONNECTION ({})", connection_options.join(", "));
        (oss_path, connection_str)
    }

    fn format_output_path(&self, catalog: &str, file_path: &str) -> String {
        let bucket = self.config.bucket();
        format!("oss://{}/{}/{}", bucket, catalog, file_path)
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        let mut masked = sql.to_string();
        masked = masked.replace(self.config.access_key_id().expose_secret(), "[REDACTED]");
        masked = masked.replace(
            self.config.access_key_secret().expose_secret(),
            "[REDACTED]",
        );
        masked
    }
}

/// GCS storage backend.
#[derive(Clone)]
pub struct GcsBackend {
    config: PrefixedGcsConnection,
}

impl GcsBackend {
    pub fn new(config: PrefixedGcsConnection) -> Result<Self, BoxedError> {
        // Validate required fields
        if config.bucket().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "GCS bucket must be set when --gcs is enabled",
                }
                .build(),
            ));
        }
        // At least one of credential_path or credential must be set
        if config.credential_path().expose_secret().is_empty()
            && config.credential().expose_secret().is_empty()
        {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "GCS credential path or credential must be set when --gcs is enabled",
                }
                .build(),
            ));
        }
        Ok(Self { config })
    }
}

#[async_trait]
impl StorageExport for GcsBackend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let bucket = self.config.bucket();
        let root = if self.config.root().is_empty() {
            String::new()
        } else {
            format!("/{}", self.config.root())
        };

        let gcs_path = format!("gcs://{}{}/{}/{}/", bucket, root, catalog, schema);

        let mut connection_options = Vec::new();

        if !self.config.credential_path().expose_secret().is_empty() {
            connection_options.push(format!(
                "CREDENTIAL_PATH='{}'",
                self.config.credential_path().expose_secret()
            ));
        }

        if !self.config.credential().expose_secret().is_empty() {
            connection_options.push(format!(
                "CREDENTIAL='{}'",
                self.config.credential().expose_secret()
            ));
        }

        if !self.config.endpoint().is_empty() {
            connection_options.push(format!("ENDPOINT='{}'", self.config.endpoint()));
        }

        let connection_str = if connection_options.is_empty() {
            String::new()
        } else {
            format!(" CONNECTION ({})", connection_options.join(", "))
        };

        (gcs_path, connection_str)
    }

    fn format_output_path(&self, _catalog: &str, file_path: &str) -> String {
        let bucket = self.config.bucket();
        let root = if self.config.root().is_empty() {
            String::new()
        } else {
            format!("/{}", self.config.root())
        };
        format!("gcs://{}{}/{}", bucket, root, file_path)
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        let mut masked = sql.to_string();
        if !self.config.credential_path().expose_secret().is_empty() {
            masked = masked.replace(self.config.credential_path().expose_secret(), "[REDACTED]");
        }
        if !self.config.credential().expose_secret().is_empty() {
            masked = masked.replace(self.config.credential().expose_secret(), "[REDACTED]");
        }
        masked
    }
}

/// Azure Blob storage backend.
#[derive(Clone)]
pub struct AzblobBackend {
    config: PrefixedAzblobConnection,
}

impl AzblobBackend {
    pub fn new(config: PrefixedAzblobConnection) -> Result<Self, BoxedError> {
        // Validate required fields
        if config.container().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "Azure Blob container must be set when --azblob is enabled",
                }
                .build(),
            ));
        }
        if config.account_name().expose_secret().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "Azure Blob account name must be set when --azblob is enabled",
                }
                .build(),
            ));
        }
        if config.account_key().expose_secret().is_empty() {
            return Err(BoxedError::new(
                error::MissingConfigSnafu {
                    msg: "Azure Blob account key must be set when --azblob is enabled",
                }
                .build(),
            ));
        }
        Ok(Self { config })
    }
}

#[async_trait]
impl StorageExport for AzblobBackend {
    fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        let container = self.config.container();
        let root = if self.config.root().is_empty() {
            String::new()
        } else {
            format!("/{}", self.config.root())
        };

        let azblob_path = format!("azblob://{}{}/{}/{}/", container, root, catalog, schema);

        let mut connection_options = vec![
            format!(
                "ACCOUNT_NAME='{}'",
                self.config.account_name().expose_secret()
            ),
            format!(
                "ACCOUNT_KEY='{}'",
                self.config.account_key().expose_secret()
            ),
        ];

        if !self.config.endpoint().is_empty() {
            connection_options.push(format!("ENDPOINT='{}'", self.config.endpoint()));
        }

        if let Some(sas_token) = self.config.sas_token() {
            connection_options.push(format!("SAS_TOKEN='{}'", sas_token));
        }

        let connection_str = format!(" CONNECTION ({})", connection_options.join(", "));
        (azblob_path, connection_str)
    }

    fn format_output_path(&self, _catalog: &str, file_path: &str) -> String {
        let container = self.config.container();
        let root = if self.config.root().is_empty() {
            String::new()
        } else {
            format!("/{}", self.config.root())
        };
        format!("azblob://{}{}/{}", container, root, file_path)
    }

    fn mask_sensitive_info(&self, sql: &str) -> String {
        let mut masked = sql.to_string();
        masked = masked.replace(self.config.account_name().expose_secret(), "[REDACTED]");
        masked = masked.replace(self.config.account_key().expose_secret(), "[REDACTED]");
        if let Some(sas_token) = self.config.sas_token() {
            masked = masked.replace(sas_token, "[REDACTED]");
        }
        masked
    }
}

/// Enum to represent different storage backend types.
#[derive(Clone)]
pub enum StorageType {
    Fs(FsBackend),
    S3(S3Backend),
    Oss(OssBackend),
    Gcs(GcsBackend),
    Azblob(AzblobBackend),
}

impl StorageType {
    /// Get storage path and connection string.
    pub fn get_storage_path(&self, catalog: &str, schema: &str) -> (String, String) {
        match self {
            StorageType::Fs(backend) => backend.get_storage_path(catalog, schema),
            StorageType::S3(backend) => backend.get_storage_path(catalog, schema),
            StorageType::Oss(backend) => backend.get_storage_path(catalog, schema),
            StorageType::Gcs(backend) => backend.get_storage_path(catalog, schema),
            StorageType::Azblob(backend) => backend.get_storage_path(catalog, schema),
        }
    }

    /// Format output path for logging.
    pub fn format_output_path(&self, catalog: &str, file_path: &str) -> String {
        match self {
            StorageType::Fs(backend) => backend.format_output_path(catalog, file_path),
            StorageType::S3(backend) => backend.format_output_path(catalog, file_path),
            StorageType::Oss(backend) => backend.format_output_path(catalog, file_path),
            StorageType::Gcs(backend) => backend.format_output_path(catalog, file_path),
            StorageType::Azblob(backend) => backend.format_output_path(catalog, file_path),
        }
    }

    pub fn is_remote_storage(&self) -> bool {
        !matches!(self, StorageType::Fs(_))
    }

    /// Mask sensitive information in SQL commands.
    pub fn mask_sensitive_info(&self, sql: &str) -> String {
        match self {
            StorageType::Fs(backend) => backend.mask_sensitive_info(sql),
            StorageType::S3(backend) => backend.mask_sensitive_info(sql),
            StorageType::Oss(backend) => backend.mask_sensitive_info(sql),
            StorageType::Gcs(backend) => backend.mask_sensitive_info(sql),
            StorageType::Azblob(backend) => backend.mask_sensitive_info(sql),
        }
    }
}
