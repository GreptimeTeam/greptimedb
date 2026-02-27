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

//! Storage abstraction for Export/Import V2.
//!
//! This module provides a unified interface for reading and writing snapshot data
//! to various storage backends (S3, OSS, GCS, Azure Blob, local filesystem).

use async_trait::async_trait;
use object_store::services::{Azblob, Fs, Gcs, Oss, S3};
use object_store::util::{with_instrument_layers, with_retry_layers};
use object_store::{AzblobConnection, GcsConnection, ObjectStore, OssConnection, S3Connection};
use snafu::ResultExt;
use url::Url;

use crate::common::ObjectStoreConfig;
use crate::data::export_v2::error::{
    BuildObjectStoreSnafu, InvalidUriSnafu, ManifestParseSnafu, ManifestSerializeSnafu, Result,
    SnapshotNotFoundSnafu, StorageOperationSnafu, UnsupportedSchemeSnafu, UrlParseSnafu,
};
use crate::data::export_v2::manifest::{MANIFEST_FILE, Manifest};
use crate::data::export_v2::schema::{
    SCHEMA_DIR, SCHEMAS_FILE, SchemaDefinition, SchemaSnapshot, TABLES_FILE, TableDefinition,
    VIEWS_FILE, ViewDefinition,
};

/// URI schemes supported for snapshot storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageScheme {
    /// Amazon S3.
    S3,
    /// Alibaba Cloud OSS.
    Oss,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azblob,
    /// Local filesystem (file://).
    File,
}

impl StorageScheme {
    /// Parses storage scheme from URI.
    pub fn from_uri(uri: &str) -> Result<Self> {
        let url = Url::parse(uri).context(UrlParseSnafu)?;

        match url.scheme() {
            "s3" => Ok(Self::S3),
            "oss" => Ok(Self::Oss),
            "gs" | "gcs" => Ok(Self::Gcs),
            "azblob" => Ok(Self::Azblob),
            "file" => Ok(Self::File),
            scheme => UnsupportedSchemeSnafu { scheme }.fail(),
        }
    }
}

/// Extracts bucket/container and root path from a URI.
fn extract_bucket_and_root(uri: &str) -> Result<(String, String)> {
    let url = Url::parse(uri).context(UrlParseSnafu)?;
    let bucket = url.host_str().unwrap_or("").to_string();
    if bucket.is_empty() {
        return InvalidUriSnafu {
            uri: uri.to_string(),
            reason: "URI must include bucket/container in host",
        }
        .fail();
    }

    let root = url.path().trim_start_matches('/').to_string();
    Ok((bucket, root))
}

/// Validates that a URI has a proper scheme.
///
/// Rejects bare paths (e.g., `/tmp/backup`, `./backup`) because:
/// - Schema export (CLI) and data export (server) run in different processes
/// - Using bare paths would split the snapshot across machines
///
/// Supported URI schemes:
/// - `s3://bucket/path` - Amazon S3
/// - `oss://bucket/path` - Alibaba Cloud OSS
/// - `gs://bucket/path` - Google Cloud Storage
/// - `azblob://container/path` - Azure Blob Storage
/// - `file:///absolute/path` - Local filesystem
pub fn validate_uri(uri: &str) -> Result<StorageScheme> {
    // Must have a scheme
    if !uri.contains("://") {
        return InvalidUriSnafu {
            uri: uri.to_string(),
            reason: "URI must have a scheme (e.g., s3://, file://). Bare paths are not supported.",
        }
        .fail();
    }

    StorageScheme::from_uri(uri)
}

/// Extracts the path component from a URI.
fn extract_path_from_uri(uri: &str) -> Result<String> {
    let url = Url::parse(uri).context(UrlParseSnafu)?;

    match url.scheme() {
        "file" => Ok(url.path().to_string()),
        // For object storage, combine host (bucket) and path
        _ => {
            let bucket = url.host_str().unwrap_or("");
            let path = url.path();
            if path.is_empty() || path == "/" {
                Ok(bucket.to_string())
            } else {
                Ok(format!("{}{}", bucket, path))
            }
        }
    }
}

/// Snapshot storage abstraction.
///
/// Provides operations for reading and writing snapshot data to various storage backends.
#[async_trait]
pub trait SnapshotStorage: Send + Sync {
    /// Checks if a snapshot exists at this location (manifest.json exists).
    async fn exists(&self) -> Result<bool>;

    /// Reads the manifest file.
    async fn read_manifest(&self) -> Result<Manifest>;

    /// Writes the manifest file.
    async fn write_manifest(&self, manifest: &Manifest) -> Result<()>;

    /// Reads schema definitions from schema/*.json files.
    async fn read_schema(&self) -> Result<SchemaSnapshot>;

    /// Writes schema definitions to schema/*.json files.
    async fn write_schema(&self, schema: &SchemaSnapshot) -> Result<()>;

    /// Writes a text file to a relative path under the snapshot root.
    async fn write_text(&self, path: &str, content: &str) -> Result<()>;

    /// Reads a text file from a relative path under the snapshot root.
    async fn read_text(&self, path: &str) -> Result<String>;

    /// Deletes the entire snapshot (for --force).
    async fn delete_snapshot(&self) -> Result<()>;

    /// Returns the underlying object store.
    fn object_store(&self) -> &ObjectStore;

    /// Returns the snapshot root path.
    fn root_path(&self) -> &str;
}

/// OpenDAL-based implementation of SnapshotStorage.
pub struct OpenDalStorage {
    object_store: ObjectStore,
    root_path: String,
}

impl OpenDalStorage {
    /// Creates a new storage from a file:// URI.
    pub fn from_file_uri(uri: &str) -> Result<Self> {
        let path = extract_path_from_uri(uri)?;

        let builder = Fs::default().root(&path);
        let object_store = ObjectStore::new(builder)
            .context(BuildObjectStoreSnafu)?
            .finish();
        let object_store = with_instrument_layers(object_store, false);

        Ok(Self {
            object_store,
            root_path: String::new(), // Root is already set in the operator
        })
    }

    /// Creates a new storage from a URI and object store config.
    pub fn from_uri(uri: &str, storage: &ObjectStoreConfig) -> Result<Self> {
        let scheme = StorageScheme::from_uri(uri)?;

        match scheme {
            StorageScheme::File => {
                if storage.enable_s3
                    || storage.enable_oss
                    || storage.enable_gcs
                    || storage.enable_azblob
                {
                    return InvalidUriSnafu {
                        uri,
                        reason: "file:// cannot be used with remote storage flags",
                    }
                    .fail();
                }
                Self::from_file_uri(uri)
            }
            StorageScheme::S3 => {
                if !storage.enable_s3 {
                    return InvalidUriSnafu {
                        uri,
                        reason: "s3:// requires --s3 and related options",
                    }
                    .fail();
                }
                let (bucket, root) = extract_bucket_and_root(uri)?;
                let mut config = storage.clone();
                config.s3.s3_bucket = bucket;
                config.s3.s3_root = root;
                config.s3.validate().map_err(|e| {
                    InvalidUriSnafu {
                        uri,
                        reason: format!("invalid s3 config: {}", e),
                    }
                    .build()
                })?;

                let conn: S3Connection = config.s3.clone().into();
                let object_store = ObjectStore::new(S3::from(&conn))
                    .context(BuildObjectStoreSnafu)?
                    .finish();
                let object_store = with_instrument_layers(with_retry_layers(object_store), false);

                Ok(Self {
                    object_store,
                    root_path: String::new(),
                })
            }
            StorageScheme::Oss => {
                if !storage.enable_oss {
                    return InvalidUriSnafu {
                        uri,
                        reason: "oss:// requires --oss and related options",
                    }
                    .fail();
                }
                let (bucket, root) = extract_bucket_and_root(uri)?;
                let mut config = storage.clone();
                config.oss.oss_bucket = bucket;
                config.oss.oss_root = root;
                config.oss.validate().map_err(|e| {
                    InvalidUriSnafu {
                        uri,
                        reason: format!("invalid oss config: {}", e),
                    }
                    .build()
                })?;

                let conn: OssConnection = config.oss.clone().into();
                let object_store = ObjectStore::new(Oss::from(&conn))
                    .context(BuildObjectStoreSnafu)?
                    .finish();
                let object_store = with_instrument_layers(with_retry_layers(object_store), false);

                Ok(Self {
                    object_store,
                    root_path: String::new(),
                })
            }
            StorageScheme::Gcs => {
                if !storage.enable_gcs {
                    return InvalidUriSnafu {
                        uri,
                        reason: "gs:// or gcs:// requires --gcs and related options",
                    }
                    .fail();
                }
                let (bucket, root) = extract_bucket_and_root(uri)?;
                let mut config = storage.clone();
                config.gcs.gcs_bucket = bucket;
                config.gcs.gcs_root = root;
                config.gcs.validate().map_err(|e| {
                    InvalidUriSnafu {
                        uri,
                        reason: format!("invalid gcs config: {}", e),
                    }
                    .build()
                })?;

                let conn: GcsConnection = config.gcs.clone().into();
                let object_store = ObjectStore::new(Gcs::from(&conn))
                    .context(BuildObjectStoreSnafu)?
                    .finish();
                let object_store = with_instrument_layers(with_retry_layers(object_store), false);

                Ok(Self {
                    object_store,
                    root_path: String::new(),
                })
            }
            StorageScheme::Azblob => {
                if !storage.enable_azblob {
                    return InvalidUriSnafu {
                        uri,
                        reason: "azblob:// requires --azblob and related options",
                    }
                    .fail();
                }
                let (container, root) = extract_bucket_and_root(uri)?;
                let mut config = storage.clone();
                config.azblob.azblob_container = container;
                config.azblob.azblob_root = root;
                config.azblob.validate().map_err(|e| {
                    InvalidUriSnafu {
                        uri,
                        reason: format!("invalid azblob config: {}", e),
                    }
                    .build()
                })?;

                let conn: AzblobConnection = config.azblob.clone().into();
                let object_store = ObjectStore::new(Azblob::from(&conn))
                    .context(BuildObjectStoreSnafu)?
                    .finish();
                let object_store = with_instrument_layers(with_retry_layers(object_store), false);

                Ok(Self {
                    object_store,
                    root_path: String::new(),
                })
            }
        }
    }

    /// Creates a new storage with an existing ObjectStore and root path.
    pub fn new(object_store: ObjectStore, root_path: String) -> Self {
        Self {
            object_store,
            root_path,
        }
    }

    /// Joins the root path with a relative path.
    fn full_path(&self, relative: &str) -> String {
        if self.root_path.is_empty() {
            relative.to_string()
        } else {
            format!("{}/{}", self.root_path.trim_end_matches('/'), relative)
        }
    }

    /// Reads a file as bytes.
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.full_path(path);
        let data = self
            .object_store
            .read(&full_path)
            .await
            .context(StorageOperationSnafu {
                operation: format!("read {}", full_path),
            })?;
        Ok(data.to_vec())
    }

    /// Writes bytes to a file.
    async fn write_file(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let full_path = self.full_path(path);
        self.object_store
            .write(&full_path, data)
            .await
            .map(|_| ())
            .context(StorageOperationSnafu {
                operation: format!("write {}", full_path),
            })
    }

    /// Checks if a file exists using stat.
    async fn file_exists(&self, path: &str) -> Result<bool> {
        let full_path = self.full_path(path);
        match self.object_store.stat(&full_path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == object_store::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e).context(StorageOperationSnafu {
                operation: format!("check exists {}", full_path),
            }),
        }
    }
}

#[async_trait]
impl SnapshotStorage for OpenDalStorage {
    async fn exists(&self) -> Result<bool> {
        self.file_exists(MANIFEST_FILE).await
    }

    async fn read_manifest(&self) -> Result<Manifest> {
        if !self.exists().await? {
            return SnapshotNotFoundSnafu {
                uri: self.root_path.as_str(),
            }
            .fail();
        }

        let data = self.read_file(MANIFEST_FILE).await?;
        serde_json::from_slice(&data).context(ManifestParseSnafu)
    }

    async fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        let data = serde_json::to_vec_pretty(manifest).context(ManifestSerializeSnafu)?;
        self.write_file(MANIFEST_FILE, data).await
    }

    async fn read_schema(&self) -> Result<SchemaSnapshot> {
        // Read schemas.json
        let schemas_path = format!("{}/{}", SCHEMA_DIR, SCHEMAS_FILE);
        let schemas: Vec<SchemaDefinition> = if self.file_exists(&schemas_path).await? {
            let data = self.read_file(&schemas_path).await?;
            serde_json::from_slice(&data).context(ManifestParseSnafu)?
        } else {
            vec![]
        };

        // Read tables.json
        let tables_path = format!("{}/{}", SCHEMA_DIR, TABLES_FILE);
        let tables: Vec<TableDefinition> = if self.file_exists(&tables_path).await? {
            let data = self.read_file(&tables_path).await?;
            serde_json::from_slice(&data).context(ManifestParseSnafu)?
        } else {
            vec![]
        };

        // Read views.json
        let views_path = format!("{}/{}", SCHEMA_DIR, VIEWS_FILE);
        let views: Vec<ViewDefinition> = if self.file_exists(&views_path).await? {
            let data = self.read_file(&views_path).await?;
            serde_json::from_slice(&data).context(ManifestParseSnafu)?
        } else {
            vec![]
        };

        Ok(SchemaSnapshot {
            schemas,
            tables,
            views,
        })
    }

    async fn write_schema(&self, schema: &SchemaSnapshot) -> Result<()> {
        // Write schemas.json
        let schemas_path = format!("{}/{}", SCHEMA_DIR, SCHEMAS_FILE);
        let schemas_data =
            serde_json::to_vec_pretty(&schema.schemas).context(ManifestSerializeSnafu)?;
        self.write_file(&schemas_path, schemas_data).await?;

        // Write tables.json
        let tables_path = format!("{}/{}", SCHEMA_DIR, TABLES_FILE);
        let tables_data =
            serde_json::to_vec_pretty(&schema.tables).context(ManifestSerializeSnafu)?;
        self.write_file(&tables_path, tables_data).await?;

        // Write views.json
        let views_path = format!("{}/{}", SCHEMA_DIR, VIEWS_FILE);
        let views_data =
            serde_json::to_vec_pretty(&schema.views).context(ManifestSerializeSnafu)?;
        self.write_file(&views_path, views_data).await?;

        Ok(())
    }

    async fn write_text(&self, path: &str, content: &str) -> Result<()> {
        self.write_file(path, content.as_bytes().to_vec()).await
    }

    async fn read_text(&self, path: &str) -> Result<String> {
        let data = self.read_file(path).await?;
        Ok(String::from_utf8_lossy(&data).to_string())
    }

    async fn delete_snapshot(&self) -> Result<()> {
        let root = if self.root_path.is_empty() {
            "/".to_string()
        } else {
            format!("{}/", self.root_path.trim_end_matches('/'))
        };

        self.object_store
            .remove_all(&root)
            .await
            .context(StorageOperationSnafu {
                operation: "delete snapshot",
            })
    }

    fn object_store(&self) -> &ObjectStore {
        &self.object_store
    }

    fn root_path(&self) -> &str {
        &self.root_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_uri_valid() {
        assert_eq!(validate_uri("s3://bucket/path").unwrap(), StorageScheme::S3);
        assert_eq!(
            validate_uri("oss://bucket/path").unwrap(),
            StorageScheme::Oss
        );
        assert_eq!(
            validate_uri("gs://bucket/path").unwrap(),
            StorageScheme::Gcs
        );
        assert_eq!(
            validate_uri("gcs://bucket/path").unwrap(),
            StorageScheme::Gcs
        );
        assert_eq!(
            validate_uri("azblob://container/path").unwrap(),
            StorageScheme::Azblob
        );
        assert_eq!(
            validate_uri("file:///tmp/backup").unwrap(),
            StorageScheme::File
        );
    }

    #[test]
    fn test_validate_uri_invalid() {
        // Bare paths should be rejected
        assert!(validate_uri("/tmp/backup").is_err());
        assert!(validate_uri("./backup").is_err());
        assert!(validate_uri("backup").is_err());

        // Unknown schemes
        assert!(validate_uri("ftp://server/path").is_err());
    }

    #[test]
    fn test_extract_path_from_uri() {
        assert_eq!(
            extract_path_from_uri("file:///tmp/backup").unwrap(),
            "/tmp/backup"
        );
        assert_eq!(
            extract_path_from_uri("s3://mybucket/snapshots/prod").unwrap(),
            "mybucket/snapshots/prod"
        );
        assert_eq!(extract_path_from_uri("s3://mybucket").unwrap(), "mybucket");
    }
}
