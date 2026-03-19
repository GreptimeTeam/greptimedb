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
    SnapshotNotFoundSnafu, StorageOperationSnafu, TextDecodeSnafu, UnsupportedSchemeSnafu,
    UrlParseSnafu,
};
use crate::data::export_v2::manifest::{MANIFEST_FILE, Manifest};
#[cfg(test)]
use crate::data::export_v2::schema::SchemaDefinition;
use crate::data::export_v2::schema::{SCHEMA_DIR, SCHEMAS_FILE, SchemaSnapshot};

struct RemoteLocation {
    bucket_or_container: String,
    root: String,
}

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
fn extract_remote_location(uri: &str) -> Result<RemoteLocation> {
    let url = Url::parse(uri).context(UrlParseSnafu)?;
    let bucket_or_container = url.host_str().unwrap_or("").to_string();
    if bucket_or_container.is_empty() {
        return InvalidUriSnafu {
            uri,
            reason: "URI must include bucket/container in host",
        }
        .fail();
    }

    let root = url.path().trim_start_matches('/').to_string();
    if root.is_empty() {
        return InvalidUriSnafu {
            uri,
            reason: "snapshot URI must include a non-empty path after the bucket/container",
        }
        .fail();
    }

    Ok(RemoteLocation {
        bucket_or_container,
        root,
    })
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
            uri,
            reason: "URI must have a scheme (e.g., s3://, file://). Bare paths are not supported.",
        }
        .fail();
    }

    StorageScheme::from_uri(uri)
}

fn schema_index_path() -> String {
    format!("{}/{}", SCHEMA_DIR, SCHEMAS_FILE)
}

/// Extracts the absolute filesystem path from a file:// URI.
fn extract_file_path_from_uri(uri: &str) -> Result<String> {
    let url = Url::parse(uri).context(UrlParseSnafu)?;

    match url.host_str() {
        Some(host) if !host.is_empty() && host != "localhost" => InvalidUriSnafu {
            uri,
            reason: "file:// URI must use an absolute path like file:///tmp/backup",
        }
        .fail(),
        _ => Ok(url.path().to_string()),
    }
}

async fn ensure_snapshot_exists(storage: &OpenDalStorage) -> Result<()> {
    if storage.exists().await? {
        Ok(())
    } else {
        SnapshotNotFoundSnafu {
            uri: storage.target_uri.as_str(),
        }
        .fail()
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

    /// Writes the schema index to schema/schemas.json.
    async fn write_schema(&self, schema: &SchemaSnapshot) -> Result<()>;

    /// Writes a text file to a relative path under the snapshot root.
    async fn write_text(&self, path: &str, content: &str) -> Result<()>;

    /// Reads a text file from a relative path under the snapshot root.
    async fn read_text(&self, path: &str) -> Result<String>;

    /// Deletes the entire snapshot (for --force).
    async fn delete_snapshot(&self) -> Result<()>;
}

/// OpenDAL-based implementation of SnapshotStorage.
pub struct OpenDalStorage {
    object_store: ObjectStore,
    target_uri: String,
}

impl OpenDalStorage {
    fn new_operator_rooted(object_store: ObjectStore, target_uri: &str) -> Self {
        Self {
            object_store,
            target_uri: target_uri.to_string(),
        }
    }

    fn finish_local_store(object_store: ObjectStore) -> ObjectStore {
        with_instrument_layers(object_store, false)
    }

    fn finish_remote_store(object_store: ObjectStore) -> ObjectStore {
        with_instrument_layers(with_retry_layers(object_store), false)
    }

    fn ensure_backend_enabled(uri: &str, enabled: bool, reason: &'static str) -> Result<()> {
        if enabled {
            Ok(())
        } else {
            InvalidUriSnafu { uri, reason }.fail()
        }
    }

    fn validate_remote_config<E: std::fmt::Display>(
        uri: &str,
        backend: &str,
        result: std::result::Result<(), E>,
    ) -> Result<()> {
        result.map_err(|error| {
            InvalidUriSnafu {
                uri,
                reason: format!("invalid {} config: {}", backend, error),
            }
            .build()
        })
    }

    /// Creates a new storage from a file:// URI.
    pub fn from_file_uri(uri: &str) -> Result<Self> {
        let path = extract_file_path_from_uri(uri)?;

        let builder = Fs::default().root(&path);
        let object_store = ObjectStore::new(builder)
            .context(BuildObjectStoreSnafu)?
            .finish();
        Ok(Self::new_operator_rooted(
            Self::finish_local_store(object_store),
            uri,
        ))
    }

    fn from_file_uri_with_config(uri: &str, storage: &ObjectStoreConfig) -> Result<Self> {
        if storage.enable_s3 || storage.enable_oss || storage.enable_gcs || storage.enable_azblob {
            return InvalidUriSnafu {
                uri,
                reason: "file:// cannot be used with remote storage flags",
            }
            .fail();
        }

        Self::from_file_uri(uri)
    }

    fn from_s3_uri(uri: &str, storage: &ObjectStoreConfig) -> Result<Self> {
        Self::ensure_backend_enabled(
            uri,
            storage.enable_s3,
            "s3:// requires --s3 and related options",
        )?;

        let location = extract_remote_location(uri)?;
        let mut config = storage.s3.clone();
        config.s3_bucket = location.bucket_or_container;
        config.s3_root = location.root;
        Self::validate_remote_config(uri, "s3", config.validate())?;

        let conn: S3Connection = config.into();
        let object_store = ObjectStore::new(S3::from(&conn))
            .context(BuildObjectStoreSnafu)?
            .finish();
        Ok(Self::new_operator_rooted(
            Self::finish_remote_store(object_store),
            uri,
        ))
    }

    fn from_oss_uri(uri: &str, storage: &ObjectStoreConfig) -> Result<Self> {
        Self::ensure_backend_enabled(
            uri,
            storage.enable_oss,
            "oss:// requires --oss and related options",
        )?;

        let location = extract_remote_location(uri)?;
        let mut config = storage.oss.clone();
        config.oss_bucket = location.bucket_or_container;
        config.oss_root = location.root;
        Self::validate_remote_config(uri, "oss", config.validate())?;

        let conn: OssConnection = config.into();
        let object_store = ObjectStore::new(Oss::from(&conn))
            .context(BuildObjectStoreSnafu)?
            .finish();
        Ok(Self::new_operator_rooted(
            Self::finish_remote_store(object_store),
            uri,
        ))
    }

    fn from_gcs_uri(uri: &str, storage: &ObjectStoreConfig) -> Result<Self> {
        Self::ensure_backend_enabled(
            uri,
            storage.enable_gcs,
            "gs:// or gcs:// requires --gcs and related options",
        )?;

        let location = extract_remote_location(uri)?;
        let mut config = storage.gcs.clone();
        config.gcs_bucket = location.bucket_or_container;
        config.gcs_root = location.root;
        Self::validate_remote_config(uri, "gcs", config.validate())?;

        let conn: GcsConnection = config.into();
        let object_store = ObjectStore::new(Gcs::from(&conn))
            .context(BuildObjectStoreSnafu)?
            .finish();
        Ok(Self::new_operator_rooted(
            Self::finish_remote_store(object_store),
            uri,
        ))
    }

    fn from_azblob_uri(uri: &str, storage: &ObjectStoreConfig) -> Result<Self> {
        Self::ensure_backend_enabled(
            uri,
            storage.enable_azblob,
            "azblob:// requires --azblob and related options",
        )?;

        let location = extract_remote_location(uri)?;
        let mut config = storage.azblob.clone();
        config.azblob_container = location.bucket_or_container;
        config.azblob_root = location.root;
        Self::validate_remote_config(uri, "azblob", config.validate())?;

        let conn: AzblobConnection = config.into();
        let object_store = ObjectStore::new(Azblob::from(&conn))
            .context(BuildObjectStoreSnafu)?
            .finish();
        Ok(Self::new_operator_rooted(
            Self::finish_remote_store(object_store),
            uri,
        ))
    }

    /// Creates a new storage from a URI and object store config.
    pub fn from_uri(uri: &str, storage: &ObjectStoreConfig) -> Result<Self> {
        match StorageScheme::from_uri(uri)? {
            StorageScheme::File => Self::from_file_uri_with_config(uri, storage),
            StorageScheme::S3 => Self::from_s3_uri(uri, storage),
            StorageScheme::Oss => Self::from_oss_uri(uri, storage),
            StorageScheme::Gcs => Self::from_gcs_uri(uri, storage),
            StorageScheme::Azblob => Self::from_azblob_uri(uri, storage),
        }
    }

    /// Reads a file as bytes.
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let data = self
            .object_store
            .read(path)
            .await
            .context(StorageOperationSnafu {
                operation: format!("read {}", path),
            })?;
        Ok(data.to_vec())
    }

    /// Writes bytes to a file.
    async fn write_file(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.object_store
            .write(path, data)
            .await
            .map(|_| ())
            .context(StorageOperationSnafu {
                operation: format!("write {}", path),
            })
    }

    /// Checks if a file exists using stat.
    async fn file_exists(&self, path: &str) -> Result<bool> {
        match self.object_store.stat(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == object_store::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e).context(StorageOperationSnafu {
                operation: format!("check exists {}", path),
            }),
        }
    }

    #[cfg(test)]
    pub async fn read_schema(&self) -> Result<SchemaSnapshot> {
        let schemas_path = schema_index_path();
        let schemas: Vec<SchemaDefinition> = if self.file_exists(&schemas_path).await? {
            let data = self.read_file(&schemas_path).await?;
            serde_json::from_slice(&data).context(ManifestParseSnafu)?
        } else {
            vec![]
        };

        Ok(SchemaSnapshot { schemas })
    }
}

#[async_trait]
impl SnapshotStorage for OpenDalStorage {
    async fn exists(&self) -> Result<bool> {
        self.file_exists(MANIFEST_FILE).await
    }

    async fn read_manifest(&self) -> Result<Manifest> {
        ensure_snapshot_exists(self).await?;

        let data = self.read_file(MANIFEST_FILE).await?;
        serde_json::from_slice(&data).context(ManifestParseSnafu)
    }

    async fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        let data = serde_json::to_vec_pretty(manifest).context(ManifestSerializeSnafu)?;
        self.write_file(MANIFEST_FILE, data).await
    }

    async fn write_schema(&self, schema: &SchemaSnapshot) -> Result<()> {
        let schemas_path = schema_index_path();
        let schemas_data =
            serde_json::to_vec_pretty(&schema.schemas).context(ManifestSerializeSnafu)?;
        self.write_file(&schemas_path, schemas_data).await
    }

    async fn write_text(&self, path: &str, content: &str) -> Result<()> {
        self.write_file(path, content.as_bytes().to_vec()).await
    }

    async fn read_text(&self, path: &str) -> Result<String> {
        let data = self.read_file(path).await?;
        String::from_utf8(data).context(TextDecodeSnafu)
    }

    async fn delete_snapshot(&self) -> Result<()> {
        self.object_store
            .remove_all("/")
            .await
            .context(StorageOperationSnafu {
                operation: "delete snapshot",
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use object_store::ObjectStore;
    use object_store::services::Fs;
    use tempfile::tempdir;
    use url::Url;

    use super::*;
    use crate::data::export_v2::manifest::{DataFormat, TimeRange};
    use crate::data::export_v2::schema::SchemaDefinition;

    fn make_storage_with_rooted_fs(dir: &std::path::Path) -> OpenDalStorage {
        let object_store = ObjectStore::new(Fs::default().root(dir.to_str().unwrap()))
            .unwrap()
            .finish();
        OpenDalStorage::new_operator_rooted(
            OpenDalStorage::finish_local_store(object_store),
            Url::from_directory_path(dir).unwrap().as_ref(),
        )
    }

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
    fn test_extract_remote_location_requires_non_empty_root() {
        assert!(extract_remote_location("s3://bucket").is_err());
        assert!(extract_remote_location("s3://bucket/").is_err());
        assert!(extract_remote_location("oss://bucket").is_err());
        assert!(extract_remote_location("gs://bucket").is_err());
        assert!(extract_remote_location("azblob://container").is_err());
    }

    #[test]
    fn test_extract_path_from_uri() {
        assert_eq!(
            extract_file_path_from_uri("file:///tmp/backup").unwrap(),
            "/tmp/backup"
        );
        assert_eq!(
            extract_file_path_from_uri("file://localhost/tmp/backup").unwrap(),
            "/tmp/backup"
        );
    }

    #[test]
    fn test_extract_file_path_from_uri_rejects_file_host() {
        assert!(extract_file_path_from_uri("file://tmp/backup").is_err());
    }

    #[tokio::test]
    async fn test_read_manifest_reports_requested_uri() {
        let dir = tempdir().unwrap();
        let uri = Url::from_directory_path(dir.path()).unwrap().to_string();
        let storage = OpenDalStorage::from_file_uri(&uri).unwrap();

        let error = storage.read_manifest().await.unwrap_err().to_string();

        assert!(error.contains(uri.as_str()));
    }

    #[tokio::test]
    async fn test_manifest_round_trip() {
        let dir = tempdir().unwrap();
        let storage = make_storage_with_rooted_fs(dir.path());

        let manifest = Manifest::new_full(
            "greptime".to_string(),
            vec!["public".to_string()],
            TimeRange::unbounded(),
            DataFormat::Parquet,
        );

        storage.write_manifest(&manifest).await.unwrap();
        let loaded = storage.read_manifest().await.unwrap();

        assert_eq!(loaded.catalog, manifest.catalog);
        assert_eq!(loaded.schemas, manifest.schemas);
        assert_eq!(loaded.schema_only, manifest.schema_only);
        assert_eq!(loaded.format, manifest.format);
        assert_eq!(loaded.snapshot_id, manifest.snapshot_id);
    }

    #[tokio::test]
    async fn test_schema_round_trip() {
        let dir = tempdir().unwrap();
        let storage = make_storage_with_rooted_fs(dir.path());

        let mut snapshot = SchemaSnapshot::new();
        snapshot.add_schema(SchemaDefinition {
            catalog: "greptime".to_string(),
            name: "test_db".to_string(),
            options: HashMap::from([("ttl".to_string(), "7d".to_string())]),
        });

        storage.write_schema(&snapshot).await.unwrap();
        let loaded = storage.read_schema().await.unwrap();

        assert_eq!(loaded, snapshot);
    }

    #[tokio::test]
    async fn test_text_round_trip() {
        let dir = tempdir().unwrap();
        let storage = make_storage_with_rooted_fs(dir.path());
        let content = "CREATE TABLE metrics (ts TIMESTAMP TIME INDEX);";

        storage
            .write_text("schema/ddl/public.sql", content)
            .await
            .unwrap();
        let loaded = storage.read_text("schema/ddl/public.sql").await.unwrap();

        assert_eq!(loaded, content);
    }

    #[tokio::test]
    async fn test_read_text_rejects_invalid_utf8() {
        let dir = tempdir().unwrap();
        let storage = make_storage_with_rooted_fs(dir.path());

        storage
            .write_file("schema/ddl/public.sql", vec![0xff, 0xfe, 0xfd])
            .await
            .unwrap();

        let error = storage
            .read_text("schema/ddl/public.sql")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("UTF-8"));
    }

    #[tokio::test]
    async fn test_exists_follows_manifest_presence() {
        let dir = tempdir().unwrap();
        let storage = make_storage_with_rooted_fs(dir.path());

        assert!(!storage.exists().await.unwrap());

        storage
            .write_manifest(&Manifest::new_schema_only(
                "greptime".to_string(),
                vec!["public".to_string()],
            ))
            .await
            .unwrap();

        assert!(storage.exists().await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_snapshot_only_removes_rooted_contents() {
        let parent = tempdir().unwrap();
        let snapshot_root = parent.path().join("snapshot");
        let sibling = parent.path().join("sibling");
        std::fs::create_dir_all(&snapshot_root).unwrap();
        std::fs::create_dir_all(&sibling).unwrap();
        std::fs::write(snapshot_root.join("manifest.json"), b"{}").unwrap();
        std::fs::write(sibling.join("keep.txt"), b"keep").unwrap();

        let storage = make_storage_with_rooted_fs(&snapshot_root);
        storage.delete_snapshot().await.unwrap();

        assert!(!snapshot_root.join("manifest.json").exists());
        assert!(sibling.join("keep.txt").exists());
    }
}
