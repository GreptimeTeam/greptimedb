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

pub mod file;

use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::time::Instant;

use common_telemetry::info;
use file::{Metadata, MetadataContent};
use futures::{TryStreamExt, future};
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use strum::Display;

use crate::error::{
    Error, InvalidFileExtensionSnafu, InvalidFileNameSnafu, InvalidFilePathSnafu, ReadObjectSnafu,
    Result, UnexpectedSnafu, WriteObjectSnafu,
};
use crate::key::{CANDIDATES_ROOT, ELECTION_KEY};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use crate::rpc::KeyValue;
use crate::rpc::store::{BatchPutRequest, RangeRequest};
use crate::snapshot::file::Document;

/// The format of the backup file.
#[derive(Debug, PartialEq, Eq, Display, Clone, Copy)]
pub enum FileFormat {
    #[strum(serialize = "fb")]
    FlexBuffers,
}

impl TryFrom<&str> for FileFormat {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "fb" => Ok(FileFormat::FlexBuffers),
            _ => Err(format!("Invalid file format: {}", value)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Display)]
#[strum(serialize_all = "lowercase")]
pub enum DataType {
    Metadata,
}

impl TryFrom<&str> for DataType {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "metadata" => Ok(DataType::Metadata),
            _ => Err(format!("Invalid data type: {}", value)),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FileExtension {
    format: FileFormat,
    data_type: DataType,
}

impl FileExtension {
    pub fn new(format: FileFormat, data_type: DataType) -> Self {
        Self { format, data_type }
    }
}

impl Display for FileExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.data_type, self.format)
    }
}

impl TryFrom<&str> for FileExtension {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let parts = value.split(".").collect::<Vec<&str>>();
        if parts.len() != 2 {
            return InvalidFileExtensionSnafu {
                reason: format!(
                    "Extension should be in the format of <datatype>.<format>, got: {}",
                    value
                ),
            }
            .fail();
        }

        let data_type = DataType::try_from(parts[0])
            .map_err(|e| InvalidFileExtensionSnafu { reason: e }.build())?;
        let format = FileFormat::try_from(parts[1])
            .map_err(|e| InvalidFileExtensionSnafu { reason: e }.build())?;
        Ok(FileExtension { format, data_type })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FileName {
    name: String,
    extension: FileExtension,
}

impl Display for FileName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.name, self.extension)
    }
}

impl TryFrom<&str> for FileName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        let Some((name, extension)) = value.split_once(".") else {
            return InvalidFileNameSnafu {
                reason: format!(
                    "The file name should be in the format of <name>.<extension>, got: {}",
                    value
                ),
            }
            .fail();
        };
        let extension = FileExtension::try_from(extension)?;
        Ok(Self {
            name: name.to_string(),
            extension,
        })
    }
}

impl FileName {
    fn new(name: String, extension: FileExtension) -> Self {
        Self { name, extension }
    }
}

/// The manager of the metadata snapshot.
///
/// It manages the metadata snapshot, including dumping and restoring.
pub struct MetadataSnapshotManager {
    kv_backend: KvBackendRef,
    object_store: ObjectStore,
}

/// The maximum size of the request to put metadata, use 1MiB by default.
const MAX_REQUEST_SIZE: usize = 1024 * 1024;

/// Returns true if the key is an internal key.
fn is_internal_key(kv: &KeyValue) -> bool {
    kv.key.starts_with(ELECTION_KEY.as_bytes()) || kv.key.starts_with(CANDIDATES_ROOT.as_bytes())
}

impl MetadataSnapshotManager {
    pub fn new(kv_backend: KvBackendRef, object_store: ObjectStore) -> Self {
        Self {
            kv_backend,
            object_store,
        }
    }

    /// Restores the metadata from the backup file to the metadata store.
    pub async fn restore(&self, file_path: &str) -> Result<u64> {
        let path = Path::new(file_path);

        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .context(InvalidFilePathSnafu { file_path })?;

        let filename = FileName::try_from(file_name)?;
        let data = self
            .object_store
            .read(file_path)
            .await
            .context(ReadObjectSnafu { file_path })?;
        let document = Document::from_slice(&filename.extension.format, &data.to_bytes())?;
        let metadata_content = document.into_metadata_content()?;
        let mut req = BatchPutRequest::default();
        let mut total_request_size = 0;
        let mut count = 0;
        let now = Instant::now();
        for KeyValue { key, value } in metadata_content.into_iter() {
            count += 1;
            let key_size = key.len();
            let value_size = value.len();
            if total_request_size + key_size + value_size > MAX_REQUEST_SIZE {
                self.kv_backend.batch_put(req).await?;
                req = BatchPutRequest::default();
                total_request_size = 0;
            }
            req.kvs.push(KeyValue { key, value });
            total_request_size += key_size + value_size;
        }
        if !req.kvs.is_empty() {
            self.kv_backend.batch_put(req).await?;
        }

        info!(
            "Restored metadata from {} successfully, total {} key-value pairs, elapsed {:?}",
            file_path,
            count,
            now.elapsed()
        );
        Ok(count)
    }

    pub async fn check_target_source_clean(&self) -> Result<bool> {
        let req = RangeRequest::new().with_range(vec![0], vec![0]);
        let mut stream = Box::pin(
            PaginationStream::new(self.kv_backend.clone(), req, 1, Result::Ok).into_stream(),
        );
        let v = stream.as_mut().try_next().await?;
        Ok(v.is_none())
    }

    /// Dumps the metadata to the backup file.
    pub async fn dump(&self, file_path_str: &str) -> Result<(String, u64)> {
        let format = FileFormat::FlexBuffers;

        let path = Path::new(file_path_str)
            .parent()
            .context(InvalidFilePathSnafu {
                file_path: file_path_str,
            })?;
        let raw_file_name = Path::new(file_path_str)
            .file_name()
            .and_then(|s| s.to_str())
            .context(InvalidFilePathSnafu {
                file_path: file_path_str,
            })?;
        let parsed_file_name = FileName::try_from(raw_file_name).unwrap_or_else(|_| {
            FileName::new(
                raw_file_name.to_string(),
                FileExtension {
                    format,
                    data_type: DataType::Metadata,
                },
            )
        });
        let file_path = path.join(parsed_file_name.to_string());
        let file_path = file_path.to_str().context(InvalidFilePathSnafu {
            file_path: file_path_str,
        })?;

        // Ensure the file does not exist
        ensure!(
            !self
                .object_store
                .exists(file_path)
                .await
                .context(ReadObjectSnafu { file_path })?,
            UnexpectedSnafu {
                err_msg: format!(
                    "The file '{}' already exists. Please choose a different name or remove the existing file before proceeding.",
                    file_path
                ),
            }
        );
        let now = Instant::now();
        let req = RangeRequest::new().with_range(vec![0], vec![0]);
        let stream = PaginationStream::new(self.kv_backend.clone(), req, DEFAULT_PAGE_SIZE, |kv| {
            Ok(KeyValue {
                key: kv.key,
                value: kv.value,
            })
        })
        .into_stream();
        let keyvalues = stream
            .try_filter(|f| future::ready(!is_internal_key(f)))
            .try_collect::<Vec<_>>()
            .await?;
        let num_keyvalues = keyvalues.len();
        let document = Document::new(
            Metadata::new(),
            file::Content::Metadata(MetadataContent::new(keyvalues)),
        );
        let bytes = document.to_bytes(&format)?;
        let r = self
            .object_store
            .write(file_path, bytes)
            .await
            .context(WriteObjectSnafu { file_path })?;
        info!(
            "Dumped metadata to {} successfully, total {} key-value pairs, file size {} bytes, elapsed {:?}",
            file_path,
            num_keyvalues,
            r.content_length(),
            now.elapsed()
        );

        Ok((parsed_file_name.to_string(), num_keyvalues as u64))
    }

    fn format_output(key: Cow<'_, str>, value: Cow<'_, str>) -> String {
        format!("{} => {}", key, value)
    }

    pub async fn info(
        object_store: &ObjectStore,
        file_path: &str,
        query_str: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>> {
        let path = Path::new(file_path);

        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .context(InvalidFilePathSnafu { file_path })?;

        let filename = FileName::try_from(file_name)?;
        let data = object_store
            .read(file_path)
            .await
            .context(ReadObjectSnafu { file_path })?;
        let document = Document::from_slice(&filename.extension.format, &data.to_bytes())?;
        let metadata_content = document.into_metadata_content()?.values();
        let mut results = Vec::with_capacity(limit.unwrap_or(256));
        for kv in metadata_content {
            let key_str = String::from_utf8_lossy(&kv.key);
            if let Some(prefix) = query_str.strip_suffix('*') {
                if key_str.starts_with(prefix) {
                    let value_str = String::from_utf8_lossy(&kv.value);
                    results.push(Self::format_output(key_str, value_str));
                }
            } else if key_str == query_str {
                let value_str = String::from_utf8_lossy(&kv.value);
                results.push(Self::format_output(key_str, value_str));
            }
            if results.len() == limit.unwrap_or(usize::MAX) {
                break;
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use common_test_util::temp_dir::{TempDir, create_temp_dir};
    use object_store::services::Fs;

    use super::*;
    use crate::kv_backend::KvBackend;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::rpc::store::PutRequest;

    #[test]
    fn test_file_name() {
        let file_name = FileName::try_from("test.metadata.fb").unwrap();
        assert_eq!(file_name.name, "test");
        assert_eq!(file_name.extension.format, FileFormat::FlexBuffers);
        assert_eq!(file_name.extension.data_type, DataType::Metadata);
        assert_eq!(file_name.to_string(), "test.metadata.fb");

        let invalid_file_name = FileName::try_from("test.metadata").unwrap_err();
        assert_eq!(
            invalid_file_name.to_string(),
            "Invalid file extension: Extension should be in the format of <datatype>.<format>, got: metadata"
        );

        let invalid_file_extension = FileName::try_from("test.metadata.hello").unwrap_err();
        assert_eq!(
            invalid_file_extension.to_string(),
            "Invalid file extension: Invalid file format: hello"
        );
    }

    fn test_env(
        prefix: &str,
    ) -> (
        TempDir,
        Arc<MemoryKvBackend<Error>>,
        MetadataSnapshotManager,
    ) {
        let temp_dir = create_temp_dir(prefix);
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let temp_path = temp_dir.path();
        let data_path = temp_path.join("data").as_path().display().to_string();
        let builder = Fs::default().root(&data_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();
        let manager = MetadataSnapshotManager::new(kv_backend.clone(), object_store);
        (temp_dir, kv_backend, manager)
    }

    #[tokio::test]
    async fn test_dump_and_restore() {
        common_telemetry::init_default_ut_logging();
        let (temp_dir, kv_backend, manager) = test_env("test_dump_and_restore");
        let temp_path = temp_dir.path();

        for i in 0..10 {
            kv_backend
                .put(
                    PutRequest::new()
                        .with_key(format!("test_{}", i).as_bytes().to_vec())
                        .with_value(format!("value_{}", i).as_bytes().to_vec()),
                )
                .await
                .unwrap();
        }
        let dump_path = temp_path.join("snapshot");
        manager
            .dump(&format!(
                "{}/metadata_snapshot",
                &dump_path.as_path().display().to_string()
            ))
            .await
            .unwrap();
        // Clean up the kv backend
        kv_backend.clear();
        let err = manager
            .dump(&format!(
                "{}/metadata_snapshot.metadata.fb",
                &dump_path.as_path().display().to_string()
            ))
            .await
            .unwrap_err();
        assert_matches!(err, Error::Unexpected { .. });
        assert!(err.to_string().contains("already exists"));

        let restore_path = dump_path
            .join("metadata_snapshot.metadata.fb")
            .as_path()
            .display()
            .to_string();
        manager.restore(&restore_path).await.unwrap();

        for i in 0..10 {
            let key = format!("test_{}", i);
            let value = kv_backend.get(key.as_bytes()).await.unwrap().unwrap();
            assert_eq!(value.value, format!("value_{}", i).as_bytes());
        }
    }

    #[tokio::test]
    async fn test_restore_from_nonexistent_file() {
        let (temp_dir, _kv_backend, manager) = test_env("test_restore_from_nonexistent_file");
        let restore_path = temp_dir
            .path()
            .join("nonexistent.metadata.fb")
            .as_path()
            .display()
            .to_string();
        let err = manager.restore(&restore_path).await.unwrap_err();
        assert_matches!(err, Error::ReadObject { .. })
    }
}
