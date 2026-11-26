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

use std::sync::Arc;
use std::time::Instant;

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, error, info, warn};
use futures::future::join_all;
use object_store::ObjectStore;
use object_store::manager::ObjectStoreManagerRef;
use snafu::{IntoError, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::PathType;
use store_api::storage::{FileId, IndexVersion, RegionId};

use crate::access_layer::AccessLayerRef;
use crate::config::MitoConfig;
use crate::error::{self, InvalidSourceAndTargetRegionSnafu, Result};
use crate::manifest::action::RegionManifest;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::region::opener::get_object_store;
use crate::region::options::RegionOptions;
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::file_metadata::FileMetadataInternerRef;
use crate::sst::location;

/// A loader for loading metadata from a region dir.
#[derive(Debug, Clone)]
pub struct RegionMetadataLoader {
    config: Arc<MitoConfig>,
    object_store_manager: ObjectStoreManagerRef,
    lease_enabled: bool,
    file_metadata_interner: FileMetadataInternerRef,
}

impl RegionMetadataLoader {
    /// Creates a new `RegionMetadataLoader`.
    pub fn new(
        config: Arc<MitoConfig>,
        object_store_manager: ObjectStoreManagerRef,
        lease_enabled: bool,
        file_metadata_interner: FileMetadataInternerRef,
    ) -> Self {
        Self {
            config,
            object_store_manager,
            lease_enabled,
            file_metadata_interner,
        }
    }

    /// Loads the metadata of the region from the region dir.
    pub async fn load(
        &self,
        region_dir: &str,
        region_options: &RegionOptions,
    ) -> Result<Option<RegionMetadataRef>> {
        let manifest = self.load_manifest(region_dir, region_options).await?;
        Ok(manifest.map(|m| m.metadata.clone()))
    }

    /// Loads the manifest of the region from the region dir.
    pub async fn load_manifest(
        &self,
        region_dir: &str,
        region_options: &RegionOptions,
    ) -> Result<Option<Arc<RegionManifest>>> {
        let object_store = get_object_store(&region_options.storage, &self.object_store_manager)?;
        let region_manifest_options = RegionManifestOptions::new(
            &self.config,
            region_dir,
            &object_store,
            self.file_metadata_interner.clone(),
        )
        .with_conditional_publication(
            region_options.requires_conditional_manifest_publication(self.lease_enabled),
        );
        let Some(manifest_manager) =
            RegionManifestManager::open(region_manifest_options, &Default::default()).await?
        else {
            return Ok(None);
        };

        let manifest = manifest_manager.manifest();
        Ok(Some(manifest))
    }
}

/// A copier for copying files from a region to another region.
#[derive(Debug, Clone)]
pub struct RegionFileCopier {
    access_layer: AccessLayerRef,
}

/// A descriptor for a file.
#[derive(Debug, Clone)]
pub enum FileDescriptor {
    /// An index file.
    Index {
        file_id: FileId,
        version: IndexVersion,
        size: u64,
    },
    /// A data file.
    Data { file_id: FileId, size: u64 },
    /// One narthex object; manifests carry rewritten owner metadata.
    NarthexObject {
        source: String,
        target: String,
        size: u64,
        manifest: Option<bytes::Bytes>,
    },
}

impl FileDescriptor {
    pub fn size(&self) -> u64 {
        match self {
            FileDescriptor::Index { size, .. } => *size,
            FileDescriptor::Data { size, .. } => *size,
            FileDescriptor::NarthexObject { size, .. } => *size,
        }
    }

    fn file_path(&self, region_id: RegionId, table_dir: &str, path_type: PathType) -> String {
        match *self {
            FileDescriptor::NarthexObject { ref target, .. } => target.clone(),
            FileDescriptor::Index {
                file_id, version, ..
            } => location::index_file_path(
                table_dir,
                RegionIndexId::new(RegionFileId::new(region_id, file_id), version),
                path_type,
            ),
            FileDescriptor::Data { file_id, .. } => {
                location::sst_file_path(table_dir, RegionFileId::new(region_id, file_id), path_type)
            }
        }
    }
}

/// Builds the source and target file paths for a given file descriptor.
///
/// # Arguments
///
/// * `source_region_id`: The ID of the source region.
/// * `target_region_id`: The ID of the target region.
/// * `file_id`: The ID of the file.
///
/// # Returns
///
/// A tuple containing the source and target file paths.
fn build_copy_file_paths(
    source_region_id: RegionId,
    target_region_id: RegionId,
    file_descriptor: FileDescriptor,
    table_dir: &str,
    path_type: PathType,
) -> (String, String) {
    if let FileDescriptor::NarthexObject { source, target, .. } = file_descriptor {
        return (source, target);
    }
    (
        file_descriptor.file_path(source_region_id, table_dir, path_type),
        file_descriptor.file_path(target_region_id, table_dir, path_type),
    )
}

fn build_delete_file_path(
    target_region_id: RegionId,
    file_descriptor: FileDescriptor,
    table_dir: &str,
    path_type: PathType,
) -> String {
    file_descriptor.file_path(target_region_id, table_dir, path_type)
}

/// What one copy destination turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyCreation {
    /// This attempt created the destination, so a rollback may delete it.
    Created,
    /// The destination already existed and its size matched: another reference owns it, a
    /// rollback must keep it.
    Reused,
    /// The backend offers no conditional create, so the destination was written
    /// unconditionally and this attempt does not claim it. A rollback may leave an orphan,
    /// but it can never delete an object another reference still serves.
    Unclaimed,
}

/// Chunk size of the buffered create used when a backend cannot copy conditionally.
const CONDITIONAL_COPY_CHUNK: u64 = 8 * 1024 * 1024;

impl RegionFileCopier {
    /// Creates one copy destination if no object occupies it yet.
    ///
    /// The destination is created with a conditional operation so an object that appears
    /// after the copy was planned is never overwritten: when the destination turns out to
    /// exist, its size is verified and the copy reuses it. Only [CopyCreation::Created]
    /// lets a rollback delete the object.
    async fn create_destination(
        object_store: &ObjectStore,
        target_region_id: RegionId,
        file_desc: &FileDescriptor,
        source_path: &str,
        target_path: &str,
        path_type: PathType,
    ) -> Result<CopyCreation> {
        let conditional = if let FileDescriptor::NarthexObject {
            manifest: Some(bytes),
            ..
        } = file_desc
        {
            // The rewritten artifact manifest is a create-if-absent write.
            object_store
                .write_with(target_path, bytes.clone())
                .if_not_exists(true)
                .await
                .map(|_| ())
        } else {
            object_store
                .copy_with(source_path, target_path)
                .if_not_exists(true)
                .await
                .map(|_| ())
        };
        match conditional {
            Ok(()) => return Ok(CopyCreation::Created),
            Err(err) => {
                if Self::is_conflict(&err) {
                    return Self::reuse_destination(
                        object_store,
                        target_region_id,
                        file_desc,
                        target_path,
                    )
                    .await;
                }
                if err.kind() != object_store::ErrorKind::Unsupported {
                    return Err(Self::map_copy_error(
                        err,
                        target_region_id,
                        source_path,
                        path_type,
                    ));
                }
                warn!(
                    "Conditional copy is unsupported by {}; creating {} without overwriting",
                    object_store.info().scheme(),
                    target_path
                );
            }
        }

        // The backend cannot copy conditionally. Keep creation conditional with a buffered
        // write when the backend supports one: a rollback must never delete a retained
        // object, and reusing a destination is only safe when it was never overwritten.
        if object_store.info().capability().write_with_if_not_exists {
            return Self::create_by_conditional_write(
                object_store,
                target_region_id,
                file_desc,
                source_path,
                target_path,
                path_type,
            )
            .await;
        }

        // No conditional create at all: write the destination but do not claim it, so a
        // rollback can only leak an orphan, never delete an object another reference owns.
        warn!(
            "Conditional create is unsupported by {}; writing {} unconditionally without a rollback claim",
            object_store.info().scheme(),
            target_path
        );
        let result = if let FileDescriptor::NarthexObject {
            manifest: Some(bytes),
            ..
        } = file_desc
        {
            object_store
                .write(target_path, bytes.clone())
                .await
                .map(|_| ())
        } else {
            object_store
                .copy(source_path, target_path)
                .await
                .map(|_| ())
        };
        result
            .map_err(|err| Self::map_copy_error(err, target_region_id, source_path, path_type))
            .map(|()| CopyCreation::Unclaimed)
    }

    /// Copies `source_path` into an absent `target_path` through a conditional write.
    async fn create_by_conditional_write(
        object_store: &ObjectStore,
        target_region_id: RegionId,
        file_desc: &FileDescriptor,
        source_path: &str,
        target_path: &str,
        path_type: PathType,
    ) -> Result<CopyCreation> {
        match Self::stream_into_absent_destination(object_store, source_path, target_path).await {
            Ok(()) => Ok(CopyCreation::Created),
            Err(err) if Self::is_conflict(&err) => {
                Self::reuse_destination(object_store, target_region_id, file_desc, target_path)
                    .await
            }
            Err(err) => Err(Self::map_copy_error(
                err,
                target_region_id,
                source_path,
                path_type,
            )),
        }
    }

    /// Streams one object into a destination that must not exist yet.
    async fn stream_into_absent_destination(
        object_store: &ObjectStore,
        source_path: &str,
        target_path: &str,
    ) -> std::result::Result<(), object_store::Error> {
        let source_size = object_store.stat(source_path).await?.content_length();
        let reader = object_store.reader(source_path).await?;
        let mut writer = object_store
            .writer_with(target_path)
            .if_not_exists(true)
            .await?;
        let mut offset = 0u64;
        while offset < source_size {
            let end = (offset + CONDITIONAL_COPY_CHUNK).min(source_size);
            let buffer = reader.read(offset..end).await?;
            if buffer.is_empty() {
                break;
            }
            offset += buffer.len() as u64;
            writer.write(buffer).await?;
        }
        writer.close().await?;
        Ok(())
    }

    /// Verifies a destination another reference already owns and reports it as reused.
    async fn reuse_destination(
        object_store: &ObjectStore,
        target_region_id: RegionId,
        file_desc: &FileDescriptor,
        target_path: &str,
    ) -> Result<CopyCreation> {
        let metadata = object_store
            .stat(target_path)
            .await
            .map_err(|err| error::OpenDalSnafu.into_error(err))?;
        if metadata.content_length() != file_desc.size() {
            return error::InvalidRequestSnafu {
                region_id: target_region_id,
                reason: format!(
                    "existing copy destination {target_path} has {} bytes but the copied object has {}",
                    metadata.content_length(),
                    file_desc.size()
                ),
            }
            .fail();
        }
        Ok(CopyCreation::Reused)
    }

    /// Whether `err` reports that the destination already exists.
    fn is_conflict(err: &object_store::Error) -> bool {
        matches!(
            err.kind(),
            object_store::ErrorKind::AlreadyExists | object_store::ErrorKind::ConditionNotMatch
        )
    }

    /// Maps an object-store failure of one copied object into a region error.
    fn map_copy_error(
        err: object_store::Error,
        _target_region_id: RegionId,
        source_path: &str,
        path_type: PathType,
    ) -> crate::error::Error {
        if path_type == PathType::Metadata && err.kind() == object_store::ErrorKind::NotFound {
            error::MetadataCopyFileNotFoundSnafu {
                path: source_path.to_string(),
            }
            .into_error(err)
        } else {
            error::OpenDalSnafu.into_error(err)
        }
    }

    pub fn new(access_layer: AccessLayerRef) -> Self {
        Self { access_layer }
    }

    /// Copies files from a source region to a target region.
    ///
    /// # Arguments
    ///
    /// * `source_region_id`: The ID of the source region.
    /// * `target_region_id`: The ID of the target region.
    /// * `file_ids`: The IDs of the files to copy.
    pub async fn copy_files(
        &self,
        source_region_id: RegionId,
        target_region_id: RegionId,
        file_ids: Vec<FileDescriptor>,
        parallelism: usize,
    ) -> Result<()> {
        ensure!(
            source_region_id.table_id() == target_region_id.table_id(),
            InvalidSourceAndTargetRegionSnafu {
                source_region_id,
                target_region_id,
            },
        );
        let table_dir = self.access_layer.table_dir();
        let path_type = self.access_layer.path_type();
        let object_store = self.access_layer.object_store();

        info!(
            "Copying {} files from region {} to region {}",
            file_ids.len(),
            source_region_id,
            target_region_id
        );
        debug!(
            "Copying files: {:?} from region {} to region {}",
            file_ids, source_region_id, target_region_id
        );
        let mut tasks = Vec::with_capacity(parallelism);
        for skip in 0..parallelism {
            let target_file_ids: Vec<_> = file_ids
                .iter()
                .skip(skip)
                .step_by(parallelism)
                .cloned()
                .collect();
            let object_store = object_store.clone();
            tasks.push(async move {
                // The destinations this lane created, which a failure must reclaim, and the
                // artifact directories it reused, which a rollback must keep. A lane owns its
                // half of the descriptors, so the lanes never overlap.
                let mut created = Vec::new();
                let mut reused_dirs = std::collections::HashSet::new();
                for file_desc in target_file_ids {
                    let (source_path, target_path) = build_copy_file_paths(
                        source_region_id,
                        target_region_id,
                        file_desc.clone(),
                        table_dir,
                        path_type,
                    );
                    let now = Instant::now();
                    // Create the destination only if it is absent: a destination that already
                    // exists belongs to another reference to the same file id (a copy another
                    // region still retains, or a repeated attempt of this one) and must not be
                    // overwritten. Only a destination this attempt created can be rolled back.
                    match Self::create_destination(
                        &object_store,
                        target_region_id,
                        &file_desc,
                        &source_path,
                        &target_path,
                        path_type,
                    )
                    .await
                    {
                        Ok(CopyCreation::Created) => created.push(file_desc.clone()),
                        Ok(CopyCreation::Reused) => {
                            if let Some(dir) = location::parent_dir(&target_path) {
                                reused_dirs.insert(dir.to_string());
                            }
                            info!(
                                "Reused existing copy destination {}, file size: {}",
                                target_path,
                                ReadableSize(file_desc.size()),
                            );
                            continue;
                        }
                        Ok(CopyCreation::Unclaimed) => {
                            info!(
                                "Copied file {source_path} to {target_path} without a rollback claim, file size: {}",
                                ReadableSize(file_desc.size()),
                            );
                            continue;
                        }
                        Err(err) => {
                            error!(err; "Failed to copy file {} to {}", source_path, target_path);
                            return (created, reused_dirs, Err(err));
                        }
                    }
                    let file_size = ReadableSize(file_desc.size());
                    info!(
                        "Copied file {} to {}, file size: {}, elapsed: {:?}",
                        source_path,
                        target_path,
                        file_size,
                        now.elapsed(),
                    );
                }

                (created, reused_dirs, Ok(()))
            });
        }

        // Finish sibling copies before deleting the objects this attempt created.
        let mut created = Vec::new();
        let mut reused_dirs = std::collections::HashSet::new();
        let mut failure = None;
        for (lane_created, lane_reused_dirs, lane_result) in join_all(tasks).await {
            created.extend(lane_created);
            reused_dirs.extend(lane_reused_dirs);
            if failure.is_none()
                && let Err(err) = lane_result
            {
                failure = Some(err);
            }
        }
        if let Some(err) = failure {
            error!(err; "Failed to copy files from region {} to region {}", source_region_id, target_region_id);
            self.clean_target_region(target_region_id, &created, &reused_dirs)
                .await;
            return Err(err);
        }

        Ok(())
    }

    /// Cleans the objects this attempt created in the target region.
    ///
    /// `created` holds the destinations this attempt wrote and `reused_dirs` the artifact
    /// directories that already held an object it reused, so an artifact another region
    /// retains survives the rollback.
    async fn clean_target_region(
        &self,
        target_region_id: RegionId,
        created: &[FileDescriptor],
        reused_dirs: &std::collections::HashSet<String>,
    ) {
        if created.is_empty() {
            return;
        }
        let table_dir = self.access_layer.table_dir();
        let path_type = self.access_layer.path_type();
        let object_store = self.access_layer.object_store();
        let created_paths: std::collections::HashSet<_> = created
            .iter()
            .map(|file| {
                build_delete_file_path(target_region_id, file.clone(), table_dir, path_type)
            })
            .collect();
        let narthex_dirs: std::collections::BTreeSet<_> = created
            .iter()
            .filter_map(|file| {
                if let FileDescriptor::NarthexObject { target, .. } = file {
                    location::parent_dir(target)
                        .map(str::to_string)
                        .filter(|dir| !reused_dirs.contains(dir))
                } else {
                    None
                }
            })
            .collect();
        let delete_file_path = created_paths.into_iter().collect::<Vec<_>>();
        debug!(
            "Deleting files: {:?} after failed to copy files to target region {}",
            delete_file_path, target_region_id
        );
        if let Err(err) = object_store.delete_iter(delete_file_path).await {
            error!(err; "Failed to delete files from region {}", target_region_id);
            return;
        }
        for dir in narthex_dirs {
            if let Err(err) = object_store.delete(&dir).await {
                error!(err; "Failed to remove copied artifact directory {dir}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "hdfs-object-store")]
    use object_store::ObjectStore;
    #[cfg(feature = "hdfs-object-store")]
    use object_store::layers::HdfsCompatibilityLayer;
    #[cfg(feature = "hdfs-object-store")]
    use object_store::services::Fs;

    use super::*;
    #[cfg(feature = "hdfs-object-store")]
    use crate::access_layer::AccessLayer;
    #[cfg(feature = "hdfs-object-store")]
    use crate::sst::index::intermediate::IntermediateManager;
    #[cfg(feature = "hdfs-object-store")]
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    #[rstest::rstest]
    #[case(PathType::Metadata, common_error::ext::RetryHint::Retryable)]
    #[case(PathType::Bare, common_error::ext::RetryHint::NonRetryable)]
    #[tokio::test]
    async fn test_missing_copy_file_retry_hint(
        #[case] path_type: PathType,
        #[case] expected: common_error::ext::RetryHint,
    ) {
        use crate::test_util::{CreateRequestBuilder, TestEnv};
        use common_error::ext::ErrorExt;
        use store_api::region_engine::RegionEngine;
        use store_api::region_request::RegionRequest;

        let mut env = TestEnv::new().await;
        let engine = env.create_engine(Default::default()).await;
        let target = RegionId::new(1, 2);
        let mut create = CreateRequestBuilder::new().build();
        create.path_type = path_type;
        engine
            .handle_request(target, RegionRequest::Create(create))
            .await
            .unwrap();
        let copier = RegionFileCopier::new(engine.get_region(target).unwrap().access_layer());
        let missing = FileDescriptor::Data {
            file_id: FileId::random(),
            size: 1,
        };
        let err = copier
            .copy_files(RegionId::new(1, 1), target, vec![missing], 1)
            .await
            .unwrap_err();
        assert_eq!(err.retry_hint(), expected);
    }

    #[test]
    fn test_build_copy_file_paths() {
        common_telemetry::init_default_ut_logging();
        let file_id = FileId::random();
        let source_region_id = RegionId::new(1, 1);
        let target_region_id = RegionId::new(1, 2);
        let file_descriptor = FileDescriptor::Data { file_id, size: 100 };
        let table_dir = "/table_dir";
        let path_type = PathType::Bare;
        let (source_path, target_path) = build_copy_file_paths(
            source_region_id,
            target_region_id,
            file_descriptor,
            table_dir,
            path_type,
        );
        assert_eq!(
            source_path,
            format!("/table_dir/1_0000000001/{}.parquet", file_id)
        );
        assert_eq!(
            target_path,
            format!("/table_dir/1_0000000002/{}.parquet", file_id)
        );

        let version = 1;
        let file_descriptor = FileDescriptor::Index {
            file_id,
            version,
            size: 100,
        };
        let (source_path, target_path) = build_copy_file_paths(
            source_region_id,
            target_region_id,
            file_descriptor,
            table_dir,
            path_type,
        );
        assert_eq!(
            source_path,
            format!(
                "/table_dir/1_0000000001/index/{}.{}.puffin",
                file_id, version
            )
        );
        assert_eq!(
            target_path,
            format!(
                "/table_dir/1_0000000002/index/{}.{}.puffin",
                file_id, version
            )
        );
    }

    #[test]
    fn test_build_delete_file_path() {
        common_telemetry::init_default_ut_logging();
        let file_id = FileId::random();
        let target_region_id = RegionId::new(1, 2);
        let table_dir = "/table_dir";
        let path_type = PathType::Bare;

        let file_descriptor = FileDescriptor::Data { file_id, size: 100 };
        let path = build_delete_file_path(target_region_id, file_descriptor, table_dir, path_type);
        assert_eq!(path, format!("/table_dir/1_0000000002/{}.parquet", file_id));

        let file_descriptor = FileDescriptor::Index {
            file_id,
            version: 1,
            size: 100,
        };
        let path = build_delete_file_path(target_region_id, file_descriptor, table_dir, path_type);
        assert_eq!(
            path,
            format!("/table_dir/1_0000000002/index/{}.1.puffin", file_id)
        );
    }

    #[cfg(feature = "hdfs-object-store")]
    #[tokio::test]
    async fn test_copy_region_files_with_hdfs_fallback() {
        let (temp_dir, puffin_manager) =
            PuffinManagerFactory::new_for_test_async("hdfs-copy-region").await;
        let intermediate_manager = IntermediateManager::init_fs(temp_dir.path().to_string_lossy())
            .await
            .unwrap();
        let storage_dir = temp_dir.path().join("storage");
        std::fs::create_dir(&storage_dir).unwrap();
        let object_store = ObjectStore::new(Fs::default().root(storage_dir.to_str().unwrap()))
            .unwrap()
            .layer(HdfsCompatibilityLayer::new_for_test());
        let access_layer = Arc::new(AccessLayer::new(
            "table_dir",
            PathType::Bare,
            object_store.clone(),
            puffin_manager,
            intermediate_manager,
        ));
        let copier = RegionFileCopier::new(access_layer);
        let source_region_id = RegionId::new(1, 1);
        let target_region_id = RegionId::new(1, 2);
        let file_id = FileId::random();
        let descriptor = FileDescriptor::Data { file_id, size: 8 };
        let (source_path, target_path) = build_copy_file_paths(
            source_region_id,
            target_region_id,
            descriptor.clone(),
            "table_dir",
            PathType::Bare,
        );
        object_store.write(&source_path, "contents").await.unwrap();

        copier
            .copy_files(source_region_id, target_region_id, vec![descriptor], 1)
            .await
            .unwrap();

        assert_eq!(
            b"contents",
            object_store
                .read(&target_path)
                .await
                .unwrap()
                .to_bytes()
                .as_ref()
        );
    }
}

#[cfg(test)]
mod conditional_create_tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use common_test_util::temp_dir::create_temp_dir;
    use object_store::ObjectStore;
    use object_store::layers::mock::MockLayerBuilder;
    use object_store::services::Fs;
    use store_api::region_request::PathType;

    use super::*;
    use crate::access_layer::AccessLayer;
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    /// Creates a destination from inside the copy, which is what an object appearing after the
    /// copy planned its descriptors looks like to the copier.
    #[allow(clippy::type_complexity)]
    fn races_layer(
        store: ObjectStore,
        plan: Arc<Mutex<HashMap<String, (String, &'static str)>>>,
    ) -> ObjectStore {
        let race_store = store.clone();
        let layer = MockLayerBuilder::default()
            .stat_interceptor(Arc::new(move |path: &str| {
                let store = race_store.clone();
                let plan = plan.clone();
                let path = path.to_string();
                Box::pin(async move {
                    let created = plan.lock().unwrap().remove(&path);
                    if let Some((target, bytes)) = created {
                        store.write(&target, bytes).await.unwrap();
                    }
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            }))
            .build()
            .unwrap();
        store.layer(layer)
    }

    async fn copier(
        dir: &common_test_util::temp_dir::TempDir,
        races: Arc<Mutex<HashMap<String, (String, &'static str)>>>,
    ) -> RegionFileCopier {
        let store = ObjectStore::new(Fs::default().root(dir.path().to_str().unwrap())).unwrap();
        let store = races_layer(store, races);
        let (puffin, puffin_manager) =
            PuffinManagerFactory::new_for_test_async("copy-conditional-create").await;
        let intermediate = IntermediateManager::init_fs(puffin.path().to_str().unwrap())
            .await
            .unwrap();
        let access = Arc::new(AccessLayer::new(
            "table",
            PathType::Bare,
            store,
            puffin_manager,
            intermediate,
        ));
        RegionFileCopier::new(access)
    }

    fn descriptor(source: &str, target: &str, size: u64) -> FileDescriptor {
        FileDescriptor::NarthexObject {
            source: source.to_string(),
            target: target.to_string(),
            size,
            manifest: None,
        }
    }

    /// A destination that appears after the copy planned it is created conditionally: the
    /// attempt reuses it once its size matches and never overwrites it.
    #[tokio::test]
    async fn test_copy_reuses_destination_created_after_planning() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("copy-conditional-create-reuse");
        let races = Arc::new(Mutex::new(HashMap::from([(
            "table/copy-a".to_string(),
            ("table/copy-a-target".to_string(), "XX"),
        )])));
        let copier = copier(&dir, races.clone()).await;
        let access = AccessLayer::new(
            "table",
            PathType::Bare,
            copier.access_layer.object_store().clone(),
            copier.access_layer.puffin_manager_factory().clone(),
            copier.access_layer.intermediate_manager().clone(),
        );
        access
            .object_store()
            .write("table/copy-a", "AA")
            .await
            .unwrap();

        copier
            .copy_files(
                RegionId::new(1, 1),
                RegionId::new(1, 2),
                vec![descriptor("table/copy-a", "table/copy-a-target", 2)],
                1,
            )
            .await
            .unwrap();

        assert_eq!(
            b"XX",
            access
                .object_store()
                .read("table/copy-a-target")
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            "the destination created after planning must not be overwritten"
        );
    }

    /// A destination that appears after the copy planned it but does not match the expected
    /// size fails the copy without being overwritten.
    #[tokio::test]
    async fn test_copy_rejects_mismatched_existing_destination() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("copy-conditional-create-mismatch");
        let races = Arc::new(Mutex::new(HashMap::from([(
            "table/copy-a".to_string(),
            ("table/copy-a-target".to_string(), "XXXX"),
        )])));
        let copier = copier(&dir, races.clone()).await;
        let store = copier.access_layer.object_store().clone();
        store.write("table/copy-a", "AA").await.unwrap();

        let err = copier
            .copy_files(
                RegionId::new(1, 1),
                RegionId::new(1, 2),
                vec![descriptor("table/copy-a", "table/copy-a-target", 2)],
                1,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, error::Error::InvalidRequest { .. }),
            "unexpected err: {err}"
        );
        assert_eq!(
            b"XXXX",
            store
                .read("table/copy-a-target")
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            "a reused destination must keep its bytes when the sizes disagree"
        );
    }

    /// A rollback deletes only the destinations this attempt created.
    #[tokio::test]
    async fn test_copy_rollback_keeps_existing_destination() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("copy-conditional-create-rollback");
        let copier = copier(&dir, Arc::new(Mutex::new(HashMap::new()))).await;
        let store = copier.access_layer.object_store().clone();
        store.write("table/copy-a", "AA").await.unwrap();
        // The destination exists before the copy: another reference owns it.
        store.write("table/copy-a-target", "XX").await.unwrap();

        // The second descriptor fails because its source object does not exist.
        let err = copier
            .copy_files(
                RegionId::new(1, 1),
                RegionId::new(1, 2),
                vec![
                    descriptor("table/copy-a", "table/copy-a-target", 2),
                    descriptor("table/copy-missing", "table/copy-missing-target", 2),
                ],
                1,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, error::Error::OpenDal { .. }),
            "unexpected err: {err:?}"
        );
        assert_eq!(
            b"XX",
            store
                .read("table/copy-a-target")
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            "the rollback must keep a destination this attempt reused"
        );
    }

    /// A destination this attempt created is reclaimed when a sibling fails.
    #[tokio::test]
    async fn test_copy_rollback_removes_created_destination() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("copy-conditional-create-created");
        let races = Arc::new(Mutex::new(HashMap::new()));
        let copier = copier(&dir, races).await;
        let store = copier.access_layer.object_store().clone();
        store.write("table/copy-a", "AA").await.unwrap();

        let err = copier
            .copy_files(
                RegionId::new(1, 1),
                RegionId::new(1, 2),
                vec![
                    descriptor("table/copy-a", "table/copy-a-target", 2),
                    descriptor("table/copy-missing", "table/copy-missing-target", 2),
                ],
                1,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, error::Error::OpenDal { .. }),
            "unexpected err: {err:?}"
        );
        assert!(
            !store.exists("table/copy-a-target").await.unwrap(),
            "the rollback must reclaim a destination this attempt created"
        );
    }
}
