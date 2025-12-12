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

use common_telemetry::{debug, error, info};
use futures::future::try_join_all;
use object_store::manager::ObjectStoreManagerRef;
use snafu::{ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::PathType;
use store_api::storage::{FileId, IndexVersion, RegionId};
use tokio::sync::Semaphore;

use crate::access_layer::AccessLayerRef;
use crate::config::MitoConfig;
use crate::error::{self, InvalidSourceAndTargetRegionSnafu, Result};
use crate::manifest::action::RegionManifest;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::region::opener::get_object_store;
use crate::region::options::RegionOptions;
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::location;

/// A loader for loading metadata from a region dir.
#[derive(Debug, Clone)]
pub struct RegionMetadataLoader {
    config: Arc<MitoConfig>,
    object_store_manager: ObjectStoreManagerRef,
}

impl RegionMetadataLoader {
    /// Creates a new `RegionOpenerBuilder`.
    pub fn new(config: Arc<MitoConfig>, object_store_manager: ObjectStoreManagerRef) -> Self {
        Self {
            config,
            object_store_manager,
        }
    }

    /// Loads the metadata of the region from the region dir.
    pub async fn load(
        &self,
        region_dir: &str,
        region_options: &RegionOptions,
    ) -> Result<Option<RegionMetadataRef>> {
        let manifest = self
            .load_manifest(region_dir, &region_options.storage)
            .await?;
        Ok(manifest.map(|m| m.metadata.clone()))
    }

    /// Loads the manifest of the region from the region dir.
    pub async fn load_manifest(
        &self,
        region_dir: &str,
        storage: &Option<String>,
    ) -> Result<Option<Arc<RegionManifest>>> {
        let object_store = get_object_store(storage, &self.object_store_manager)?;
        let region_manifest_options =
            RegionManifestOptions::new(&self.config, region_dir, &object_store);
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
#[derive(Debug, Clone, Copy)]
pub enum FileDescriptor {
    /// An index file.
    Index((FileId, IndexVersion)),
    /// A data file.
    Data(FileId),
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
    match file_descriptor {
        FileDescriptor::Index((file_id, version)) => (
            location::index_file_path(
                table_dir,
                RegionIndexId::new(RegionFileId::new(source_region_id, file_id), version),
                path_type,
            ),
            location::index_file_path(
                table_dir,
                RegionIndexId::new(RegionFileId::new(target_region_id, file_id), version),
                path_type,
            ),
        ),
        FileDescriptor::Data(file_id) => (
            location::sst_file_path(
                table_dir,
                RegionFileId::new(source_region_id, file_id),
                path_type,
            ),
            location::sst_file_path(
                table_dir,
                RegionFileId::new(target_region_id, file_id),
                path_type,
            ),
        ),
    }
}

fn build_delete_file_path(
    target_region_id: RegionId,
    file_descriptor: FileDescriptor,
    table_dir: &str,
    path_type: PathType,
) -> String {
    match file_descriptor {
        FileDescriptor::Index((file_id, version)) => location::index_file_path(
            table_dir,
            RegionIndexId::new(RegionFileId::new(target_region_id, file_id), version),
            path_type,
        ),
        FileDescriptor::Data(file_id) => location::sst_file_path(
            table_dir,
            RegionFileId::new(target_region_id, file_id),
            path_type,
        ),
    }
}

impl RegionFileCopier {
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
        let semaphore = Arc::new(Semaphore::new(parallelism));
        let mut tasks = Vec::with_capacity(file_ids.len());
        let num_file = file_ids.len();
        for (index, file_desc) in file_ids.iter().enumerate() {
            let (source_path, target_path) = build_copy_file_paths(
                source_region_id,
                target_region_id,
                *file_desc,
                table_dir,
                path_type,
            );
            let moved_semaphore = semaphore.clone();
            tasks.push(async move {
                let _permit = moved_semaphore.acquire().await.unwrap();
                let now = Instant::now();
                object_store
                    .copy(&source_path, &target_path)
                    .await
                    .inspect_err(
                        |e| error!(e; "Failed to copy file {} to {}", source_path, target_path),
                    )
                    .context(error::OpenDalSnafu)?;
                info!(
                    "Copied file {} to {}, elapsed: {:?}, progress: {}/{}",
                    source_path,
                    target_path,
                    now.elapsed(),
                    index + 1,
                    num_file
                );
                Ok(())
            });
        }
        if let Err(err) = try_join_all(tasks).await {
            error!(err; "Failed to copy files from region {} to region {}", source_region_id, target_region_id);
            self.clean_target_region(target_region_id, file_ids).await;
            return Err(err);
        }
        Ok(())
    }

    /// Cleans the copied files from the target region.
    async fn clean_target_region(&self, target_region_id: RegionId, file_ids: Vec<FileDescriptor>) {
        let table_dir = self.access_layer.table_dir();
        let path_type = self.access_layer.path_type();
        let object_store = self.access_layer.object_store();
        let delete_file_path = file_ids
            .into_iter()
            .map(|file_descriptor| {
                build_delete_file_path(target_region_id, file_descriptor, table_dir, path_type)
            })
            .collect::<Vec<_>>();
        debug!(
            "Deleting files: {:?} after failed to copy files to target region {}",
            delete_file_path, target_region_id
        );
        if let Err(err) = object_store.delete_iter(delete_file_path).await {
            error!(err; "Failed to delete files from region {}", target_region_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_copy_file_paths() {
        common_telemetry::init_default_ut_logging();
        let file_id = FileId::random();
        let source_region_id = RegionId::new(1, 1);
        let target_region_id = RegionId::new(1, 2);
        let file_descriptor = FileDescriptor::Data(file_id);
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
        let file_descriptor = FileDescriptor::Index((file_id, version));
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
}
