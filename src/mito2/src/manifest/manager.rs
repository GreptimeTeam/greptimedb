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
use std::sync::atomic::{AtomicU64, Ordering};

use common_datasource::compression::CompressionType;
use common_telemetry::{debug, info};
use futures::TryStreamExt;
use object_store::ObjectStore;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;
use store_api::{MAX_VERSION, MIN_VERSION, ManifestVersion};

use crate::config::MitoConfig;
use crate::error::{
    self, InstallManifestToSnafu, NoCheckpointSnafu, NoManifestsSnafu, RegionStoppedSnafu, Result,
};
use crate::manifest::action::{
    RegionChange, RegionCheckpoint, RegionEdit, RegionManifest, RegionManifestBuilder,
    RegionMetaAction, RegionMetaActionList,
};
use crate::manifest::checkpointer::Checkpointer;
use crate::manifest::storage::{
    ManifestObjectStore, file_version, is_checkpoint_file, is_delta_file, manifest_compress_type,
    manifest_dir,
};
use crate::metrics::MANIFEST_OP_ELAPSED;
use crate::region::{ManifestStats, RegionLeaderState, RegionRoleState};
use crate::sst::FormatType;

/// Options for [RegionManifestManager].
#[derive(Debug, Clone)]
pub struct RegionManifestOptions {
    /// Directory to store manifest.
    pub manifest_dir: String,
    pub object_store: ObjectStore,
    pub compress_type: CompressionType,
    /// Interval of version ([ManifestVersion](store_api::manifest::ManifestVersion)) between two checkpoints.
    /// Set to 0 to disable checkpoint.
    pub checkpoint_distance: u64,
    pub remove_file_options: RemoveFileOptions,
}

impl RegionManifestOptions {
    /// Creates a new [RegionManifestOptions] with the given region directory, object store, and configuration.
    pub fn new(config: &MitoConfig, region_dir: &str, object_store: &ObjectStore) -> Self {
        RegionManifestOptions {
            manifest_dir: manifest_dir(region_dir),
            object_store: object_store.clone(),
            // We don't allow users to set the compression algorithm as we use it as a file suffix.
            // Currently, the manifest storage doesn't have good support for changing compression algorithms.
            compress_type: manifest_compress_type(config.compress_manifest),
            checkpoint_distance: config.manifest_checkpoint_distance,
            remove_file_options: RemoveFileOptions {
                enable_gc: config.gc.enable,
            },
        }
    }
}

/// Options for updating `removed_files` field in [RegionManifest].
#[derive(Debug, Clone)]
#[cfg_attr(any(test, feature = "test"), derive(Default))]
pub struct RemoveFileOptions {
    /// Whether GC is enabled. If not, the removed files should always be empty when persisting manifest.
    pub enable_gc: bool,
}

// rewrite note:
// trait Checkpoint -> struct RegionCheckpoint
// trait MetaAction -> struct RegionMetaActionList
// trait MetaActionIterator -> struct MetaActionIteratorImpl

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Manage region's manifest. Provide APIs to access (create/modify/recover) region's persisted
/// metadata.
///
/// ```mermaid
/// classDiagram
/// class RegionManifestManager {
///     -ManifestObjectStore store
///     -RegionManifestOptions options
///     -RegionManifest manifest
///     +new() RegionManifestManager
///     +open() Option~RegionManifestManager~
///     +stop()
///     +update(RegionMetaActionList action_list) ManifestVersion
///     +manifest() RegionManifest
/// }
/// class ManifestObjectStore {
///     -ObjectStore object_store
/// }
/// class RegionChange {
///     -RegionMetadataRef metadata
/// }
/// class RegionEdit {
///     -VersionNumber region_version
///     -Vec~FileMeta~ files_to_add
///     -Vec~FileMeta~ files_to_remove
///     -SequenceNumber flushed_sequence
/// }
/// class RegionRemove {
///     -RegionId region_id
/// }
/// RegionManifestManager o-- ManifestObjectStore
/// RegionManifestManager o-- RegionManifest
/// RegionManifestManager o-- RegionManifestOptions
/// RegionManifestManager -- RegionMetaActionList
/// RegionManifestManager -- RegionCheckpoint
/// ManifestObjectStore o-- ObjectStore
/// RegionMetaActionList o-- RegionMetaAction
/// RegionMetaAction o-- ProtocolAction
/// RegionMetaAction o-- RegionChange
/// RegionMetaAction o-- RegionEdit
/// RegionMetaAction o-- RegionRemove
/// RegionChange o-- RegionMetadata
/// RegionEdit o-- FileMeta
///
/// class RegionManifest {
///     -RegionMetadataRef metadata
///     -HashMap&lt;FileId, FileMeta&gt; files
///     -ManifestVersion manifest_version
/// }
/// class RegionMetadata
/// class FileMeta
/// RegionManifest o-- RegionMetadata
/// RegionManifest o-- FileMeta
///
/// class RegionCheckpoint {
///     -ManifestVersion last_version
///     -Option~RegionManifest~ checkpoint
/// }
/// RegionCheckpoint o-- RegionManifest
/// ```
#[derive(Debug)]
pub struct RegionManifestManager {
    store: ManifestObjectStore,
    last_version: Arc<AtomicU64>,
    checkpointer: Checkpointer,
    manifest: Arc<RegionManifest>,
    // Staging manifest is used to store the manifest of the staging region before it becomes available.
    // It is initially inherited from the previous manifest(i.e., `self.manifest`).
    // When the staging manifest becomes available, it will be used to construct the new manifest.
    staging_manifest: Option<Arc<RegionManifest>>,
    stats: ManifestStats,
    stopped: bool,
}

impl RegionManifestManager {
    /// Constructs a region's manifest and persist it.
    pub async fn new(
        metadata: RegionMetadataRef,
        flushed_entry_id: u64,
        options: RegionManifestOptions,
        sst_format: FormatType,
        stats: &ManifestStats,
    ) -> Result<Self> {
        // construct storage
        let mut store = ManifestObjectStore::new(
            &options.manifest_dir,
            options.object_store.clone(),
            options.compress_type,
            stats.total_manifest_size.clone(),
        );
        let manifest_version = stats.manifest_version.clone();

        info!(
            "Creating region manifest in {} with metadata {:?}, flushed_entry_id: {}",
            options.manifest_dir, metadata, flushed_entry_id
        );

        let version = MIN_VERSION;
        let mut manifest_builder = RegionManifestBuilder::default();
        // set the initial metadata.
        manifest_builder.apply_change(
            version,
            RegionChange {
                metadata: metadata.clone(),
                sst_format,
            },
        );
        let manifest = manifest_builder.try_build()?;
        let region_id = metadata.region_id;

        debug!(
            "Build region manifest in {}, manifest: {:?}",
            options.manifest_dir, manifest
        );

        let mut actions = vec![RegionMetaAction::Change(RegionChange {
            metadata,
            sst_format,
        })];
        if flushed_entry_id > 0 {
            actions.push(RegionMetaAction::Edit(RegionEdit {
                files_to_add: vec![],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: Some(flushed_entry_id),
                flushed_sequence: None,
                committed_sequence: None,
            }));
        }

        // Persist region change.
        let action_list = RegionMetaActionList::new(actions);

        // New region is not in staging mode.
        // TODO(ruihang): add staging mode support if needed.
        store.save(version, &action_list.encode()?, false).await?;

        let checkpointer = Checkpointer::new(region_id, options, store.clone(), MIN_VERSION);
        manifest_version.store(version, Ordering::Relaxed);
        manifest
            .removed_files
            .update_file_removed_cnt_to_stats(stats);
        Ok(Self {
            store,
            last_version: manifest_version,
            checkpointer,
            manifest: Arc::new(manifest),
            staging_manifest: None,
            stats: stats.clone(),
            stopped: false,
        })
    }

    /// Opens an existing manifest.
    ///
    /// Returns `Ok(None)` if no such manifest.
    pub async fn open(
        options: RegionManifestOptions,
        stats: &ManifestStats,
    ) -> Result<Option<Self>> {
        let _t = MANIFEST_OP_ELAPSED
            .with_label_values(&["open"])
            .start_timer();

        // construct storage
        let mut store = ManifestObjectStore::new(
            &options.manifest_dir,
            options.object_store.clone(),
            options.compress_type,
            stats.total_manifest_size.clone(),
        );
        let manifest_version = stats.manifest_version.clone();

        // recover from storage
        // construct manifest builder
        // calculate the manifest size from the latest checkpoint
        let mut version = MIN_VERSION;
        let checkpoint = Self::last_checkpoint(&mut store).await?;
        let last_checkpoint_version = checkpoint
            .as_ref()
            .map(|(checkpoint, _)| checkpoint.last_version)
            .unwrap_or(MIN_VERSION);
        let mut manifest_builder = if let Some((checkpoint, _)) = checkpoint {
            info!(
                "Recover region manifest {} from checkpoint version {}",
                options.manifest_dir, checkpoint.last_version
            );
            version = version.max(checkpoint.last_version + 1);
            RegionManifestBuilder::with_checkpoint(checkpoint.checkpoint)
        } else {
            info!(
                "Checkpoint not found in {}, build manifest from scratch",
                options.manifest_dir
            );
            RegionManifestBuilder::default()
        };

        // apply actions from storage
        let manifests = store.fetch_manifests(version, MAX_VERSION).await?;

        for (manifest_version, raw_action_list) in manifests {
            let action_list = RegionMetaActionList::decode(&raw_action_list)?;
            // set manifest size after last checkpoint
            store.set_delta_file_size(manifest_version, raw_action_list.len() as u64);
            for action in action_list.actions {
                match action {
                    RegionMetaAction::Change(action) => {
                        manifest_builder.apply_change(manifest_version, action);
                    }
                    RegionMetaAction::Edit(action) => {
                        manifest_builder.apply_edit(manifest_version, action);
                    }
                    RegionMetaAction::Remove(_) => {
                        debug!(
                            "Unhandled action in {}, action: {:?}",
                            options.manifest_dir, action
                        );
                    }
                    RegionMetaAction::Truncate(action) => {
                        manifest_builder.apply_truncate(manifest_version, action);
                    }
                }
            }
        }

        // set the initial metadata if necessary
        if !manifest_builder.contains_metadata() {
            debug!("No region manifest in {}", options.manifest_dir);
            return Ok(None);
        }

        let manifest = manifest_builder.try_build()?;
        debug!(
            "Recovered region manifest from {}, manifest: {:?}",
            options.manifest_dir, manifest
        );
        let version = manifest.manifest_version;

        let checkpointer = Checkpointer::new(
            manifest.metadata.region_id,
            options,
            store.clone(),
            last_checkpoint_version,
        );
        manifest_version.store(version, Ordering::Relaxed);
        manifest
            .removed_files
            .update_file_removed_cnt_to_stats(stats);
        Ok(Some(Self {
            store,
            last_version: manifest_version,
            checkpointer,
            manifest: Arc::new(manifest),
            // TODO(weny): open the staging manifest if exists.
            staging_manifest: None,
            stats: stats.clone(),
            stopped: false,
        }))
    }

    /// Stops the manager.
    pub async fn stop(&mut self) {
        self.stopped = true;
    }

    /// Installs the manifest changes from the current version to the target version (inclusive).
    ///
    /// Returns installed version.
    /// **Note**: This function is not guaranteed to install the target version strictly.
    /// The installed version may be greater than the target version.
    pub async fn install_manifest_to(
        &mut self,
        target_version: ManifestVersion,
    ) -> Result<ManifestVersion> {
        let _t = MANIFEST_OP_ELAPSED
            .with_label_values(&["install_manifest_to"])
            .start_timer();

        let last_version = self.last_version();
        // Case 1: If the target version is less than the current version, return the current version.
        if last_version >= target_version {
            debug!(
                "Target version {} is less than or equal to the current version {}, region: {}, skip install",
                target_version, last_version, self.manifest.metadata.region_id
            );
            return Ok(last_version);
        }

        ensure!(
            !self.stopped,
            RegionStoppedSnafu {
                region_id: self.manifest.metadata.region_id,
            }
        );

        let region_id = self.manifest.metadata.region_id;
        // Fetches manifests from the last version strictly.
        let mut manifests = self
            .store
            // Invariant: last_version < target_version.
            .fetch_manifests_strict_from(last_version + 1, target_version + 1, region_id)
            .await?;

        // Case 2: No manifests in range: [current_version+1, target_version+1)
        //
        // |---------Has been deleted------------|     [Checkpoint Version]...[Latest Version]
        //                                                                    [Leader region]
        // [Current Version]......[Target Version]
        // [Follower region]
        if manifests.is_empty() {
            info!(
                "Manifests are not strict from {}, region: {}, tries to install the last checkpoint",
                last_version, self.manifest.metadata.region_id
            );
            let last_version = self.install_last_checkpoint().await?;
            // Case 2.1: If the installed checkpoint version is greater than or equal to the target version, return the last version.
            if last_version >= target_version {
                return Ok(last_version);
            }

            // Fetches manifests from the installed version strictly.
            manifests = self
                .store
                // Invariant: last_version < target_version.
                .fetch_manifests_strict_from(last_version + 1, target_version + 1, region_id)
                .await?;
        }

        if manifests.is_empty() {
            return NoManifestsSnafu {
                region_id: self.manifest.metadata.region_id,
                start_version: last_version + 1,
                end_version: target_version + 1,
                last_version,
            }
            .fail();
        }

        debug_assert_eq!(manifests.first().unwrap().0, last_version + 1);
        let mut manifest_builder =
            RegionManifestBuilder::with_checkpoint(Some(self.manifest.as_ref().clone()));

        for (manifest_version, raw_action_list) in manifests {
            self.store
                .set_delta_file_size(manifest_version, raw_action_list.len() as u64);
            let action_list = RegionMetaActionList::decode(&raw_action_list)?;
            for action in action_list.actions {
                match action {
                    RegionMetaAction::Change(action) => {
                        manifest_builder.apply_change(manifest_version, action);
                    }
                    RegionMetaAction::Edit(action) => {
                        manifest_builder.apply_edit(manifest_version, action);
                    }
                    RegionMetaAction::Remove(_) => {
                        debug!(
                            "Unhandled action for region {}, action: {:?}",
                            self.manifest.metadata.region_id, action
                        );
                    }
                    RegionMetaAction::Truncate(action) => {
                        manifest_builder.apply_truncate(manifest_version, action);
                    }
                }
            }
        }

        let new_manifest = manifest_builder.try_build()?;
        ensure!(
            new_manifest.manifest_version >= target_version,
            InstallManifestToSnafu {
                region_id: self.manifest.metadata.region_id,
                target_version,
                available_version: new_manifest.manifest_version,
                last_version,
            }
        );

        let version = self.last_version();
        new_manifest
            .removed_files
            .update_file_removed_cnt_to_stats(&self.stats);
        self.manifest = Arc::new(new_manifest);
        let last_version = self.set_version(self.manifest.manifest_version);
        info!(
            "Install manifest changes from {} to {}, region: {}",
            version, last_version, self.manifest.metadata.region_id
        );

        Ok(last_version)
    }

    /// Installs the last checkpoint.
    pub(crate) async fn install_last_checkpoint(&mut self) -> Result<ManifestVersion> {
        let last_version = self.last_version();
        let Some((checkpoint, checkpoint_size)) = Self::last_checkpoint(&mut self.store).await?
        else {
            return NoCheckpointSnafu {
                region_id: self.manifest.metadata.region_id,
                last_version,
            }
            .fail();
        };
        self.store.reset_manifest_size();
        self.store
            .set_checkpoint_file_size(checkpoint.last_version, checkpoint_size);
        let builder = RegionManifestBuilder::with_checkpoint(checkpoint.checkpoint);
        let manifest = builder.try_build()?;
        let last_version = self.set_version(manifest.manifest_version);
        manifest
            .removed_files
            .update_file_removed_cnt_to_stats(&self.stats);
        self.manifest = Arc::new(manifest);
        info!(
            "Installed region manifest from checkpoint: {}, region: {}",
            checkpoint.last_version, self.manifest.metadata.region_id
        );

        Ok(last_version)
    }

    /// Updates the manifest. Returns the current manifest version number.
    pub async fn update(
        &mut self,
        action_list: RegionMetaActionList,
        is_staging: bool,
    ) -> Result<ManifestVersion> {
        let _t = MANIFEST_OP_ELAPSED
            .with_label_values(&["update"])
            .start_timer();

        ensure!(
            !self.stopped,
            RegionStoppedSnafu {
                region_id: self.manifest.metadata.region_id,
            }
        );

        let version = self.increase_version();
        self.store
            .save(version, &action_list.encode()?, is_staging)
            .await?;

        // For a staging region, the manifest is initially inherited from the previous manifest(i.e., `self.manifest`).
        // When the staging manifest becomes available, it will be used to construct the new manifest.
        let mut manifest_builder =
            if is_staging && let Some(staging_manifest) = self.staging_manifest.as_ref() {
                RegionManifestBuilder::with_checkpoint(Some(staging_manifest.as_ref().clone()))
            } else {
                RegionManifestBuilder::with_checkpoint(Some(self.manifest.as_ref().clone()))
            };

        for action in action_list.actions {
            match action {
                RegionMetaAction::Change(action) => {
                    manifest_builder.apply_change(version, action);
                }
                RegionMetaAction::Edit(action) => {
                    manifest_builder.apply_edit(version, action);
                }
                RegionMetaAction::Remove(_) => {
                    debug!(
                        "Unhandled action for region {}, action: {:?}",
                        self.manifest.metadata.region_id, action
                    );
                }
                RegionMetaAction::Truncate(action) => {
                    manifest_builder.apply_truncate(version, action);
                }
            }
        }

        if is_staging {
            let new_manifest = manifest_builder.try_build()?;
            self.staging_manifest = Some(Arc::new(new_manifest));

            info!(
                "Skipping checkpoint for region {} in staging mode, manifest version: {}",
                self.manifest.metadata.region_id, self.manifest.manifest_version
            );
        } else {
            let new_manifest = manifest_builder.try_build()?;
            new_manifest
                .removed_files
                .update_file_removed_cnt_to_stats(&self.stats);
            let updated_manifest = self
                .checkpointer
                .update_manifest_removed_files(new_manifest)?;
            self.manifest = Arc::new(updated_manifest);
            self.checkpointer
                .maybe_do_checkpoint(self.manifest.as_ref());
        }

        Ok(version)
    }

    /// Clear deleted files from manifest's `removed_files` field without update version. Notice if datanode exit before checkpoint then new manifest by open region may still contain these deleted files, which is acceptable for gc process.
    pub fn clear_deleted_files(&mut self, deleted_files: Vec<FileId>) {
        let mut manifest = (*self.manifest()).clone();
        manifest.removed_files.clear_deleted_files(deleted_files);
        self.set_manifest(Arc::new(manifest));
    }

    pub(crate) fn set_manifest(&mut self, manifest: Arc<RegionManifest>) {
        self.manifest = manifest;
    }

    /// Retrieves the current [RegionManifest].
    pub fn manifest(&self) -> Arc<RegionManifest> {
        self.manifest.clone()
    }

    /// Retrieves the current [RegionManifest].
    pub fn staging_manifest(&self) -> Option<Arc<RegionManifest>> {
        self.staging_manifest.clone()
    }

    /// Returns total manifest size.
    pub fn manifest_usage(&self) -> u64 {
        self.store.total_manifest_size()
    }

    /// Returns true if a newer version manifest file is found.
    ///
    /// It is typically used in read-only regions to catch up with manifest.
    /// It doesn't lock the manifest directory in the object store so the result
    /// may be inaccurate if there are concurrent writes.
    pub async fn has_update(&self) -> Result<bool> {
        let last_version = self.last_version();

        let streamer =
            self.store
                .manifest_lister(false)
                .await?
                .context(error::EmptyManifestDirSnafu {
                    manifest_dir: self.store.manifest_dir(),
                })?;

        let need_update = streamer
            .try_any(|entry| async move {
                let file_name = entry.name();
                if is_delta_file(file_name) || is_checkpoint_file(file_name) {
                    let version = file_version(file_name);
                    if version > last_version {
                        return true;
                    }
                }
                false
            })
            .await
            .context(error::OpenDalSnafu)?;

        Ok(need_update)
    }

    /// Increases last version and returns the increased version.
    fn increase_version(&mut self) -> ManifestVersion {
        let previous = self.last_version.fetch_add(1, Ordering::Relaxed);
        previous + 1
    }

    /// Sets the last version.
    fn set_version(&mut self, version: ManifestVersion) -> ManifestVersion {
        self.last_version.store(version, Ordering::Relaxed);
        version
    }

    fn last_version(&self) -> ManifestVersion {
        self.last_version.load(Ordering::Relaxed)
    }

    /// Fetches the last [RegionCheckpoint] from storage.
    ///
    /// If the checkpoint is not found, returns `None`.
    /// Otherwise, returns the checkpoint and the size of the checkpoint.
    pub(crate) async fn last_checkpoint(
        store: &mut ManifestObjectStore,
    ) -> Result<Option<(RegionCheckpoint, u64)>> {
        let last_checkpoint = store.load_last_checkpoint().await?;

        if let Some((_, bytes)) = last_checkpoint {
            let checkpoint = RegionCheckpoint::decode(&bytes)?;
            Ok(Some((checkpoint, bytes.len() as u64)))
        } else {
            Ok(None)
        }
    }

    pub fn store(&self) -> ManifestObjectStore {
        self.store.clone()
    }

    #[cfg(test)]
    pub(crate) fn checkpointer(&self) -> &Checkpointer {
        &self.checkpointer
    }

    /// Merge all staged manifest actions into a single action list ready for submission.
    /// This collects all staging manifests, applies them sequentially, and returns the merged actions.
    pub(crate) async fn merge_staged_actions(
        &mut self,
        region_state: RegionRoleState,
    ) -> Result<Option<RegionMetaActionList>> {
        // Only merge if we're in staging mode
        if region_state != RegionRoleState::Leader(RegionLeaderState::Staging) {
            return Ok(None);
        }

        // Fetch all staging manifests
        let staging_manifests = self.store.fetch_staging_manifests().await?;

        if staging_manifests.is_empty() {
            info!(
                "No staging manifests to merge for region {}",
                self.manifest.metadata.region_id
            );
            return Ok(None);
        }

        info!(
            "Merging {} staging manifests for region {}",
            staging_manifests.len(),
            self.manifest.metadata.region_id
        );

        // Start with current manifest state as the base
        let mut merged_actions = Vec::new();
        let mut latest_version = self.last_version();

        // Apply all staging actions in order
        for (manifest_version, raw_action_list) in staging_manifests {
            let action_list = RegionMetaActionList::decode(&raw_action_list)?;

            for action in action_list.actions {
                merged_actions.push(action);
            }

            latest_version = latest_version.max(manifest_version);
        }

        if merged_actions.is_empty() {
            return Ok(None);
        }

        info!(
            "Successfully merged {} actions from staging manifests for region {}, latest version: {}",
            merged_actions.len(),
            self.manifest.metadata.region_id,
            latest_version
        );

        Ok(Some(RegionMetaActionList::new(merged_actions)))
    }

    /// Clear all staging manifests.
    pub(crate) async fn clear_staging_manifests(&mut self) -> Result<()> {
        self.staging_manifest = None;
        self.store.clear_staging_manifests().await?;
        info!(
            "Cleared all staging manifests for region {}",
            self.manifest.metadata.region_id
        );
        Ok(())
    }
}

#[cfg(test)]
impl RegionManifestManager {
    fn validate_manifest(&self, expect: &RegionMetadataRef, last_version: ManifestVersion) {
        let manifest = self.manifest();
        assert_eq!(manifest.metadata, *expect);
        assert_eq!(self.manifest.manifest_version, self.last_version());
        assert_eq!(last_version, self.last_version());
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use api::v1::SemanticType;
    use common_datasource::compression::CompressionType;
    use common_test_util::temp_dir::create_temp_dir;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;
    use crate::manifest::action::{RegionChange, RegionEdit};
    use crate::manifest::tests::utils::basic_region_metadata;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn create_manifest_manager() {
        let metadata = Arc::new(basic_region_metadata());
        let env = TestEnv::new().await;
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, Some(metadata.clone()))
            .await
            .unwrap()
            .unwrap();

        manager.validate_manifest(&metadata, 0);
    }

    #[tokio::test]
    async fn open_manifest_manager() {
        let env = TestEnv::new().await;
        // Try to opens an empty manifest.
        assert!(
            env.create_manifest_manager(CompressionType::Uncompressed, 10, None)
                .await
                .unwrap()
                .is_none()
        );

        // Creates a manifest.
        let metadata = Arc::new(basic_region_metadata());
        let mut manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, Some(metadata.clone()))
            .await
            .unwrap()
            .unwrap();
        // Stops it.
        manager.stop().await;

        // Open it.
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, None)
            .await
            .unwrap()
            .unwrap();

        manager.validate_manifest(&metadata, 0);
    }

    #[tokio::test]
    async fn manifest_with_partition_expr_roundtrip() {
        let env = TestEnv::new().await;
        let expr_json =
            r#"{"Expr":{"lhs":{"Column":"a"},"op":"GtEq","rhs":{"Value":{"UInt32":10}}}}"#;
        let mut metadata = basic_region_metadata();
        metadata.partition_expr = Some(expr_json.to_string());
        let metadata = Arc::new(metadata);
        let mut manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, Some(metadata.clone()))
            .await
            .unwrap()
            .unwrap();

        // persisted manifest should contain the same partition_expr JSON
        let manifest = manager.manifest();
        assert_eq!(manifest.metadata.partition_expr.as_deref(), Some(expr_json));

        manager.stop().await;

        // Reopen and check again
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, None)
            .await
            .unwrap()
            .unwrap();
        let manifest = manager.manifest();
        assert_eq!(manifest.metadata.partition_expr.as_deref(), Some(expr_json));
    }

    #[tokio::test]
    async fn region_change_add_column() {
        let metadata = Arc::new(basic_region_metadata());
        let env = TestEnv::new().await;
        let mut manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, Some(metadata.clone()))
            .await
            .unwrap()
            .unwrap();

        let mut new_metadata_builder = RegionMetadataBuilder::from_existing((*metadata).clone());
        new_metadata_builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("val2", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 252,
        });
        let new_metadata = Arc::new(new_metadata_builder.build().unwrap());

        let action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Change(RegionChange {
                metadata: new_metadata.clone(),
                sst_format: FormatType::PrimaryKey,
            }));

        let current_version = manager.update(action_list, false).await.unwrap();
        assert_eq!(current_version, 1);
        manager.validate_manifest(&new_metadata, 1);

        // Reopen the manager.
        manager.stop().await;
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, None)
            .await
            .unwrap()
            .unwrap();
        manager.validate_manifest(&new_metadata, 1);
    }

    /// Just for test, refer to wal_dir_usage in src/store-api/src/logstore.rs.
    async fn manifest_dir_usage(path: &str) -> u64 {
        let mut size = 0;
        let mut read_dir = tokio::fs::read_dir(path).await.unwrap();
        while let Ok(dir_entry) = read_dir.next_entry().await {
            let Some(entry) = dir_entry else {
                break;
            };
            if entry.file_type().await.unwrap().is_file() {
                let file_name = entry.file_name().into_string().unwrap();
                if file_name.contains(".checkpoint") || file_name.contains(".json") {
                    let file_size = entry.metadata().await.unwrap().len() as usize;
                    debug!("File: {file_name:?}, size: {file_size}");
                    size += file_size;
                }
            }
        }
        size as u64
    }

    #[tokio::test]
    async fn test_manifest_size() {
        let metadata = Arc::new(basic_region_metadata());
        let data_home = create_temp_dir("");
        let data_home_path = data_home.path().to_str().unwrap().to_string();
        let env = TestEnv::with_data_home(data_home).await;

        let manifest_dir = format!("{}/manifest", data_home_path);

        let mut manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, Some(metadata.clone()))
            .await
            .unwrap()
            .unwrap();

        let mut new_metadata_builder = RegionMetadataBuilder::from_existing((*metadata).clone());
        new_metadata_builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("val2", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 252,
        });
        let new_metadata = Arc::new(new_metadata_builder.build().unwrap());

        let action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Change(RegionChange {
                metadata: new_metadata.clone(),
                sst_format: FormatType::PrimaryKey,
            }));

        let current_version = manager.update(action_list, false).await.unwrap();
        assert_eq!(current_version, 1);
        manager.validate_manifest(&new_metadata, 1);

        // get manifest size
        let manifest_size = manager.manifest_usage();
        assert_eq!(manifest_size, manifest_dir_usage(&manifest_dir).await);

        // update 10 times nop_action to trigger checkpoint
        for _ in 0..10 {
            manager
                .update(
                    RegionMetaActionList::new(vec![RegionMetaAction::Edit(RegionEdit {
                        files_to_add: vec![],
                        files_to_remove: vec![],
                        timestamp_ms: None,
                        compaction_time_window: None,
                        flushed_entry_id: None,
                        flushed_sequence: None,
                        committed_sequence: None,
                    })]),
                    false,
                )
                .await
                .unwrap();
        }

        while manager.checkpointer.is_doing_checkpoint() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // check manifest size again
        let manifest_size = manager.manifest_usage();
        assert_eq!(manifest_size, manifest_dir_usage(&manifest_dir).await);

        // Reopen the manager,
        // we just calculate the size from the latest checkpoint file
        manager.stop().await;
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, None)
            .await
            .unwrap()
            .unwrap();
        manager.validate_manifest(&new_metadata, 11);

        // get manifest size again
        let manifest_size = manager.manifest_usage();
        assert_eq!(manifest_size, 1378);
    }
}
