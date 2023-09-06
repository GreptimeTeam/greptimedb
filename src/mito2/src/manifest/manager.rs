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

use common_datasource::compression::CompressionType;
use common_telemetry::{debug, info};
use object_store::ObjectStore;
use store_api::manifest::{ManifestVersion, MAX_VERSION, MIN_VERSION};
use store_api::metadata::RegionMetadataRef;
use tokio::sync::RwLock;

use crate::error::Result;
use crate::manifest::action::{
    RegionChange, RegionCheckpoint, RegionManifest, RegionManifestBuilder, RegionMetaAction,
    RegionMetaActionList,
};
use crate::manifest::storage::ManifestObjectStore;

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
///     -VersionNumber regoin_version
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
    inner: RwLock<RegionManifestManagerInner>,
}

impl RegionManifestManager {
    /// Construct a region's manifest and persist it.
    pub async fn new(metadata: RegionMetadataRef, options: RegionManifestOptions) -> Result<Self> {
        let inner = RegionManifestManagerInner::new(metadata, options).await?;
        Ok(Self {
            inner: RwLock::new(inner),
        })
    }

    /// Open an existing manifest.
    pub async fn open(options: RegionManifestOptions) -> Result<Option<Self>> {
        if let Some(inner) = RegionManifestManagerInner::open(options).await? {
            Ok(Some(Self {
                inner: RwLock::new(inner),
            }))
        } else {
            Ok(None)
        }
    }

    /// Stop background tasks gracefully.
    pub async fn stop(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.stop().await
    }

    /// Update the manifest. Return the current manifest version number.
    pub async fn update(&self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        let mut inner = self.inner.write().await;
        inner.update(action_list).await
    }

    /// Retrieve the current [RegionManifest].
    pub async fn manifest(&self) -> Arc<RegionManifest> {
        let inner = self.inner.read().await;
        inner.manifest.clone()
    }

    #[cfg(test)]
    pub async fn store(&self) -> ManifestObjectStore {
        let inner = self.inner.read().await;
        inner.store.clone()
    }
}

#[cfg(test)]
impl RegionManifestManager {
    pub(crate) async fn validate_manifest(
        &self,
        expect: &RegionMetadataRef,
        last_version: ManifestVersion,
    ) {
        let manifest = self.manifest().await;
        assert_eq!(manifest.metadata, *expect);

        let inner = self.inner.read().await;
        assert_eq!(inner.manifest.manifest_version, inner.last_version);
        assert_eq!(last_version, inner.last_version);
    }
}

#[derive(Debug)]
pub(crate) struct RegionManifestManagerInner {
    store: ManifestObjectStore,
    options: RegionManifestOptions,
    last_version: ManifestVersion,
    /// The last version included in checkpoint file.
    last_checkpoint_version: ManifestVersion,
    manifest: Arc<RegionManifest>,
}

impl RegionManifestManagerInner {
    /// Creates a new manifest.
    async fn new(metadata: RegionMetadataRef, options: RegionManifestOptions) -> Result<Self> {
        // construct storage
        let store = ManifestObjectStore::new(
            &options.manifest_dir,
            options.object_store.clone(),
            options.compress_type,
        );

        info!(
            "Creating region manifest in {} with metadata {:?}",
            options.manifest_dir, metadata
        );

        let version = MIN_VERSION;
        let mut manifest_builder = RegionManifestBuilder::default();
        // set the initial metadata.
        manifest_builder.apply_change(
            version,
            RegionChange {
                metadata: metadata.clone(),
            },
        );
        let manifest = manifest_builder.try_build()?;

        debug!(
            "Build region manifest in {}, manifest: {:?}",
            options.manifest_dir, manifest
        );

        // Persist region change.
        let action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Change(RegionChange { metadata }));
        store.save(version, &action_list.encode()?).await?;

        Ok(Self {
            store,
            options,
            last_version: version,
            last_checkpoint_version: MIN_VERSION,
            manifest: Arc::new(manifest),
        })
    }

    /// Open an existing manifest.
    ///
    /// Returns `Ok(None)` if no such manifest.
    async fn open(options: RegionManifestOptions) -> Result<Option<Self>> {
        // construct storage
        let store = ManifestObjectStore::new(
            &options.manifest_dir,
            options.object_store.clone(),
            options.compress_type,
        );

        // recover from storage
        // construct manifest builder
        let mut version = MIN_VERSION;
        let checkpoint = Self::last_checkpoint(&store).await?;
        let last_checkpoint_version = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.last_version)
            .unwrap_or(MIN_VERSION);
        let mut manifest_builder = if let Some(checkpoint) = checkpoint {
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
        let mut action_iter = store.scan(version, MAX_VERSION).await?;
        while let Some((manifest_version, raw_action_list)) = action_iter.next_log().await? {
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
                            "Unhandled action in {}, action: {:?}",
                            options.manifest_dir, action
                        );
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

        Ok(Some(Self {
            store,
            options,
            last_version: version,
            last_checkpoint_version,
            manifest: Arc::new(manifest),
        }))
    }

    async fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update(&mut self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        let version = self.increase_version();
        self.store.save(version, &action_list.encode()?).await?;

        let mut manifest_builder =
            RegionManifestBuilder::with_checkpoint(Some(self.manifest.as_ref().clone()));
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
            }
        }
        let new_manifest = manifest_builder.try_build()?;
        self.manifest = Arc::new(new_manifest);
        self.may_do_checkpoint(version).await?;

        Ok(version)
    }
}

impl RegionManifestManagerInner {
    /// Increases last version and returns the increased version.
    fn increase_version(&mut self) -> ManifestVersion {
        self.last_version += 1;
        self.last_version
    }

    pub(crate) async fn may_do_checkpoint(&mut self, version: ManifestVersion) -> Result<()> {
        if version - self.last_checkpoint_version >= self.options.checkpoint_distance
            && self.options.checkpoint_distance != 0
        {
            debug!(
                "Going to do checkpoint for version [{} ~ {}]",
                self.last_checkpoint_version, version
            );
            if let Some(checkpoint) = self.do_checkpoint().await? {
                self.last_checkpoint_version = checkpoint.last_version();
            }
        }

        Ok(())
    }

    /// Make a new checkpoint. Return the fresh one if there are some actions to compact.
    async fn do_checkpoint(&self) -> Result<Option<RegionCheckpoint>> {
        let last_checkpoint = Self::last_checkpoint(&self.store).await?;
        let current_version = self.last_version;

        let (start_version, mut manifest_builder) = if let Some(checkpoint) = last_checkpoint {
            (
                checkpoint.last_version + 1,
                RegionManifestBuilder::with_checkpoint(checkpoint.checkpoint),
            )
        } else {
            (MIN_VERSION, RegionManifestBuilder::default())
        };
        let end_version = current_version;

        if start_version >= end_version {
            return Ok(None);
        }

        let mut iter = self.store.scan(start_version, end_version).await?;
        let mut last_version = start_version;
        let mut compacted_actions = 0;
        while let Some((version, raw_action_list)) = iter.next_log().await? {
            let action_list = RegionMetaActionList::decode(&raw_action_list)?;
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
                }
            }
            last_version = version;
            compacted_actions += 1;
        }

        if compacted_actions == 0 {
            return Ok(None);
        }

        let region_manifest = manifest_builder.try_build()?;
        let checkpoint = RegionCheckpoint {
            last_version,
            compacted_actions,
            checkpoint: Some(region_manifest),
        };

        self.store
            .save_checkpoint(last_version, &checkpoint.encode()?)
            .await?;
        // TODO(ruihang): this task can be detached
        self.store.delete_until(last_version, true).await?;

        info!(
            "Done manifest checkpoint for region {}, version: [{}, {}], current latest version: {}, compacted {} actions.",
            self.manifest.metadata.region_id, start_version, end_version, last_version, compacted_actions
        );
        Ok(Some(checkpoint))
    }

    /// Fetch the last [RegionCheckpoint] from storage.
    pub(crate) async fn last_checkpoint(
        store: &ManifestObjectStore,
    ) -> Result<Option<RegionCheckpoint>> {
        let last_checkpoint = store.load_last_checkpoint().await?;

        if let Some((_, bytes)) = last_checkpoint {
            let checkpoint = RegionCheckpoint::decode(&bytes)?;
            Ok(Some(checkpoint))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use api::v1::SemanticType;
    use common_datasource::compression::CompressionType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;
    use crate::manifest::action::RegionChange;
    use crate::manifest::tests::utils::basic_region_metadata;
    use crate::test_util::TestEnv;

    #[tokio::test]
    async fn create_manifest_manager() {
        let metadata = Arc::new(basic_region_metadata());
        let env = TestEnv::new();
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, Some(metadata.clone()))
            .await
            .unwrap()
            .unwrap();

        manager.validate_manifest(&metadata, 0).await;
    }

    #[tokio::test]
    async fn open_manifest_manager() {
        let env = TestEnv::new();
        // Try to opens an empty manifest.
        assert!(env
            .create_manifest_manager(CompressionType::Uncompressed, 10, None)
            .await
            .unwrap()
            .is_none());

        // Creates a manifest.
        let metadata = Arc::new(basic_region_metadata());
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, Some(metadata.clone()))
            .await
            .unwrap()
            .unwrap();
        // Stops it.
        manager.stop().await.unwrap();

        // Open it.
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, None)
            .await
            .unwrap()
            .unwrap();

        manager.validate_manifest(&metadata, 0).await;
    }

    #[tokio::test]
    async fn region_change_add_column() {
        let metadata = Arc::new(basic_region_metadata());
        let env = TestEnv::new();
        let manager = env
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
            }));

        let current_version = manager.update(action_list).await.unwrap();
        assert_eq!(current_version, 1);
        manager.validate_manifest(&new_metadata, 1).await;

        // Reopen the manager.
        manager.stop().await.unwrap();
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, 10, None)
            .await
            .unwrap()
            .unwrap();
        manager.validate_manifest(&new_metadata, 1).await;
    }
}
