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

use common_telemetry::{debug, info};
use store_api::manifest::action::{ProtocolAction, ProtocolVersion};
use store_api::manifest::{ManifestVersion, MAX_VERSION, MIN_VERSION};
use tokio::sync::RwLock;

use crate::error::Result;
use crate::manifest::action::{
    RegionChange, RegionCheckpoint, RegionManifest, RegionManifestBuilder, RegionMetaAction,
    RegionMetaActionIter, RegionMetaActionList,
};
use crate::manifest::options::RegionManifestOptions;
use crate::manifest::storage::ManifestObjectStore;
use crate::metadata::RegionMetadataRef;

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
struct RegionManifestManagerInner {
    store: ManifestObjectStore,
    options: RegionManifestOptions,
    last_version: ManifestVersion,
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

        // todo: start gc task

        Ok(Self {
            store,
            options,
            last_version: version,
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
        let last_checkpoint = store.load_last_checkpoint().await?;
        let checkpoint = last_checkpoint
            .map(|(_, raw_checkpoint)| RegionCheckpoint::decode(&raw_checkpoint))
            .transpose()?;
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
                    RegionMetaAction::Remove(_) | RegionMetaAction::Protocol(_) => {
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

        // todo: start gc task

        Ok(Some(Self {
            store,
            options,
            last_version: version,
            manifest: Arc::new(manifest),
        }))
    }

    async fn stop(&mut self) -> Result<()> {
        // todo: stop gc task
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
                RegionMetaAction::Remove(_) | RegionMetaAction::Protocol(_) => {
                    debug!(
                        "Unhandled action for region {}, action: {:?}",
                        self.manifest.metadata.region_id, action
                    );
                }
            }
        }
        let new_manifest = manifest_builder.try_build()?;
        self.manifest = Arc::new(new_manifest);

        Ok(version)
    }
}

impl RegionManifestManagerInner {
    /// Increases last version and returns the increased version.
    fn increase_version(&mut self) -> ManifestVersion {
        self.last_version += 1;
        self.last_version
    }

    // pub (crate) fn checkpointer(&self) -> Checkpointer {
    //     todo!()
    // }

    pub(crate) fn set_last_checkpoint_version(&self, _version: ManifestVersion) {
        todo!()
    }

    /// Update inner state.
    pub fn update_state(&self, _version: ManifestVersion, _protocol: Option<ProtocolAction>) {
        todo!()
    }

    pub(crate) async fn save_checkpoint(&self, checkpoint: &RegionCheckpoint) -> Result<()> {
        todo!()
    }

    pub(crate) async fn may_do_checkpoint(&self, version: ManifestVersion) -> Result<()> {
        todo!()
    }

    // pub(crate) fn manifest_store(&self) -> &Arc<ManifestObjectStore> {
    //     todo!()
    // }

    // from Manifest

    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<RegionMetaActionIter> {
        todo!()
    }

    async fn do_checkpoint(&self) -> Result<Option<RegionCheckpoint>> {
        todo!()
    }

    async fn last_checkpoint(&self) -> Result<Option<RegionCheckpoint>> {
        todo!()
    }

    // from Checkpoint

    /// Set a protocol action into checkpoint
    pub fn set_protocol(&mut self, _action: ProtocolAction) {
        todo!()
    }

    /// The last compacted action's version of checkpoint
    pub fn last_version(&self) -> ManifestVersion {
        todo!()
    }

    /// Encode this checkpoint into a byte vector
    pub fn encode(&self) -> Result<Vec<u8>> {
        todo!()
    }

    pub fn decode(_bytes: &[u8], _reader_version: ProtocolVersion) -> Result<Self> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use common_datasource::compression::CompressionType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::storage::RegionId;

    use super::*;
    use crate::manifest::action::RegionChange;
    use crate::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder, SemanticType};
    use crate::test_util::TestEnv;

    fn basic_region_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(23, 33), 0);
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 45,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("pk", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 36,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "val",
                    ConcreteDataType::float64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 251,
            })
            .primary_key(vec![36]);
        builder.build().unwrap()
    }

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

        let mut new_metadata_builder = RegionMetadataBuilder::from_existing((*metadata).clone(), 1);
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
