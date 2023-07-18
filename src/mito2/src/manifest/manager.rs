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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use arc_swap::ArcSwap;
use common_telemetry::{debug, info};
use snafu::OptionExt;
use store_api::manifest::action::{ProtocolAction, ProtocolVersion};
use store_api::manifest::{AtomicManifestVersion, ManifestVersion, MAX_VERSION, MIN_VERSION};

use crate::error::{InitialMetadataSnafu, Result};
use crate::manifest::action::{
    RegionChange, RegionCheckpoint, RegionManifest, RegionManifestBuilder, RegionMetaAction,
    RegionMetaActionIter, RegionMetaActionList,
};
use crate::manifest::options::RegionManifestOptions;
use crate::manifest::storage::ManifestObjectStore;

// rewrite note:
// trait Checkpoint -> struct RegionCheckpoint
// trait MetaAction -> struct RegionMetaActionList
// trait MetaActionIterator -> struct MetaActionIteratorImpl

/// Manage region's manifest. Provide APIs to access (create/modify/recover) region's persisted
/// metadata.
#[derive(Clone, Debug)]
pub struct RegionManifestManager {
    inner: Arc<RegionManifestManagerInner>,
}

impl RegionManifestManager {
    /// Construct and recover a region's manifest from storage.
    pub async fn new(options: RegionManifestOptions) -> Result<Self> {
        let inner = RegionManifestManagerInner::new(options).await?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }

    /// Update the manifest. Return the current manifest version number.
    pub async fn update(&self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        self.inner.update(action_list).await
    }

    /// Retrieve the current [RegionManifest].
    pub fn manifest(&self) -> Arc<RegionManifest> {
        self.inner.manifest.load().clone()
    }
}

#[derive(Debug)]
struct RegionManifestManagerInner {
    store: ManifestObjectStore,
    options: RegionManifestOptions,
    version: AtomicManifestVersion,
    manifest: ArcSwap<RegionManifest>,
}

impl RegionManifestManagerInner {
    pub async fn new(mut options: RegionManifestOptions) -> Result<Self> {
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
                "Recover region manifest from checkpoint version {}",
                checkpoint.last_version
            );
            version = version.max(checkpoint.last_version + 1);
            RegionManifestBuilder::with_checkpoint(checkpoint.checkpoint)
        } else {
            info!("Checkpoint not found, build manifest from scratch");
            RegionManifestBuilder::default()
        };

        // apply actions from storage
        let mut action_iter = store.scan(version, MAX_VERSION).await?;
        while let Some((manifest_version, raw_action_list)) = action_iter.next_log().await? {
            let action_list = RegionMetaActionList::decode(&raw_action_list)?;
            for action in action_list.actions {
                match action {
                    RegionMetaAction::Change(action) => {
                        manifest_builder.apply_change(action);
                    }
                    RegionMetaAction::Edit(action) => {
                        manifest_builder.apply_edit(manifest_version, action);
                    }
                    RegionMetaAction::Remove(_) | RegionMetaAction::Protocol(_) => {
                        debug!("Unhandled action: {:?}", action);
                    }
                }
            }
        }

        // set the initial metadata if necessary
        if !manifest_builder.contains_metadata() {
            let metadata = options
                .initial_metadata
                .take()
                .context(InitialMetadataSnafu)?;
            info!("Creating region manifest with metadata {:?}", metadata);
            manifest_builder.apply_change(RegionChange { metadata });
        }

        let manifest = manifest_builder.try_build()?;
        debug!("Recovered region manifest: {:?}", manifest);
        let version = manifest.version.manifest_version;

        // todo: start gc task

        Ok(Self {
            store,
            options,
            version: AtomicManifestVersion::new(version),
            manifest: ArcSwap::new(Arc::new(manifest)),
        })
    }

    pub async fn stop(&self) -> Result<()> {
        // todo: stop gc task
        Ok(())
    }

    pub async fn update(&self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        let version = self.inc_version();

        self.store.save(version, &action_list.encode()?).await?;

        let mut manifest_builder =
            RegionManifestBuilder::with_checkpoint(Some(self.manifest.load().as_ref().clone()));
        for action in action_list.actions {
            match action {
                RegionMetaAction::Change(action) => {
                    manifest_builder.apply_change(action);
                }
                RegionMetaAction::Edit(action) => {
                    manifest_builder.apply_edit(version, action);
                }
                RegionMetaAction::Remove(_) | RegionMetaAction::Protocol(_) => {
                    debug!("Unhandled action: {:?}", action);
                }
            }
        }
        let new_manifest = manifest_builder.try_build()?;
        self.manifest.store(Arc::new(new_manifest));

        Ok(version)
    }
}

impl RegionManifestManagerInner {
    fn inc_version(&self) -> ManifestVersion {
        self.version.fetch_add(1, Ordering::Relaxed)
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
    use crate::error::Error;
    use crate::manifest::action::RegionChange;
    use crate::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder, SemanticType};
    use crate::test_util::TestEnv;

    fn basic_region_metadata() -> RegionMetadata {
        let builder = RegionMetadataBuilder::new(RegionId::new(23, 33), 0);
        let builder = builder.add_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 45,
        });
        let builder = builder.add_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("pk", ConcreteDataType::string_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 36,
        });
        let builder = builder.add_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("val", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 251,
        });
        builder.build()
    }

    #[tokio::test]
    async fn create_region_without_initial_metadata() {
        let env = TestEnv::new("");
        let result = env
            .create_manifest_manager(CompressionType::Uncompressed, None, None)
            .await;
        assert!(matches!(
            result.err().unwrap(),
            Error::InitialMetadata { .. }
        ))
    }

    #[tokio::test]
    async fn create_manifest_manager() {
        let metadata = basic_region_metadata();
        let env = TestEnv::new("");
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, None, Some(metadata.clone()))
            .await
            .unwrap();

        let manifest = manager.manifest();
        assert_eq!(manifest.metadata, metadata);
    }

    #[tokio::test]
    async fn region_change_add_column() {
        let metadata = basic_region_metadata();
        let env = TestEnv::new("");
        let manager = env
            .create_manifest_manager(CompressionType::Uncompressed, None, Some(metadata.clone()))
            .await
            .unwrap();

        let new_metadata_builder = RegionMetadataBuilder::from_existing(metadata, 1);
        let new_metadata_builder = new_metadata_builder.add_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("val2", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 252,
        });
        let new_metadata = new_metadata_builder.build();

        let mut action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Change(RegionChange {
                metadata: new_metadata.clone(),
            }));
        action_list.set_prev_version(0);

        let prev_version = manager.update(action_list).await.unwrap();
        assert_eq!(prev_version, 0);

        let manifest = manager.manifest();
        assert_eq!(manifest.metadata, new_metadata);
    }
}
