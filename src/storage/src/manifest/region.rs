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

//! Region manifest impl
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::info;
use object_store::ObjectStore;
use store_api::manifest::action::ProtocolAction;
use store_api::manifest::{
    Manifest, ManifestLogStorage, ManifestVersion, MetaActionIterator, MIN_VERSION,
};

use crate::error::Result;
use crate::manifest::action::*;
use crate::manifest::checkpoint::Checkpointer;
use crate::manifest::ManifestImpl;

pub type RegionManifest = ManifestImpl<RegionSnapshot, RegionMetaActionList>;

#[derive(Debug)]
pub struct RegionManifestCheckpointer {
    flushed_manifest_version: AtomicU64,
}

impl RegionManifestCheckpointer {
    pub(crate) fn set_flushed_manifest_version(&self, manifest_version: ManifestVersion) {
        self.flushed_manifest_version
            .store(manifest_version, Ordering::Relaxed);
    }
}

#[async_trait]
impl Checkpointer for RegionManifestCheckpointer {
    type Snapshot = RegionSnapshot;
    type MetaAction = RegionMetaActionList;

    async fn do_checkpoint(
        &self,
        manifest: &ManifestImpl<RegionSnapshot, RegionMetaActionList>,
    ) -> Result<Option<RegionSnapshot>> {
        let last_snapshot = manifest.last_snapshot().await?;
        let current_version = manifest.last_version();
        let (start_version, mut protocol, mut manifest_builder) =
            if let Some(snapshot) = last_snapshot {
                (
                    snapshot.last_version,
                    snapshot.protocol,
                    RegionManifestBuilder::with_snapshot(snapshot.snapshot),
                )
            } else {
                (
                    MIN_VERSION,
                    ProtocolAction::default(),
                    RegionManifestBuilder::default(),
                )
            };

        // Checkpoint can't exceed over flushed manifest version.
        // We have to keep the region metadata which are not flushed for replaying WAL.
        let end_version =
            current_version.min(self.flushed_manifest_version.load(Ordering::Relaxed));
        if start_version == end_version {
            return Ok(None);
        }

        let mut iter = manifest.scan(start_version, end_version).await?;

        let mut last_version = start_version;
        let mut compacted_actions = 0;
        while let Some((version, action_list)) = iter.next_action().await? {
            for action in action_list.actions {
                match action {
                    RegionMetaAction::Change(c) => manifest_builder.apply_change(c),
                    RegionMetaAction::Edit(e) => manifest_builder.apply_edit(version, e),
                    RegionMetaAction::Protocol(p) => protocol = p,
                    _ => todo!(),
                }
            }
            last_version = version;
            compacted_actions += 1;
        }

        if last_version == start_version {
            return Ok(None);
        }

        let region_manifest = manifest_builder.build();
        let snapshot = RegionSnapshot {
            protocol,
            last_version,
            compacted_actions,
            snapshot: Some(region_manifest),
        };

        manifest.save_snapshot(&snapshot).await?;
        manifest
            .manifest_store()
            .delete(start_version, last_version + 1)
            .await?;

        info!("Region manifest checkpoint, start_version: {}, last_version: {}, compacted actions: {}", start_version, last_version, compacted_actions);

        Ok(Some(snapshot))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RegionManifest {
    pub fn with_checkpointer(manifest_dir: &str, object_store: ObjectStore) -> Self {
        Self::new(
            manifest_dir,
            object_store,
            Some(Arc::new(RegionManifestCheckpointer {
                flushed_manifest_version: AtomicU64::new(0),
            })),
        )
    }

    // Update flushed manifest version in checkpointer
    pub fn set_flushed_manifest_version(&self, manifest_version: ManifestVersion) {
        if let Some(checkpointer) = self.checkpointer() {
            checkpointer
                .as_any()
                .downcast_ref::<RegionManifestCheckpointer>()
                .expect("Failed to downcast region checkpointer")
                .set_flushed_manifest_version(manifest_version);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::{ObjectStore, ObjectStoreBuilder};
    use store_api::manifest::action::ProtocolAction;
    use store_api::manifest::{Manifest, MetaActionIterator, MAX_VERSION};

    use super::*;
    use crate::manifest::test_utils::*;
    use crate::metadata::RegionMetadata;
    use crate::sst::FileId;

    #[tokio::test]
    async fn test_region_manifest() {
        common_telemetry::init_default_ut_logging();
        let tmp_dir = create_temp_dir("test_region_manifest");
        let object_store = ObjectStore::new(
            Fs::default()
                .root(&tmp_dir.path().to_string_lossy())
                .build()
                .unwrap(),
        )
        .finish();

        let manifest = RegionManifest::with_checkpointer("/manifest/", object_store);

        let region_meta = Arc::new(build_region_meta());

        assert!(manifest
            .scan(0, MAX_VERSION)
            .await
            .unwrap()
            .next_action()
            .await
            .unwrap()
            .is_none());

        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.as_ref().into(),
                    committed_sequence: 99,
                },
            )))
            .await
            .unwrap();

        let mut iter = manifest.scan(0, MAX_VERSION).await.unwrap();

        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(0, v);
        assert_eq!(2, action_list.actions.len());
        let protocol = &action_list.actions[0];
        assert!(matches!(
            protocol,
            RegionMetaAction::Protocol(ProtocolAction { .. })
        ));

        let action = &action_list.actions[1];

        match action {
            RegionMetaAction::Change(c) => {
                assert_eq!(
                    RegionMetadata::try_from(c.metadata.clone()).unwrap(),
                    *region_meta
                );
                assert_eq!(c.committed_sequence, 99);
            }
            _ => unreachable!(),
        }

        // Save some actions
        manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Edit(build_region_edit(1, &[FileId::random()], &[])),
                RegionMetaAction::Edit(build_region_edit(
                    2,
                    &[FileId::random(), FileId::random()],
                    &[],
                )),
            ]))
            .await
            .unwrap();

        let mut iter = manifest.scan(0, MAX_VERSION).await.unwrap();
        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(0, v);
        assert_eq!(2, action_list.actions.len());
        let protocol = &action_list.actions[0];
        assert!(matches!(
            protocol,
            RegionMetaAction::Protocol(ProtocolAction { .. })
        ));

        let action = &action_list.actions[1];
        match action {
            RegionMetaAction::Change(c) => {
                assert_eq!(
                    RegionMetadata::try_from(c.metadata.clone()).unwrap(),
                    *region_meta
                );
                assert_eq!(c.committed_sequence, 99);
            }
            _ => unreachable!(),
        }

        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(1, v);
        assert_eq!(2, action_list.actions.len());
        assert!(matches!(&action_list.actions[0], RegionMetaAction::Edit(_)));
        assert!(matches!(&action_list.actions[1], RegionMetaAction::Edit(_)));

        // Reach end
        assert!(iter.next_action().await.unwrap().is_none());
    }
}
