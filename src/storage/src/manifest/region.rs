//! Region manifest impl
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_telemetry::logging;
use object_store::ObjectStore;
use snafu::ensure;
use store_api::manifest::action::{self, ProtocolAction, ProtocolVersion};
use store_api::manifest::*;
use store_api::storage::RegionId;

use crate::error::{Error, ManifestProtocolForbidWriteSnafu, Result};
use crate::manifest::action::*;
use crate::manifest::storage::ManifestObjectStore;
use crate::manifest::storage::ObjectStoreLogIterator;

#[derive(Clone, Debug)]
pub struct RegionManifest {
    inner: Arc<RegionManifestInner>,
}

#[async_trait]
impl Manifest for RegionManifest {
    type Error = Error;
    type MetaAction = RegionMetaActionList;
    type MetadataId = RegionId;
    type Metadata = RegionManifestData;

    fn new(id: Self::MetadataId, manifest_dir: &str, object_store: ObjectStore) -> Self {
        RegionManifest {
            inner: Arc::new(RegionManifestInner::new(id, manifest_dir, object_store)),
        }
    }

    async fn update(&self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        self.inner.save(action_list).await
    }

    async fn load(&self) -> Result<Option<RegionManifestData>> {
        let last_version = self.inner.last_version();

        let start_bound = if last_version == MIN_VERSION {
            // No actions have ever saved
            MIN_VERSION
        } else {
            last_version - 1
        };

        let mut iter = self.inner.scan(start_bound, MAX_VERSION).await?;

        while let Some((_v, action_list)) = iter.next_action().await? {
            for action in action_list.actions {
                if let RegionMetaAction::Change(c) = action {
                    return Ok(Some(RegionManifestData {
                        region_meta: c.metadata,
                    }));
                }
            }
        }

        Ok(None)
    }

    async fn checkpoint(&self) -> Result<ManifestVersion> {
        unimplemented!();
    }

    fn metadata_id(&self) -> RegionId {
        self.inner.region_id
    }
}

#[derive(Debug)]
struct RegionManifestInner {
    region_id: RegionId,
    store: Arc<ManifestObjectStore>,
    version: AtomicU64,
    /// Current using protocol
    protocol: ArcSwap<ProtocolAction>,
    /// Current node supported protocols (reader_version, writer_version)
    supported_reader_version: ProtocolVersion,
    supported_writer_version: ProtocolVersion,
}

struct RegionMetaActionListIterator {
    log_iter: ObjectStoreLogIterator,
    reader_version: ProtocolVersion,
}

impl RegionMetaActionListIterator {
    async fn next_action(&mut self) -> Result<Option<(ManifestVersion, RegionMetaActionList)>> {
        match self.log_iter.next_log().await? {
            Some((v, bytes)) => {
                //TODO(dennis): save protocol into inner's protocol when recovering
                let (action_list, _protocol) =
                    RegionMetaActionList::decode(&bytes, self.reader_version)?;
                Ok(Some((v, action_list)))
            }
            None => Ok(None),
        }
    }
}

impl RegionManifestInner {
    fn new(region_id: RegionId, manifest_dir: &str, object_store: ObjectStore) -> Self {
        let (reader_version, writer_version) = action::supported_protocol_version();

        Self {
            region_id,
            store: Arc::new(ManifestObjectStore::new(manifest_dir, object_store)),
            // TODO(dennis): recover the last version from history
            version: AtomicU64::new(0),
            protocol: ArcSwap::new(Arc::new(ProtocolAction::new())),
            supported_reader_version: reader_version,
            supported_writer_version: writer_version,
        }
    }

    #[inline]
    fn inc_version(&self) -> ManifestVersion {
        self.version.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    fn last_version(&self) -> ManifestVersion {
        self.version.load(Ordering::Relaxed)
    }

    async fn save(&self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        let protocol = self.protocol.load();

        ensure!(
            protocol.is_writable(self.supported_writer_version),
            ManifestProtocolForbidWriteSnafu {
                min_version: protocol.min_writer_version,
                supported_version: self.supported_writer_version,
            }
        );

        let version = self.inc_version();

        logging::debug!(
            "Save region metadata action: {:?}, version: {}",
            action_list,
            version
        );

        self.store.save(version, &action_list.encode()?).await?;

        Ok(version)
    }

    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<RegionMetaActionListIterator> {
        Ok(RegionMetaActionListIterator {
            log_iter: self.store.scan(start, end).await?,
            reader_version: self.supported_reader_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;
    use object_store::{backend::fs, ObjectStore};
    use tempdir::TempDir;

    use super::*;
    use crate::metadata::RegionMetadata;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    #[tokio::test]
    async fn test_region_manifest() {
        common_telemetry::init_default_ut_logging();
        let tmp_dir = TempDir::new("test_region_manifest").unwrap();
        let object_store = ObjectStore::new(
            fs::Backend::build()
                .root(&tmp_dir.path().to_string_lossy())
                .finish()
                .await
                .unwrap(),
        );
        let region_id = 0;

        let manifest = RegionManifest::new(region_id, "/manifest/", object_store);
        assert_eq!(region_id, manifest.metadata_id());

        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .id(region_id)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_value_column(("v1", LogicalTypeId::Float32, true))
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();
        let region_meta = Arc::new(metadata);

        assert!(manifest.load().await.unwrap().is_none());

        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.clone(),
                },
            )))
            .await
            .unwrap();

        let manifest_data = manifest.load().await.unwrap().unwrap();
        assert_eq!(manifest_data.region_meta, region_meta);

        // save another metadata
        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .id(region_id)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_key_column(("k2", LogicalTypeId::Int64, false))
            .push_value_column(("v1", LogicalTypeId::Float32, true))
            .push_value_column(("bool", LogicalTypeId::Boolean, true))
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();
        let region_meta = Arc::new(metadata);
        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.clone(),
                },
            )))
            .await
            .unwrap();

        let manifest_data = manifest.load().await.unwrap().unwrap();
        assert_eq!(manifest_data.region_meta, region_meta);
    }
}
