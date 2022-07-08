//! Region manifest impl
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use async_trait::async_trait;
use common_telemetry::logging;
use object_store::ObjectStore;
use store_api::manifest::*;
use store_api::storage::RegionId;

use crate::error::{Error, Result};
use crate::manifest::action::*;
use crate::manifest::storage::ManifestObjectStore;
use crate::manifest::storage::ObjectStoreLogIterator;

#[derive(Clone)]
pub struct RegionManifest {
    inner: Arc<RegionManifestInner>,
}

#[async_trait]
impl Manifest for RegionManifest {
    type Error = Error;
    type MetaAction = RegionMetaAction;
    type MetadataId = RegionId;
    type Metadata = RegionManifestData;

    fn new(id: Self::MetadataId, object_store: ObjectStore) -> Self {
        RegionManifest {
            inner: Arc::new(RegionManifestInner::new(id, object_store)),
        }
    }

    async fn update(&self, action: RegionMetaAction) -> Result<()> {
        self.inner.save(&action).await
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

        match iter.next_action().await? {
            Some((_v, RegionMetaAction::Change(c))) => Ok(Some(RegionManifestData {
                region_meta: c.metadata,
            })),
            Some(_) => todo!(),
            None => Ok(None),
        }
    }

    async fn checkpoint(&self) -> Result<()> {
        unimplemented!();
    }

    fn metadata_id(&self) -> RegionId {
        self.inner.region_id
    }
}

struct RegionManifestInner {
    region_id: RegionId,
    store: Arc<ManifestObjectStore>,
    version: AtomicU64,
}

struct RegionMetaActionIterator {
    log_iter: ObjectStoreLogIterator,
}

impl RegionMetaActionIterator {
    async fn next_action(&mut self) -> Result<Option<(Version, RegionMetaAction)>> {
        match self.log_iter.next_log().await? {
            Some((v, bytes)) => {
                let action: RegionMetaAction = RegionMetaAction::decode(&bytes)?;
                Ok(Some((v, action)))
            }
            None => Ok(None),
        }
    }
}

impl RegionManifestInner {
    fn new(region_id: RegionId, object_store: ObjectStore) -> Self {
        // TODO(dennis): make manifest dir configurable.
        let path = format!("{}/manifest/", region_id);

        Self {
            region_id,
            store: Arc::new(ManifestObjectStore::new(&path, object_store)),
            // TODO(dennis): recover the last version from history
            version: AtomicU64::new(0),
        }
    }

    #[inline]
    fn inc_version(&self) -> Version {
        self.version.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    fn last_version(&self) -> Version {
        self.version.load(Ordering::Relaxed)
    }

    async fn save(&self, action: &RegionMetaAction) -> Result<()> {
        let version = self.inc_version();

        logging::debug!(
            "Save region metadata action: {:?}, version: {}",
            action,
            version
        );

        self.store.save(version, &action.encode()?).await
    }

    async fn scan(&self, start: Version, end: Version) -> Result<RegionMetaActionIterator> {
        Ok(RegionMetaActionIterator {
            log_iter: self.store.scan(start, end).await?,
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

        let manifest = RegionManifest::new(region_id, object_store);
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
            .update(RegionMetaAction::Change(RegionChange {
                metadata: region_meta.clone(),
            }))
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
            .update(RegionMetaAction::Change(RegionChange {
                metadata: region_meta.clone(),
            }))
            .await
            .unwrap();

        let manifest_data = manifest.load().await.unwrap().unwrap();
        assert_eq!(manifest_data.region_meta, region_meta);
    }
}
