//! Region manifest impl
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_telemetry::logging;
use object_store::{backend::fs, ObjectStore};
use serde_json as json;
use snafu::ResultExt;
use store_api::manifest::*;
use store_api::storage::RegionId;

use crate::error::{DecodeJsonSnafu, EncodeJsonSnafu, Error, FromUtf8Snafu, Result};
use crate::manifest::action::*;
use crate::manifest::storage::ManifestObjectStore;
use crate::manifest::storage::ObjectStoreLogIterator;

#[derive(Clone)]
pub struct RegionManifest {
    region_stores: Arc<RwLock<HashMap<RegionId, Arc<RegionManifestStore>>>>,
    regions_path: String,
}

impl RegionManifest {
    pub fn new(regions_path: &str) -> Self {
        Self {
            region_stores: Arc::new(RwLock::new(HashMap::new())),
            regions_path: regions_path.to_string(),
        }
    }

    async fn get_or_create(&self, id: RegionId) -> Result<Arc<RegionManifestStore>> {
        let store = {
            let mut stores = self.region_stores.write().unwrap();
            stores
                .entry(id)
                .or_insert_with(|| {
                    let path = &self.region_path(id);
                    Arc::new(RegionManifestStore::new(path))
                })
                .clone()
        };
        store.start().await?;

        Ok(store)
    }

    #[inline]
    fn region_path(&self, id: RegionId) -> String {
        format!("{}/{}/", self.regions_path, id)
    }
}

#[async_trait]
impl Manifest for RegionManifest {
    type Error = Error;
    type MetaAction = RegionMetaAction;
    type MetadataId = RegionId;
    type Metadata = RegionManifestData;

    async fn update(&self, action: RegionMetaAction) -> Result<()> {
        let store = self.get_or_create(*action.metadata_id()).await?;
        store.save(&action).await
    }

    async fn load(&self, id: RegionId) -> Result<Option<RegionManifestData>> {
        let store = self.get_or_create(id).await?;
        let current_version = store.current_version();
        let mut iter = store.scan(current_version, MAX_VERSION).await?;

        match iter.next_action().await? {
            Some((v, RegionMetaAction::Change(c))) => {
                assert!(v == current_version);
                Ok(Some(RegionManifestData {
                    region_meta: c.metadata,
                }))
            }
            Some(_) => todo!(),
            None => Ok(None),
        }
    }

    async fn checkpoint(&self, _id: RegionId) -> Result<()> {
        unimplemented!();
    }
}

struct RegionManifestStore {
    path: String,
    store: ArcSwap<Option<ManifestObjectStore>>,
    version: AtomicU64,
}

struct RegionMetaActionIterator {
    log_iter: ObjectStoreLogIterator,
}

impl RegionMetaActionIterator {
    async fn next_action(&mut self) -> Result<Option<(Version, RegionMetaAction)>> {
        match self.log_iter.next_log().await? {
            Some((v, bytes)) => {
                let action: RegionMetaAction =
                    json::from_str(&String::from_utf8(bytes).context(FromUtf8Snafu)?)
                        .context(DecodeJsonSnafu)?;

                Ok(Some((v, action)))
            }
            None => Ok(None),
        }
    }
}

impl RegionManifestStore {
    fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            store: ArcSwap::from(Arc::new(None)),
            version: AtomicU64::new(0),
        }
    }

    fn store(&self) -> ManifestObjectStore {
        match self.store.load().as_ref() {
            Some(s) => s.clone(),
            None => unreachable!(),
        }
    }

    async fn start(&self) -> Result<()> {
        if self.store.load().is_none() {
            //FIXME(boyan): may create several backend instances in concurrency
            let backend = fs::Backend::build()
                .root(&self.path)
                .finish()
                .await
                .unwrap();

            self.store.store(Arc::new(Some(ManifestObjectStore::new(
                &self.path,
                ObjectStore::new(backend),
            ))));
        }

        Ok(())
    }

    fn next_version(&self) -> Version {
        self.version.fetch_add(1, Ordering::Relaxed)
    }

    fn current_version(&self) -> Version {
        let v = self.version.load(Ordering::Relaxed);

        if v == MIN_VERSION {
            v
        } else {
            v - 1
        }
    }

    async fn save(&self, action: &RegionMetaAction) -> Result<()> {
        let version = self.next_version();

        logging::debug!(
            "Save region metadata action: {:?}, version: {}",
            action,
            version
        );

        self.store()
            .save(
                version,
                json::to_string(action).context(EncodeJsonSnafu)?.as_bytes(),
            )
            .await
    }

    async fn scan(&self, start: Version, end: Version) -> Result<RegionMetaActionIterator> {
        Ok(RegionMetaActionIterator {
            log_iter: self.store().scan(start, end).await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;
    use tempdir::TempDir;

    use super::*;
    use crate::metadata::RegionMetaImpl;
    use crate::metadata::RegionMetadata;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    #[tokio::test]
    async fn test_region_manifest() {
        common_telemetry::init_default_ut_logging();
        let tmp_dir = TempDir::new("test_region_manifest").unwrap();

        let manifest = RegionManifest::new(&tmp_dir.path().to_string_lossy());

        let region_id = 0;
        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_value_column(("v1", LogicalTypeId::Float32, true))
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        assert!(manifest.load(region_id).await.unwrap().is_none());

        let region_meta = RegionMetaImpl {
            metadata: Arc::new(metadata),
        };

        manifest
            .update(RegionMetaAction::Change(RegionChange {
                metadata: region_meta.clone(),
            }))
            .await
            .unwrap();

        let manifest_data = manifest.load(region_id).await.unwrap().unwrap();
        assert_eq!(manifest_data.region_meta, region_meta);
    }
}
