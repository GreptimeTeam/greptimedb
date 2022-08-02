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

use crate::error::{Error, ManifestProtocolForbidWriteSnafu, Result};
use crate::manifest::action::*;
use crate::manifest::storage::ManifestObjectStore;
use crate::manifest::storage::ObjectStoreLogIterator;

#[derive(Clone, Debug)]
pub struct RegionManifest {
    inner: Arc<RegionManifestInner>,
}

impl RegionManifest {
    pub fn new(manifest_dir: &str, object_store: ObjectStore) -> Self {
        RegionManifest {
            inner: Arc::new(RegionManifestInner::new(manifest_dir, object_store)),
        }
    }
}

#[async_trait]
impl Manifest for RegionManifest {
    type Error = Error;
    type MetaAction = RegionMetaActionList;
    type Metadata = RegionManifestData;
    type MetaActionIterator = RegionMetaActionListIterator;

    async fn update(&self, action_list: RegionMetaActionList) -> Result<ManifestVersion> {
        self.inner.save(action_list).await
    }

    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<RegionMetaActionListIterator> {
        self.inner.scan(start, end).await
    }

    async fn checkpoint(&self) -> Result<ManifestVersion> {
        unimplemented!();
    }

    fn last_version(&self) -> ManifestVersion {
        self.inner.last_version()
    }
}

#[derive(Debug)]
struct RegionManifestInner {
    store: Arc<ManifestObjectStore>,
    version: AtomicU64,
    /// Current using protocol
    protocol: ArcSwap<ProtocolAction>,
    /// Current node supported protocols (reader_version, writer_version)
    supported_reader_version: ProtocolVersion,
    supported_writer_version: ProtocolVersion,
}

pub struct RegionMetaActionListIterator {
    log_iter: ObjectStoreLogIterator,
    reader_version: ProtocolVersion,
}

#[async_trait]
impl MetaActionIterator for RegionMetaActionListIterator {
    type Error = Error;
    type MetaAction = RegionMetaActionList;

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
    fn new(manifest_dir: &str, object_store: ObjectStore) -> Self {
        let (reader_version, writer_version) = action::supported_protocol_version();

        Self {
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
    use object_store::{backend::fs, ObjectStore};
    use tempdir::TempDir;

    use super::*;
    use crate::manifest::test_utils::*;

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

        let manifest = RegionManifest::new("/manifest/", object_store);

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
                    metadata: region_meta.clone(),
                },
            )))
            .await
            .unwrap();

        let mut iter = manifest.scan(0, MAX_VERSION).await.unwrap();

        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(0, v);
        assert_eq!(1, action_list.actions.len());
        let action = &action_list.actions[0];

        match action {
            RegionMetaAction::Change(c) => {
                assert_eq!(c.metadata, region_meta);
            }
            _ => unreachable!(),
        }

        // Save some actions
        manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Edit(build_region_edit(1, &["f1"], &[])),
                RegionMetaAction::Edit(build_region_edit(2, &["f2", "f3"], &[])),
            ]))
            .await
            .unwrap();

        let mut iter = manifest.scan(0, MAX_VERSION).await.unwrap();
        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(0, v);
        assert_eq!(1, action_list.actions.len());
        let action = &action_list.actions[0];
        match action {
            RegionMetaAction::Change(c) => {
                assert_eq!(c.metadata, region_meta);
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
