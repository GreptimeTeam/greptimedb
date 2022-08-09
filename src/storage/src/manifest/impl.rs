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

    /// Update inner state.
    pub fn update_state(&self, version: ManifestVersion, protocol: Option<ProtocolAction>) {
        self.inner.update_state(version, protocol);
    }
}

#[async_trait]
impl Manifest for RegionManifest {
    type Error = Error;
    type MetaAction = RegionMetaActionList;
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
    last_protocol: Option<ProtocolAction>,
}

impl RegionMetaActionListIterator {
    pub fn last_protocol(&self) -> &Option<ProtocolAction> {
        &self.last_protocol
    }
}

#[async_trait]
impl MetaActionIterator for RegionMetaActionListIterator {
    type Error = Error;
    type MetaAction = RegionMetaActionList;

    async fn next_action(&mut self) -> Result<Option<(ManifestVersion, RegionMetaActionList)>> {
        match self.log_iter.next_log().await? {
            Some((v, bytes)) => {
                let (action_list, protocol) =
                    RegionMetaActionList::decode(&bytes, self.reader_version)?;

                if protocol.is_some() {
                    self.last_protocol = protocol;
                }

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

    fn update_state(&self, version: ManifestVersion, protocol: Option<ProtocolAction>) {
        self.version.store(version, Ordering::Relaxed);
        if let Some(p) = protocol {
            self.protocol.store(Arc::new(p));
        }
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
            last_protocol: None,
        })
    }
}
