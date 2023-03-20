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

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_telemetry::{debug, logging};
use object_store::ObjectStore;
use snafu::ensure;
use store_api::manifest::action::{self, ProtocolAction, ProtocolVersion};
use store_api::manifest::*;

use crate::error::{Error, ManifestProtocolForbidWriteSnafu, Result, UnsupportedCheckpointSnafu};
use crate::manifest::action::RegionSnapshot;
use crate::manifest::checkpoint::Checkpointer;
use crate::manifest::storage::{ManifestObjectStore, ObjectStoreLogIterator};

const CHECKPOINT_ACTIONS_MARGIN: u64 = 10;

#[derive(Clone, Debug)]
pub struct ManifestImpl<S: Snapshot<Error = Error>, M: MetaAction<Error = Error>> {
    inner: Arc<ManifestImplInner<S, M>>,
    checkpointer: Option<Arc<dyn Checkpointer<Snapshot = S, MetaAction = M>>>,
    last_checkpoint_version: Arc<AtomicU64>,
}

impl<S: Snapshot<Error = Error>, M: MetaAction<Error = Error>> ManifestImpl<S, M> {
    pub fn new(
        manifest_dir: &str,
        object_store: ObjectStore,
        checkpointer: Option<Arc<dyn Checkpointer<Snapshot = S, MetaAction = M>>>,
    ) -> Self {
        ManifestImpl {
            inner: Arc::new(ManifestImplInner::new(manifest_dir, object_store)),
            checkpointer,
            last_checkpoint_version: Arc::new(AtomicU64::new(MIN_VERSION)),
        }
    }

    pub fn checkpointer(&self) -> &Option<Arc<dyn Checkpointer<Snapshot = S, MetaAction = M>>> {
        &self.checkpointer
    }

    pub fn set_last_checkpoint_version(&self, version: ManifestVersion) {
        self.last_checkpoint_version
            .store(version, Ordering::Relaxed);
    }

    /// Update inner state.
    pub fn update_state(&self, version: ManifestVersion, protocol: Option<ProtocolAction>) {
        self.inner.update_state(version, protocol);
    }

    pub async fn save_snapshot(&self, snapshot: &RegionSnapshot) -> Result<()> {
        ensure!(
            snapshot
                .protocol
                .is_writable(self.inner.supported_writer_version),
            ManifestProtocolForbidWriteSnafu {
                min_version: snapshot.protocol.min_writer_version,
                supported_version: self.inner.supported_writer_version,
            }
        );
        let bytes = snapshot.encode()?;
        self.manifest_store()
            .save_checkpoint(snapshot.last_version, &bytes)
            .await
    }

    #[inline]
    pub fn manifest_store(&self) -> &Arc<ManifestObjectStore> {
        self.inner.manifest_store()
    }
}

#[async_trait]
impl<S: 'static + Snapshot<Error = Error>, M: 'static + MetaAction<Error = Error>> Manifest
    for ManifestImpl<S, M>
{
    type Error = Error;
    type Snapshot = S;
    type MetaAction = M;
    type MetaActionIterator = MetaActionIteratorImpl<M>;

    async fn update(&self, action_list: M) -> Result<ManifestVersion> {
        let version = self.inner.save(action_list).await?;
        if version - self.last_checkpoint_version.load(Ordering::Relaxed)
            >= CHECKPOINT_ACTIONS_MARGIN
        {
            let s = self.do_checkpoint().await?;
            debug!("Manifest checkpoint, snapshot: {:#?}", s);
        }
        Ok(version)
    }

    async fn scan(
        &self,
        start: ManifestVersion,
        end: ManifestVersion,
    ) -> Result<Self::MetaActionIterator> {
        self.inner.scan(start, end).await
    }

    async fn do_checkpoint(&self) -> Result<Option<S>> {
        if let Some(cp) = &self.checkpointer {
            let snapshot = cp.do_checkpoint(self).await?;
            if let Some(snapshot) = &snapshot {
                self.set_last_checkpoint_version(snapshot.last_version());
            }
            return Ok(snapshot);
        }
        UnsupportedCheckpointSnafu {}.fail()
    }

    async fn last_snapshot(&self) -> Result<Option<S>> {
        self.inner.last_snapshot().await
    }

    fn last_version(&self) -> ManifestVersion {
        self.inner.last_version()
    }
}

#[derive(Debug)]
struct ManifestImplInner<S: Snapshot<Error = Error>, M: MetaAction<Error = Error>> {
    store: Arc<ManifestObjectStore>,
    version: AtomicU64,
    /// Current using protocol
    protocol: ArcSwap<ProtocolAction>,
    /// Current node supported protocols (reader_version, writer_version)
    supported_reader_version: ProtocolVersion,
    supported_writer_version: ProtocolVersion,
    _phantom: PhantomData<(S, M)>,
}

pub struct MetaActionIteratorImpl<M: MetaAction<Error = Error>> {
    log_iter: ObjectStoreLogIterator,
    reader_version: ProtocolVersion,
    last_protocol: Option<ProtocolAction>,
    _phantom: PhantomData<M>,
}

impl<M: MetaAction<Error = Error>> MetaActionIteratorImpl<M> {
    pub fn last_protocol(&self) -> &Option<ProtocolAction> {
        &self.last_protocol
    }
}

#[async_trait]
impl<M: MetaAction<Error = Error>> MetaActionIterator for MetaActionIteratorImpl<M> {
    type Error = Error;
    type MetaAction = M;

    async fn next_action(&mut self) -> Result<Option<(ManifestVersion, M)>> {
        match self.log_iter.next_log().await? {
            Some((v, bytes)) => {
                let (action_list, protocol) = M::decode(&bytes, self.reader_version)?;

                if protocol.is_some() {
                    self.last_protocol = protocol;
                }

                Ok(Some((v, action_list)))
            }
            None => Ok(None),
        }
    }
}

impl<S: Snapshot<Error = Error>, M: MetaAction<Error = Error>> ManifestImplInner<S, M> {
    fn new(manifest_dir: &str, object_store: ObjectStore) -> Self {
        let (reader_version, writer_version) = action::supported_protocol_version();

        Self {
            store: Arc::new(ManifestObjectStore::new(manifest_dir, object_store)),
            version: AtomicU64::new(0),
            protocol: ArcSwap::new(Arc::new(ProtocolAction::new())),
            supported_reader_version: reader_version,
            supported_writer_version: writer_version,
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn manifest_store(&self) -> &Arc<ManifestObjectStore> {
        &self.store
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

    async fn save(&self, mut action_list: M) -> Result<ManifestVersion> {
        let protocol = self.protocol.load();

        ensure!(
            protocol.is_writable(self.supported_writer_version),
            ManifestProtocolForbidWriteSnafu {
                min_version: protocol.min_writer_version,
                supported_version: self.supported_writer_version,
            }
        );

        let version = self.inc_version();

        if version == 0 || protocol.min_writer_version < self.supported_writer_version {
            let new_protocol = ProtocolAction {
                min_reader_version: self.supported_reader_version,
                min_writer_version: self.supported_writer_version,
            };
            action_list.set_protocol(new_protocol.clone());

            logging::info!(
                "Updated manifest protocol from {} to {}.",
                protocol,
                new_protocol
            );

            self.protocol.store(Arc::new(new_protocol));
        }

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
    ) -> Result<MetaActionIteratorImpl<M>> {
        Ok(MetaActionIteratorImpl {
            log_iter: self.store.scan(start, end).await?,
            reader_version: self.supported_reader_version,
            last_protocol: None,
            _phantom: PhantomData,
        })
    }

    async fn last_snapshot(&self) -> Result<Option<S>> {
        let protocol = self.protocol.load();
        let last_checkpoint = self.store.load_checkpoint().await?;
        last_checkpoint
            .map(|(_, bytes)| S::decode(&bytes, protocol.min_reader_version))
            .transpose()
    }
}
