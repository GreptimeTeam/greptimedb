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
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::{debug, logging, warn};
use object_store::ObjectStore;
use snafu::{ensure, ResultExt};
use store_api::manifest::action::{self, ProtocolAction, ProtocolVersion};
use store_api::manifest::*;

use crate::error::{
    Error, ManifestProtocolForbidWriteSnafu, Result, StartManifestGcTaskSnafu,
    StopManifestGcTaskSnafu,
};
use crate::manifest::action::RegionCheckpoint;
use crate::manifest::checkpoint::Checkpointer;
use crate::manifest::storage::{ManifestObjectStore, ObjectStoreLogIterator};

const CHECKPOINT_ACTIONS_MARGIN: u16 = 10;
const GC_DURATION_SECS: u64 = 30;

#[derive(Clone, Debug)]
pub struct ManifestImpl<S: Checkpoint<Error = Error>, M: MetaAction<Error = Error>> {
    inner: Arc<ManifestImplInner<S, M>>,
    checkpointer: Option<Arc<dyn Checkpointer<Checkpoint = S, MetaAction = M>>>,
    last_checkpoint_version: Arc<AtomicU64>,
    checkpoint_actions_margin: u16,
    gc_task: Option<Arc<RepeatedTask<Error>>>,
}

impl<S: 'static + Checkpoint<Error = Error>, M: 'static + MetaAction<Error = Error>>
    ManifestImpl<S, M>
{
    pub fn new(
        manifest_dir: &str,
        object_store: ObjectStore,
        checkpoint_actions_margin: Option<u16>,
        gc_duration: Option<Duration>,
        checkpointer: Option<Arc<dyn Checkpointer<Checkpoint = S, MetaAction = M>>>,
    ) -> Self {
        let inner = Arc::new(ManifestImplInner::new(manifest_dir, object_store));
        let gc_task = if checkpointer.is_some() {
            // only start gc task when checkpoint is enabled.
            Some(Arc::new(RepeatedTask::new(
                gc_duration.unwrap_or_else(|| Duration::from_secs(GC_DURATION_SECS)),
                inner.clone() as _,
            )))
        } else {
            None
        };
        ManifestImpl {
            inner,
            checkpointer,
            checkpoint_actions_margin: checkpoint_actions_margin
                .unwrap_or(CHECKPOINT_ACTIONS_MARGIN),
            last_checkpoint_version: Arc::new(AtomicU64::new(MIN_VERSION)),
            gc_task,
        }
    }

    pub fn create(manifest_dir: &str, object_store: ObjectStore) -> Self {
        Self::new(manifest_dir, object_store, None, None, None)
    }

    pub fn checkpointer(&self) -> &Option<Arc<dyn Checkpointer<Checkpoint = S, MetaAction = M>>> {
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

    pub async fn save_checkpoint(&self, checkpoint: &RegionCheckpoint) -> Result<()> {
        ensure!(
            checkpoint
                .protocol
                .is_writable(self.inner.supported_writer_version),
            ManifestProtocolForbidWriteSnafu {
                min_version: checkpoint.protocol.min_writer_version,
                supported_version: self.inner.supported_writer_version,
            }
        );
        let bytes = checkpoint.encode()?;
        self.manifest_store()
            .save_checkpoint(checkpoint.last_version, &bytes)
            .await
    }

    #[inline]
    pub fn manifest_store(&self) -> &Arc<ManifestObjectStore> {
        self.inner.manifest_store()
    }
}

#[async_trait]
impl<S: 'static + Checkpoint<Error = Error>, M: 'static + MetaAction<Error = Error>> Manifest
    for ManifestImpl<S, M>
{
    type Error = Error;
    type Checkpoint = S;
    type MetaAction = M;
    type MetaActionIterator = MetaActionIteratorImpl<M>;

    async fn update(&self, action_list: M) -> Result<ManifestVersion> {
        let version = self.inner.save(action_list).await?;
        if version - self.last_checkpoint_version.load(Ordering::Relaxed)
            >= self.checkpoint_actions_margin as u64
        {
            let s = self.do_checkpoint().await?;
            debug!("Manifest checkpoint, checkpoint: {:#?}", s);
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
            let checkpoint = cp.do_checkpoint(self).await?;
            if let Some(checkpoint) = &checkpoint {
                self.set_last_checkpoint_version(checkpoint.last_version());
            }
            return Ok(checkpoint);
        }

        Ok(None)
    }

    async fn last_checkpoint(&self) -> Result<Option<S>> {
        self.inner.last_checkpoint().await
    }

    fn last_version(&self) -> ManifestVersion {
        self.inner.last_version()
    }

    async fn start(&self) -> Result<()> {
        if let Some(task) = &self.gc_task {
            task.start(common_runtime::bg_runtime())
                .await
                .context(StartManifestGcTaskSnafu)?;
        }

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(task) = &self.gc_task {
            task.stop().await.context(StopManifestGcTaskSnafu)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct ManifestImplInner<S: Checkpoint<Error = Error>, M: MetaAction<Error = Error>> {
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

#[async_trait::async_trait]
impl<S: Checkpoint<Error = Error>, M: MetaAction<Error = Error>> TaskFunction<Error>
    for ManifestImplInner<S, M>
{
    fn name(&self) -> &str {
        "region-manifest-gc"
    }

    async fn call(&self) -> Result<()> {
        if let Some((last_version, _)) = self.store.load_last_checkpoint().await? {
            // Purge all manifest and checkpoint files before last_version.
            let deleted = self.store.delete_until(last_version).await?;
            debug!(
                "Deleted {} logs from region manifest storage(path={}).",
                deleted,
                self.store.path()
            );
        }

        Ok(())
    }
}

impl<S: Checkpoint<Error = Error>, M: MetaAction<Error = Error>> ManifestImplInner<S, M> {
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

    async fn last_checkpoint(&self) -> Result<Option<S>> {
        let protocol = self.protocol.load();
        let last_checkpoint = self.store.load_last_checkpoint().await?;

        if let Some((version, bytes)) = last_checkpoint {
            let checkpoint = S::decode(&bytes, protocol.min_reader_version)?;
            assert!(checkpoint.last_version() >= version);
            if checkpoint.last_version() > version {
                // It happens when saving checkpoint successfully, but failed at saving checkpoint metadata(the "__last_checkpoint" file).
                // Then we try to use the old checkpoint and do the checkpoint next time.
                // If the old checkpoint was deleted, it's fine that we return the latest checkpoint.
                // the only side effect is leaving some unused checkpoint checkpoint files,
                // and they will be purged by gc task.
                warn!("The checkpoint manifest version {} in {} is greater than checkpoint metadata version {}.", self.store.path(), checkpoint.last_version(), version);

                if let Some((_, bytes)) = self.store.load_checkpoint(version).await? {
                    let old_checkpoint = S::decode(&bytes, protocol.min_reader_version)?;
                    return Ok(Some(old_checkpoint));
                }
            }
            Ok(Some(checkpoint))
        } else {
            Ok(None)
        }
    }
}
