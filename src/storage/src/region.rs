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

#[cfg(test)]
mod tests;
mod writer;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common_telemetry::{info, logging};
use common_time::util;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::manifest::{
    self, Manifest, ManifestLogStorage, ManifestVersion, MetaActionIterator,
};
use store_api::storage::{
    AlterRequest, CloseContext, CompactContext, CompactionStrategy, FlushContext, FlushReason,
    OpenOptions, ReadContext, Region, RegionId, SequenceNumber, WriteContext, WriteResponse,
};

use crate::compaction::{
    compaction_strategy_to_picker, CompactionPickerRef, CompactionSchedulerRef,
};
use crate::config::EngineConfig;
use crate::error::{self, Error, Result};
use crate::file_purger::FilePurgerRef;
use crate::flush::{FlushSchedulerRef, FlushStrategyRef};
use crate::manifest::action::{
    RawRegionMetadata, RegionChange, RegionCheckpoint, RegionMetaAction, RegionMetaActionList,
};
use crate::manifest::region::RegionManifest;
use crate::memtable::{MemtableBuilderRef, MemtableVersion};
use crate::metadata::{RegionMetaImpl, RegionMetadata, RegionMetadataRef};
pub(crate) use crate::region::writer::schedule_compaction;
pub use crate::region::writer::{
    AlterContext, RegionWriter, RegionWriterRef, WriterCompactRequest, WriterContext,
};
use crate::region::writer::{DropContext, TruncateContext};
use crate::schema::compat::CompatWrite;
use crate::snapshot::SnapshotImpl;
use crate::sst::{AccessLayerRef, LevelMetas};
use crate::version::{
    Version, VersionControl, VersionControlRef, VersionEdit, INIT_COMMITTED_SEQUENCE,
};
use crate::wal::Wal;
use crate::write_batch::WriteBatch;

/// [Region] implementation.
pub struct RegionImpl<S: LogStore> {
    inner: Arc<RegionInner<S>>,
}

impl<S: LogStore> Clone for RegionImpl<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: LogStore> fmt::Debug for RegionImpl<S> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RegionImpl")
            .field("id", &self.inner.shared.id)
            .field("name", &self.inner.shared.name)
            .field("wal", &self.inner.wal)
            .field("flush_strategy", &self.inner.flush_strategy)
            .field("compaction_scheduler", &self.inner.compaction_scheduler)
            .field("sst_layer", &self.inner.sst_layer)
            .field("manifest", &self.inner.manifest)
            .finish()
    }
}

#[async_trait]
impl<S: LogStore> Region for RegionImpl<S> {
    type Error = Error;
    type Meta = RegionMetaImpl;
    type WriteRequest = WriteBatch;
    type Snapshot = SnapshotImpl;

    fn id(&self) -> RegionId {
        self.inner.shared.id
    }

    fn name(&self) -> &str {
        &self.inner.shared.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        self.inner.in_memory_metadata()
    }

    async fn write(&self, ctx: &WriteContext, mut request: WriteBatch) -> Result<WriteResponse> {
        // Compat the schema of the write batch outside of the write lock.
        self.inner.compat_write_batch(&mut request)?;

        self.inner.write(ctx, request).await
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<SnapshotImpl> {
        Ok(self.inner.create_snapshot())
    }

    fn write_request(&self) -> Self::WriteRequest {
        let metadata = self.inner.version_control().metadata();
        let user_schema = metadata.user_schema().clone();
        let row_key_end = metadata.schema().store_schema().row_key_end();

        WriteBatch::new(user_schema, row_key_end)
    }

    async fn alter(&self, request: AlterRequest) -> Result<()> {
        self.inner.alter(request).await
    }

    async fn drop_region(&self) -> Result<()> {
        crate::metrics::REGION_COUNT.dec();
        self.inner.drop_region().await
    }

    fn disk_usage_bytes(&self) -> u64 {
        let version = self.inner.version_control().current();
        version
            .ssts()
            .levels()
            .iter()
            .map(|level_ssts| level_ssts.files().map(|sst| sst.file_size()).sum::<u64>())
            .sum()
    }

    async fn flush(&self, ctx: &FlushContext) -> Result<()> {
        self.inner.flush(ctx).await
    }

    async fn compact(&self, ctx: &CompactContext) -> std::result::Result<(), Self::Error> {
        self.inner.compact(ctx).await
    }

    async fn truncate(&self) -> Result<()> {
        self.inner.truncate().await
    }
}

/// Storage related config for region.
///
/// Contains all necessary storage related components needed by the region, such as logstore,
/// manifest, memtable builder.
pub struct StoreConfig<S: LogStore> {
    pub log_store: Arc<S>,
    pub sst_layer: AccessLayerRef,
    pub manifest: RegionManifest,
    pub memtable_builder: MemtableBuilderRef,
    pub flush_scheduler: FlushSchedulerRef<S>,
    pub flush_strategy: FlushStrategyRef,
    pub compaction_scheduler: CompactionSchedulerRef<S>,
    pub engine_config: Arc<EngineConfig>,
    pub file_purger: FilePurgerRef,
    pub ttl: Option<Duration>,
    pub write_buffer_size: usize,
    pub compaction_strategy: CompactionStrategy,
}

pub type RecoveredMetadata = (SequenceNumber, (ManifestVersion, RawRegionMetadata));
pub type RecoveredMetadataMap = BTreeMap<SequenceNumber, (ManifestVersion, RawRegionMetadata)>;

impl<S: LogStore> RegionImpl<S> {
    /// Create a new region and also persist the region metadata to manifest.
    ///
    /// The caller should avoid calling this method simultaneously.
    pub async fn create(
        metadata: RegionMetadata,
        store_config: StoreConfig<S>,
    ) -> Result<RegionImpl<S>> {
        let metadata = Arc::new(metadata);

        // Try to persist region data to manifest, ensure the new region could be recovered from
        // the manifest.
        let manifest_version = {
            let _timer = crate::metrics::CREATE_REGION_UPDATE_MANIFEST.start_timer();
            store_config
                .manifest
                .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                    RegionChange {
                        metadata: metadata.as_ref().into(),
                        committed_sequence: INIT_COMMITTED_SEQUENCE,
                    },
                )))
                .await?
        };

        let mutable_memtable = store_config
            .memtable_builder
            .build(metadata.schema().clone());
        let version = Version::with_manifest_version(
            metadata,
            manifest_version,
            mutable_memtable,
            store_config.sst_layer.clone(),
            store_config.file_purger.clone(),
        );
        let region = RegionImpl::new(version, store_config);
        crate::metrics::REGION_COUNT.inc();

        Ok(region)
    }

    /// Create a new region without persisting manifest.
    fn new(version: Version, store_config: StoreConfig<S>) -> RegionImpl<S> {
        let metadata = version.metadata();
        let id = metadata.id();
        let name = metadata.name().to_string();
        let version_control = VersionControl::with_version(version);
        let wal = Wal::new(id, store_config.log_store);

        let compaction_picker = compaction_strategy_to_picker(&store_config.compaction_strategy);
        let inner = Arc::new(RegionInner {
            shared: Arc::new(SharedData {
                id,
                name,
                version_control: Arc::new(version_control),
                last_flush_millis: AtomicI64::new(0),
            }),
            writer: Arc::new(RegionWriter::new(
                store_config.memtable_builder,
                store_config.engine_config.clone(),
                store_config.ttl,
                store_config.write_buffer_size,
                store_config.compaction_scheduler.clone(),
                compaction_picker.clone(),
            )),
            wal,
            flush_strategy: store_config.flush_strategy,
            flush_scheduler: store_config.flush_scheduler,
            compaction_scheduler: store_config.compaction_scheduler,
            compaction_picker,
            sst_layer: store_config.sst_layer,
            manifest: store_config.manifest,
        });

        RegionImpl { inner }
    }

    /// Open an existing region and recover its data.
    ///
    /// The caller should avoid calling this method simultaneously.
    pub async fn open(
        name: String,
        store_config: StoreConfig<S>,
        _opts: &OpenOptions,
    ) -> Result<Option<RegionImpl<S>>> {
        // Load version meta data from manifest.
        let (version, mut recovered_metadata) = match Self::recover_from_manifest(
            &store_config.manifest,
            &store_config.memtable_builder,
            &store_config.sst_layer,
            &store_config.file_purger,
        )
        .await?
        {
            (None, _) => return Ok(None),
            (Some(v), m) => (v, m),
        };

        logging::debug!(
            "Region recovered version from manifest, version: {:?}",
            version
        );

        let metadata = version.metadata().clone();
        let flushed_sequence = version.flushed_sequence();
        let version_control = Arc::new(VersionControl::with_version(version));

        let recovered_metadata_after_flushed =
            recovered_metadata.split_off(&(flushed_sequence + 1));
        // apply the last flushed metadata
        if let Some((sequence, (manifest_version, metadata))) = recovered_metadata.pop_last() {
            let metadata: RegionMetadataRef = Arc::new(
                metadata
                    .try_into()
                    .context(error::InvalidRawRegionSnafu { region: &name })?,
            );
            let mutable_memtable = store_config
                .memtable_builder
                .build(metadata.schema().clone());
            version_control.freeze_mutable_and_apply_metadata(
                metadata,
                manifest_version,
                mutable_memtable,
            );

            logging::debug!(
                "Applied the last flushed metadata to region: {}, sequence: {}, manifest: {}",
                name,
                sequence,
                manifest_version,
            );
        }

        let wal = Wal::new(metadata.id(), store_config.log_store);
        wal.obsolete(flushed_sequence).await?;
        info!(
            "Obsolete WAL entries on startup, region: {}, flushed sequence: {}",
            metadata.id(),
            flushed_sequence
        );

        let shared = Arc::new(SharedData {
            id: metadata.id(),
            name,
            version_control,
            last_flush_millis: AtomicI64::new(0),
        });

        let compaction_picker = compaction_strategy_to_picker(&store_config.compaction_strategy);
        let writer = Arc::new(RegionWriter::new(
            store_config.memtable_builder,
            store_config.engine_config.clone(),
            store_config.ttl,
            store_config.write_buffer_size,
            store_config.compaction_scheduler.clone(),
            compaction_picker.clone(),
        ));

        let writer_ctx = WriterContext {
            shared: &shared,
            flush_strategy: &store_config.flush_strategy,
            flush_scheduler: &store_config.flush_scheduler,
            compaction_scheduler: &store_config.compaction_scheduler,
            sst_layer: &store_config.sst_layer,
            wal: &wal,
            writer: &writer,
            manifest: &store_config.manifest,
            compaction_picker: compaction_picker.clone(),
        };
        // Replay all unflushed data.
        writer
            .replay(recovered_metadata_after_flushed, writer_ctx)
            .await?;

        let inner = Arc::new(RegionInner {
            shared,
            writer,
            wal,
            flush_strategy: store_config.flush_strategy,
            flush_scheduler: store_config.flush_scheduler,
            compaction_scheduler: store_config.compaction_scheduler,
            compaction_picker,
            sst_layer: store_config.sst_layer,
            manifest: store_config.manifest,
        });

        crate::metrics::REGION_COUNT.inc();
        Ok(Some(RegionImpl { inner }))
    }

    /// Get ID of this region.
    pub fn id(&self) -> RegionId {
        self.inner.shared.id()
    }

    /// Returns last flush timestamp in millis.
    pub(crate) fn last_flush_millis(&self) -> i64 {
        self.inner.shared.last_flush_millis()
    }

    /// Returns the [VersionControl] of the region.
    pub(crate) fn version_control(&self) -> &VersionControl {
        self.inner.version_control()
    }

    fn create_version_with_checkpoint(
        checkpoint: RegionCheckpoint,
        memtable_builder: &MemtableBuilderRef,
        sst_layer: &AccessLayerRef,
        file_purger: &FilePurgerRef,
    ) -> Result<Option<Version>> {
        if checkpoint.checkpoint.is_none() {
            return Ok(None);
        }
        // Safety: it's safe to unwrap here, checking it above.
        let s = checkpoint.checkpoint.unwrap();

        let region = s.metadata.name.clone();
        let region_metadata: RegionMetadata = s
            .metadata
            .try_into()
            .context(error::InvalidRawRegionSnafu { region })?;

        let memtable = memtable_builder.build(region_metadata.schema().clone());
        let mut version = Version::with_manifest_version(
            Arc::new(region_metadata),
            checkpoint.last_version,
            memtable,
            sst_layer.clone(),
            file_purger.clone(),
        );

        if let Some(v) = s.version {
            version.apply_checkpoint(
                v.flushed_sequence,
                v.manifest_version,
                v.files.into_values(),
            );
        }

        Ok(Some(version))
    }

    async fn recover_from_manifest(
        manifest: &RegionManifest,
        memtable_builder: &MemtableBuilderRef,
        sst_layer: &AccessLayerRef,
        file_purger: &FilePurgerRef,
    ) -> Result<(Option<Version>, RecoveredMetadataMap)> {
        let checkpoint = manifest.last_checkpoint().await?;

        let (start, end, mut version) = if let Some(checkpoint) = checkpoint {
            (
                checkpoint.last_version + 1,
                manifest::MAX_VERSION,
                Self::create_version_with_checkpoint(
                    checkpoint,
                    memtable_builder,
                    sst_layer,
                    file_purger,
                )?,
            )
        } else {
            (manifest::MIN_VERSION, manifest::MAX_VERSION, None)
        };

        let mut iter = manifest.scan(start, end).await?;

        let mut actions = Vec::new();
        let mut last_manifest_version = manifest::MIN_VERSION;
        let mut recovered_metadata = BTreeMap::new();

        while let Some((manifest_version, action_list)) = iter.next_action().await? {
            last_manifest_version = manifest_version;

            for action in action_list.actions {
                match (action, version) {
                    (RegionMetaAction::Change(c), None) => {
                        let region = c.metadata.name.clone();
                        let region_metadata: RegionMetadata = c
                            .metadata
                            .try_into()
                            .context(error::InvalidRawRegionSnafu { region })?;
                        // Use current schema to build a memtable. This might be replaced later
                        // in `freeze_mutable_and_apply_metadata()`.
                        let memtable = memtable_builder.build(region_metadata.schema().clone());
                        version = Some(Version::with_manifest_version(
                            Arc::new(region_metadata),
                            last_manifest_version,
                            memtable,
                            sst_layer.clone(),
                            file_purger.clone(),
                        ));
                        for (manifest_version, action) in actions.drain(..) {
                            version = Self::replay_edit(manifest_version, action, version);
                        }
                    }
                    (RegionMetaAction::Change(c), Some(v)) => {
                        let _ = recovered_metadata
                            .insert(c.committed_sequence, (manifest_version, c.metadata));
                        version = Some(v);
                    }
                    (RegionMetaAction::Remove(r), Some(v)) => {
                        manifest.stop().await?;

                        let files = v.ssts().mark_all_files_deleted();
                        logging::info!(
                            "Try to remove all SSTs, region: {}, files: {:?}",
                            r.region_id,
                            files
                        );

                        manifest
                            .manifest_store()
                            .delete_all(v.manifest_version())
                            .await?;
                        return Ok((None, recovered_metadata));
                    }
                    (RegionMetaAction::Truncate(t), Some(mut v)) => {
                        let files = v.ssts().mark_all_files_deleted();
                        logging::info!(
                            "Try to remove all SSTs on truncate, region: {}, files: {:?}",
                            t.region_id,
                            files
                        );
                        let region_metadata = v.metadata().clone();
                        let memtables = Arc::new(MemtableVersion::new(
                            memtable_builder.build(region_metadata.schema().clone()),
                        ));
                        let ssts =
                            Arc::new(LevelMetas::new(sst_layer.clone(), file_purger.clone()));
                        v.reset(
                            v.manifest_version() + 1,
                            memtables,
                            ssts,
                            t.committed_sequence,
                        );
                        version = Some(v);
                    }
                    (action, None) => {
                        actions.push((manifest_version, action));
                        version = None;
                    }
                    (action, Some(v)) => {
                        version = Self::replay_edit(manifest_version, action, Some(v));
                    }
                }
            }
        }

        assert!(actions.is_empty() || version.is_none());

        if let Some(version) = &version {
            // update manifest state after recovering
            let protocol = iter.last_protocol();
            manifest.update_state(last_manifest_version + 1, protocol.clone());
            manifest.set_flushed_manifest_version(version.manifest_version());
        }

        Ok((version, recovered_metadata))
    }

    fn replay_edit(
        manifest_version: ManifestVersion,
        action: RegionMetaAction,
        version: Option<Version>,
    ) -> Option<Version> {
        if let RegionMetaAction::Edit(e) = action {
            let edit = VersionEdit {
                files_to_add: e.files_to_add,
                files_to_remove: e.files_to_remove,
                flushed_sequence: e.flushed_sequence,
                manifest_version,
                max_memtable_id: None,
                compaction_time_window: e.compaction_time_window,
            };
            version.map(|mut v| {
                v.apply_edit(edit);
                v
            })
        } else {
            version
        }
    }

    /// Compact the region manually.
    pub async fn compact(&self, ctx: &CompactContext) -> Result<()> {
        self.inner.compact(ctx).await
    }

    pub async fn close(&self, ctx: &CloseContext) -> Result<()> {
        crate::metrics::REGION_COUNT.dec();
        self.inner.close(ctx).await
    }
}

// Private methods for tests.
#[cfg(test)]
impl<S: LogStore> RegionImpl<S> {
    #[inline]
    fn committed_sequence(&self) -> store_api::storage::SequenceNumber {
        self.inner.version_control().committed_sequence()
    }

    fn current_manifest_version(&self) -> ManifestVersion {
        self.inner.version_control().current_manifest_version()
    }

    /// Write to inner, also the `RegionWriter` directly.
    async fn write_inner(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        self.inner.write(ctx, request).await
    }

    // Replay metadata to inner.
    async fn replay_inner(&self, recovered_metadata: RecoveredMetadataMap) -> Result<()> {
        let inner = &self.inner;
        let writer_ctx = WriterContext {
            shared: &inner.shared,
            flush_strategy: &inner.flush_strategy,
            flush_scheduler: &inner.flush_scheduler,
            compaction_scheduler: &inner.compaction_scheduler,
            sst_layer: &inner.sst_layer,
            wal: &inner.wal,
            writer: &inner.writer,
            manifest: &inner.manifest,
            compaction_picker: inner.compaction_picker.clone(),
        };

        inner.writer.replay(recovered_metadata, writer_ctx).await
    }

    pub(crate) async fn write_buffer_size(&self) -> usize {
        self.inner.writer.write_buffer_size().await
    }
}

/// Shared data of region.
#[derive(Debug)]
pub struct SharedData {
    // Region id and name is immutable, so we cache them in shared data to avoid loading
    // current version from `version_control` each time we need to access them.
    id: RegionId,
    name: String,
    // TODO(yingwen): Maybe no need to use Arc for version control.
    pub version_control: VersionControlRef,

    /// Last flush time in millis.
    last_flush_millis: AtomicI64,
}

impl SharedData {
    #[inline]
    pub fn id(&self) -> RegionId {
        self.id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Update flush time to current time.
    pub(crate) fn update_flush_millis(&self) {
        let now = util::current_time_millis();
        self.last_flush_millis.store(now, Ordering::Relaxed);
    }

    /// Returns last flush timestamp in millis.
    fn last_flush_millis(&self) -> i64 {
        self.last_flush_millis.load(Ordering::Relaxed)
    }
}

pub type SharedDataRef = Arc<SharedData>;

struct RegionInner<S: LogStore> {
    shared: SharedDataRef,
    writer: RegionWriterRef<S>,
    wal: Wal<S>,
    flush_strategy: FlushStrategyRef,
    flush_scheduler: FlushSchedulerRef<S>,
    compaction_scheduler: CompactionSchedulerRef<S>,
    compaction_picker: CompactionPickerRef<S>,
    sst_layer: AccessLayerRef,
    manifest: RegionManifest,
}

impl<S: LogStore> RegionInner<S> {
    #[inline]
    fn version_control(&self) -> &VersionControl {
        &self.shared.version_control
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        let metadata = self.version_control().metadata();

        RegionMetaImpl::new(metadata)
    }

    fn create_snapshot(&self) -> SnapshotImpl {
        let version = self.version_control().current();
        let sequence = self.version_control().committed_sequence();

        SnapshotImpl::new(version, sequence, self.sst_layer.clone())
    }

    fn compat_write_batch(&self, request: &mut WriteBatch) -> Result<()> {
        let metadata = self.version_control().metadata();
        let schema = metadata.schema();

        // Try to make request schema compatible with region's outside of write lock. Note that
        // schema might be altered after this step.
        request.compat_write(schema.user_schema())
    }

    /// Write to writer directly.
    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        let writer_ctx = WriterContext {
            shared: &self.shared,
            flush_strategy: &self.flush_strategy,
            flush_scheduler: &self.flush_scheduler,
            compaction_scheduler: &self.compaction_scheduler,
            sst_layer: &self.sst_layer,
            wal: &self.wal,
            writer: &self.writer,
            manifest: &self.manifest,
            compaction_picker: self.compaction_picker.clone(),
        };
        // The writer would also try to compat the schema of write batch if it finds out the
        // schema version of request is less than current schema version.
        self.writer.write(ctx, request, writer_ctx).await
    }

    async fn alter(&self, request: AlterRequest) -> Result<()> {
        logging::info!(
            "Alter region {}, name: {}, request: {:?}",
            self.shared.id,
            self.shared.name,
            request
        );

        let alter_ctx = AlterContext {
            shared: &self.shared,
            wal: &self.wal,
            manifest: &self.manifest,
        };

        self.writer.alter(alter_ctx, request).await
    }

    async fn close(&self, ctx: &CloseContext) -> Result<()> {
        self.writer.close().await?;
        if ctx.flush {
            let ctx = FlushContext {
                wait: true,
                reason: FlushReason::Manually,
                force: true,
            };
            self.flush(&ctx).await?;
        }
        self.manifest.stop().await
    }

    async fn drop_region(&self) -> Result<()> {
        logging::info!("Drop region {}, name: {}", self.shared.id, self.shared.name);
        let drop_ctx = DropContext {
            shared: &self.shared,
            wal: &self.wal,
            manifest: &self.manifest,
            flush_scheduler: &self.flush_scheduler,
            compaction_scheduler: &self.compaction_scheduler,
            sst_layer: &self.sst_layer,
        };

        self.manifest.stop().await?;
        self.writer.on_drop(drop_ctx).await
    }

    async fn flush(&self, ctx: &FlushContext) -> Result<()> {
        let writer_ctx = WriterContext {
            shared: &self.shared,
            flush_strategy: &self.flush_strategy,
            flush_scheduler: &self.flush_scheduler,
            compaction_scheduler: &self.compaction_scheduler,
            sst_layer: &self.sst_layer,
            wal: &self.wal,
            writer: &self.writer,
            manifest: &self.manifest,
            compaction_picker: self.compaction_picker.clone(),
        };
        self.writer.flush(writer_ctx, ctx).await
    }

    /// Compact the region manually.
    async fn compact(&self, compact_ctx: &CompactContext) -> Result<()> {
        self.writer
            .compact(WriterCompactRequest {
                shared_data: self.shared.clone(),
                sst_layer: self.sst_layer.clone(),
                manifest: self.manifest.clone(),
                wal: self.wal.clone(),
                region_writer: self.writer.clone(),
                compact_ctx: *compact_ctx,
            })
            .await
    }

    async fn truncate(&self) -> Result<()> {
        logging::info!(
            "Truncate region {}, name: {}",
            self.shared.id,
            self.shared.name
        );

        let ctx = TruncateContext {
            shared: &self.shared,
            wal: &self.wal,
            manifest: &self.manifest,
            sst_layer: &self.sst_layer,
        };

        self.writer.truncate(&ctx).await?;
        Ok(())
    }
}
