// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
use std::sync::Arc;

use async_trait::async_trait;
use common_telemetry::logging;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::manifest::{self, Manifest, ManifestVersion, MetaActionIterator};
use store_api::storage::{
    AlterRequest, OpenOptions, ReadContext, Region, RegionId, RegionMeta, SequenceNumber,
    WriteContext, WriteResponse,
};

use crate::error::{self, Error, Result};
use crate::flush::{FlushSchedulerRef, FlushStrategyRef};
use crate::manifest::action::{
    RawRegionMetadata, RegionChange, RegionMetaAction, RegionMetaActionList,
};
use crate::manifest::region::RegionManifest;
use crate::memtable::MemtableBuilderRef;
use crate::metadata::{RegionMetaImpl, RegionMetadata, RegionMetadataRef};
pub use crate::region::writer::{AlterContext, RegionWriter, RegionWriterRef, WriterContext};
use crate::schema::compat::CompatWrite;
use crate::snapshot::SnapshotImpl;
use crate::sst::AccessLayerRef;
use crate::version::{
    Version, VersionControl, VersionControlRef, VersionEdit, INIT_COMMITTED_SEQUENCE,
};
use crate::wal::Wal;
use crate::write_batch::WriteBatch;

/// [Region] implementation.
#[derive(Debug)]
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
        WriteBatch::new(self.in_memory_metadata().schema().clone())
    }

    async fn alter(&self, request: AlterRequest) -> Result<()> {
        self.inner.alter(request).await
    }
}

/// Storage related config for region.
///
/// Contains all necessary storage related components needed by the region, such as logstore,
/// manifest, memtable builder.
pub struct StoreConfig<S> {
    pub log_store: Arc<S>,
    pub sst_layer: AccessLayerRef,
    pub manifest: RegionManifest,
    pub memtable_builder: MemtableBuilderRef,
    pub flush_scheduler: FlushSchedulerRef,
    pub flush_strategy: FlushStrategyRef,
}

pub type RecoverdMetadata = (SequenceNumber, (ManifestVersion, RawRegionMetadata));
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
        let manifest_version = store_config
            .manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: metadata.as_ref().into(),
                    committed_sequence: INIT_COMMITTED_SEQUENCE,
                },
            )))
            .await?;

        let mutable_memtable = store_config
            .memtable_builder
            .build(metadata.schema().clone());
        let version = Version::with_manifest_version(metadata, manifest_version, mutable_memtable);
        let region = RegionImpl::new(version, store_config);

        Ok(region)
    }

    /// Create a new region without persisting manifest.
    fn new(version: Version, store_config: StoreConfig<S>) -> RegionImpl<S> {
        let metadata = version.metadata();
        let id = metadata.id();
        let name = metadata.name().to_string();
        let version_control = VersionControl::with_version(version);
        let wal = Wal::new(id, store_config.log_store);

        let inner = Arc::new(RegionInner {
            shared: Arc::new(SharedData {
                id,
                name,
                version_control: Arc::new(version_control),
            }),
            writer: Arc::new(RegionWriter::new(store_config.memtable_builder)),
            wal,
            flush_strategy: store_config.flush_strategy,
            flush_scheduler: store_config.flush_scheduler,
            sst_layer: store_config.sst_layer,
            manifest: store_config.manifest,
        });

        RegionImpl { inner }
    }

    /// Open an exsiting region and recover its data.
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
        let shared = Arc::new(SharedData {
            id: metadata.id(),
            name,
            version_control,
        });

        let writer = Arc::new(RegionWriter::new(store_config.memtable_builder));
        let writer_ctx = WriterContext {
            shared: &shared,
            flush_strategy: &store_config.flush_strategy,
            flush_scheduler: &store_config.flush_scheduler,
            sst_layer: &store_config.sst_layer,
            wal: &wal,
            writer: &writer,
            manifest: &store_config.manifest,
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
            sst_layer: store_config.sst_layer,
            manifest: store_config.manifest,
        });

        Ok(Some(RegionImpl { inner }))
    }

    /// Get ID of this region.
    pub fn id(&self) -> RegionId {
        self.inner.shared.id()
    }

    async fn recover_from_manifest(
        manifest: &RegionManifest,
        memtable_builder: &MemtableBuilderRef,
    ) -> Result<(Option<Version>, RecoveredMetadataMap)> {
        let (start, end) = Self::manifest_scan_range();
        let mut iter = manifest.scan(start, end).await?;

        let mut version = None;
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
                        ));
                        for (manifest_version, action) in actions.drain(..) {
                            version = Self::replay_edit(manifest_version, action, version);
                        }
                    }
                    (RegionMetaAction::Change(c), Some(v)) => {
                        recovered_metadata
                            .insert(c.committed_sequence, (manifest_version, c.metadata));
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

        if version.is_some() {
            // update manifest state after recovering
            let protocol = iter.last_protocol();
            manifest.update_state(last_manifest_version + 1, protocol.clone());
        }

        Ok((version, recovered_metadata))
    }

    fn manifest_scan_range() -> (ManifestVersion, ManifestVersion) {
        // TODO(dennis): use manifest version in WAL
        (manifest::MIN_VERSION, manifest::MAX_VERSION)
    }

    fn replay_edit(
        manifest_version: ManifestVersion,
        action: RegionMetaAction,
        version: Option<Version>,
    ) -> Option<Version> {
        if let RegionMetaAction::Edit(e) = action {
            let edit = VersionEdit {
                files_to_add: e.files_to_add,
                flushed_sequence: Some(e.flushed_sequence),
                manifest_version,
                max_memtable_id: None,
            };
            version.map(|mut v| {
                v.apply_edit(edit);
                v
            })
        } else {
            version
        }
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

    async fn wait_flush_done(&self) -> Result<()> {
        self.inner.writer.wait_flush_done().await
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
            sst_layer: &inner.sst_layer,
            wal: &inner.wal,
            writer: &inner.writer,
            manifest: &inner.manifest,
        };

        inner.writer.replay(recovered_metadata, writer_ctx).await
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
}

pub type SharedDataRef = Arc<SharedData>;

#[derive(Debug)]
struct RegionInner<S: LogStore> {
    shared: SharedDataRef,
    writer: RegionWriterRef,
    wal: Wal<S>,
    flush_strategy: FlushStrategyRef,
    flush_scheduler: FlushSchedulerRef,
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
            sst_layer: &self.sst_layer,
            wal: &self.wal,
            writer: &self.writer,
            manifest: &self.manifest,
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
}
