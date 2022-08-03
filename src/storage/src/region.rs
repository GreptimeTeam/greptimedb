#[cfg(test)]
mod tests;
mod writer;

use std::sync::Arc;

use async_trait::async_trait;
use datatypes::schema::SchemaRef;
use snafu::{ensure, OptionExt};
use store_api::logstore::LogStore;
use store_api::manifest::{
    self, action::ProtocolAction, Manifest, ManifestVersion, MetaActionIterator,
};
use store_api::storage::{
    OpenOptions, ReadContext, Region, RegionId, RegionMeta, WriteContext, WriteResponse,
};

use crate::error::{self, Error, Result};
use crate::flush::{FlushSchedulerRef, FlushStrategyRef};
use crate::manifest::{
    action::{RegionChange, RegionMetaAction, RegionMetaActionList},
    region::RegionManifest,
};
use crate::memtable::MemtableBuilderRef;
use crate::metadata::{RegionMetaImpl, RegionMetadata};
pub use crate::region::writer::{RegionWriter, RegionWriterRef, WriterContext};
use crate::snapshot::SnapshotImpl;
use crate::sst::AccessLayerRef;
use crate::version::VersionEdit;
use crate::version::{Version, VersionControl, VersionControlRef};
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

    fn name(&self) -> &str {
        &self.inner.shared.name
    }

    fn in_memory_metadata(&self) -> RegionMetaImpl {
        self.inner.in_memory_metadata()
    }

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        self.inner.write(ctx, request).await
    }

    fn snapshot(&self, _ctx: &ReadContext) -> Result<SnapshotImpl> {
        Ok(self.inner.create_snapshot())
    }

    fn write_request(&self, schema: SchemaRef) -> Self::WriteRequest {
        WriteBatch::new(schema)
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

impl<S: LogStore> RegionImpl<S> {
    /// Create a new region and also persist the region metadata to manifest.
    ///
    /// The caller should avoid calling this method simultaneously.
    pub async fn create(
        id: RegionId,
        name: String,
        metadata: RegionMetadata,
        store_config: StoreConfig<S>,
    ) -> Result<RegionImpl<S>> {
        let metadata = Arc::new(metadata);
        // Try to persist region data to manifest, ensure the new region could be recovered from
        // the manifest.
        let manifest_version = store_config
            .manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Protocol(ProtocolAction::new()),
                RegionMetaAction::Change(RegionChange {
                    metadata: metadata.clone(),
                }),
            ]))
            .await?;

        let version = Version::with_manifest_version(metadata, manifest_version);
        let region = RegionImpl::new(id, name, version, store_config);

        Ok(region)
    }

    /// Create a new region without persisting manifest.
    fn new(
        id: RegionId,
        name: String,
        version: Version,
        store_config: StoreConfig<S>,
    ) -> RegionImpl<S> {
        let version_control = VersionControl::with_version(version);
        let wal = Wal::new(name.clone(), store_config.log_store);

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
    ) -> Result<RegionImpl<S>> {
        // Load version meta data from manifest.
        let version = Self::recover_from_manifest(&name, &store_config.manifest).await?;
        let metadata = version.metadata().clone();
        let version_control = Arc::new(VersionControl::with_version(version));
        let wal = Wal::new(name.clone(), store_config.log_store);
        let shared = Arc::new(SharedData {
            id: metadata.id,
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
        writer.replay(writer_ctx).await?;

        let inner = Arc::new(RegionInner {
            shared,
            writer,
            wal,
            flush_strategy: store_config.flush_strategy,
            flush_scheduler: store_config.flush_scheduler,
            sst_layer: store_config.sst_layer,
            manifest: store_config.manifest,
        });

        Ok(RegionImpl { inner })
    }

    async fn recover_from_manifest(
        region_name: &str,
        manifest: &RegionManifest,
    ) -> Result<Version> {
        let (start, end) = Self::manifest_scan_range();
        let mut iter = manifest.scan(start, end).await?;

        let mut version = None;
        let mut actions = Vec::new();
        let mut last_manifest_version = manifest::MIN_VERSION;
        while let Some((manifest_version, action_list)) = iter.next_action().await? {
            last_manifest_version = manifest_version;

            for action in action_list.actions {
                match (action, version) {
                    (RegionMetaAction::Change(c), None) => {
                        version = Some(Version::with_manifest_version(
                            c.metadata,
                            last_manifest_version,
                        ));
                        for (manifest_version, action) in actions.drain(..) {
                            version = Self::replay_edit(manifest_version, action, version);
                        }
                    }
                    (RegionMetaAction::Change(_), Some(_)) => {
                        unimplemented!("alter schema is not implemented")
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

        version.context(error::VersionNotFoundSnafu { region_name })
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

    async fn wait_flush_done(&self) -> Result<()> {
        self.inner.writer.wait_flush_done().await
    }
}

/// Shared data of region.
#[derive(Debug)]
pub struct SharedData {
    pub id: RegionId,
    pub name: String,
    // TODO(yingwen): Maybe no need to use Arc for version control.
    pub version_control: VersionControlRef,
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
        &*self.shared.version_control
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

    async fn write(&self, ctx: &WriteContext, request: WriteBatch) -> Result<WriteResponse> {
        let metadata = self.in_memory_metadata();
        let schema = metadata.schema();
        // Only compare column schemas.
        ensure!(
            schema.column_schemas() == request.schema().column_schemas(),
            error::InvalidInputSchemaSnafu {
                region: &self.shared.name
            }
        );

        let writer_ctx = WriterContext {
            shared: &self.shared,
            flush_strategy: &self.flush_strategy,
            flush_scheduler: &self.flush_scheduler,
            sst_layer: &self.sst_layer,
            wal: &self.wal,
            writer: &self.writer,
            manifest: &self.manifest,
        };
        // Now altering schema is not allowed, so it is safe to validate schema outside of the lock.
        self.writer.write(ctx, request, writer_ctx).await
    }
}
