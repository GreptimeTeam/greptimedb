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

//! Region opener.

use std::sync::Arc;

use common_query::Output;
use common_telemetry::info;
use futures::StreamExt;
use object_store::util::join_dir;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadata;
use store_api::storage::{RegionId, SequenceNumber};

use crate::config::MitoConfig;
use crate::error::{RecvSnafu, RegionCorruptedSnafu, RegionNotFoundSnafu, Result};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::memtable::MemtableBuilderRef;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};
use crate::region::MitoRegion;
use crate::region_writer::RegionWriteCtx;
use crate::wal::Wal;

/// Builder to create a new [MitoRegion] or open an existing one.
pub(crate) struct RegionOpener<S> {
    region_id: RegionId,
    metadata: Option<RegionMetadata>,
    memtable_builder: MemtableBuilderRef,
    object_store: ObjectStore,
    region_dir: String,
    wal: Wal<S>,
}

impl<S: LogStore> RegionOpener<S> {
    /// Returns a new opener.
    pub(crate) fn new(
        region_id: RegionId,
        memtable_builder: MemtableBuilderRef,
        object_store: ObjectStore,
        wal: Wal<S>,
    ) -> RegionOpener<S> {
        RegionOpener {
            region_id,
            metadata: None,
            memtable_builder,
            object_store,
            region_dir: String::new(),
            wal,
        }
    }

    /// Sets metadata of the region to create.
    pub(crate) fn metadata(mut self, metadata: RegionMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Sets the region dir.
    pub(crate) fn region_dir(mut self, value: &str) -> Self {
        self.region_dir = value.to_string();
        self
    }

    /// Writes region manifest and creates a new region.
    ///
    /// # Panics
    /// Panics if metadata is not set.
    pub(crate) async fn create(self, config: &MitoConfig) -> Result<MitoRegion> {
        let region_id = self.region_id;
        let metadata = Arc::new(self.metadata.unwrap());

        // Create a manifest manager for this region.
        let options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(&self.region_dir),
            object_store: self.object_store,
            compress_type: config.manifest_compress_type,
            checkpoint_distance: config.manifest_checkpoint_distance,
        };
        // Writes regions to the manifest file.
        let manifest_manager = RegionManifestManager::new(metadata.clone(), options).await?;

        let mutable = self.memtable_builder.build(&metadata);

        let version = VersionBuilder::new(metadata, mutable).build();
        let version_control = Arc::new(VersionControl::new(version));

        Ok(MitoRegion {
            region_id,
            version_control,
            region_dir: self.region_dir,
            manifest_manager,
        })
    }

    /// Opens an existing region.
    ///
    /// Returns error if the region doesn't exist.
    pub(crate) async fn open(self, config: &MitoConfig) -> Result<MitoRegion> {
        let options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(&self.region_dir),
            object_store: self.object_store,
            compress_type: config.manifest_compress_type,
            checkpoint_distance: config.manifest_checkpoint_distance,
        };
        let manifest_manager =
            RegionManifestManager::open(options)
                .await?
                .context(RegionNotFoundSnafu {
                    region_id: self.region_id,
                })?;

        let manifest = manifest_manager.manifest().await;
        let metadata = manifest.metadata.clone();

        ensure!(
            metadata.region_id == self.region_id,
            RegionCorruptedSnafu {
                region_id: self.region_id,
                reason: format!("region id in metadata is {}", metadata.region_id),
            }
        );

        let region_id = metadata.region_id;
        let mutable = self.memtable_builder.build(&metadata);
        let version = VersionBuilder::new(metadata, mutable).build();
        let flushed_sequence = version.flushed_sequence;

        let version_control = Arc::new(VersionControl::new(version));
        replay_memtable(
            &self.wal,
            region_id,
            flushed_sequence,
            version_control.clone(),
        )
        .await?;

        let region = MitoRegion {
            region_id: self.region_id,
            version_control,
            region_dir: self.region_dir,
            manifest_manager,
        };
        Ok(region)
    }
}

/// Replays the mutations from WAL and inserts mutations to memtable of given region.
async fn replay_memtable<S: LogStore>(
    wal: &Wal<S>,
    region_id: RegionId,
    flushed_sequence: SequenceNumber,
    version_control: VersionControlRef,
) -> Result<()> {
    let mut region_writer_ctx = RegionWriteCtx::new(region_id, version_control);

    let mut notify_waiter = vec![];
    let mut wal_stream = wal.scan(region_id, flushed_sequence)?;
    while let Some(res) = wal_stream.next().await {
        let (_, entry) = res?;
        for mutation in entry.mutations.into_iter() {
            let (tx, rx) = tokio::sync::oneshot::channel();
            region_writer_ctx.push_mutation(mutation.op_type, mutation.rows, Some(tx));
            notify_waiter.push(rx);
        }
    }

    let rows_replayed: usize = futures::future::try_join_all(notify_waiter)
        .await
        .context(RecvSnafu)?
        .into_iter()
        .collect::<Result<Vec<Output>>>()?
        .into_iter()
        .map(|output| {
            let Output::AffectedRows(rows_inserted) = output else {
                unreachable!("Only expect affected rows during WAL replay.")
            };
            rows_inserted
        })
        .sum();

    info!(
        "Replay WAL for region: {}, rows recovered: {}",
        region_id, rows_replayed
    );
    Ok(())
}

/// Returns the directory to the manifest files.
fn new_manifest_dir(region_dir: &str) -> String {
    join_dir(region_dir, "manifest")
}
