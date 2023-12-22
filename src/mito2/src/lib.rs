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

//! # Mito
//!
//! Mito is the a region engine to store timeseries data.

#![feature(let_chains)]
#![feature(assert_matches)]

#[cfg(any(test, feature = "test"))]
#[cfg_attr(feature = "test", allow(unused))]
pub mod test_util;

mod access_layer;
mod cache;
mod compaction;
pub mod config;
pub mod engine;
pub mod error;
pub mod flush;
pub mod manifest;
pub mod memtable;
mod metrics;
pub mod read;
pub mod region;
mod region_write_ctx;
pub mod request;
mod row_converter;
pub(crate) mod schedule;
pub mod sst;
pub mod wal;
mod worker;

#[cfg_attr(doc, aquamarine::aquamarine)]
/// # Mito developer document
///
/// ## Engine
///
/// Engine hierarchy:
///
/// ```mermaid
/// classDiagram
/// class MitoEngine {
///     -WorkerGroup workers
/// }
/// class MitoRegion {
///     +VersionControlRef version_control
///     -RegionId region_id
///     -String manifest_dir
///     -AtomicI64 last_flush_millis
///     +region_id() RegionId
///     +scan() ChunkReaderImpl
/// }
/// class RegionMap {
///     -HashMap&lt;RegionId, MitoRegionRef&gt; regions
/// }
/// class ChunkReaderImpl
///
/// class WorkerGroup {
///     -Vec~RegionWorker~ workers
/// }
/// class RegionWorker {
///     -RegionMap regions
///     -Sender sender
///     -JoinHandle handle
/// }
/// class RegionWorkerThread~LogStore~ {
///     -RegionMap regions
///     -Receiver receiver
///     -Wal~LogStore~ wal
///     -ObjectStore object_store
///     -MemtableBuilderRef memtable_builder
///     -FlushSchedulerRef~LogStore~ flush_scheduler
///     -FlushStrategy flush_strategy
///     -CompactionSchedulerRef~LogStore~ compaction_scheduler
///     -FilePurgerRef file_purger
/// }
/// class Wal~LogStore~ {
///     -LogStore log_store
/// }
/// class MitoConfig
///
/// MitoEngine o-- MitoConfig
/// MitoEngine o-- MitoRegion
/// MitoEngine o-- WorkerGroup
/// MitoRegion o-- VersionControl
/// MitoRegion -- ChunkReaderImpl
/// WorkerGroup o-- RegionWorker
/// RegionWorker o-- RegionMap
/// RegionWorker -- RegionWorkerThread~LogStore~
/// RegionWorkerThread~LogStore~ o-- RegionMap
/// RegionWorkerThread~LogStore~ o-- Wal~LogStore~
/// ```
///
/// ## Metadata
///
/// Metadata hierarchy:
///
/// ```mermaid
/// classDiagram
/// class VersionControl {
///     -CowCell~Version~ version
///     -AtomicU64 committed_sequence
/// }
/// class Version {
///     -RegionMetadataRef metadata
///     -MemtableVersionRef memtables
///     -SstVersionRef ssts
///     -SequenceNumber flushed_sequence
///     -ManifestVersion manifest_version
/// }
/// class MemtableVersion {
///     -MemtableRef mutable
///     -Vec~MemtableRef~ immutables
///     +mutable_memtable() MemtableRef
///     +immutable_memtables() &[MemtableRef]
///     +freeze_mutable(MemtableRef new_mutable) MemtableVersion
/// }
/// class SstVersion {
///     -LevelMetaVec levels
///     -AccessLayerRef sst_layer
///     -FilePurgerRef file_purger
///     -Option~i64~ compaction_time_window
/// }
/// class LevelMeta {
///     -Level level
///     -HashMap&lt;FileId, FileHandle&gt; files
/// }
/// class FileHandle {
///     -FileMeta meta
///     -bool compacting
///     -AtomicBool deleted
///     -AccessLayerRef sst_layer
///     -FilePurgerRef file_purger
/// }
/// class FileMeta {
///     +RegionId region_id
///     +FileId file_id
///     +Option&lt;Timestamp, Timestamp&gt; time_range
///     +Level level
///     +u64 file_size
/// }
/// VersionControl o-- Version
/// Version o-- RegionMetadata
/// Version o-- MemtableVersion
/// Version o-- SstVersion
/// SstVersion o-- LevelMeta
/// LevelMeta o-- FileHandle
/// FileHandle o-- FileMeta
/// class RegionMetadata
/// ```
///
/// ## Region workers
///
/// The engine handles DMLs and DDLs in dedicated [workers](crate::worker::WorkerGroup).
///
/// ## Region manifest
///
/// The [RegionManifestManager](crate::manifest::manager::RegionManifestManager) manages metadata of the engine.
///
mod docs {}
