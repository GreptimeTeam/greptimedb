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

use std::any::Any;
use std::sync::Arc;

use common_datasource::compression::CompressionType;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use common_memory_manager;
use common_runtime::JoinError;
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datatypes::arrow::error::ArrowError;
use datatypes::prelude::ConcreteDataType;
use object_store::ErrorKind;
use partition::error::Error as PartitionError;
use prost::DecodeError;
use snafu::{Location, Snafu};
use store_api::ManifestVersion;
use store_api::logstore::provider::Provider;
use store_api::storage::{FileId, RegionId};
use tokio::time::error::Elapsed;

use crate::cache::file_cache::FileType;
use crate::region::RegionRoleState;
use crate::schedule::remote_job_scheduler::JobId;
use crate::worker::WorkerId;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Unexpected data type"))]
    DataTypeMismatch {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("External error, context: {}", context))]
    External {
        source: BoxedError,
        context: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode sparse primary key, reason: {}", reason))]
    EncodeSparsePrimaryKey {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("OpenDAL operator failed"))]
    OpenDal {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Fail to compress object by {}, path: {}", compress_type, path))]
    CompressObject {
        compress_type: CompressionType,
        path: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Fail to decompress object by {}, path: {}", compress_type, path))]
    DecompressObject {
        compress_type: CompressionType,
        path: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to ser/de json object"))]
    SerdeJson {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: serde_json::Error,
    },

    #[snafu(display("Failed to serialize column metadata"))]
    SerializeColumnMetadata {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize manifest, region_id: {}", region_id))]
    SerializeManifest {
        region_id: RegionId,
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid scan index, start: {}, end: {}", start, end))]
    InvalidScanIndex {
        start: ManifestVersion,
        end: ManifestVersion,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid UTF-8 content"))]
    Utf8 {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: std::str::Utf8Error,
    },

    #[snafu(display("Cannot find RegionMetadata"))]
    RegionMetadataNotFound {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to join handle"))]
    Join {
        #[snafu(source)]
        error: common_runtime::JoinError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Worker {} is stopped", id))]
    WorkerStopped {
        id: WorkerId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to recv result"))]
    Recv {
        #[snafu(source)]
        error: tokio::sync::oneshot::error::RecvError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid metadata, {}", reason))]
    InvalidMeta {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid region metadata"))]
    InvalidMetadata {
        source: store_api::metadata::MetadataError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create RecordBatch from vectors"))]
    NewRecordBatch {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: ArrowError,
    },

    #[snafu(display("Failed to read parquet file, path: {}", path))]
    ReadParquet {
        path: String,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to write parquet file"))]
    WriteParquet {
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} not found", region_id))]
    RegionNotFound {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Object store not found: {}", object_store))]
    ObjectStoreNotFound {
        object_store: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} is corrupted, reason: {}", region_id, reason))]
    RegionCorrupted {
        region_id: RegionId,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid request to region {}, reason: {}", region_id, reason))]
    InvalidRequest {
        region_id: RegionId,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Old manifest missing for region {}", region_id))]
    MissingOldManifest {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("New manifest missing for region {}", region_id))]
    MissingNewManifest {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Manifest missing for region {}", region_id))]
    MissingManifest {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("File consistency check failed for file {}: {}", file_id, reason))]
    InconsistentFile {
        file_id: FileId,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Files lost during remapping: old={}, new={}", old_count, new_count))]
    FilesLost {
        old_count: usize,
        new_count: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No old manifests provided (need at least one for template)"))]
    NoOldManifests {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to fetch manifests"))]
    FetchManifests {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Partition expression missing for region {}", region_id))]
    MissingPartitionExpr {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize partition expression: {}", source))]
    SerializePartitionExpr {
        #[snafu(source)]
        source: PartitionError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to convert ConcreteDataType to ColumnDataType, reason: {}",
        reason
    ))]
    ConvertColumnDataType {
        reason: String,
        source: api::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    /// An error type to indicate that schema is changed and we need
    /// to fill default values again.
    #[snafu(display("Need to fill default value for region {}", region_id))]
    FillDefault {
        region_id: RegionId,
        // The error is for internal use so we don't need a location.
    },

    #[snafu(display(
        "Failed to create default value for column {} of region {}",
        column,
        region_id
    ))]
    CreateDefault {
        region_id: RegionId,
        column: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build entry, region_id: {}", region_id))]
    BuildEntry {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to write WAL"))]
    WriteWal {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to read WAL, provider: {}", provider))]
    ReadWal {
        provider: Provider,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to decode WAL entry, region_id: {}", region_id))]
    DecodeWal {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: DecodeError,
    },

    #[snafu(display("Failed to delete WAL, region_id: {}", region_id))]
    DeleteWal {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    // Shared error for each writer in the write group.
    #[snafu(display("Failed to write region"))]
    WriteGroup { source: Arc<Error> },

    #[snafu(display("Invalid parquet SST file {}, reason: {}", file, reason))]
    InvalidParquet {
        file: String,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid batch, {}", reason))]
    InvalidBatch {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid arrow record batch, {}", reason))]
    InvalidRecordBatch {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid wal read request, {}", reason))]
    InvalidWalReadRequest {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert array to vector"))]
    ConvertVector {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to compute arrow arrays"))]
    ComputeArrow {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: datatypes::arrow::error::ArrowError,
    },

    #[snafu(display("Failed to compute vector"))]
    ComputeVector {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Primary key length mismatch, expect: {}, actual: {}", expect, actual))]
    PrimaryKeyLengthMismatch {
        expect: usize,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid sender",))]
    InvalidSender {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid scheduler state"))]
    InvalidSchedulerState {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to stop scheduler"))]
    StopScheduler {
        #[snafu(source)]
        error: JoinError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to delete SST file, file id: {}", file_id))]
    DeleteSst {
        file_id: FileId,
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to delete index file, file id: {}", file_id))]
    DeleteIndex {
        file_id: FileId,
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to flush region {}", region_id))]
    FlushRegion {
        region_id: RegionId,
        source: Arc<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} is dropped", region_id))]
    RegionDropped {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} is closed", region_id))]
    RegionClosed {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} is truncated", region_id))]
    RegionTruncated {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Engine write buffer is full, rejecting write requests of region {}",
        region_id,
    ))]
    RejectWrite {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to compact region {}", region_id))]
    CompactRegion {
        region_id: RegionId,
        source: Arc<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to compat readers for region {}, reason: {}",
        region_id,
        reason,
    ))]
    CompatReader {
        region_id: RegionId,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalue region req"))]
    InvalidRegionRequest {
        source: store_api::metadata::MetadataError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Region {} is in {:?} state, which does not permit manifest updates.",
        region_id,
        state
    ))]
    UpdateManifest {
        region_id: RegionId,
        state: RegionRoleState,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} is in {:?} state, expect: {:?}", region_id, state, expect))]
    RegionState {
        region_id: RegionId,
        state: RegionRoleState,
        expect: RegionRoleState,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Region {} is in {:?} state, expect: Leader or Leader(Downgrading)",
        region_id,
        state
    ))]
    FlushableRegionState {
        region_id: RegionId,
        state: RegionRoleState,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid options"))]
    JsonOptions {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Empty region directory, region_id: {}, region_dir: {}",
        region_id,
        region_dir,
    ))]
    EmptyRegionDir {
        region_id: RegionId,
        region_dir: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty manifest directory, manifest_dir: {}", manifest_dir,))]
    EmptyManifestDir {
        manifest_dir: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read arrow record batch from parquet file {}", path))]
    ArrowReader {
        path: String,
        #[snafu(source)]
        error: ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid file metadata"))]
    ConvertMetaData {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
    },

    #[snafu(display("Column not found, column: {column}"))]
    ColumnNotFound {
        column: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build index applier"))]
    BuildIndexApplier {
        source: index::inverted_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build index asynchronously in region {}", region_id))]
    BuildIndexAsync {
        region_id: RegionId,
        source: Arc<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert value"))]
    ConvertValue {
        source: datatypes::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to apply inverted index"))]
    ApplyInvertedIndex {
        source: index::inverted_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to apply bloom filter index"))]
    ApplyBloomFilterIndex {
        source: index::bloom_filter::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to push index value"))]
    PushIndexValue {
        source: index::inverted_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to write index completely"))]
    IndexFinish {
        source: index::inverted_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Operate on aborted index"))]
    OperateAbortedIndex {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read puffin blob"))]
    PuffinReadBlob {
        source: puffin::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to add blob to puffin file"))]
    PuffinAddBlob {
        source: puffin::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to clean dir {dir}"))]
    CleanDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid config, {reason}"))]
    InvalidConfig {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Stale log entry found during replay, region: {}, flushed: {}, replayed: {}",
        region_id,
        flushed_entry_id,
        unexpected_entry_id
    ))]
    StaleLogEntry {
        region_id: RegionId,
        flushed_entry_id: u64,
        unexpected_entry_id: u64,
    },

    #[snafu(display(
        "Failed to download file, region_id: {}, file_id: {}, file_type: {:?}",
        region_id,
        file_id,
        file_type,
    ))]
    Download {
        region_id: RegionId,
        file_id: FileId,
        file_type: FileType,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to upload file, region_id: {}, file_id: {}, file_type: {:?}",
        region_id,
        file_id,
        file_type,
    ))]
    Upload {
        region_id: RegionId,
        file_id: FileId,
        file_type: FileType,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create directory {}", dir))]
    CreateDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Record batch error"))]
    RecordBatch {
        source: common_recordbatch::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("BiErrors, first: {first}, second: {second}"))]
    BiErrors {
        first: Box<Error>,
        second: Box<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Encode null value"))]
    IndexEncodeNull {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to encode memtable to Parquet bytes"))]
    EncodeMemtable {
        #[snafu(source)]
        error: parquet::errors::ParquetError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Partition {} out of range, {} in total", given, all))]
    PartitionOutOfRange {
        given: usize,
        all: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to iter data part"))]
    ReadDataPart {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: parquet::errors::ParquetError,
    },

    #[snafu(display("Failed to read row group in memtable"))]
    DecodeArrowRowGroup {
        #[snafu(source)]
        error: ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid region options, {}", reason))]
    InvalidRegionOptions {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("checksum mismatch (actual: {}, expected: {})", actual, expected))]
    ChecksumMismatch { actual: u32, expected: u32 },

    #[snafu(display(
        "No checkpoint found, region: {}, last_version: {}",
        region_id,
        last_version
    ))]
    NoCheckpoint {
        region_id: RegionId,
        last_version: ManifestVersion,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "No manifests found in range: [{}..{}), region: {}, last_version: {}",
        start_version,
        end_version,
        region_id,
        last_version
    ))]
    NoManifests {
        region_id: RegionId,
        start_version: ManifestVersion,
        end_version: ManifestVersion,
        last_version: ManifestVersion,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to install manifest to {}, region: {}, available manifest version: {}, last version: {}",
        target_version,
        region_id,
        available_version,
        last_version
    ))]
    InstallManifestTo {
        region_id: RegionId,
        target_version: ManifestVersion,
        available_version: ManifestVersion,
        #[snafu(implicit)]
        location: Location,
        last_version: ManifestVersion,
    },

    #[snafu(display("Region {} is stopped", region_id))]
    RegionStopped {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Time range predicate overflows, timestamp: {:?}, target unit: {}",
        timestamp,
        unit
    ))]
    TimeRangePredicateOverflow {
        timestamp: Timestamp,
        unit: TimeUnit,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to open region"))]
    OpenRegion {
        #[snafu(implicit)]
        location: Location,
        source: Arc<Error>,
    },

    #[snafu(display("Failed to parse job id"))]
    ParseJobId {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: uuid::Error,
    },

    #[snafu(display("Operation is not supported: {}", err_msg))]
    UnsupportedOperation {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to remotely compact region {} by job {:?} due to {}",
        region_id,
        job_id,
        reason
    ))]
    RemoteCompaction {
        region_id: RegionId,
        job_id: Option<JobId>,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to initialize puffin stager"))]
    PuffinInitStager {
        source: puffin::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to purge puffin stager"))]
    PuffinPurgeStager {
        source: puffin::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build puffin reader"))]
    PuffinBuildReader {
        source: puffin::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to retrieve index options from column metadata"))]
    IndexOptions {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
        column_name: String,
    },

    #[snafu(display("Failed to create fulltext index creator"))]
    CreateFulltextCreator {
        source: index::fulltext_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to cast vector of {from} to {to}"))]
    CastVector {
        #[snafu(implicit)]
        location: Location,
        from: ConcreteDataType,
        to: ConcreteDataType,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to push text to fulltext index"))]
    FulltextPushText {
        source: index::fulltext_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to finalize fulltext index creator"))]
    FulltextFinish {
        source: index::fulltext_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to apply fulltext index"))]
    ApplyFulltextIndex {
        source: index::fulltext_index::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("SST file {} does not contain valid stats info", file_path))]
    StatsNotPresent {
        file_path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode stats of file {}", file_path))]
    DecodeStats {
        file_path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Region {} is busy", region_id))]
    RegionBusy {
        region_id: RegionId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to get schema metadata"))]
    GetSchemaMetadata {
        source: common_meta::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Timeout"))]
    Timeout {
        #[snafu(source)]
        error: Elapsed,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read file metadata"))]
    Metadata {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to push value to bloom filter"))]
    PushBloomFilterValue {
        source: index::bloom_filter::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to finish bloom filter"))]
    BloomFilterFinish {
        source: index::bloom_filter::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Manual compaction is override by following operations."))]
    ManualCompactionOverride {},

    #[snafu(display(
        "Compaction memory limit exceeded for region {}: required {} bytes, limit {} bytes (policy: {})",
        region_id,
        required_bytes,
        limit_bytes,
        policy
    ))]
    CompactionMemoryExhausted {
        region_id: RegionId,
        required_bytes: u64,
        limit_bytes: u64,
        policy: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to acquire memory: {source}"))]
    MemoryAcquireFailed {
        #[snafu(source)]
        source: common_memory_manager::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Incompatible WAL provider change. This is typically caused by changing WAL provider in database config file without completely cleaning existing files. Global provider: {}, region provider: {}",
        global,
        region
    ))]
    IncompatibleWalProviderChange { global: String, region: String },

    #[snafu(display("Expected mito manifest info"))]
    MitoManifestInfo {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to scan series"))]
    ScanSeries {
        #[snafu(implicit)]
        location: Location,
        source: Arc<Error>,
    },

    #[snafu(display("Partition {} scan multiple times", partition))]
    ScanMultiTimes {
        partition: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid partition expression: {}", expr))]
    InvalidPartitionExpr {
        expr: String,
        #[snafu(implicit)]
        location: Location,
        source: partition::error::Error,
    },

    #[snafu(display("Failed to decode bulk wal entry"))]
    ConvertBulkWalEntry {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::Error,
    },

    #[snafu(display("Failed to encode"))]
    Encode {
        #[snafu(implicit)]
        location: Location,
        source: mito_codec::error::Error,
    },

    #[snafu(display("Failed to decode"))]
    Decode {
        #[snafu(implicit)]
        location: Location,
        source: mito_codec::error::Error,
    },

    #[snafu(display("Unexpected: {reason}"))]
    Unexpected {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[cfg(feature = "enterprise")]
    #[snafu(display("Failed to scan external range"))]
    ScanExternalRange {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Inconsistent timestamp column length, expect: {}, actual: {}",
        expected,
        actual
    ))]
    InconsistentTimestampLength {
        expected: usize,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Too many files to read concurrently: {}, max allowed: {}",
        actual,
        max
    ))]
    TooManyFilesToRead {
        actual: usize,
        max: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Duration out of range: {input:?}"))]
    DurationOutOfRange {
        input: std::time::Duration,
        #[snafu(source)]
        error: chrono::OutOfRangeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("GC job permit exhausted"))]
    TooManyGcJobs {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Staging partition expr mismatch, manifest: {:?}, request: {}",
        manifest_expr,
        request_expr
    ))]
    StagingPartitionExprMismatch {
        manifest_expr: Option<String>,
        request_expr: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Returns true if we need to fill default value for a region.
    pub(crate) fn is_fill_default(&self) -> bool {
        matches!(self, Error::FillDefault { .. })
    }

    /// Returns true if the file is not found on the object store.
    pub(crate) fn is_object_not_found(&self) -> bool {
        match self {
            Error::OpenDal { error, .. } => error.kind() == ErrorKind::NotFound,
            _ => false,
        }
    }
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            DataTypeMismatch { source, .. } => source.status_code(),
            OpenDal { .. } | ReadParquet { .. } => StatusCode::StorageUnavailable,
            WriteWal { source, .. } | ReadWal { source, .. } | DeleteWal { source, .. } => {
                source.status_code()
            }
            CompressObject { .. }
            | DecompressObject { .. }
            | SerdeJson { .. }
            | Utf8 { .. }
            | NewRecordBatch { .. }
            | RegionCorrupted { .. }
            | InconsistentFile { .. }
            | CreateDefault { .. }
            | InvalidParquet { .. }
            | OperateAbortedIndex { .. }
            | IndexEncodeNull { .. }
            | NoCheckpoint { .. }
            | NoManifests { .. }
            | FilesLost { .. }
            | InstallManifestTo { .. }
            | Unexpected { .. }
            | SerializeColumnMetadata { .. }
            | SerializeManifest { .. }
            | StagingPartitionExprMismatch { .. } => StatusCode::Unexpected,

            RegionNotFound { .. } => StatusCode::RegionNotFound,
            ObjectStoreNotFound { .. }
            | InvalidScanIndex { .. }
            | InvalidMeta { .. }
            | InvalidRequest { .. }
            | FillDefault { .. }
            | ConvertColumnDataType { .. }
            | ColumnNotFound { .. }
            | InvalidMetadata { .. }
            | InvalidRegionOptions { .. }
            | InvalidWalReadRequest { .. }
            | PartitionOutOfRange { .. }
            | ParseJobId { .. }
            | DurationOutOfRange { .. }
            | MissingOldManifest { .. }
            | MissingNewManifest { .. }
            | MissingManifest { .. }
            | NoOldManifests { .. }
            | MissingPartitionExpr { .. }
            | SerializePartitionExpr { .. } => StatusCode::InvalidArguments,

            RegionMetadataNotFound { .. }
            | Join { .. }
            | WorkerStopped { .. }
            | Recv { .. }
            | ConvertMetaData { .. }
            | DecodeWal { .. }
            | ComputeArrow { .. }
            | BiErrors { .. }
            | StopScheduler { .. }
            | ComputeVector { .. }
            | EncodeMemtable { .. }
            | CreateDir { .. }
            | ReadDataPart { .. }
            | BuildEntry { .. }
            | Metadata { .. }
            | MitoManifestInfo { .. } => StatusCode::Internal,

            FetchManifests { source, .. } => source.status_code(),

            OpenRegion { source, .. } => source.status_code(),

            WriteParquet { .. } => StatusCode::StorageUnavailable,
            WriteGroup { source, .. } => source.status_code(),
            EncodeSparsePrimaryKey { .. } => StatusCode::Unexpected,
            InvalidBatch { .. } => StatusCode::InvalidArguments,
            InvalidRecordBatch { .. } => StatusCode::InvalidArguments,
            ConvertVector { source, .. } => source.status_code(),

            PrimaryKeyLengthMismatch { .. } => StatusCode::InvalidArguments,
            InvalidSender { .. } => StatusCode::InvalidArguments,
            InvalidSchedulerState { .. } => StatusCode::InvalidArguments,
            DeleteSst { .. } | DeleteIndex { .. } => StatusCode::StorageUnavailable,
            FlushRegion { source, .. } | BuildIndexAsync { source, .. } => source.status_code(),
            RegionDropped { .. } => StatusCode::Cancelled,
            RegionClosed { .. } => StatusCode::Cancelled,
            RegionTruncated { .. } => StatusCode::Cancelled,
            RejectWrite { .. } => StatusCode::StorageUnavailable,
            CompactRegion { source, .. } => source.status_code(),
            CompatReader { .. } => StatusCode::Unexpected,
            InvalidRegionRequest { source, .. } => source.status_code(),
            RegionState { .. } | UpdateManifest { .. } => StatusCode::RegionNotReady,
            FlushableRegionState { .. } => StatusCode::RegionNotReady,
            JsonOptions { .. } => StatusCode::InvalidArguments,
            EmptyRegionDir { .. } | EmptyManifestDir { .. } => StatusCode::RegionNotFound,
            ArrowReader { .. } => StatusCode::StorageUnavailable,
            ConvertValue { source, .. } => source.status_code(),
            ApplyBloomFilterIndex { source, .. } => source.status_code(),
            InvalidPartitionExpr { source, .. } => source.status_code(),
            BuildIndexApplier { source, .. }
            | PushIndexValue { source, .. }
            | ApplyInvertedIndex { source, .. }
            | IndexFinish { source, .. } => source.status_code(),
            PuffinReadBlob { source, .. }
            | PuffinAddBlob { source, .. }
            | PuffinInitStager { source, .. }
            | PuffinBuildReader { source, .. }
            | PuffinPurgeStager { source, .. } => source.status_code(),
            CleanDir { .. } => StatusCode::Unexpected,
            InvalidConfig { .. } => StatusCode::InvalidArguments,
            StaleLogEntry { .. } => StatusCode::Unexpected,

            External { source, .. } => source.status_code(),

            RecordBatch { source, .. } => source.status_code(),

            Download { .. } | Upload { .. } => StatusCode::StorageUnavailable,
            ChecksumMismatch { .. } => StatusCode::Unexpected,
            RegionStopped { .. } => StatusCode::RegionNotReady,
            TimeRangePredicateOverflow { .. } => StatusCode::InvalidArguments,
            UnsupportedOperation { .. } => StatusCode::Unsupported,
            RemoteCompaction { .. } => StatusCode::Unexpected,

            IndexOptions { source, .. } => source.status_code(),
            CreateFulltextCreator { source, .. } => source.status_code(),
            CastVector { source, .. } => source.status_code(),
            FulltextPushText { source, .. }
            | FulltextFinish { source, .. }
            | ApplyFulltextIndex { source, .. } => source.status_code(),
            DecodeStats { .. } | StatsNotPresent { .. } => StatusCode::Internal,
            RegionBusy { .. } => StatusCode::RegionBusy,
            GetSchemaMetadata { source, .. } => source.status_code(),
            Timeout { .. } => StatusCode::Cancelled,

            DecodeArrowRowGroup { .. } => StatusCode::Internal,

            PushBloomFilterValue { source, .. } | BloomFilterFinish { source, .. } => {
                source.status_code()
            }

            ManualCompactionOverride {} => StatusCode::Cancelled,

            CompactionMemoryExhausted { .. } => StatusCode::RuntimeResourcesExhausted,

            MemoryAcquireFailed { source, .. } => source.status_code(),

            IncompatibleWalProviderChange { .. } => StatusCode::InvalidArguments,

            ScanSeries { source, .. } => source.status_code(),

            ScanMultiTimes { .. } => StatusCode::InvalidArguments,
            ConvertBulkWalEntry { source, .. } => source.status_code(),

            Encode { source, .. } | Decode { source, .. } => source.status_code(),

            #[cfg(feature = "enterprise")]
            ScanExternalRange { source, .. } => source.status_code(),

            InconsistentTimestampLength { .. } => StatusCode::InvalidArguments,

            TooManyFilesToRead { .. } | TooManyGcJobs { .. } => StatusCode::RateLimited,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
