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

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common_base::Plugins;
use common_time::Timestamp;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::FileId;

use crate::cache::CacheManager;
use crate::compaction::compactor::{CompactionRegion, CompactionVersion};
use crate::config::MitoConfig;
use crate::region::options::RegionOptions;
use crate::sst::file::{FileHandle, FileMeta, Level};
use crate::sst::primary_key::PrimaryKeyRangeMapper;
use crate::sst::version::SstVersion;
use crate::test_util::memtable_util::metadata_for_test;
use crate::test_util::new_noop_file_purger;
use crate::test_util::scheduler_util::SchedulerEnv;

pub(crate) fn primary_key_metadata_for_test() -> RegionMetadataRef {
    let mut metadata = crate::test_util::memtable_util::metadata_with_primary_key(vec![0], false);
    metadata.region_id = 0.into();
    Arc::new(metadata)
}

pub(crate) fn primary_key_mapper_for_test() -> PrimaryKeyRangeMapper {
    PrimaryKeyRangeMapper::new(primary_key_metadata_for_test())
}

/// Encodes the single string tag used by compaction range fixtures.
pub(crate) fn pk_range(min: &[u8], max: &[u8]) -> Option<(Bytes, Bytes)> {
    let encode = |key| {
        crate::test_util::sst_util::new_primary_key(&[std::str::from_utf8(key).unwrap()]).into()
    };
    Some((encode(min), encode(max)))
}

/// Test util to create file handles.
pub fn new_file_handle(
    file_id: FileId,
    start_ts_millis: i64,
    end_ts_millis: i64,
    level: Level,
) -> FileHandle {
    new_file_handle_with_sequence(
        file_id,
        start_ts_millis,
        end_ts_millis,
        level,
        start_ts_millis as u64,
    )
}

/// Test util to create file handles.
pub fn new_file_handle_with_sequence(
    file_id: FileId,
    start_ts_millis: i64,
    end_ts_millis: i64,
    level: Level,
    sequence: u64,
) -> FileHandle {
    new_file_handle_with_size_and_sequence(
        file_id,
        start_ts_millis,
        end_ts_millis,
        level,
        sequence,
        0,
    )
}

/// Test util to create file handles with custom size.
pub fn new_file_handle_with_size_and_sequence(
    file_id: FileId,
    start_ts_millis: i64,
    end_ts_millis: i64,
    level: Level,
    sequence: u64,
    file_size: u64,
) -> FileHandle {
    let file_purger = new_noop_file_purger();
    FileHandle::new(
        FileMeta {
            region_id: 0.into(),
            file_id,
            time_range: (
                Timestamp::new_millisecond(start_ts_millis),
                Timestamp::new_millisecond(end_ts_millis),
            ),
            level,
            file_size,
            max_row_group_uncompressed_size: file_size,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 0,
            num_row_groups: 0,
            num_series: 0,
            sequence: NonZeroU64::new(sequence),
            partition_expr: None,
            ..Default::default()
        },
        file_purger,
    )
}

/// Test util to create file handles with custom size and primary-key range.
pub fn new_file_handle_with_size_sequence_and_primary_key_range(
    file_id: FileId,
    start_ts_millis: i64,
    end_ts_millis: i64,
    level: Level,
    sequence: u64,
    file_size: u64,
    primary_key_range: Option<(Bytes, Bytes)>,
) -> FileHandle {
    let file_purger = new_noop_file_purger();
    FileHandle::new_with_primary_key_range(
        FileMeta {
            region_id: 0.into(),
            file_id,
            time_range: (
                Timestamp::new_millisecond(start_ts_millis),
                Timestamp::new_millisecond(end_ts_millis),
            ),
            level,
            file_size,
            max_row_group_uncompressed_size: file_size,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0,
            num_rows: 0,
            num_row_groups: 0,
            num_series: 0,
            sequence: NonZeroU64::new(sequence),
            partition_expr: None,
            ..Default::default()
        },
        file_purger,
        primary_key_range,
    )
}

pub(crate) async fn compaction_region_with_ssts(
    files: impl IntoIterator<Item = FileMeta>,
    ttl: Duration,
) -> CompactionRegion {
    let env = SchedulerEnv::new().await;
    let mut metadata = (*metadata_for_test()).clone();
    // Match the table used by new_file_handle* and default FileMeta fixtures.
    metadata.region_id = 0.into();
    let metadata = Arc::new(metadata);
    let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
    let mut ssts = SstVersion::new(metadata.clone());
    ssts.add_files(
        Arc::new(crate::sst::file_purger::NoopFilePurger),
        files.into_iter(),
    );

    CompactionRegion {
        region_id: metadata.region_id,
        region_options: RegionOptions::default(),
        engine_config: Arc::new(MitoConfig::default()),
        region_metadata: metadata.clone(),
        cache_manager: Arc::new(CacheManager::default()),
        access_layer: env.access_layer,
        manifest_ctx,
        current_version: CompactionVersion {
            metadata,
            options: RegionOptions::default(),
            ssts: Arc::new(ssts),
            memtable_min_sequence: None,
            compaction_time_window: None,
        },
        file_purger: None,
        ttl: Some(ttl.into()),
        max_parallelism: 1,
        plugins: Plugins::new(),
    }
}
