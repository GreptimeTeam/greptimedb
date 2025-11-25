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

use common_time::Timestamp;
use store_api::storage::FileId;

use crate::sst::file::{FileHandle, FileMeta, Level};
use crate::test_util::new_noop_file_purger;

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
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_file_id: None,
            num_rows: 0,
            num_row_groups: 0,
            num_series: 0,
            sequence: NonZeroU64::new(sequence),
            partition_expr: None,
        },
        file_purger,
    )
}
