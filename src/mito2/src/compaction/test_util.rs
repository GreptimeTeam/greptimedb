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

use common_time::Timestamp;

use crate::sst::file::{FileHandle, FileId, FileMeta, Level};
use crate::test_util::new_noop_file_purger;

/// Test util to create file handles.
pub fn new_file_handle(
    file_id: FileId,
    start_ts_millis: i64,
    end_ts_millis: i64,
    level: Level,
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
            file_size: 0,
            available_indexes: Default::default(),
            index_file_size: 0,
            num_rows: 0,
            num_row_groups: 0,
        },
        file_purger,
    )
}

pub(crate) fn new_file_handles(file_specs: &[(i64, i64, u64)]) -> Vec<FileHandle> {
    let file_purger = new_noop_file_purger();
    file_specs
        .iter()
        .map(|(start, end, size)| {
            FileHandle::new(
                FileMeta {
                    region_id: 0.into(),
                    file_id: FileId::random(),
                    time_range: (
                        Timestamp::new_millisecond(*start),
                        Timestamp::new_millisecond(*end),
                    ),
                    level: 0,
                    file_size: *size,
                    available_indexes: Default::default(),
                    index_file_size: 0,
                    num_rows: 0,
                    num_row_groups: 0,
                },
                file_purger.clone(),
            )
        })
        .collect()
}
