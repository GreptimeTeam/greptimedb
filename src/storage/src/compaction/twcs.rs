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

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;

use common_telemetry::tracing::warn;
use common_telemetry::{debug, info};
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use store_api::logstore::LogStore;

use crate::compaction::picker::get_expired_ssts;
use crate::compaction::task::CompactionOutput;
use crate::compaction::{CompactionRequestImpl, CompactionTaskImpl, Picker};
use crate::sst::{FileHandle, LevelMeta};

pub struct TwcsPicker<S> {
    max_files_in_active_window: usize,
    max_files_in_non_active_window: usize,
    _phantom_data: PhantomData<S>,
}

impl<S> Debug for TwcsPicker<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwcsPicker")
            .field(
                "max_files_in_active_window",
                &self.max_files_in_active_window,
            )
            .field(
                "max_files_in_non_active_window",
                &self.max_files_in_non_active_window,
            )
            .finish()
    }
}

impl<S> Default for TwcsPicker<S> {
    fn default() -> Self {
        Self {
            max_files_in_active_window: 4,
            max_files_in_non_active_window: 1,
            _phantom_data: Default::default(),
        }
    }
}

impl<S> TwcsPicker<S> {
    /// Builds compaction output from files.
    /// For active writing window, we allow for at most `max_files_in_active_window` files to alleviate
    /// fragmentation. For other windows, we allow at most 1 file at each window.
    fn build_output(
        &self,
        time_windows: &BTreeMap<i64, Vec<FileHandle>>,
        active_window: Option<i64>,
        window_size: i64,
    ) -> Vec<CompactionOutput> {
        let mut output = vec![];
        for (window, files) in time_windows {
            if let Some(active_window) = active_window && *window == active_window {
                if files.len() > self.max_files_in_active_window {
                    output.push(CompactionOutput {
                        output_level: 1, // we only have two levels and always compact to l1 
                        time_window_bound: *window,
                        time_window_sec: window_size,
                        inputs: files.clone(),
                        // Strict window is not needed since we always compact many files to one 
                        // single file in TWCS.
                        strict_window: false,
                    });
                } else {
                    debug!("Active window not present or no enough files in active window {:?}", active_window);
                }
            } else {
                // not active writing window
                if files.len() > self.max_files_in_non_active_window {
                    output.push(CompactionOutput {
                        output_level: 1,
                        time_window_bound: *window,
                        time_window_sec: window_size,
                        inputs: files.clone(),
                        strict_window: false,
                    });
                }
            }
        }
        output
    }
}

impl<S: LogStore> Picker for TwcsPicker<S> {
    type Request = CompactionRequestImpl<S>;
    type Task = CompactionTaskImpl<S>;

    fn pick(&self, req: &Self::Request) -> crate::error::Result<Option<Self::Task>> {
        let levels = req.levels();
        let expired_ssts = get_expired_ssts(levels.levels(), req.ttl, Timestamp::current_millis())?;
        if !expired_ssts.is_empty() {
            info!(
                "Expired SSTs in region {}: {:?}",
                req.region_id, expired_ssts
            );
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.mark_compacting(true));
        }

        // infer if not present
        let time_window_size = req.compaction_time_window.unwrap();

        // Find active window from files in level 0.
        let active_window =
            find_latest_window_in_seconds(levels.level(0).files(), time_window_size);

        let windows = assign_to_windows(
            levels.levels().iter().flat_map(LevelMeta::files),
            time_window_size,
        );

        let outputs = self.build_output(&windows, active_window, time_window_size);
        let task = CompactionTaskImpl {
            schema: req.schema(),
            sst_layer: req.sst_layer.clone(),
            outputs,
            writer: req.writer.clone(),
            shared_data: req.shared.clone(),
            wal: req.wal.clone(),
            manifest: req.manifest.clone(),
            expired_ssts,
            sst_write_buffer_size: req.sst_write_buffer_size,
            compaction_time_window: None,
        };
        Ok(Some(task))
    }
}

/// Assigns files to windows with predefined window size (in seconds) by their max timestamps.
fn assign_to_windows<'a>(
    files: impl Iterator<Item = &'a FileHandle>,
    time_window_size: i64,
) -> BTreeMap<i64, Vec<FileHandle>> {
    let mut windows: BTreeMap<i64, Vec<FileHandle>> = BTreeMap::new();
    // Iterates all files and assign to time windows according to max timestamp
    for file in files {
        if let Some((_, end)) = file.time_range() {
            let time_window = end
                .convert_to(TimeUnit::Second)
                .unwrap()
                .value()
                .align_to_ceil_by_bucket(time_window_size)
                .unwrap_or(i64::MIN);
            windows.entry(time_window).or_default().push(file.clone());
        } else {
            warn!("Unexpected file w/o timestamp: {:?}", file.file_id());
        }
    }
    windows
}

/// Finds the latest active writing window among all files.
/// Returns `None`
fn find_latest_window_in_seconds<'a>(
    files: impl Iterator<Item = &'a FileHandle>,
    time_window_size: i64,
) -> Option<i64> {
    let mut latest_timestamp = None;
    for f in files {
        if let Some((_, end)) = f.time_range() {
            if let Some(latest) = latest_timestamp && end > latest{
                latest_timestamp = Some(end);
            } else {
                latest_timestamp = Some(end);
            }
        }
    }
    latest_timestamp
        .and_then(|ts| ts.convert_to_ceil(TimeUnit::Second))
        .and_then(|ts| ts.value().align_by_bucket(time_window_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::tests::new_file_handle;
    use crate::sst::FileId;

    #[test]
    fn test_get_latest_window_in_seconds() {
        assert_eq!(
            Some(1),
            find_latest_window_in_seconds([new_file_handle(FileId::random(), 0, 999, 0)].iter(), 1)
        );
        assert_eq!(
            Some(1),
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), 0, 1000, 0)].iter(),
                1
            )
        );

        assert_eq!(
            Some(-9223372036857600),
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), i64::MIN, i64::MIN + 1, 0)].iter(),
                3600,
            )
        );

        assert_eq!(
            i64::MAX / 10000000 * 10000,
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), i64::MIN, i64::MAX, 0)].iter(),
                10000,
            )
            .unwrap()
        );
    }

    #[test]
    fn test_assign_to_windows() {
        let windows = assign_to_windows(
            [
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
            ]
            .iter(),
            3,
        );
        assert_eq!(5, windows.get(&0).unwrap().len());

        let files = [FileId::random(); 5];
        let windows = assign_to_windows(
            [
                new_file_handle(files[0], -2000, -3, 0),
                new_file_handle(files[1], 0, 2999, 0),
                new_file_handle(files[2], 50, 10001, 0),
            ]
            .iter(),
            3,
        );
        assert_eq!(files[0], windows.get(&0).unwrap().get(0).unwrap().file_id());
        assert_eq!(files[1], windows.get(&3).unwrap().get(0).unwrap().file_id());
        assert_eq!(
            files[2],
            windows.get(&12).unwrap().get(0).unwrap().file_id()
        );
    }
}
