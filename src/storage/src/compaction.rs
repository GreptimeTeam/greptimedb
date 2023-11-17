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

pub mod noop;
mod picker;
mod scheduler;
mod task;
mod twcs;
mod writer;

use std::sync::Arc;

use common_telemetry::warn;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
pub use picker::{LeveledTimeWindowPicker, Picker, PickerContext};
pub use scheduler::{CompactionHandler, CompactionRequestImpl};
use store_api::logstore::LogStore;
use store_api::storage::CompactionStrategy;
pub use task::{CompactionTask, CompactionTaskImpl};
pub use twcs::TwcsPicker;

use crate::scheduler::Scheduler;
use crate::sst::FileHandle;

pub type CompactionPickerRef<S> =
    Arc<dyn Picker<Request = CompactionRequestImpl<S>, Task = CompactionTaskImpl<S>> + Send + Sync>;

pub type CompactionSchedulerRef<S> =
    Arc<dyn Scheduler<Request = CompactionRequestImpl<S>> + Send + Sync>;

/// Infers the suitable time bucket duration.
/// Now it simply find the max and min timestamp across all SSTs in level and fit the time span
/// into time bucket.
pub(crate) fn infer_time_bucket<'a>(files: impl Iterator<Item = &'a FileHandle>) -> i64 {
    let mut max_ts = Timestamp::new(i64::MIN, TimeUnit::Second);
    let mut min_ts = Timestamp::new(i64::MAX, TimeUnit::Second);

    for f in files {
        if let Some((start, end)) = f.time_range() {
            min_ts = min_ts.min(*start);
            max_ts = max_ts.max(*end);
        } else {
            // we don't expect an SST file without time range,
            // it's either a bug or data corruption.
            warn!("Found SST file without time range metadata: {f:?}");
        }
    }

    // safety: Convert whatever timestamp into seconds will not cause overflow.
    let min_sec = min_ts.convert_to(TimeUnit::Second).unwrap().value();
    let max_sec = max_ts.convert_to(TimeUnit::Second).unwrap().value();

    max_sec
        .checked_sub(min_sec)
        .map(|span| TIME_BUCKETS.fit_time_bucket(span)) // return the max bucket on subtraction overflow.
        .unwrap_or_else(|| TIME_BUCKETS.max()) // safety: TIME_BUCKETS cannot be empty.
}

pub(crate) struct TimeBuckets([i64; 7]);

impl TimeBuckets {
    /// Fits a given time span into time bucket by find the minimum bucket that can cover the span.
    /// Returns the max bucket if no such bucket can be found.
    fn fit_time_bucket(&self, span_sec: i64) -> i64 {
        assert!(span_sec >= 0);
        match self.0.binary_search(&span_sec) {
            Ok(idx) => self.0[idx],
            Err(idx) => {
                if idx < self.0.len() {
                    self.0[idx]
                } else {
                    self.0.last().copied().unwrap()
                }
            }
        }
    }

    #[cfg(test)]
    fn get(&self, idx: usize) -> i64 {
        self.0[idx]
    }

    fn max(&self) -> i64 {
        self.0.last().copied().unwrap()
    }
}

/// A set of predefined time buckets.
pub(crate) const TIME_BUCKETS: TimeBuckets = TimeBuckets([
    60 * 60,                 // one hour
    2 * 60 * 60,             // two hours
    12 * 60 * 60,            // twelve hours
    24 * 60 * 60,            // one day
    7 * 24 * 60 * 60,        // one week
    365 * 24 * 60 * 60,      // one year
    10 * 365 * 24 * 60 * 60, // ten years
]);

pub fn compaction_strategy_to_picker<S: LogStore>(
    strategy: &CompactionStrategy,
) -> CompactionPickerRef<S> {
    match strategy {
        CompactionStrategy::Twcs(twcs_opts) => Arc::new(TwcsPicker::new(
            twcs_opts.max_active_window_files,
            twcs_opts.max_inactive_window_files,
            twcs_opts.time_window_seconds,
        )) as Arc<_>,
    }
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;

    use super::*;
    use crate::file_purger::noop::new_noop_file_purger;
    use crate::sst::{FileHandle, FileId, FileMeta, Level};

    /// Test util to create file handles.
    pub fn new_file_handle(
        file_id: FileId,
        start_ts_millis: i64,
        end_ts_millis: i64,
        level: Level,
    ) -> FileHandle {
        let file_purger = new_noop_file_purger();
        let layer = Arc::new(crate::test_util::access_layer_util::MockAccessLayer {});
        FileHandle::new(
            FileMeta {
                region_id: 0.into(),
                file_id,
                time_range: Some((
                    Timestamp::new_millisecond(start_ts_millis),
                    Timestamp::new_millisecond(end_ts_millis),
                )),
                level,
                file_size: 0,
            },
            layer,
            file_purger,
        )
    }

    #[test]
    fn test_time_bucket() {
        assert_eq!(TIME_BUCKETS.get(0), TIME_BUCKETS.fit_time_bucket(1));
        assert_eq!(TIME_BUCKETS.get(0), TIME_BUCKETS.fit_time_bucket(60 * 60));
        assert_eq!(
            TIME_BUCKETS.get(1),
            TIME_BUCKETS.fit_time_bucket(60 * 60 + 1)
        );

        assert_eq!(
            TIME_BUCKETS.get(2),
            TIME_BUCKETS.fit_time_bucket(TIME_BUCKETS.get(2) - 1)
        );
        assert_eq!(
            TIME_BUCKETS.get(2),
            TIME_BUCKETS.fit_time_bucket(TIME_BUCKETS.get(2))
        );
        assert_eq!(
            TIME_BUCKETS.get(3),
            TIME_BUCKETS.fit_time_bucket(TIME_BUCKETS.get(3) - 1)
        );
        assert_eq!(TIME_BUCKETS.get(6), TIME_BUCKETS.fit_time_bucket(i64::MAX));
    }

    #[test]
    fn test_infer_time_buckets() {
        assert_eq!(
            TIME_BUCKETS.get(0),
            infer_time_bucket(
                [
                    new_file_handle(FileId::random(), 0, TIME_BUCKETS.get(0) * 1000 - 1, 0),
                    new_file_handle(FileId::random(), 1, 10_000, 0)
                ]
                .iter()
            )
        );
    }
}
