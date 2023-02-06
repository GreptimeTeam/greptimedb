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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use common_telemetry::debug;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;

use crate::compaction::picker::PickerContext;
use crate::compaction::task::CompactionOutput;
use crate::sst::{FileHandle, LevelMeta};

/// Compaction strategy that defines which SSTs need to be compacted at given level.
pub trait Strategy {
    fn pick(&self, ctx: &PickerContext, level: &LevelMeta) -> Vec<CompactionOutput>;
}

pub type StrategyRef = Arc<dyn Strategy + Send + Sync>;

/// SimpleTimeWindowStrategy only handles level 0 to level 1 compaction in a time-window tiered
/// manner. It picks all SSTs in level 0 and writes rows in these SSTs to a new file partitioned
/// by a predefined time bucket in level 1.
/// After compaction, level 1 may contains more than one sorted run within the same time bucket.
pub struct SimpleTimeWindowStrategy {}

impl Strategy for SimpleTimeWindowStrategy {
    fn pick(&self, _ctx: &PickerContext, level: &LevelMeta) -> Vec<CompactionOutput> {
        // SimpleTimeWindowStrategy only handles level 0 to level 1 compaction.
        if level.level() != 0 {
            return vec![];
        }
        let files = find_compactable_files(level);
        if files.is_empty() {
            return vec![];
        }

        let time_bucket = infer_time_bucket(&files);
        let buckets = calculate_time_buckets(time_bucket.to_secs(), &files);
        debug!("File buckets: {:?}", buckets);
        buckets
            .into_iter()
            .map(|(bound, files)| CompactionOutput {
                output_level: 1,
                bucket_bound: bound,
                bucket: time_bucket,
                inputs: files,
            })
            .collect()
    }
}

/// Finds files that can be compacted in given level.
/// Currently they're files that is not currently under compaction.
#[inline]
fn find_compactable_files(level: &LevelMeta) -> Vec<FileHandle> {
    level
        .files()
        .iter()
        .filter(|f| !f.compacting())
        .cloned()
        .collect()
}

fn calculate_time_buckets(bucket_sec: i64, files: &[FileHandle]) -> HashMap<i64, Vec<FileHandle>> {
    let mut buckets = HashMap::new();

    for file in files {
        if let Some((start, end)) = file.time_range().clone() {
            let bounds = file_time_bucket_span(
                start.convert_to(TimeUnit::Second).unwrap().value(),
                end.convert_to(TimeUnit::Second).unwrap().value(),
                bucket_sec,
            );
            for bound in bounds {
                buckets
                    .entry(bound)
                    .or_insert_with(Vec::new)
                    .push(file.clone());
            }
        } else {
            // Files without timestamp range is assign to a special bucket `i64::MAX`,
            // so that they can be compacted together.
            buckets
                .entry(i64::MAX)
                .or_insert_with(Vec::new)
                .push(file.clone());
        }
    }
    buckets
}

/// Calculates timestamp span between start and end timestamp.
fn file_time_bucket_span(start_sec: i64, end_sec: i64, bucket_sec: i64) -> Vec<i64> {
    assert!(start_sec <= end_sec);

    // if timestamp is between `[i64::MIN, i64::MIN.align_by_bucket(bucket)]`, which cannot
    // be aligned to a valid i64 bound, simply return `i64::MIN` rather than just underflow.
    let mut start_aligned = start_sec.align_by_bucket(bucket_sec).unwrap_or(i64::MIN);
    let end_aligned = end_sec.align_by_bucket(bucket_sec).unwrap_or(i64::MIN);

    let mut res = Vec::with_capacity(((end_aligned - start_aligned) / bucket_sec + 1) as usize);
    while start_aligned < end_aligned {
        res.push(start_aligned);
        start_aligned += bucket_sec;
    }
    res.push(end_aligned);
    res
}

/// Infers the suitable time bucket duration.
/// Now it simply find the max and min timestamp across all SSTs in level and fit the time span
/// into `TimeBucket`.
fn infer_time_bucket(files: &[FileHandle]) -> TimeBucket {
    let mut max_ts = &Timestamp::new(i64::MIN, TimeUnit::Second);
    let mut min_ts = &Timestamp::new(i64::MAX, TimeUnit::Second);

    for f in files {
        if let Some((start, end)) = f.time_range() {
            min_ts = min_ts.min(start);
            max_ts = max_ts.max(end);
        } else {
            return TimeBucket::MAX;
        }
    }

    // safety: Convert whatever timestamp into seconds will not cause overflow.
    let min_sec = min_ts.convert_to(TimeUnit::Second).unwrap().value();
    let max_sec = max_ts.convert_to(TimeUnit::Second).unwrap().value();
    TimeBucket::fit(max_sec - min_sec)
}

/// A set of predefined time bucket used to align timestamp.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TimeBucket {
    OneHour,
    TwoHours,
    TwelveHours,
    OneDay,
    OneWeek,
}

impl TimeBucket {
    const ONE_HOUR_SECS: i64 = 60 * 60;
    const TWO_HOUR_SECS: i64 = 2 * Self::ONE_HOUR_SECS;
    const TWELVE_HOUR_SECS: i64 = 12 * Self::ONE_HOUR_SECS;
    const ONE_DAY_SECS: i64 = 24 * Self::ONE_HOUR_SECS;
    const ONE_WEEK_SECS: i64 = 7 * Self::ONE_DAY_SECS;

    const BUCKETS: [TimeBucket; 5] = [
        Self::OneHour,
        Self::TwoHours,
        Self::TwelveHours,
        Self::OneDay,
        Self::OneWeek,
    ];

    pub const MAX: TimeBucket = Self::OneWeek;

    /// Converts time bucket to seconds.
    pub fn to_secs(&self) -> i64 {
        match self {
            Self::OneHour => Self::ONE_HOUR_SECS,
            Self::TwoHours => Self::TWO_HOUR_SECS,
            Self::TwelveHours => Self::TWELVE_HOUR_SECS,
            Self::OneDay => Self::ONE_DAY_SECS,
            Self::OneWeek => Self::ONE_WEEK_SECS,
        }
    }

    /// Fits a given time span into time bucket by find the minimum bucket that can cover the span.
    /// Returns `TimeBucket::MAX` if no such bucket can be found.
    pub fn fit(span_sec: i64) -> Self {
        assert!(span_sec >= 0);
        for b in Self::BUCKETS {
            if b.to_secs() >= span_sec {
                return b;
            }
        }
        return Self::MAX;
    }
}

impl PartialOrd for TimeBucket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.to_secs().cmp(&other.to_secs()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::sst::FileMeta;

    #[test]
    fn test_time_bucket_span() {
        assert_eq!(vec![0], file_time_bucket_span(1, 9, 10));

        assert_eq!(vec![0, 10], file_time_bucket_span(1, 10, 10));

        assert_eq!(vec![-10], file_time_bucket_span(-10, -1, 10));

        assert_eq!(vec![-10, 0], file_time_bucket_span(-10, 0, 10));
    }

    #[test]
    fn test_time_bucket_span_large() {
        assert_eq!(
            vec![
                (i64::MAX - 10).align_by_bucket(10).unwrap(),
                i64::MAX.align_by_bucket(10).unwrap(),
            ],
            file_time_bucket_span(i64::MAX - 10, i64::MAX, 10)
        );

        // magic hmmm?
        for bucket in 1..100 {
            assert_eq!(
                vec![
                    i64::MIN,
                    (i64::MIN + bucket).align_by_bucket(bucket).unwrap()
                ],
                file_time_bucket_span(i64::MIN, i64::MIN + bucket, bucket)
            );
        }
    }

    #[test]
    fn test_time_bucket() {
        assert_eq!(
            TimeBucket::MAX,
            TimeBucket::BUCKETS.last().copied().unwrap()
        );

        assert_eq!(TimeBucket::OneHour, TimeBucket::fit(1));
        assert_eq!(TimeBucket::OneHour, TimeBucket::fit(60 * 60));
        assert_eq!(TimeBucket::TwoHours, TimeBucket::fit(60 * 60 + 1));

        assert_eq!(
            TimeBucket::TwelveHours,
            TimeBucket::fit(TimeBucket::TwelveHours.to_secs() - 1)
        );
        assert_eq!(
            TimeBucket::TwelveHours,
            TimeBucket::fit(TimeBucket::TwelveHours.to_secs())
        );
        assert_eq!(
            TimeBucket::OneDay,
            TimeBucket::fit(TimeBucket::OneDay.to_secs() - 1)
        );
        assert_eq!(TimeBucket::OneWeek, TimeBucket::fit(i64::MAX));
    }

    #[test]
    fn test_infer_time_buckets() {
        assert_eq!(
            TimeBucket::OneHour,
            infer_time_bucket(&[
                new_file_handle("a", 0, TimeBucket::OneHour.to_secs() * 1000 - 1),
                new_file_handle("b", 1, 10_000)
            ])
        );
    }

    fn new_file_handle(name: &str, start_ts_millis: i64, end_ts_millis: i64) -> FileHandle {
        FileHandle::new(FileMeta {
            file_name: name.to_string(),
            time_range: Some((
                Timestamp::new_millisecond(start_ts_millis),
                Timestamp::new_millisecond(end_ts_millis),
            )),
            level: 0,
        })
    }

    fn new_file_handles(input: &[(&str, i64, i64)]) -> Vec<FileHandle> {
        input
            .iter()
            .map(|(name, start, end)| new_file_handle(name, *start, *end))
            .collect()
    }

    fn check_bucket_calculation(
        bucket_sec: i64,
        files: Vec<FileHandle>,
        expected: &[(i64, &[&str])],
    ) {
        let res = calculate_time_buckets(bucket_sec, &files);

        let expected = expected
            .iter()
            .map(|(bucket, file_names)| {
                (
                    *bucket,
                    file_names
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<HashSet<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();

        for (bucket, file_names) in expected {
            let actual = res
                .get(&bucket)
                .unwrap()
                .iter()
                .map(|f| f.file_name().to_string())
                .collect();
            assert_eq!(
                file_names, actual,
                "bucket: {bucket}, expected: {file_names:?}, actual: {actual:?}",
            );
        }
    }

    #[test]
    fn test_calculate_time_buckets() {
        // simple case, files with disjoint
        check_bucket_calculation(
            10,
            new_file_handles(&[("a", 0, 9000), ("b", 10000, 19000)]),
            &[(0, &["a"]), (10, &["b"])],
        );

        // files across buckets
        check_bucket_calculation(
            10,
            new_file_handles(&[("a", 0, 10001), ("b", 10000, 19000)]),
            &[(0, &["a"]), (10, &["a", "b"])],
        );
        check_bucket_calculation(
            10,
            new_file_handles(&[("a", 0, 10000)]),
            &[(0, &["a"]), (10, &["a"])],
        );

        // files without timestamp are align to a special bucket: i64::MAX
        check_bucket_calculation(
            10,
            vec![FileHandle::new(FileMeta {
                file_name: "a".to_string(),
                time_range: None,
                level: 0,
            })],
            &[(i64::MAX, &["a"])],
        );

        // file with an large time range

        let expected = (0..(TimeBucket::ONE_WEEK_SECS / TimeBucket::ONE_HOUR_SECS))
            .into_iter()
            .map(|b| (b * TimeBucket::ONE_HOUR_SECS, &["a"] as _))
            .collect::<Vec<_>>();
        check_bucket_calculation(
            TimeBucket::OneHour.to_secs(),
            new_file_handles(&[("a", 0, TimeBucket::ONE_WEEK_SECS * 1000)]),
            &expected,
        );
    }
}
