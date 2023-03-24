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

use std::collections::HashMap;
use std::sync::Arc;

use common_telemetry::{debug, warn};
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
/// by a inferred time bucket in level 1.
pub struct SimpleTimeWindowStrategy {}

impl Strategy for SimpleTimeWindowStrategy {
    fn pick(&self, _ctx: &PickerContext, level: &LevelMeta) -> Vec<CompactionOutput> {
        // SimpleTimeWindowStrategy only handles level 0 to level 1 compaction.
        if level.level() != 0 {
            return vec![];
        }
        let files = find_compactable_files(level);
        debug!("Compactable files found: {:?}", files);
        if files.is_empty() {
            return vec![];
        }

        let time_bucket = infer_time_bucket(&files);
        let buckets = calculate_time_buckets(time_bucket, &files);
        debug!("File bucket:{}, file groups: {:?}", time_bucket, buckets);
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
    level.files().filter(|f| !f.compacting()).cloned().collect()
}

/// Calculates buckets for files. If file does not contain a time range in metadata, it will be
/// assigned to a special bucket `i64::MAX` (normally no timestamp can be aligned to this bucket)
/// so that all files without timestamp can be compacted together.
fn calculate_time_buckets(bucket_sec: i64, files: &[FileHandle]) -> HashMap<i64, Vec<FileHandle>> {
    let mut buckets = HashMap::new();

    for file in files {
        if let Some((start, end)) = file.time_range() {
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
            warn!("Found corrupted SST without timestamp bounds: {:?}", file);
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
/// into time bucket.
fn infer_time_bucket(files: &[FileHandle]) -> i64 {
    let mut max_ts = &Timestamp::new(i64::MIN, TimeUnit::Second);
    let mut min_ts = &Timestamp::new(i64::MAX, TimeUnit::Second);

    for f in files {
        if let Some((start, end)) = f.time_range() {
            min_ts = min_ts.min(start);
            max_ts = max_ts.max(end);
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
        .map(fit_time_bucket) // return the max bucket on subtraction overflow.
        .unwrap_or_else(|| *TIME_BUCKETS.last().unwrap()) // safety: TIME_BUCKETS cannot be empty.
}

/// A set of predefined time buckets.
const TIME_BUCKETS: [i64; 7] = [
    60 * 60,                 // one hour
    2 * 60 * 60,             // two hours
    12 * 60 * 60,            // twelve hours
    24 * 60 * 60,            // one day
    7 * 24 * 60 * 60,        // one week
    365 * 24 * 60 * 60,      // one year
    10 * 365 * 24 * 60 * 60, // ten years
];

/// Fits a given time span into time bucket by find the minimum bucket that can cover the span.
/// Returns the max bucket if no such bucket can be found.
fn fit_time_bucket(span_sec: i64) -> i64 {
    assert!(span_sec >= 0);
    for b in TIME_BUCKETS {
        if b >= span_sec {
            return b;
        }
    }
    *TIME_BUCKETS.last().unwrap()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::file_purger::noop::new_noop_file_purger;
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
        assert_eq!(TIME_BUCKETS[0], fit_time_bucket(1));
        assert_eq!(TIME_BUCKETS[0], fit_time_bucket(60 * 60));
        assert_eq!(TIME_BUCKETS[1], fit_time_bucket(60 * 60 + 1));

        assert_eq!(TIME_BUCKETS[2], fit_time_bucket(TIME_BUCKETS[2] - 1));
        assert_eq!(TIME_BUCKETS[2], fit_time_bucket(TIME_BUCKETS[2]));
        assert_eq!(TIME_BUCKETS[3], fit_time_bucket(TIME_BUCKETS[3] - 1));
        assert_eq!(TIME_BUCKETS[6], fit_time_bucket(i64::MAX));
    }

    #[test]
    fn test_infer_time_buckets() {
        assert_eq!(
            TIME_BUCKETS[0],
            infer_time_bucket(&[
                new_file_handle("a", 0, TIME_BUCKETS[0] * 1000 - 1),
                new_file_handle("b", 1, 10_000)
            ])
        );
    }

    fn new_file_handle(name: &str, start_ts_millis: i64, end_ts_millis: i64) -> FileHandle {
        let file_purger = new_noop_file_purger();
        let layer = Arc::new(crate::test_util::access_layer_util::MockAccessLayer {});
        FileHandle::new(
            FileMeta {
                region_id: 0,
                file_name: name.to_string(),
                time_range: Some((
                    Timestamp::new_millisecond(start_ts_millis),
                    Timestamp::new_millisecond(end_ts_millis),
                )),
                level: 0,
            },
            layer,
            file_purger,
        )
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

        // file with an large time range
        let expected = (0..(TIME_BUCKETS[4] / TIME_BUCKETS[0]))
            .map(|b| (b * TIME_BUCKETS[0], &["a"] as _))
            .collect::<Vec<_>>();
        check_bucket_calculation(
            TIME_BUCKETS[0],
            new_file_handles(&[("a", 0, TIME_BUCKETS[4] * 1000)]),
            &expected,
        );
    }
}
