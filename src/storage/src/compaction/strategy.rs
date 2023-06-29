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

use crate::compaction::infer_time_bucket;
use crate::compaction::picker::PickerContext;
use crate::compaction::task::CompactionOutput;
use crate::sst::{FileHandle, LevelMeta};

/// Compaction strategy that defines which SSTs need to be compacted at given level.
pub trait Strategy {
    fn pick(&self, ctx: &PickerContext, level: &LevelMeta) -> (Option<i64>, Vec<CompactionOutput>);
}

pub type StrategyRef = Arc<dyn Strategy + Send + Sync>;

/// SimpleTimeWindowStrategy only handles level 0 to level 1 compaction in a time-window tiered
/// manner. It picks all SSTs in level 0 and writes rows in these SSTs to a new file partitioned
/// by a inferred time bucket in level 1.
pub struct SimpleTimeWindowStrategy {}

impl Strategy for SimpleTimeWindowStrategy {
    fn pick(&self, ctx: &PickerContext, level: &LevelMeta) -> (Option<i64>, Vec<CompactionOutput>) {
        // SimpleTimeWindowStrategy only handles level 0 to level 1 compaction.
        if level.level() != 0 {
            return (None, vec![]);
        }
        let files = find_compactable_files(level);
        debug!("Compactable files found: {:?}", files);
        if files.is_empty() {
            return (None, vec![]);
        }
        let time_window = ctx.compaction_time_window().unwrap_or_else(|| {
            let inferred = infer_time_bucket(files.iter());
            debug!(
                "Compaction window is not present, inferring from files: {:?}",
                inferred
            );
            inferred
        });
        let buckets = calculate_time_buckets(time_window, &files);
        debug!("File bucket:{}, file groups: {:?}", time_window, buckets);
        (
            Some(time_window),
            buckets
                .into_iter()
                .map(|(bound, files)| CompactionOutput {
                    output_level: 1,
                    bucket_bound: bound,
                    bucket: time_window,
                    inputs: files,
                })
                .collect(),
        )
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::compaction::tests::new_file_handle;
    use crate::compaction::TIME_BUCKETS;
    use crate::sst::FileId;

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

    fn new_file_handles(input: &[(FileId, i64, i64)]) -> Vec<FileHandle> {
        input
            .iter()
            .map(|(file_id, start, end)| new_file_handle(*file_id, *start, *end))
            .collect()
    }

    fn check_bucket_calculation(
        bucket_sec: i64,
        files: Vec<FileHandle>,
        expected: &[(i64, &[FileId])],
    ) {
        let res = calculate_time_buckets(bucket_sec, &files);

        let expected = expected
            .iter()
            .map(|(bucket, file_ids)| (*bucket, file_ids.iter().copied().collect::<HashSet<_>>()))
            .collect::<HashMap<_, _>>();

        for (bucket, file_ids) in expected {
            let actual = res
                .get(&bucket)
                .unwrap()
                .iter()
                .map(|f| f.file_id())
                .collect();
            assert_eq!(
                file_ids, actual,
                "bucket: {bucket}, expected: {file_ids:?}, actual: {actual:?}",
            );
        }
    }

    #[test]
    fn test_calculate_time_buckets() {
        let file_id_a = FileId::random();
        let file_id_b = FileId::random();
        // simple case, files with disjoint
        check_bucket_calculation(
            10,
            new_file_handles(&[(file_id_a, 0, 9000), (file_id_b, 10000, 19000)]),
            &[(0, &[file_id_a]), (10, &[file_id_b])],
        );

        // files across buckets
        check_bucket_calculation(
            10,
            new_file_handles(&[(file_id_a, 0, 10001), (file_id_b, 10000, 19000)]),
            &[(0, &[file_id_a]), (10, &[file_id_a, file_id_b])],
        );
        check_bucket_calculation(
            10,
            new_file_handles(&[(file_id_a, 0, 10000)]),
            &[(0, &[file_id_a]), (10, &[file_id_a])],
        );

        // file with an large time range
        let file_id_array = &[file_id_a];
        let expected = (0..(TIME_BUCKETS.get(4) / TIME_BUCKETS.get(0)))
            .map(|b| (b * TIME_BUCKETS.get(0), file_id_array as _))
            .collect::<Vec<_>>();
        check_bucket_calculation(
            TIME_BUCKETS.get(0),
            new_file_handles(&[(file_id_a, 0, TIME_BUCKETS.get(4) * 1000)]),
            &expected,
        );
    }
}
