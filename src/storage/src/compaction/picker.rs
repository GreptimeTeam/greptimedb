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
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::time::Duration;

use common_telemetry::{debug, error, info, warn};
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use snafu::ResultExt;
use store_api::logstore::LogStore;

use crate::compaction::infer_time_bucket;
use crate::compaction::scheduler::CompactionRequestImpl;
use crate::compaction::task::{CompactionOutput, CompactionTask, CompactionTaskImpl};
use crate::error::{Result, TtlCalculationSnafu};
use crate::scheduler::Request;
use crate::sst::{FileHandle, FileId, LevelMeta};

/// Picker picks input SST files and builds the compaction task.
/// Different compaction strategy may implement different pickers.
pub trait Picker: Debug + Send + 'static {
    type Request: Request;
    type Task: CompactionTask;

    fn pick(&self, req: &Self::Request) -> Result<Option<Self::Task>>;
}

pub(crate) fn get_expired_ssts(
    levels: &[LevelMeta],
    ttl: Option<Duration>,
    now: Timestamp,
) -> Result<Vec<FileHandle>> {
    let Some(ttl) = ttl else {
        return Ok(vec![]);
    };

    let expire_time = now.sub_duration(ttl).context(TtlCalculationSnafu)?;

    let expired_ssts = levels
        .iter()
        .flat_map(|l| l.get_expired_files(&expire_time).into_iter())
        .collect();
    Ok(expired_ssts)
}

pub struct PickerContext {
    compaction_time_window: Option<i64>,
}

impl PickerContext {
    pub fn with(compaction_time_window: Option<i64>) -> Self {
        Self {
            compaction_time_window,
        }
    }

    pub fn compaction_time_window(&self) -> Option<i64> {
        self.compaction_time_window
    }
}

/// `LeveledTimeWindowPicker` only handles level 0 to level 1 compaction in a time-window tiered
/// manner. It picks all SSTs in level 0 and writes rows in these SSTs to a new file partitioned
/// by a inferred time bucket in level 1.
pub struct LeveledTimeWindowPicker<S> {
    _phantom_data: PhantomData<S>,
}

impl<S> Debug for LeveledTimeWindowPicker<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeveledTimeWindowPicker{{..}}")
    }
}

impl<S> Default for LeveledTimeWindowPicker<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> LeveledTimeWindowPicker<S> {
    pub fn new() -> Self {
        Self {
            _phantom_data: Default::default(),
        }
    }
}

impl<S: LogStore> Picker for LeveledTimeWindowPicker<S> {
    type Request = CompactionRequestImpl<S>;
    type Task = CompactionTaskImpl<S>;

    fn pick(&self, req: &CompactionRequestImpl<S>) -> Result<Option<CompactionTaskImpl<S>>> {
        let levels = &req.levels();
        let expired_ssts = get_expired_ssts(levels.levels(), req.ttl, Timestamp::current_millis())
            .map_err(|e| {
                error!(e;"Failed to get region expired SST files, region: {}, ttl: {:?}", req.region_id, req.ttl);
                e
            })
            .unwrap_or_default();

        if !expired_ssts.is_empty() {
            info!(
                "Expired SSTs in region {}: {:?}",
                req.region_id, expired_ssts
            );
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.mark_compacting(true));
        }

        let ctx = &PickerContext::with(req.compaction_time_window);

        let mut outputs = vec![];
        for level_num in 0..levels.level_num() {
            let level = levels.level(level_num as u8);
            let compaction_time_window = Self::pick_level(ctx, level, &mut outputs);

            if outputs.is_empty() {
                debug!(
                    "No SST file can be compacted at level {}, path: {:?}",
                    level_num, req.sst_layer
                );
                continue;
            }

            debug!(
                "Found SST files to compact {:?} on level: {}, compaction window: {:?}",
                outputs, level_num, compaction_time_window,
            );
            return Ok(Some(CompactionTaskImpl {
                schema: req.schema(),
                sst_layer: req.sst_layer.clone(),
                outputs,
                writer: req.writer.clone(),
                shared_data: req.shared.clone(),
                wal: req.wal.clone(),
                manifest: req.manifest.clone(),
                expired_ssts,
                sst_write_buffer_size: req.sst_write_buffer_size,
                compaction_time_window,
                reschedule_on_finish: req.reschedule_on_finish,
            }));
        }

        Ok(None)
    }
}

impl<S> LeveledTimeWindowPicker<S> {
    fn pick_level(
        ctx: &PickerContext,
        level: &LevelMeta,
        results: &mut Vec<CompactionOutput>,
    ) -> Option<i64> {
        // SimpleTimeWindowStrategy only handles level 0 to level 1 compaction.
        if level.level() != 0 {
            return None;
        }
        let files = find_compactable_files(level);
        debug!("Compactable files found: {:?}", files);
        if files.is_empty() {
            return None;
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

        results.extend(buckets.into_iter().map(|(bound, files)| CompactionOutput {
            output_file_id: FileId::random(),
            output_level: 1,
            time_window_bound: bound,
            time_window_sec: time_window,
            inputs: files,
            // strict window is used in simple time window strategy in that rows in one file
            // may get compacted to multiple destinations.
            strict_window: true,
        }));
        Some(time_window)
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
    use std::sync::Arc;

    use super::*;
    use crate::compaction::tests::new_file_handle;
    use crate::compaction::TIME_BUCKETS;
    use crate::file_purger::noop::new_noop_file_purger;
    use crate::sst::{FileId, Level, LevelMetas};

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
            .map(|(file_id, start, end)| new_file_handle(*file_id, *start, *end, 0))
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

    struct TtlTester {
        files: Vec<(FileId, i64, i64, Level)>,
        ttl: Option<Duration>,
        expired: Vec<usize>,
        now: Timestamp,
    }

    impl TtlTester {
        fn check(&self) {
            let expected_expired = self
                .expired
                .iter()
                .map(|idx| self.files[*idx].0)
                .collect::<HashSet<_>>();
            let file_purger = new_noop_file_purger();
            let layer = Arc::new(crate::test_util::access_layer_util::MockAccessLayer {});
            let file_handles = self
                .files
                .iter()
                .map(|(file_id, start_ts, end_ts, level)| {
                    new_file_handle(*file_id, *start_ts, *end_ts, *level).meta()
                })
                .collect::<Vec<_>>();
            let levels = LevelMetas::new(layer, file_purger).merge(
                file_handles.into_iter(),
                vec![].into_iter(),
                None,
            );
            let expired = get_expired_ssts(levels.levels(), self.ttl, self.now)
                .unwrap()
                .into_iter()
                .map(|f| f.file_id())
                .collect::<HashSet<_>>();
            assert_eq!(expected_expired, expired);
        }
    }

    #[test]
    fn test_find_expired_ssts() {
        TtlTester {
            files: vec![
                (FileId::random(), 8000, 9000, 0),
                (FileId::random(), 10000, 11000, 0),
                (FileId::random(), 8000, 11000, 1),
                (FileId::random(), 2000, 3000, 1),
            ],
            ttl: Some(Duration::from_secs(1)),
            expired: vec![3],
            now: Timestamp::new_second(10),
        }
        .check();

        TtlTester {
            files: vec![
                (FileId::random(), 8000, 8999, 0),
                (FileId::random(), 10000, 11000, 0),
                (FileId::random(), 8000, 11000, 1),
                (FileId::random(), 2000, 3000, 1),
            ],
            ttl: Some(Duration::from_secs(1)),
            expired: vec![0, 3],
            now: Timestamp::new_second(10),
        }
        .check();
    }
}
