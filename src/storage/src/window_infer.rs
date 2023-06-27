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

use std::collections::HashSet;

use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use itertools::Itertools;

use crate::memtable::MemtableStats;
use crate::sst::FileMeta;

/// A set of predefined time windows.
const TIME_WINDOW_SIZE: [i64; 10] = [
    1,                // 1 second
    60,               // 1 minute
    60 * 10,          // 10 minutes
    60 * 30,          // 30 minutes
    60 * 60,          // 1 hour
    2 * 60 * 60,      // 2 hours
    6 * 60 * 60,      // 6 hours
    12 * 60 * 60,     // 12 hours
    24 * 60 * 60,     // 1 day
    7 * 24 * 60 * 60, // 1 week
];

/// [WindowInfer] infers the time windows that can be used to optimize table scans ordered by
/// timestamp column or have explicit time windows. By splitting time spans of tables into
/// time windows, we can scan entries window by window.
pub(crate) trait WindowInfer {
    /// Infers time windows according to the SST files and memtables.
    ///
    /// ### Note
    /// The order of returned vector defines how records are yielded.
    fn infer_window(
        &self,
        files: &[FileMeta],
        mem_tables: &[MemtableStats],
        ts_desc: bool,
    ) -> Vec<TimestampRange>;
}

/// [PlainWindowInference] simply finds the minimum time span within all SST files in level 0 and
/// memtables, matches that time span into a set of predefined time windows.
pub(crate) struct PlainWindowInference;

impl WindowInfer for PlainWindowInference {
    fn infer_window(
        &self,
        files: &[FileMeta],
        mem_tables: &[MemtableStats],
        ts_desc: bool,
    ) -> Vec<TimestampRange> {
        let mut min_duration_sec = i64::MAX;
        let mut durations = Vec::with_capacity(files.len() + mem_tables.len());

        for meta in files {
            if let Some((start, end)) = &meta.time_range {
                // unwrap safety: converting timestamps with any unit to seconds won't overflow.
                let start_sec = start.convert_to(TimeUnit::Second).unwrap().value();
                // file timestamp range end is inclusive
                let end_sec = end.convert_to_ceil(TimeUnit::Second).unwrap().value();
                debug_assert!(end_sec >= start_sec);
                if meta.level == 0 {
                    // only level 0 is involved when calculating time windows.
                    min_duration_sec = min_duration_sec.min(end_sec - start_sec);
                }
                durations.push((start_sec, end_sec));
            }
        }

        for stats in mem_tables {
            // unwrap safety: converting timestamps with any unit to seconds won't overflow.
            let start_sec = stats
                .min_timestamp
                .convert_to(TimeUnit::Second)
                .unwrap()
                .value();
            let end_sec = stats
                .max_timestamp
                .convert_to_ceil(TimeUnit::Second)
                .unwrap()
                .value();
            min_duration_sec = min_duration_sec.min(end_sec - start_sec);
            durations.push((start_sec, end_sec));
        }

        let window_size = min_duration_to_window_size(min_duration_sec);
        align_time_spans_to_windows(&durations, window_size)
            .into_iter()
            .sorted_by(|(l_start, _), (r_start, _)| {
                if ts_desc {
                    l_start.cmp(r_start)
                } else {
                    r_start.cmp(l_start)
                }
            }) // sort time windows in descending order
            // unwrap safety: we ensure that end>=start so that TimestampRange::with_unit won't return None
            .map(|(start, end)| TimestampRange::with_unit(start, end, TimeUnit::Second).unwrap())
            .collect()
    }
}

/// Given a set of time spans and a min duration, this function aligns the time spans to windows that
/// collectively covers all the time spans.
///
/// For example, given time span `[1, 6)` and duration 5, the span can be aligned and split to
/// two windows with length 5: `[0, 5)` and `[5, 10]`, and these two windows can cover the original
/// span `[1, 6)`.
fn align_time_spans_to_windows(durations: &[(i64, i64)], min_duration: i64) -> HashSet<(i64, i64)> {
    let mut res = HashSet::new();
    for (start, end) in durations {
        let mut next = *start;
        while next <= *end {
            let next_aligned = next.align_by_bucket(min_duration).unwrap_or(i64::MIN);
            if let Some(next_end_aligned) = next_aligned.checked_add(min_duration) {
                let _ = res.insert((next_aligned, next_end_aligned));
                next = next_end_aligned;
            } else {
                // arithmetic overflow, clamp to i64::MAX and break the loop.
                let _ = res.insert((next_aligned, i64::MAX));
                break;
            }
        }
    }
    res
}

/// Find the most suitable time window size according to the `min_duration` found across all
/// SST files and memtables through a binary search.
fn min_duration_to_window_size(min_duration: i64) -> i64 {
    match TIME_WINDOW_SIZE.binary_search(&min_duration) {
        Ok(idx) => TIME_WINDOW_SIZE[idx],
        Err(idx) => {
            if idx < TIME_WINDOW_SIZE.len() {
                TIME_WINDOW_SIZE[idx]
            } else {
                TIME_WINDOW_SIZE.last().copied().unwrap()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;

    use super::*;

    #[test]
    fn test_get_time_window_size() {
        assert_eq!(1, min_duration_to_window_size(0));
        for window in TIME_WINDOW_SIZE {
            assert_eq!(window, min_duration_to_window_size(window));
        }
        assert_eq!(1, min_duration_to_window_size(1));
        assert_eq!(60, min_duration_to_window_size(60));
        assert_eq!(60 * 10, min_duration_to_window_size(100));
        assert_eq!(60 * 30, min_duration_to_window_size(1800));
        assert_eq!(60 * 60, min_duration_to_window_size(3000));
        assert_eq!(2 * 60 * 60, min_duration_to_window_size(4000));
        assert_eq!(6 * 60 * 60, min_duration_to_window_size(21599));
        assert_eq!(12 * 60 * 60, min_duration_to_window_size(21601));
        assert_eq!(24 * 60 * 60, min_duration_to_window_size(43201));
        assert_eq!(7 * 24 * 60 * 60, min_duration_to_window_size(604799));
        assert_eq!(7 * 24 * 60 * 60, min_duration_to_window_size(31535999));
        assert_eq!(7 * 24 * 60 * 60, min_duration_to_window_size(i64::MAX));
    }

    fn check_align_durations_to_windows(
        durations: &[(i64, i64)],
        min_duration: i64,
        expected: &[(i64, i64)],
    ) {
        let res = align_time_spans_to_windows(durations, min_duration);
        let expected = expected.iter().copied().collect::<HashSet<_>>();
        assert_eq!(res, expected);
    }

    #[test]
    fn test_duration_to_windows() {
        check_align_durations_to_windows(&[(0, 1)], 2, &[(0, 2)]);
        check_align_durations_to_windows(&[(-3, 1)], 2, &[(-4, -2), (-2, 0), (0, 2)]);
        check_align_durations_to_windows(&[(1, 3)], 2, &[(0, 2), (2, 4)]);
        check_align_durations_to_windows(
            &[(i64::MIN, i64::MIN + 3)],
            2,
            &[(i64::MIN, i64::MIN + 2), (i64::MIN + 2, i64::MIN + 4)],
        );

        check_align_durations_to_windows(
            &[(i64::MAX - 3, i64::MAX)],
            2,
            &[(i64::MAX - 3, i64::MAX - 1), (i64::MAX - 1, i64::MAX)],
        );

        check_align_durations_to_windows(&[(-3, 10)], 7, &[(-7, 0), (0, 7), (7, 14)]);
    }

    #[test]
    fn test_multiple_duration_to_windows() {
        check_align_durations_to_windows(&[(0, 1), (1, 3)], 3, &[(0, 3), (3, 6)]);
        check_align_durations_to_windows(&[(0, 1), (1, 2), (7, 11)], 3, &[(0, 3), (6, 9), (9, 12)]);

        check_align_durations_to_windows(
            &[(-2, 1), (i64::MAX - 2, i64::MAX)],
            3,
            &[
                (-3, 0),
                (0, 3),
                (i64::MAX - 4, i64::MAX - 1),
                (i64::MAX - 1, i64::MAX),
            ],
        );
    }

    #[test]
    fn test_plain_window_inference() {
        let window_inference = PlainWindowInference {};

        let res = window_inference.infer_window(
            &[FileMeta {
                time_range: Some((
                    Timestamp::new(1000, TimeUnit::Millisecond),
                    Timestamp::new(3000, TimeUnit::Millisecond),
                )),
                ..Default::default()
            }],
            &[MemtableStats {
                max_timestamp: Timestamp::new(3001, TimeUnit::Millisecond),
                min_timestamp: Timestamp::new(2001, TimeUnit::Millisecond),
                ..Default::default()
            }],
            true,
        );
        assert_eq!(
            vec![TimestampRange::with_unit(0, 60, TimeUnit::Second).unwrap(),],
            res
        );

        let res = window_inference.infer_window(
            &[FileMeta {
                time_range: Some((
                    Timestamp::new(0, TimeUnit::Millisecond),
                    Timestamp::new(60 * 1000 + 1, TimeUnit::Millisecond),
                )),
                ..Default::default()
            }],
            &[MemtableStats {
                max_timestamp: Timestamp::new(3001, TimeUnit::Millisecond),
                min_timestamp: Timestamp::new(2001, TimeUnit::Millisecond),
                ..Default::default()
            }],
            true,
        );
        assert_eq!(
            vec![
                TimestampRange::with_unit(0, 60, TimeUnit::Second).unwrap(),
                TimestampRange::with_unit(60, 120, TimeUnit::Second).unwrap(),
            ],
            res
        );

        let res = window_inference.infer_window(
            &[
                FileMeta {
                    time_range: Some((
                        Timestamp::new(0, TimeUnit::Millisecond),
                        Timestamp::new(60 * 1000 + 1, TimeUnit::Millisecond),
                    )),
                    ..Default::default()
                },
                FileMeta {
                    time_range: Some((
                        Timestamp::new(60 * 60 * 1000, TimeUnit::Millisecond),
                        Timestamp::new(60 * 60 * 1000 + 1, TimeUnit::Millisecond),
                    )),
                    ..Default::default()
                },
            ],
            &[MemtableStats {
                max_timestamp: Timestamp::new(3001, TimeUnit::Millisecond),
                min_timestamp: Timestamp::new(2001, TimeUnit::Millisecond),
                ..Default::default()
            }],
            true,
        );

        let mut expect = (0..=61)
            .map(|s| TimestampRange::with_unit(s, s + 1, TimeUnit::Second).unwrap())
            .collect::<Vec<_>>();
        expect.push(TimestampRange::with_unit(60 * 60, 60 * 60 + 1, TimeUnit::Second).unwrap());
        expect.push(TimestampRange::with_unit(60 * 60 + 1, 60 * 60 + 2, TimeUnit::Second).unwrap());

        assert_eq!(expect, res);

        let res = window_inference.infer_window(
            &[
                FileMeta {
                    time_range: Some((
                        Timestamp::new(0, TimeUnit::Millisecond),
                        Timestamp::new(60 * 1000, TimeUnit::Millisecond),
                    )),
                    level: 1, // this SST will be ignored
                    ..Default::default()
                },
                FileMeta {
                    time_range: Some((
                        Timestamp::new(0, TimeUnit::Millisecond),
                        Timestamp::new(10 * 60 * 1000, TimeUnit::Millisecond),
                    )),
                    ..Default::default()
                },
            ],
            &[MemtableStats {
                max_timestamp: Timestamp::new(60 * 30 * 1000 + 1, TimeUnit::Millisecond),
                min_timestamp: Timestamp::new(0, TimeUnit::Millisecond),
                ..Default::default()
            }],
            true,
        );

        // inferred window size should be 600 sec
        assert_eq!(
            vec![
                TimestampRange::with_unit(0, 600, TimeUnit::Second).unwrap(),
                TimestampRange::with_unit(600, 1200, TimeUnit::Second).unwrap(),
                TimestampRange::with_unit(1200, 1800, TimeUnit::Second).unwrap(),
                TimestampRange::with_unit(1800, 2400, TimeUnit::Second).unwrap(),
            ],
            res
        );

        let res = window_inference.infer_window(
            &[
                FileMeta {
                    time_range: Some((
                        Timestamp::new(0, TimeUnit::Millisecond),
                        Timestamp::new(60 * 1000, TimeUnit::Millisecond),
                    )),
                    level: 1, // this SST will be ignored
                    ..Default::default()
                },
                FileMeta {
                    time_range: Some((
                        Timestamp::new(0, TimeUnit::Millisecond),
                        Timestamp::new(10 * 60 * 1000, TimeUnit::Millisecond),
                    )),
                    ..Default::default()
                },
            ],
            &[MemtableStats {
                max_timestamp: Timestamp::new(60 * 30 * 1000 + 1, TimeUnit::Millisecond),
                min_timestamp: Timestamp::new(0, TimeUnit::Millisecond),
                ..Default::default()
            }],
            false,
        );

        // timestamp asc order
        assert_eq!(
            vec![
                TimestampRange::with_unit(1800, 2400, TimeUnit::Second).unwrap(),
                TimestampRange::with_unit(1200, 1800, TimeUnit::Second).unwrap(),
                TimestampRange::with_unit(600, 1200, TimeUnit::Second).unwrap(),
                TimestampRange::with_unit(0, 600, TimeUnit::Second).unwrap(),
            ],
            res
        );
    }
}
