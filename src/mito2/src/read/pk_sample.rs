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

//! Deterministic primary-key interval sampling for classic (non-metric) tables.
//!
//! A classic table stores its rows in encoded-primary-key order, and every SST
//! file records the encoded key range of its rows in the manifest. Given a
//! requested fraction `num/den`, [`choose_interval`] builds the cumulative
//! row-count step function over those ranges, inverts it once, and returns one
//! contiguous key interval covering about that fraction of the rows.
//!
//! The interval is applied as an ordinary range predicate on the encoded key, so
//! the existing pruning paths see it: whole files are pruned by their manifest
//! range, whole row groups by their encoded key statistics, and the rows of a
//! straddling row group are filtered right after decoding. A hash of the key
//! would keep the same fraction of the rows but destroy the locality that lets
//! those two levels prune.

use bytes::Bytes;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datatypes::arrow::array::{Array, BinaryArray, BooleanArray};
use datatypes::arrow::compute::cast;
use datatypes::arrow::datatypes::DataType;
use datatypes::arrow::record_batch::RecordBatch;
use snafu::{OptionExt, ResultExt};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::error::{ComputeArrowSnafu, InvalidRecordBatchSnafu, Result};
use crate::metrics::PK_SAMPLE_STATS;

/// Encoded primary-key statistics of one SST file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PkSampleFileStat {
    /// Minimum encoded primary key of the file.
    pub min: Bytes,
    /// Maximum encoded primary key of the file.
    pub max: Bytes,
    /// Number of rows in the file.
    pub num_rows: u64,
}

/// The inclusive interval of encoded primary keys kept by a sample.
///
/// Both bounds are actual keys taken from manifest statistics, so an inclusive
/// upper bound never keeps a row that the covering files do not contain. The
/// scan reads `lo <= key <= hi`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PkSampleInterval {
    /// First kept encoded primary key.
    pub lo: Bytes,
    /// Last kept encoded primary key.
    pub hi: Bytes,
}

impl PkSampleInterval {
    /// Returns whether `key` is inside the interval.
    pub(crate) fn covers(&self, key: &[u8]) -> bool {
        self.lo.as_ref() <= key && key <= self.hi.as_ref()
    }

    /// Returns whether the key bounds `[min, max]` intersect the interval.
    ///
    /// Unknown bounds must not be passed here: they are included instead.
    pub(crate) fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        !(max < self.lo.as_ref() || self.hi.as_ref() < min)
    }
}

/// Result of planning a sample over the files a scan would read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PkSamplePlan {
    /// The chosen interval, or `None` if no file carries key statistics, which the
    /// scan rejects unless there is nothing to sample at all.
    pub interval: Option<PkSampleInterval>,
    /// In-range files without primary-key statistics. They cannot be ordered by
    /// key, so the scan keeps them whole (over-sampling) and counts them.
    pub files_without_stats: usize,
    /// Rows of the files the interval overlaps, summed by file: a file that only one
    /// boundary of the interval cuts through still counts whole.
    pub rows_covered: u64,
    /// Rows the request asks for, `num/den` of `rows_total`.
    pub rows_target: u64,
    /// Rows of the files the step function was built from.
    pub rows_total: u64,
}

/// What a planned primary-key sample covers, in rows, for the scan's metrics.
///
/// The figures are the ones [`plan_from_stats`] derived from the manifest row counts
/// of the files this scan reads, plus the row counts of the in-range files it could
/// not order. They describe the sample this scan runs, not the fraction of the
/// request, so `EXPLAIN ANALYZE` reports the coverage the plan actually chose.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PkSampleStats {
    /// Rows of the candidate files the sample could order by key, summed from their
    /// manifest row counts. Files the key predicate excludes are not counted, files
    /// without encoded-key statistics are counted by `rows_without_stats` instead, and a
    /// file whose key range overlaps another candidate is still counted although the
    /// step function the interval is inverted against leaves it out.
    pub rows_total: u64,
    /// Upper bound of the rows the sample keeps, at file granularity: the manifest
    /// `num_rows` of every file whose key range the interval overlaps, summed. A file the
    /// interval covers only in part still contributes all of its rows, so this is not the
    /// number of rows the scan returns; the scan node's `output_rows` is.
    pub rows_covered: u64,
    /// Rows held by in-range files without encoded-key statistics. Such a file cannot
    /// be placed on the key axis, so the sample never prunes the file and the interval
    /// does not account for its rows. Its row groups are still pruned by their own key
    /// statistics, so this is an upper bound of the rows the file contributes.
    pub rows_without_stats: u64,
}

impl PkSampleStats {
    /// Reports the sample as counters of the scan node, once per sampled scan.
    ///
    /// The counters are registered here, so an unsampled scan never shows these names,
    /// not even with a `0` value. They are figures of the whole scan, not of one
    /// partition, so the caller reports them from a single partition.
    pub(crate) fn record(&self, metrics_set: &ExecutionPlanMetricsSet) {
        MetricBuilder::new(metrics_set)
            .global_counter("sample_rows_total")
            .add(self.rows_total as usize);
        MetricBuilder::new(metrics_set)
            .global_counter("sample_rows_in_kept_files")
            .add(self.rows_covered as usize);
        if self.rows_without_stats > 0 {
            MetricBuilder::new(metrics_set)
                .global_counter("sample_rows_unaccounted")
                .add(self.rows_without_stats as usize);
        }
    }
}

/// Chooses the key interval covering about `num/den` of the rows of `files`.
///
/// The files are ordered by their minimum key and the row counts are summed in
/// that order, which yields the step function `F(key) = rows before key`. The
/// deterministic proof-of-concept start is `s = 0`, so the interval is the
/// prefix whose cumulative row count first reaches `num/den` of the total.
///
/// Files whose ranges overlap a file already accepted are left out of the step
/// function: a classic table's compaction output is key-disjoint, but the region
/// file list is not sorted and may overlap across levels, and counting the same
/// keys twice would inflate the interval. They are still scanned, so the sample
/// can over- instead of under-shoot.
///
/// Returns `None` if there is nothing to sample.
pub(crate) fn choose_interval(
    files: &[PkSampleFileStat],
    num: u32,
    den: u32,
) -> Option<PkSampleInterval> {
    if num == 0 || num >= den || files.is_empty() {
        return None;
    }

    let mut sorted: Vec<&PkSampleFileStat> = files.iter().collect();
    sorted.sort_by(|a, b| a.min.cmp(&b.min));

    // Greedy key-disjoint chain: a file that starts at or before the maximum key
    // of the last accepted file overlaps it.
    let mut chain: Vec<&PkSampleFileStat> = Vec::with_capacity(sorted.len());
    let mut last_max: Option<&Bytes> = None;
    for file in sorted {
        if let Some(max) = last_max
            && file.min <= *max
        {
            continue;
        }
        last_max = Some(&file.max);
        chain.push(file);
    }

    let total: u128 = chain.iter().map(|file| u128::from(file.num_rows)).sum();
    if total == 0 {
        return None;
    }
    // `num < den`, so the target is strictly smaller than the total; round up so a
    // non-zero fraction always keeps at least one row.
    let target = (total * u128::from(num)).div_ceil(u128::from(den)).max(1);

    let mut covered = 0u128;
    let mut lo: Option<Bytes> = None;
    let mut hi: Option<Bytes> = None;
    for file in &chain {
        if file.num_rows == 0 {
            // A zero-row file contributes no rows; keep it out of the bounds.
            continue;
        }
        lo.get_or_insert_with(|| file.min.clone());
        hi = Some(file.max.clone());
        covered += u128::from(file.num_rows);
        if covered >= target {
            break;
        }
    }

    Some(PkSampleInterval { lo: lo?, hi: hi? })
}

/// Plans a sample over the encoded key statistics of the files a scan would read.
///
/// `files_without_stats` counts the in-range files that carry no key statistics;
/// they cannot be ordered by key, so the scan keeps them whole and only counts
/// them. When no file carries statistics at all, `interval` is `None` and the
/// caller must reject the request instead of reading every row.
pub(crate) fn plan_from_stats(
    stats: Vec<PkSampleFileStat>,
    num: u32,
    den: u32,
    files_without_stats: usize,
) -> PkSamplePlan {
    let rows_total = stats
        .iter()
        .fold(0u64, |total, file| total.saturating_add(file.num_rows));

    let interval = choose_interval(&stats, num, den);
    let rows_target = interval
        .as_ref()
        .map(|_| {
            let total = u128::from(rows_total);
            u64::try_from((total * u128::from(num)) / u128::from(den)).unwrap_or(u64::MAX)
        })
        .unwrap_or(0);
    let rows_covered = interval
        .as_ref()
        .map(|interval| {
            // File granularity: the files are the only unit with a row count here, so a
            // file the interval cuts through contributes all of its rows. That is why the
            // counter name says "in kept files" instead of "kept".
            stats
                .iter()
                .filter(|file| interval.overlaps(&file.min, &file.max))
                .map(|file| file.num_rows)
                .sum()
        })
        .unwrap_or(0);

    PkSamplePlan {
        interval,
        files_without_stats,
        rows_covered,
        rows_target,
        rows_total,
    }
}

/// Records one sampling counter.
pub(crate) fn inc(kind: &'static str, value: u64) {
    PK_SAMPLE_STATS.with_label_values(&[kind]).inc_by(value);
}

/// Applies the exact interval test to the encoded primary keys of a flat batch.
///
/// The flat format stores the encoded key of every row in its `__primary_key`
/// column (dictionary encoded or plain binary), so the test needs no re-encoding
/// and drops a series everywhere or nowhere: every source of a scan, memtable or
/// SST, is filtered by the same bytes.
///
/// Returns `None` when no row of the batch is kept.
pub(crate) fn filter_flat_batch(
    batch: RecordBatch,
    interval: &PkSampleInterval,
) -> Result<Option<RecordBatch>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Some(batch));
    }

    let pk_index = match batch.schema().index_of(PRIMARY_KEY_COLUMN_NAME) {
        Ok(pk_index) => pk_index,
        Err(_) => {
            return InvalidRecordBatchSnafu {
                reason: format!(
                    "a sampled flat batch must carry a {PRIMARY_KEY_COLUMN_NAME} column, got {:?}",
                    batch.schema()
                ),
            }
            .fail();
        }
    };
    let column = batch.column(pk_index);
    let values = if matches!(column.data_type(), DataType::Dictionary(_, _)) {
        cast(column.as_ref(), &DataType::Binary).context(ComputeArrowSnafu)?
    } else {
        column.clone()
    };
    let values = values
        .as_any()
        .downcast_ref::<BinaryArray>()
        .with_context(|| InvalidRecordBatchSnafu {
            reason: format!(
                "expected a binary {PRIMARY_KEY_COLUMN_NAME} column, got {:?}",
                values.data_type()
            ),
        })?;

    let mut mask = Vec::with_capacity(num_rows);
    let mut kept = 0usize;
    for value in values.iter() {
        let keep = value.is_some_and(|value| interval.covers(value));
        kept += usize::from(keep);
        mask.push(keep);
    }

    if kept == num_rows {
        return Ok(Some(batch));
    }
    inc("rows_filtered", (num_rows - kept) as u64);
    if kept == 0 {
        return Ok(None);
    }

    let mask = BooleanArray::from(mask);
    let filtered =
        datatypes::arrow::compute::filter_record_batch(&batch, &mask).context(ComputeArrowSnafu)?;
    Ok(Some(filtered))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        DictionaryArray, TimestampMillisecondArray, UInt8Array, UInt32Array, UInt64Array,
    };
    use datatypes::arrow::datatypes::{Field, Schema, TimeUnit, UInt32Type};

    use super::*;
    use crate::sst::parquet::flat_format::primary_key_column_index;

    fn stat(min: &[u8], max: &[u8], num_rows: u64) -> PkSampleFileStat {
        PkSampleFileStat {
            min: Bytes::copy_from_slice(min),
            max: Bytes::copy_from_slice(max),
            num_rows,
        }
    }

    /// Rows of `files` that an interval keeps, counting a straddling file whole.
    fn covered_rows(files: &[PkSampleFileStat], interval: &PkSampleInterval) -> u64 {
        files
            .iter()
            .filter(|file| interval.overlaps(&file.min, &file.max))
            .map(|file| file.num_rows)
            .sum()
    }

    #[test]
    fn test_choose_interval_covers_requested_fraction() {
        // Two key-disjoint files of 100 rows each, with a key gap between them and a
        // zero-row file in the middle that carries statistics but no rows.
        let files = vec![
            stat(b"a000", b"a099", 100),
            stat(b"b500", b"b500", 0),
            stat(b"c000", b"c099", 100),
        ];
        let total = 200;

        let header = choose_interval(&files, 1, 4).unwrap();
        // A quarter of 200 rows is the first 50 rows of the first file; the interval
        // ends at that file's maximum key, because the interval can only be stated in
        // whole file bounds here.
        assert_eq!(b"a000".as_slice(), header.lo.as_ref());
        assert_eq!(b"a099".as_slice(), header.hi.as_ref());
        let covered = covered_rows(&files, &header);
        assert_eq!(100, covered);
        assert!((covered as f64 / total as f64) <= 0.5);

        // Half the rows: still the first file only (it ends exactly at the target).
        let half = choose_interval(&files, 1, 2).unwrap();
        assert_eq!(b"a000".as_slice(), half.lo.as_ref());
        assert_eq!(b"a099".as_slice(), half.hi.as_ref());

        // Three quarters: the second file with rows must be included, and the zero-row
        // file must not move the bounds.
        let three_quarters = choose_interval(&files, 3, 4).unwrap();
        assert_eq!(b"a000".as_slice(), three_quarters.lo.as_ref());
        assert_eq!(b"c099".as_slice(), three_quarters.hi.as_ref());
        assert_eq!(200, covered_rows(&files, &three_quarters));

        // The un-sorted list gives the same answer: files are ordered by their minimum.
        let mut shuffled = files.clone();
        shuffled.reverse();
        assert_eq!(Some(three_quarters), choose_interval(&shuffled, 3, 4));
    }

    #[test]
    fn test_choose_interval_skips_overlapping_files_in_step_function() {
        // A level-0 file fully inside a level-1 file must not be counted twice.
        let files = vec![
            stat(b"a000", b"a099", 100),
            stat(b"a000", b"a099", 100),
            stat(b"b000", b"b099", 100),
        ];
        let interval = choose_interval(&files, 1, 3).unwrap();
        assert_eq!(b"a000".as_slice(), interval.lo.as_ref());
        assert_eq!(b"a099".as_slice(), interval.hi.as_ref());
    }

    #[test]
    fn test_choose_interval_rejects_degenerate_fraction() {
        let files = vec![stat(b"a", b"b", 10)];
        assert_eq!(None, choose_interval(&files, 0, 4));
        assert_eq!(None, choose_interval(&files, 4, 4));
        assert_eq!(None, choose_interval(&files, 5, 4));
        assert_eq!(None, choose_interval(&[], 1, 4));
        assert_eq!(None, choose_interval(&[stat(b"a", b"b", 0)], 1, 4));
    }

    #[test]
    fn test_plan_counts_files_without_stats() {
        let plan = plan_from_stats(vec![stat(b"a", b"b", 10)], 1, 2, 1);
        assert_eq!(1, plan.files_without_stats);
        assert_eq!(10, plan.rows_total);
        assert_eq!(5, plan.rows_target);
        assert_eq!(10, plan.rows_covered);
        assert!(plan.interval.is_some());

        // Only a file without statistics: nothing can be ordered, so there is no
        // interval and the scan rejects the request instead of reading every row.
        let plan = plan_from_stats(vec![], 1, 2, 1);
        assert_eq!(1, plan.files_without_stats);
        assert!(plan.interval.is_none());
    }

    fn flat_batch(keys: &[&[u8]]) -> RecordBatch {
        let num_rows = keys.len();
        let values = Arc::new(BinaryArray::from_iter_values(keys.iter().copied()));
        let pk = Arc::new(DictionaryArray::<UInt32Type>::new(
            UInt32Array::from_iter_values(0..keys.len() as u32),
            values,
        ));
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("__primary_key", pk.data_type().clone(), false),
                Field::new("__sequence", DataType::UInt64, false),
                Field::new("__op_type", DataType::UInt8, false),
            ])),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1i64; num_rows])),
                pk,
                Arc::new(UInt64Array::from(vec![1u64; num_rows])),
                Arc::new(UInt8Array::from(vec![1u8; num_rows])),
            ],
        )
        .unwrap()
    }

    fn keys_of(batch: &RecordBatch) -> Vec<Vec<u8>> {
        let pk = batch.column(primary_key_column_index(batch.num_columns()));
        let values = cast(pk.as_ref(), &DataType::Binary).unwrap();
        let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
        values.iter().map(|v| v.unwrap().to_vec()).collect()
    }

    #[test]
    fn test_filter_flat_batch_by_interval() {
        let interval = PkSampleInterval {
            lo: Bytes::from_static(b"b"),
            hi: Bytes::from_static(b"d"),
        };
        let batch = flat_batch(&[b"a", b"b", b"c", b"d", b"e"]);
        let filtered = filter_flat_batch(batch, &interval)
            .unwrap()
            .expect("some rows are kept");
        assert_eq!(
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
            keys_of(&filtered)
        );

        // A series is kept entirely or dropped entirely.
        let batch = flat_batch(&[b"c", b"c", b"c"]);
        let filtered = filter_flat_batch(batch, &interval).unwrap().unwrap();
        assert_eq!(3, filtered.num_rows());

        let batch = flat_batch(&[b"a", b"e"]);
        assert!(
            filter_flat_batch(batch, &interval).unwrap().is_none(),
            "no row is inside the interval"
        );
    }

    #[test]
    fn test_interval_overlaps() {
        let interval = PkSampleInterval {
            lo: Bytes::from_static(b"b"),
            hi: Bytes::from_static(b"d"),
        };
        assert!(interval.overlaps(b"a", b"b"));
        assert!(interval.overlaps(b"c", b"c"));
        assert!(interval.overlaps(b"d", b"z"));
        assert!(!interval.overlaps(b"a", b"az"));
        assert!(!interval.overlaps(b"e", b"z"));
    }
}
