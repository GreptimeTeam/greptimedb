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

//! Rows a substitution produced, which a scan drops instead of delivering.
//!
//! A physical-sequence read (`ScanRequest::read_row_sequence`) drops every row whose number is
//! at or below the file's own substituted cut
//! ([`FileMeta::substituted_up_to`](crate::sst::file::FileMeta::substituted_up_to)) and records
//! the timestamps of the dropped rows here, so the caller recomputes those windows.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_recordbatch::adapter::{RecordBatchMetrics, SubstitutedRange, SubstitutedRowRanges};
use common_recordbatch::{OrderOption, RecordBatchStream, SendableRecordBatchStream};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datatypes::arrow::array::BooleanArray;
use datatypes::arrow::compute::filter_record_batch;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::schema::SchemaRef;
use futures::Stream;
use snafu::ResultExt;
use store_api::storage::SequenceNumber;

use crate::error::{ComputeArrowSnafu, Result};
use crate::read::timestamp_array_to_i64_slice;
use crate::sst::parquet::flat_format::{sequence_column_index, time_index_column_index};

/// Accumulates the timestamp ranges of the rows a scan substituted.
///
/// Shared by every reader of one scan: ranges are merged when read, so the caller sees a sorted,
/// non-overlapping set per region.
#[derive(Debug, Default)]
pub(crate) struct SubstitutedRowsAccumulator {
    /// Raw `(start_millis, end_millis)` ranges, both inclusive.
    ranges: Mutex<Vec<(i64, i64)>>,
}

pub(crate) type SubstitutedRowsAccumulatorRef = Arc<SubstitutedRowsAccumulator>;

impl SubstitutedRowsAccumulator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Records that the rows in `[start_millis, end_millis]` were substituted.
    pub(crate) fn record(&self, start_millis: i64, end_millis: i64) {
        self.lock().push((start_millis, end_millis));
    }

    /// Locks the recorded ranges.
    ///
    /// A panic while the ranges are held cannot leave them inconsistent (a push is the
    /// only mutation), so a poisoned lock is recovered instead of failing the scan.
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(i64, i64)>> {
        self.ranges.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Returns the merged ranges of `region_id`, sorted and non-overlapping.
    pub(crate) fn entries(&self, region_id: u64) -> Vec<SubstitutedRowRanges> {
        let mut ranges = self.lock().clone();
        if ranges.is_empty() {
            return Vec::new();
        }
        ranges.sort_unstable();
        let mut merged: Vec<(i64, i64)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            match merged.last_mut() {
                // Both ends are inclusive, so `start == last_end + 1` is still contiguous.
                Some(last) if start <= last.1.saturating_add(1) => {
                    last.1 = last.1.max(end);
                }
                _ => merged.push((start, end)),
            }
        }
        vec![SubstitutedRowRanges {
            region_id,
            ranges: merged
                .into_iter()
                .map(|(start, end)| SubstitutedRange {
                    start_millis: start,
                    end_millis: end,
                })
                .collect(),
        }]
    }
}

/// Drops the rows of `record_batch` whose physical sequence is at or below `cut`.
///
/// The batch must still carry the flat format's internal columns, i.e. this runs before
/// any projection that drops them. Returns `None` when every row is substituted. The
/// substituted rows' timestamp range is recorded in `ranges`, so the caller can report
/// which time windows it has to recompute.
pub(crate) fn filter_flat_batch_by_substituted_sequence(
    record_batch: RecordBatch,
    cut: Option<SequenceNumber>,
    ranges: &SubstitutedRowsAccumulator,
) -> Result<Option<RecordBatch>> {
    let Some(cut) = cut else {
        return Ok(Some(record_batch));
    };
    if record_batch.num_rows() == 0 {
        return Ok(Some(record_batch));
    }

    let sequence_column = record_batch.column(sequence_column_index(record_batch.num_columns()));
    let sequences = sequence_column
        .as_any()
        .downcast_ref::<datatypes::arrow::array::UInt64Array>()
        .ok_or_else(|| {
            crate::error::InvalidRecordBatchSnafu {
                reason: "the sequence column of a flat batch must be UInt64".to_string(),
            }
            .build()
        })?;
    // `Some(0)` says the column carries no per-row numbering at all (a legacy import): every row
    // is substituted, even when a producer left real numbers behind.
    let substituted_all = cut == 0;
    // `true` keeps the row: its number is above every substituted number, so this region never
    // manufactured it from a substitution.
    let keep = BooleanArray::from_iter(
        sequences
            .values()
            .iter()
            .map(|value| !substituted_all && *value > cut),
    );
    let keep_count = keep.true_count();
    if keep_count == record_batch.num_rows() {
        return Ok(Some(record_batch));
    }

    record_substituted_time_range(&record_batch, &keep, ranges)?;
    if keep_count == 0 {
        return Ok(None);
    }

    let filtered = filter_record_batch(&record_batch, &keep).context(ComputeArrowSnafu)?;
    Ok(Some(filtered))
}

/// Records the timestamp range of the rows `keep` rejects.
fn record_substituted_time_range(
    record_batch: &RecordBatch,
    keep: &BooleanArray,
    ranges: &SubstitutedRowsAccumulator,
) -> Result<()> {
    let time_index = record_batch.column(time_index_column_index(record_batch.num_columns()));
    let datatypes::arrow::datatypes::DataType::Timestamp(unit, _) = time_index.data_type() else {
        return crate::error::InvalidRecordBatchSnafu {
            reason: "the time index column of a flat batch must be a timestamp".to_string(),
        }
        .fail();
    };
    let unit = to_time_unit(*unit);
    let values: &[i64] = timestamp_array_to_i64_slice(time_index);

    let mut range: Option<(i64, i64)> = None;
    for (idx, keep_row) in keep.values().iter().enumerate() {
        if keep_row {
            continue;
        }
        let value = values[idx];
        range = Some(match range {
            Some((start, end)) => (start.min(value), end.max(value)),
            None => (value, value),
        });
    }
    let Some((start, end)) = range else {
        return Ok(());
    };

    // Report in milliseconds so a consumer can align them without knowing this region's time
    // unit. The start floors and the end ceils, so the range only widens.
    let start_millis = Timestamp::new(start, unit)
        .convert_to(TimeUnit::Millisecond)
        .map(|ts| ts.value())
        .unwrap_or(start);
    let end_millis = Timestamp::new(end, unit)
        .convert_to_ceil(TimeUnit::Millisecond)
        .map(|ts| ts.value())
        .unwrap_or(end);
    ranges.record(start_millis, end_millis);
    Ok(())
}

/// Maps the arrow time unit of the time index column to the engine's.
fn to_time_unit(unit: datatypes::arrow::datatypes::TimeUnit) -> TimeUnit {
    match unit {
        datatypes::arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
        datatypes::arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
        datatypes::arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
        datatypes::arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
    }
}

/// Wraps a scan stream so its terminal metrics carry what the scan substituted.
///
/// Mirrors how a region reports its snapshot watermark: the proof travels with the
/// result instead of a side channel, so a caller that stops reading early never
/// observes a range set that belongs to rows it did not receive.
pub(crate) struct SubstitutedRowRangesStream {
    stream: SendableRecordBatchStream,
    ranges: Option<SubstitutedRowsAccumulatorRef>,
    region_id: u64,
}

impl SubstitutedRowRangesStream {
    /// Wraps `stream`, or returns it unchanged when the scan reports no range.
    pub(crate) fn wrap(
        stream: SendableRecordBatchStream,
        ranges: Option<SubstitutedRowsAccumulatorRef>,
        region_id: u64,
    ) -> SendableRecordBatchStream {
        if ranges.is_none() {
            return stream;
        }
        Box::pin(Self {
            stream,
            ranges,
            region_id,
        })
    }
}

impl RecordBatchStream for SubstitutedRowRangesStream {
    fn name(&self) -> &str {
        self.stream.name()
    }

    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.stream.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        let mut metrics = self.stream.metrics().unwrap_or_default();
        if let Some(ranges) = &self.ranges {
            let substituted = ranges.entries(self.region_id);
            if !substituted.is_empty() {
                metrics.substituted_row_ranges = substituted;
            }
        }
        Some(metrics)
    }
}

impl Stream for SubstitutedRowRangesStream {
    type Item = common_recordbatch::error::Result<common_recordbatch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
