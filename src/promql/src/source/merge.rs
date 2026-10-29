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

#[cfg(test)]
mod tests;

use datafusion::arrow::buffer::ScalarBuffer;
use datafusion_common::DataFusionError;
use datatypes::error::{InvalidSeriesSnafu, Result};
use datatypes::series::histogram::HistogramView;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{SampleRef, Samples, SeriesFields, SeriesSchema};

/// Rebuilds one raw series from every piece of one complete physical identity, reporting each
/// allocation before it happens.
///
/// Reconstruction holds three intermediate vectors whose sizes depend on how the pieces interleave,
/// so what it needs cannot be known up front. The caller charges `reserve` as the work grows
/// instead of reserving a worst case it may never reach.
pub(crate) fn merge_raw_identity_charging(
    schema: &SeriesSchema,
    chunks: &[RawSeriesChunk],
    reserve: &mut dyn FnMut(usize) -> std::result::Result<(), DataFusionError>,
) -> Result<RawSeriesChunk> {
    let invalid = |reason: &str| {
        InvalidSeriesSnafu {
            reason: reason.to_string(),
        }
        .build()
    };
    let Some(first) = chunks.first() else {
        return Err(invalid("cannot merge an empty raw identity"));
    };
    if schema.eval_grid.is_some() {
        return Err(invalid("raw series schema cannot have an evaluation grid"));
    }
    let key = first.primary_key.clone();
    let labels = first
        .metric
        .as_ref()
        .ok_or_else(|| invalid("raw identity chunk has no explicit labels"))?
        .clone();
    for chunk in chunks {
        if chunk.primary_key != key {
            return Err(invalid("raw identity chunks have different primary keys"));
        }
        if chunk.metric.as_ref() != Some(&labels) {
            return Err(invalid(
                "raw identity chunks have different or missing labels",
            ));
        }
        if chunk.fields.len() != schema.fields.len()
            || chunk
                .fields
                .iter()
                .any(|field| field.len() != chunk.time.len())
        {
            return Err(invalid("raw identity chunk shape does not match schema"));
        }
        if chunk
            .time
            .as_buffer()
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid(
                "raw identity chunk timestamps are not strictly increasing",
            ));
        }
    }

    let mut row_count = 0usize;
    for chunk in chunks.iter() {
        row_count = row_count.saturating_add(chunk.time.len());
    }
    reserve(row_count.saturating_mul(std::mem::size_of::<(i64, usize, usize)>()))
        .map_err(|error| invalid(error.to_string().as_str()))?;
    let mut rows = Vec::with_capacity(row_count);
    for (chunk_index, chunk) in chunks.iter().enumerate() {
        for row_index in 0..chunk.time.len() {
            rows.push((chunk.time.as_buffer()[row_index], chunk_index, row_index));
        }
    }
    rows.sort_unstable();

    reserve(
        rows.len()
            .saturating_mul(std::mem::size_of::<(i64, usize, usize)>()),
    )
    .map_err(|error| invalid(error.to_string().as_str()))?;
    let mut selected = Vec::with_capacity(rows.len());
    let mut index = 0;
    while index < rows.len() {
        let mut end = index + 1;
        while end < rows.len() && rows[end].0 == rows[index].0 {
            end += 1;
        }
        for duplicate in &rows[index + 1..end] {
            for field_index in 0..schema.fields.len() {
                if !samples_equal(
                    chunks[rows[index].1]
                        .fields
                        .get(field_index)
                        .ok_or_else(|| invalid("field index out of bounds"))?
                        .sample(rows[index].2),
                    chunks[duplicate.1]
                        .fields
                        .get(field_index)
                        .ok_or_else(|| invalid("field index out of bounds"))?
                        .sample(duplicate.2),
                ) {
                    return Err(invalid("duplicate raw timestamp has conflicting samples"));
                }
            }
        }
        selected.push(rows[index]);
        index = end;
    }

    reserve(selected.len().saturating_mul(std::mem::size_of::<i64>()))
        .map_err(|error| invalid(error.to_string().as_str()))?;
    let mut times = Vec::with_capacity(selected.len());
    for row in &selected {
        times.push(row.0);
    }
    let mut output_fields = Vec::with_capacity(schema.fields.len());
    for field_index in 0..schema.fields.len() {
        let mut parts = Vec::new();
        let mut start = 0;
        while start < selected.len() {
            let (chunk_index, row_index) = (selected[start].1, selected[start].2);
            let mut end = start + 1;
            while end < selected.len()
                && selected[end].1 == chunk_index
                && selected[end].2 == selected[end - 1].2 + 1
            {
                end += 1;
            }
            let source = chunks[chunk_index]
                .fields
                .get(field_index)
                .ok_or_else(|| invalid("field index out of bounds"))?;
            reserve(std::mem::size_of::<Samples>())
                .map_err(|error| invalid(error.to_string().as_str()))?;
            parts.push(source.slice(row_index, end - start)?);
            start = end;
        }
        output_fields.push(Samples::try_from_chunks(parts)?);
    }
    RawSeriesChunk::try_new(
        schema,
        key,
        Some(labels),
        RawTimestamps::try_new(ScalarBuffer::from(times))?,
        SeriesFields::try_from_vec(output_fields)?,
        true,
    )
}

fn samples_equal(left: SampleRef<'_>, right: SampleRef<'_>) -> bool {
    match (left, right) {
        (SampleRef::Missing, SampleRef::Missing) => true,
        (SampleRef::Float(a), SampleRef::Float(b)) => a.to_bits() == b.to_bits(),
        (SampleRef::Histogram(a), SampleRef::Histogram(b)) => histograms_equal(a, b),
        _ => false,
    }
}

fn histograms_equal(a: HistogramView<'_>, b: HistogramView<'_>) -> bool {
    a.schema == b.schema
        && a.zero_threshold.to_bits() == b.zero_threshold.to_bits()
        && a.sum.to_bits() == b.sum.to_bits()
        && a.count.to_bits() == b.count.to_bits()
        && a.zero_count.to_bits() == b.zero_count.to_bits()
        && a.reset_hint == b.reset_hint
        && a.start_timestamp == b.start_timestamp
        && float_slices_equal(a.custom_values, b.custom_values)
        && a.positive.span_offsets == b.positive.span_offsets
        && a.positive.span_lengths == b.positive.span_lengths
        && float_slices_equal(a.positive.buckets, b.positive.buckets)
        && a.negative.span_offsets == b.negative.span_offsets
        && a.negative.span_lengths == b.negative.span_lengths
        && float_slices_equal(a.negative.buckets, b.negative.buckets)
}

fn float_slices_equal(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.to_bits() == b.to_bits())
}
