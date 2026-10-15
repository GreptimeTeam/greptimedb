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

use arrow::buffer::{Buffer, ScalarBuffer};
use snafu::OptionExt;

use crate::error::{InvalidSeriesSnafu, Result};
use crate::series::samples::{SeriesFields, SeriesSchema};
use crate::series::{MetricName, invalid_series};

/// Explicit source timestamps in milliseconds for a raw series chunk.
#[derive(Clone, Debug)]
pub enum RawTimeAxis {
    /// Source timestamps in strictly increasing order.
    Explicit(ScalarBuffer<i64>),
}

impl RawTimeAxis {
    /// Validates and wraps strictly increasing timestamps; an empty axis is valid on its own.
    pub fn try_new(timestamps: ScalarBuffer<i64>) -> Result<Self> {
        if timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
            return invalid_series("raw timestamps must be strictly increasing");
        }
        Ok(Self::Explicit(timestamps))
    }

    /// Returns the number of timestamps.
    pub fn len(&self) -> usize {
        match self {
            Self::Explicit(values) => values.len(),
        }
    }

    /// Returns whether the axis has no timestamps.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the timestamp at `index`, or `None` when out of bounds.
    pub fn timestamp(&self, index: usize) -> Option<i64> {
        match self {
            Self::Explicit(values) => values.get(index).copied(),
        }
    }

    /// Returns a zero-copy slice, reporting overflowing or out-of-range bounds as an error.
    pub fn slice(&self, offset: usize, len: usize) -> Result<Self> {
        let end = offset
            .checked_add(len)
            .with_context(|| InvalidSeriesSnafu {
                reason: "raw time axis slice bounds overflow".to_string(),
            })?;
        if end > self.len() {
            return invalid_series("raw time axis slice is out of bounds");
        }
        match self {
            Self::Explicit(values) => Ok(Self::Explicit(values.slice(offset, len))),
        }
    }
}

/// Ordering guarantee for chunks in one input stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawSeriesOrdering {
    /// Each read key's chunks are contiguous; distinct read keys need not be ordered.
    Contiguous,
    /// Chunks are ordered by read key and then by timestamp.
    ReadKeyThenTime,
}

/// One raw source batch, with fields aligned to its timestamps.
///
/// Public fields allow direct access and construction, which bypasses the checks in
/// [`RawSeriesChunk::try_new`]; callers crossing a trust boundary must validate input.
#[derive(Clone, Debug)]
pub struct RawSeriesChunk {
    /// Complete encoded identity of the source series, not a TSID or hash alone.
    pub read_key: Buffer,
    /// `None` leaves labels unmaterialized in the complete read key; `Some` contains
    /// complete explicit labels.
    pub metric: Option<MetricName>,
    /// Source timestamps aligned with every field.
    pub time: RawTimeAxis,
    /// Field values aligned with the source timestamps.
    pub fields: SeriesFields,
    /// Whether this is the final chunk for this input stream's series, not all regions.
    pub is_last: bool,
}

impl RawSeriesChunk {
    /// Creates a nonempty chunk after checking its explicit time axis and schema alignment.
    ///
    /// Validation is local to this chunk and does not verify ordering between chunks.
    pub fn try_new(
        schema: &SeriesSchema,
        read_key: Buffer,
        metric: Option<MetricName>,
        time: RawTimeAxis,
        fields: SeriesFields,
        is_last: bool,
    ) -> Result<Self> {
        if schema.eval_grid.is_some() {
            return invalid_series("raw series schema cannot have an evaluation grid");
        }
        if fields.len() != schema.fields.len() {
            return invalid_series("raw series field count does not match schema");
        }
        if fields.iter().any(|field| field.len() != time.len()) {
            return invalid_series("raw series field length does not match time axis");
        }
        if time.is_empty() {
            return invalid_series("raw series chunks must not be empty");
        }
        let RawTimeAxis::Explicit(timestamps) = &time;
        if timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
            return invalid_series("raw series timestamps must be strictly increasing");
        }
        Ok(Self {
            read_key,
            metric,
            time,
            fields,
            is_last,
        })
    }
}
