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
use crate::series::{SeriesLabels, invalid_series};

/// Strictly increasing source timestamps in milliseconds for a raw series chunk.
///
/// Its private buffer ensures timestamps are validated once at construction. Direct
/// construction is intentionally unavailable:
///
/// ```compile_fail
/// use arrow::buffer::ScalarBuffer;
/// use datatypes::series::raw::RawTimestamps;
///
/// let timestamps = RawTimestamps {
///     values: ScalarBuffer::from(vec![1_i64, 2]),
/// };
/// ```
#[derive(Clone, Debug)]
pub struct RawTimestamps {
    values: ScalarBuffer<i64>,
}

impl RawTimestamps {
    /// Validates and wraps strictly increasing timestamps; an empty wrapper is valid.
    pub fn try_new(values: ScalarBuffer<i64>) -> Result<Self> {
        if values.windows(2).any(|pair| pair[0] >= pair[1]) {
            return invalid_series("raw timestamps must be strictly increasing");
        }
        Ok(Self { values })
    }

    /// Returns the underlying timestamp buffer.
    pub fn as_buffer(&self) -> &ScalarBuffer<i64> {
        &self.values
    }

    /// Returns the number of timestamps.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether there are no timestamps.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the timestamp at `index`, or `None` when out of bounds.
    pub fn timestamp(&self, index: usize) -> Option<i64> {
        self.values.get(index).copied()
    }

    /// Returns a zero-copy slice, reporting overflowing or out-of-range bounds as an error.
    pub fn slice(&self, offset: usize, len: usize) -> Result<Self> {
        let end = offset
            .checked_add(len)
            .with_context(|| InvalidSeriesSnafu {
                reason: "raw timestamps slice bounds overflow".to_string(),
            })?;
        if end > self.len() {
            return invalid_series("raw timestamps slice is out of bounds");
        }
        Ok(Self {
            values: self.values.slice(offset, len),
        })
    }
}

/// One raw source batch, with fields aligned to its timestamps.
///
/// `metric: None` means labels are not yet decoded because identity is bound by the source; it
/// does not mean the metric name is absent. An explicit `Some(metric)` is always complete.
///
/// Public fields allow direct access and construction, which bypasses the checks in
/// [`RawSeriesChunk::try_new`]; callers crossing a trust boundary must validate input.
#[derive(Clone, Debug)]
pub struct RawSeriesChunk {
    /// Complete encoded identity of the source series, not a TSID or hash alone.
    pub primary_key: Buffer,
    /// `None` denotes an encoded key under bound-label mode; `Some` contains complete
    /// explicit labels and retains their identity.
    pub metric: Option<SeriesLabels>,
    /// Source timestamps aligned with every field.
    pub time: RawTimestamps,
    /// Field values aligned with the source timestamps.
    pub fields: SeriesFields,
    /// Whether this is the final chunk for this series in the input stream exposing it.
    /// Merge code must recompute finalization globally across all relevant inputs before
    /// downstream finalization.
    pub is_last: bool,
}

impl RawSeriesChunk {
    /// Creates a nonempty chunk after checking its time axis and schema alignment.
    ///
    /// Validation is local to this chunk and does not verify ordering between chunks.
    pub fn try_new(
        schema: &SeriesSchema,
        primary_key: Buffer,
        metric: Option<SeriesLabels>,
        time: RawTimestamps,
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
        Ok(Self {
            primary_key,
            metric,
            time,
            fields,
            is_last,
        })
    }
}
