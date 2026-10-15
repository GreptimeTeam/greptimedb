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

use std::sync::Arc;

use arrow::buffer::ScalarBuffer;
use snafu::OptionExt;

use crate::error::{InvalidSeriesSnafu, Result};

#[cfg(test)]
mod tests;

/// Value description of a non-empty, fixed-step evaluation grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GridSpec {
    /// First evaluation timestamp in milliseconds.
    pub start_ms: i64,
    /// Positive time step in milliseconds.
    pub step_ms: i64,
    /// Number of evaluation points.
    pub len: u64,
}

impl GridSpec {
    /// Creates a validated grid specification.
    pub fn try_new(start_ms: i64, step_ms: i64, len: u64) -> Result<Self> {
        if step_ms <= 0 || len == 0 {
            return invalid("grid step must be positive and grid length nonzero");
        }
        let Some(last) = ((len - 1) as i128)
            .checked_mul(step_ms as i128)
            .and_then(|offset| (start_ms as i128).checked_add(offset))
        else {
            return invalid("grid timestamp overflows i128");
        };
        let Ok(platform_len) = usize::try_from(len) else {
            return invalid("grid timestamp or length overflows supported range");
        };
        if i64::try_from(last).is_err()
            || platform_len
                .checked_mul(std::mem::size_of::<i64>())
                .is_none()
        {
            return invalid("grid timestamp or length overflows supported range");
        }
        Ok(Self {
            start_ms,
            step_ms,
            len,
        })
    }

    /// Creates a grid for an inclusive query range, retaining the last point not after end.
    pub fn try_from_range(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<Self> {
        if step_ms <= 0 || end_ms < start_ms {
            return invalid("invalid inclusive grid range or step");
        }
        let len = ((end_ms as i128 - start_ms as i128) / step_ms as i128) + 1;
        Self::try_new(
            start_ms,
            step_ms,
            u64::try_from(len).map_err(|_| invalid_series("grid length overflow"))?,
        )
    }
}

/// A materialized, shareable timestamp grid.
#[derive(Debug)]
pub struct EvalGrid {
    spec: GridSpec,
    timestamps: ScalarBuffer<i64>,
}

impl EvalGrid {
    /// Allocates the grid timestamps once for sharing across result series.
    pub fn try_new(spec: GridSpec) -> Result<Self> {
        GridSpec::try_new(spec.start_ms, spec.step_ms, spec.len)?;
        let len =
            usize::try_from(spec.len).map_err(|_| invalid_series("grid length overflows usize"))?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(len)
            .map_err(|_| invalid_series("grid timestamp allocation failed"))?;
        for index in 0..len {
            // The validated endpoint bounds every point in this positive-step grid.
            let timestamp = (spec.start_ms as i128) + (index as i128) * (spec.step_ms as i128);
            values.push(timestamp as i64);
        }
        Ok(Self {
            spec,
            timestamps: values.into(),
        })
    }

    /// Returns the value specification.
    pub fn spec(&self) -> GridSpec {
        self.spec
    }

    /// Returns the shared timestamp buffer.
    pub fn timestamps(&self) -> &ScalarBuffer<i64> {
        &self.timestamps
    }
}

/// A contiguous range referencing a shared evaluation grid.
#[derive(Clone, Debug)]
pub struct GridRange {
    grid: Arc<EvalGrid>,
    start: usize,
    len: usize,
}

impl GridRange {
    /// Creates a checked subrange; an empty range may begin at the grid end.
    pub fn try_new(grid: Arc<EvalGrid>, start: usize, len: usize) -> Result<Self> {
        let end = start.checked_add(len).context(InvalidSeriesSnafu {
            reason: "grid range overflow".to_string(),
        })?;
        if end > grid.timestamps.len() {
            return invalid("grid range exceeds its grid");
        }
        Ok(Self { grid, start, len })
    }

    /// Returns the shared grid.
    pub fn grid(&self) -> &Arc<EvalGrid> {
        &self.grid
    }
    /// Returns the start index in the grid.
    pub fn start(&self) -> usize {
        self.start
    }
    /// Returns the range length.
    pub fn len(&self) -> usize {
        self.len
    }
    /// Returns whether this range is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    /// Returns this range as a zero-copy Arrow buffer slice.
    pub fn timestamps(&self) -> ScalarBuffer<i64> {
        self.grid.timestamps.slice(self.start, self.len)
    }
}

impl PartialEq for GridRange {
    fn eq(&self, other: &Self) -> bool {
        self.grid.spec == other.grid.spec && self.start == other.start && self.len == other.len
    }
}
impl Eq for GridRange {}

/// A timestamp axis represented as a grid or explicit shared Arrow buffers.
///
/// Checked constructors validate timestamp ordering. Public payloads are validated again by the
/// downstream series boundary.
#[derive(Clone, Debug)]
pub enum TimeAxis {
    /// A slice of a shared regular evaluation grid.
    Grid(GridRange),
    /// An explicit timestamp sequence.
    Explicit(ScalarBuffer<i64>),
    /// Explicit timestamps held in ordered chunks without flattening.
    ExplicitChunks(Arc<[ScalarBuffer<i64>]>),
}

impl TimeAxis {
    /// Validates an explicit, strictly increasing timestamp sequence.
    pub fn try_new(timestamps: ScalarBuffer<i64>) -> Result<Self> {
        if timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
            return invalid("timestamps must be strictly increasing");
        }
        Ok(Self::Explicit(timestamps))
    }

    /// Validates and stores timestamp chunks without concatenating their buffers.
    pub fn try_from_chunks(chunks: Vec<ScalarBuffer<i64>>) -> Result<Self> {
        let mut previous = None;
        let mut nonempty = Vec::new();
        for chunk in chunks {
            if chunk.windows(2).any(|pair| pair[0] >= pair[1])
                || chunk
                    .first()
                    .is_some_and(|first| previous.is_some_and(|last| last >= *first))
            {
                return invalid("timestamps must be strictly increasing across chunks");
            }
            if let Some(last) = chunk.last() {
                previous = Some(*last);
            }
            if !chunk.is_empty() {
                nonempty.push(chunk);
            }
        }
        Ok(Self::ExplicitChunks(nonempty.into()))
    }

    /// Returns the timestamp count.
    pub fn len(&self) -> usize {
        match self {
            Self::Grid(range) => range.len,
            Self::Explicit(values) => values.len(),
            Self::ExplicitChunks(chunks) => chunks.iter().map(|chunk| chunk.len()).sum(),
        }
    }
    /// Returns whether there are no timestamps.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a timestamp by checked index.
    pub fn timestamp(&self, index: usize) -> Option<i64> {
        match self {
            Self::Grid(range) if index < range.len => {
                range.grid.timestamps.get(range.start + index).copied()
            }
            Self::Explicit(values) => values.get(index).copied(),
            Self::ExplicitChunks(chunks) => {
                let mut offset = index;
                for chunk in chunks.iter() {
                    if offset < chunk.len() {
                        return chunk.get(offset).copied();
                    }
                    offset -= chunk.len();
                }
                None
            }
            _ => None,
        }
    }

    /// Returns a zero-copy slice, retaining Arrow owners; chunked axes remain chunked.
    pub fn slice(&self, offset: usize, len: usize) -> Result<Self> {
        let end = offset.checked_add(len).context(InvalidSeriesSnafu {
            reason: "timestamp slice overflow".to_string(),
        })?;
        if end > self.len() {
            return invalid("timestamp slice exceeds axis");
        }
        match self {
            Self::Grid(range) => Ok(Self::Grid(GridRange::try_new(
                range.grid.clone(),
                range.start + offset,
                len,
            )?)),
            Self::Explicit(values) => Ok(Self::Explicit(values.slice(offset, len))),
            Self::ExplicitChunks(chunks) => {
                let mut remain = len;
                let mut skip = offset;
                let mut sliced = Vec::new();
                for chunk in chunks.iter() {
                    if skip >= chunk.len() {
                        skip -= chunk.len();
                        continue;
                    }
                    let take = remain.min(chunk.len() - skip);
                    if take > 0 {
                        sliced.push(chunk.slice(skip, take));
                        remain -= take;
                    }
                    skip = 0;
                    if remain == 0 {
                        break;
                    }
                }
                Ok(Self::ExplicitChunks(sliced.into()))
            }
        }
    }
}

fn invalid_series(reason: &str) -> crate::error::Error {
    InvalidSeriesSnafu {
        reason: reason.to_string(),
    }
    .build()
}

fn invalid<T>(reason: &str) -> Result<T> {
    InvalidSeriesSnafu {
        reason: reason.to_string(),
    }
    .fail()
}
