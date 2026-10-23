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

use std::borrow::Cow;
use std::sync::Arc;

use arrow::buffer::ScalarBuffer;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{InvalidSeriesSnafu, Result};

#[cfg(test)]
mod tests;

/// Value description of a non-empty, fixed-step evaluation grid.
///
/// The serde form is the wire form a series frame writes under `greptime.series.grid`: field names
/// in declaration order, no optional members. Two nodes compare that text byte for byte, so the
/// only place that spells it is a series frame's own grid metadata entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// A lazily materialized, shareable timestamp grid.
#[derive(Debug)]
pub struct EvalGrid {
    spec: GridSpec,
    timestamps: OnceCell<ScalarBuffer<i64>>,
}

impl EvalGrid {
    /// Allocates the grid timestamps once for sharing across result series.
    pub fn try_new(spec: GridSpec) -> Result<Self> {
        let grid = Self::try_deferred(spec)?;
        grid.timestamps()?;
        Ok(grid)
    }

    /// Creates a validated grid without allocating its timestamp array.
    pub fn try_deferred(spec: GridSpec) -> Result<Self> {
        let spec = GridSpec::try_new(spec.start_ms, spec.step_ms, spec.len)?;
        Ok(Self {
            spec,
            timestamps: OnceCell::new(),
        })
    }

    /// Returns the value specification.
    pub fn spec(&self) -> GridSpec {
        self.spec
    }

    /// Returns the shared timestamp buffer, materializing it on first access.
    pub fn timestamps(&self) -> Result<&ScalarBuffer<i64>> {
        self.timestamps.get_or_try_init(|| {
            let len = usize::try_from(self.spec.len)
                .map_err(|_| invalid_series("grid length overflows usize"))?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(len)
                .map_err(|_| invalid_series("grid timestamp allocation failed"))?;
            for index in 0..len {
                let timestamp =
                    (self.spec.start_ms as i128) + (index as i128) * (self.spec.step_ms as i128);
                values.push(timestamp as i64);
            }
            Ok(values.into())
        })
    }

    fn timestamp_at(&self, position: usize) -> Option<i64> {
        if position as u64 >= self.spec.len {
            return None;
        }
        let timestamp =
            (self.spec.start_ms as i128) + (position as i128) * (self.spec.step_ms as i128);
        // GridSpec validation bounds every position, and i128 holds usize * i64.
        Some(timestamp as i64)
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
        if end
            > usize::try_from(grid.spec.len)
                .map_err(|_| invalid_series("grid length overflows usize"))?
        {
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
    pub fn timestamps(&self) -> Result<ScalarBuffer<i64>> {
        Ok(self.grid.timestamps()?.slice(self.start, self.len))
    }
}

impl PartialEq for GridRange {
    fn eq(&self, other: &Self) -> bool {
        self.grid.spec == other.grid.spec && self.start == other.start && self.len == other.len
    }
}
impl Eq for GridRange {}

/// One contiguous run of positions on a shared evaluation grid.
///
/// The span is half-open: it names the absolute grid indices `start..start + len`. A span is
/// never empty, so `start + len - 1` is always its last position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GridSpan {
    start: usize,
    len: usize,
}

// A span is never empty (the checked constructors reject a zero length), so there is no
// `is_empty` to pair with `len`.
#[allow(clippy::len_without_is_empty)]
impl GridSpan {
    /// Creates a span of `len` positions starting at absolute grid index `start`.
    ///
    /// [`TimeAxis::from_grid_spans`] validates it against its grid; this constructor only records
    /// it.
    pub fn new(start: usize, len: usize) -> Self {
        Self { start, len }
    }

    /// Absolute grid index of the first position.
    pub fn start(&self) -> usize {
        self.start
    }
    /// Number of positions in the span.
    pub fn len(&self) -> usize {
        self.len
    }
}

/// Several ordered, non-overlapping runs of one shared evaluation grid.
///
/// The runs are the rows an axis carries: everything outside them holds no value, so an axis over
/// a few scattered positions costs a few spans instead of a row per empty position. The spans are
/// strictly increasing and never overlap (`prev.start + prev.len <= next.start`); they may sit
/// next to each other, and nothing is stored between them.
///
/// An axis with no rows is an empty span list that still carries its grid, so a consumer can tell
/// "nothing here" from "not on a grid".
#[derive(Clone, Debug)]
pub struct GridRanges {
    grid: Arc<EvalGrid>,
    /// The runs, in absolute grid order; no span is empty.
    spans: Arc<[GridSpan]>,
    /// Compact row of each span's first row, parallel to `spans`: prefix sums of the lengths,
    /// kept so that compact-row lookups need a binary search and not a scan.
    rows: Arc<[usize]>,
    /// Number of carried rows, cached so that [`Self::len`] never walks the spans.
    len: usize,
}

// `len` is the number of carried rows; "carries no row" is answered by `TimeAxis::is_empty`.
#[allow(clippy::len_without_is_empty)]
impl GridRanges {
    /// Creates a validated list of runs over `grid`.
    ///
    /// Rejects an empty span, a span past the end of the grid, spans that are out of order or
    /// overlap, and any length or index that overflows `usize`.
    ///
    /// Not public: a list of one span is not normalized into the single run a [`TimeAxis`] fast
    /// path expects, so callers outside this crate build axes through
    /// [`TimeAxis::from_grid_spans`], which keeps [`TimeAxis::Grid`] for a single span.
    pub(crate) fn try_new(grid: Arc<EvalGrid>, spans: Vec<GridSpan>) -> Result<Self> {
        let grid_len = usize::try_from(grid.spec.len)
            .map_err(|_| invalid_series("grid length overflows usize"))?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(spans.len())
            .map_err(|_| invalid_series("grid span allocation failed"))?;
        let mut len = 0usize;
        let mut previous_end: Option<usize> = None;
        for span in &spans {
            if span.len == 0 {
                return invalid("grid spans must not be empty");
            }
            let end = span
                .start
                .checked_add(span.len)
                .context(InvalidSeriesSnafu {
                    reason: "grid span overflow".to_string(),
                })?;
            if end > grid_len {
                return invalid("grid span exceeds its grid");
            }
            if previous_end.is_some_and(|previous_end| span.start < previous_end) {
                return invalid("grid spans must be ordered and must not overlap");
            }
            rows.push(len);
            len = len.checked_add(span.len).context(InvalidSeriesSnafu {
                reason: "grid span length overflow".to_string(),
            })?;
            previous_end = Some(end);
        }
        Ok(Self {
            grid,
            spans: spans.into(),
            rows: rows.into(),
            len,
        })
    }

    /// Creates an axis over `grid` that carries no row.
    pub fn empty(grid: Arc<EvalGrid>) -> Self {
        Self {
            grid,
            spans: Arc::from([]),
            rows: Arc::from([]),
            len: 0,
        }
    }

    /// Returns the shared grid the spans refer to.
    pub fn grid(&self) -> &Arc<EvalGrid> {
        &self.grid
    }

    /// Returns the number of carried rows, not the distance from the first to the last position.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Maps an absolute grid position to its compact row, or `None` when it holds no row.
    ///
    /// Binary search over the span starts: a point lookup never scans.
    pub fn row_of(&self, position: usize) -> Option<usize> {
        let index = self
            .spans
            .partition_point(|span| span.start <= position)
            .checked_sub(1)?;
        let span = self.spans[index];
        let offset = position - span.start;
        (offset < span.len).then(|| self.rows[index] + offset)
    }

    /// Returns the span holding compact row `index`, with the offset of the row inside it.
    fn span_of(&self, index: usize) -> Option<(GridSpan, usize)> {
        let span_index = self
            .rows
            .partition_point(|&row| row <= index)
            .checked_sub(1)?;
        let span = self.spans[span_index];
        let offset = index - self.rows[span_index];
        (offset < span.len).then_some((span, offset))
    }

    /// Returns the span that starts at or after compact row `index`, as an index into `spans`.
    fn span_index_at(&self, index: usize) -> usize {
        self.rows
            .partition_point(|&row| row <= index)
            .saturating_sub(1)
    }

    /// The spans covering compact rows `offset..offset + len`, renumbered from zero.
    fn spans_between(&self, offset: usize, len: usize) -> Vec<GridSpan> {
        let end = offset + len;
        let mut sliced = Vec::new();
        for index in self.span_index_at(offset)..self.spans.len() {
            let row = self.rows[index];
            if row >= end {
                break;
            }
            let span = self.spans[index];
            let from = offset.max(row) - row;
            let to = (end - row).min(span.len);
            if from < to {
                sliced.push(GridSpan::new(span.start + from, to - from));
            }
        }
        sliced
    }

    /// The absolute position of compact row `offset`, or the position after the last row when
    /// `offset` is one past the end; `None` when the axis carries no row at all.
    fn position_at(&self, offset: usize) -> Option<usize> {
        let last = self.spans.last()?;
        if offset >= self.len {
            return Some(last.start + last.len);
        }
        let (span, inside) = self.span_of(offset)?;
        Some(span.start + inside)
    }

    /// Cuts compact rows `offset..offset + len`, keeping the grid.
    ///
    /// The result is a [`TimeAxis::Grid`] when it stays within one span, including every empty
    /// result over a non-empty axis, and [`TimeAxis::GridRanges`] otherwise.
    fn slice(&self, offset: usize, len: usize) -> Result<TimeAxis> {
        if len == 0 {
            return Ok(match self.position_at(offset) {
                Some(position) => {
                    TimeAxis::Grid(GridRange::try_new(self.grid.clone(), position, 0)?)
                }
                None => TimeAxis::GridRanges(self.clone()),
            });
        }
        let spans = self.spans_between(offset, len);
        match spans.as_slice() {
            [span] => Ok(TimeAxis::Grid(GridRange::try_new(
                self.grid.clone(),
                span.start,
                span.len,
            )?)),
            _ => Ok(TimeAxis::GridRanges(Self::try_new(
                self.grid.clone(),
                spans,
            )?)),
        }
    }
}

impl PartialEq for GridRanges {
    fn eq(&self, other: &Self) -> bool {
        self.grid.spec == other.grid.spec && self.spans == other.spans && self.len == other.len
    }
}
impl Eq for GridRanges {}

/// A timestamp axis represented as a grid or explicit shared Arrow buffers.
///
/// Checked constructors validate timestamp ordering. Public payloads are validated again by the
/// downstream series boundary.
///
/// Two coordinate conventions meet here, and every method says which one it speaks:
///
/// * a **compact row** is an index into the axis and into the fields aligned with it, and it
///   counts only the rows the axis carries: [`TimeAxis::len`], [`TimeAxis::timestamp`],
///   [`TimeAxis::slice`] and the row half of [`TimeAxis::positions`] speak compact rows;
/// * an **absolute grid position** is an index into the shared [`EvalGrid`], holes included;
///   [`TimeAxis::positions`], [`TimeAxis::row_of`], [`TimeAxis::first_position`] and
///   [`TimeAxis::last_position`] speak absolute grid positions.
#[derive(Clone, Debug)]
pub enum TimeAxis {
    /// A slice of a shared regular evaluation grid.
    Grid(GridRange),
    /// Several ordered runs of one shared evaluation grid.
    GridRanges(GridRanges),
    /// An explicit timestamp sequence.
    Explicit(ScalarBuffer<i64>),
    /// Explicit timestamps held in ordered chunks without flattening.
    ExplicitChunks(Arc<[ScalarBuffer<i64>]>),
}

/// Sequential traversal of an axis, as returned by [`TimeAxis::positions`].
///
/// Yields `(absolute grid position, compact row)` pairs in increasing position order, and walks
/// the runs of a [`TimeAxis::GridRanges`] with one cursor, so a row never costs a scan over the
/// spans before it.
pub struct Positions<'a> {
    source: PositionSource<'a>,
}

enum PositionSource<'a> {
    /// A dense grid range: position and row advance together.
    Grid {
        start: usize,
        row: usize,
        len: usize,
    },
    /// Several runs: the cursor is the span index plus the offset inside it.
    GridRanges {
        spans: &'a [GridSpan],
        span: usize,
        offset: usize,
        row: usize,
        len: usize,
    },
    /// No grid: a row sits at its own position.
    Rows { row: usize, len: usize },
}

impl Iterator for Positions<'_> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<(usize, usize)> {
        match &mut self.source {
            PositionSource::Grid { start, row, len } => {
                if *row >= *len {
                    return None;
                }
                let item = (*start + *row, *row);
                *row += 1;
                Some(item)
            }
            PositionSource::GridRanges {
                spans,
                span,
                offset,
                row,
                len,
            } => {
                if *row >= *len {
                    return None;
                }
                loop {
                    let current = spans.get(*span)?;
                    if *offset < current.len {
                        let item = (current.start + *offset, *row);
                        *offset += 1;
                        *row += 1;
                        return Some(item);
                    }
                    *span += 1;
                    *offset = 0;
                }
            }
            PositionSource::Rows { row, len } => {
                if *row >= *len {
                    return None;
                }
                let item = (*row, *row);
                *row += 1;
                Some(item)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (row, len) = match &self.source {
            PositionSource::Grid { row, len, .. } => (*row, *len),
            PositionSource::GridRanges { row, len, .. } => (*row, *len),
            PositionSource::Rows { row, len } => (*row, *len),
        };
        let remaining = len - row;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for Positions<'_> {}

impl TimeAxis {
    /// Validates an explicit, strictly increasing timestamp sequence.
    pub fn try_new(timestamps: ScalarBuffer<i64>) -> Result<Self> {
        let axis = Self::Explicit(timestamps);
        axis.try_len()?;
        Ok(axis)
    }

    /// Validates and stores timestamp chunks without concatenating their buffers.
    pub fn try_from_chunks(chunks: Vec<ScalarBuffer<i64>>) -> Result<Self> {
        let axis = Self::ExplicitChunks(
            chunks
                .into_iter()
                .filter(|chunk| !chunk.is_empty())
                .collect(),
        );
        axis.try_len()?;
        Ok(axis)
    }

    /// Builds an axis over `grid` from the runs `spans`, keeping the single-run fast path.
    ///
    /// This is what a producer holding only the rows it decided should use: one span becomes
    /// [`Self::Grid`], several become [`Self::GridRanges`], and no span becomes an empty
    /// [`Self::GridRanges`] that still carries the grid. In particular it never fills the
    /// positions between two spans, which is what keeps a sparse result from costing a row per
    /// empty position.
    pub fn from_grid_spans(grid: Arc<EvalGrid>, spans: Vec<GridSpan>) -> Result<Self> {
        match spans.as_slice() {
            [] => Ok(Self::GridRanges(GridRanges::empty(grid))),
            [span] => Ok(Self::Grid(GridRange::try_new(grid, span.start, span.len)?)),
            _ => Ok(Self::GridRanges(GridRanges::try_new(grid, spans)?)),
        }
    }

    /// Returns the validated timestamp count.
    pub(crate) fn try_len(&self) -> Result<usize> {
        match self {
            Self::Grid(range) => Ok(range.len()),
            // `GridRanges` is only reachable through its checked constructor.
            Self::GridRanges(ranges) => Ok(ranges.len()),
            Self::Explicit(values) => {
                if values.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return invalid("timestamps must be strictly increasing");
                }
                Ok(values.len())
            }
            Self::ExplicitChunks(chunks) => {
                let mut total = 0usize;
                let mut previous = None;
                for chunk in chunks.iter() {
                    total = total
                        .checked_add(chunk.len())
                        .ok_or_else(|| invalid_series("timestamp length overflow"))?;
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
                }
                Ok(total)
            }
        }
    }

    /// Returns the compact row count, not the distance from the first to the last position.
    ///
    /// A [`Self::GridRanges`] answers from its cached total and does not walk its spans.
    pub fn len(&self) -> usize {
        match self {
            Self::Grid(range) => range.len,
            Self::GridRanges(ranges) => ranges.len(),
            Self::Explicit(values) => values.len(),
            Self::ExplicitChunks(chunks) => chunks.iter().map(|chunk| chunk.len()).sum(),
        }
    }
    /// Returns whether there are no timestamps.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a timestamp by compact row index.
    pub fn timestamp(&self, index: usize) -> Option<i64> {
        match self {
            Self::Grid(range) => (index < range.len)
                .then(|| range.grid.timestamp_at(range.start.checked_add(index)?))
                .flatten(),
            Self::GridRanges(ranges) => {
                let (span, offset) = ranges.span_of(index)?;
                ranges.grid.timestamp_at(span.start.checked_add(offset)?)
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
        }
    }

    /// Returns a zero-copy slice, retaining Arrow owners; chunked axes remain chunked.
    ///
    /// `offset` and `len` count compact rows, so the result stays aligned with the fields of the
    /// series the axis belongs to. A slice that stays within one run of a [`Self::GridRanges`]
    /// comes back as [`Self::Grid`]; every empty slice keeps the grid it was cut from.
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
            Self::GridRanges(ranges) => ranges.slice(offset, len),
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

    /// Returns the single contiguous range this axis covers, if it covers exactly one.
    ///
    /// [`Self::Grid`] always answers `Some`, including for an empty range that still names the
    /// position it sits at. A [`Self::GridRanges`] holding several runs, and every explicit axis,
    /// answer `None`: their rows are not one range, and a caller that assumed one would read the
    /// holes between them as rows.
    pub fn as_grid_range(&self) -> Option<&GridRange> {
        match self {
            Self::Grid(range) => Some(range),
            Self::GridRanges(_) | Self::Explicit(_) | Self::ExplicitChunks(_) => None,
        }
    }

    /// Returns the evaluation grid the axis is addressed on, if it has one.
    ///
    /// Both [`Self::Grid`] and [`Self::GridRanges`] answer `Some`; the explicit axes answer `None`.
    pub fn grid(&self) -> Option<&Arc<EvalGrid>> {
        match self {
            Self::Grid(range) => Some(range.grid()),
            Self::GridRanges(ranges) => Some(ranges.grid()),
            Self::Explicit(_) | Self::ExplicitChunks(_) => None,
        }
    }

    /// Maps an absolute grid position to its compact row, or `None` when the axis carries no row
    /// there.
    ///
    /// A point lookup: a [`Self::GridRanges`] binary-searches its spans, it does not scan them.
    /// On an explicit axis a row sits at its own position, so only the length bounds it.
    pub fn row_of(&self, position: usize) -> Option<usize> {
        match self {
            Self::Grid(range) => (position >= range.start
                && position
                    .checked_sub(range.start)
                    .is_some_and(|row| row < range.len))
            .then(|| position - range.start),
            Self::GridRanges(ranges) => ranges.row_of(position),
            Self::Explicit(_) | Self::ExplicitChunks(_) => {
                (position < self.len()).then_some(position)
            }
        }
    }

    /// Walks the axis once, yielding `(absolute grid position, compact row)` in position order.
    ///
    /// This is the traversal every consumer of a grid axis should use: a helper walks the runs of
    /// a [`Self::GridRanges`] with a single cursor, so a row never rescans the spans before it.
    /// On an explicit axis there is no grid, so a row sits at its own position.
    pub fn positions(&self) -> Positions<'_> {
        let len = self.len();
        let source = match self {
            Self::Grid(range) => PositionSource::Grid {
                start: range.start,
                row: 0,
                len,
            },
            Self::GridRanges(ranges) => PositionSource::GridRanges {
                spans: &ranges.spans,
                span: 0,
                offset: 0,
                row: 0,
                len,
            },
            Self::Explicit(_) | Self::ExplicitChunks(_) => PositionSource::Rows { row: 0, len },
        };
        Positions { source }
    }

    /// Returns the absolute grid position of the first carried row.
    pub fn first_position(&self) -> Option<usize> {
        match self {
            Self::Grid(range) => (!range.is_empty()).then_some(range.start),
            Self::GridRanges(ranges) => ranges.spans.first().map(GridSpan::start),
            Self::Explicit(_) | Self::ExplicitChunks(_) => (!self.is_empty()).then_some(0),
        }
    }

    /// Returns the absolute grid position of the last carried row.
    pub fn last_position(&self) -> Option<usize> {
        match self {
            Self::Grid(range) => (!range.is_empty()).then(|| range.start + range.len - 1),
            Self::GridRanges(ranges) => ranges.spans.last().map(|span| span.start + span.len - 1),
            Self::Explicit(_) | Self::ExplicitChunks(_) => self.len().checked_sub(1),
        }
    }

    /// Returns whether the carried rows form one run, which is what a fast path can rely on.
    ///
    /// A [`Self::GridRanges`] answers `true` only for a single span, so an empty span list reports
    /// `false`: it names no run, and therefore no position a fast path could start at.
    pub fn is_contiguous(&self) -> bool {
        match self {
            Self::Grid(_) => true,
            Self::GridRanges(ranges) => ranges.spans.len() == 1,
            Self::Explicit(_) => true,
            Self::ExplicitChunks(chunks) => chunks.len() <= 1,
        }
    }

    /// Returns the timestamps of the carried rows, in compact-row order.
    ///
    /// A single-run grid axis lends its buffer without copying; an axis of several runs, and a
    /// chunked explicit axis, allocate one array of the carried rows.
    pub fn timestamps(&self) -> Result<Cow<'_, [i64]>> {
        match self {
            Self::Grid(range) => Ok(Cow::Borrowed(
                &range.grid.timestamps()?[range.start()..range.start() + range.len()],
            )),
            Self::GridRanges(ranges) => {
                let mut timestamps = Vec::with_capacity(ranges.len());
                for (position, _) in self.positions() {
                    timestamps.push(
                        ranges
                            .grid
                            .timestamp_at(position)
                            .ok_or_else(|| invalid_series("grid span timestamp is out of range"))?,
                    );
                }
                Ok(Cow::Owned(timestamps))
            }
            Self::Explicit(values) => Ok(Cow::Borrowed(values)),
            Self::ExplicitChunks(_) => Ok(Cow::Owned(
                (0..self.len())
                    .filter_map(|row| self.timestamp(row))
                    .collect(),
            )),
        }
    }

    /// Returns one run per stored span of carried rows, as [`GridRun`]s.
    ///
    /// One run for a contiguous axis, one per stored span for [`Self::GridRanges`], and none when
    /// no row is carried. Adjacent stored spans stay two runs, so a run is a maximal run only when
    /// the axis was built from maximal spans (as the series builder does). A run is where bulk
    /// work pays off: the rows inside it are consecutive in both coordinates, so its samples can
    /// be sliced and zipped instead of read one by one.
    pub fn runs(&self) -> GridRuns<'_> {
        let (one, spans) = match self {
            Self::Grid(range) => (
                Some(GridRun {
                    position: range.start,
                    row: 0,
                    len: range.len,
                }),
                &[][..],
            ),
            Self::GridRanges(ranges) => (None, &ranges.spans[..]),
            Self::Explicit(_) | Self::ExplicitChunks(_) => (
                Some(GridRun {
                    position: 0,
                    row: 0,
                    len: self.len(),
                }),
                &[][..],
            ),
        };
        GridRuns {
            one: one.filter(|run| run.len > 0),
            spans,
            span: 0,
            row: 0,
        }
    }

    /// Intersects the carried rows of two axes, yielding one shared run per pair of overlapping
    /// stored runs as `(first absolute position, row in `self`, row in `other`, length)`.
    ///
    /// A single merge pass over the two run lists, so the cost is the number of runs and not their
    /// product. This is how a pairwise operator aligns two axes: it never builds the dense
    /// envelope `first_position()..last_position()`, which would visit -- and densify -- the holes,
    /// and it never re-derives the row of a position by subtracting starts, which is wrong as soon
    /// as either side skips a row.
    ///
    /// Both axes must be addressed on the same [`GridSpec`]: the positions it yields are indices
    /// into one shared [`EvalGrid`], so two different grids line up only by coincidence. An
    /// explicit axis carries no grid and must not be passed at all -- its rows are not absolute
    /// positions on any shared grid, so the pairs would be silent nonsense. Debug builds assert
    /// the precondition and therefore panic on a violation; release builds trust the caller.
    pub fn intersect_runs<'a>(&'a self, other: &'a Self) -> impl Iterator<Item = GridRunPair> + 'a {
        debug_assert!(
            matches!(
                (self.grid(), other.grid()),
                (Some(lhs), Some(rhs)) if lhs.spec() == rhs.spec()
            ),
            "intersect_runs requires two axes on one grid, \
             but an explicit axis carries none and two grids only meet by coincidence",
        );
        let mut lhs = self.runs().peekable();
        let mut rhs = other.runs().peekable();
        std::iter::from_fn(move || {
            loop {
                let left = *lhs.peek()?;
                let right = *rhs.peek()?;
                let start = left.position.max(right.position);
                let end = (left.position + left.len).min(right.position + right.len);
                if start >= end {
                    // No shared position in this pair of runs: drop the one that ends first.
                    if left.position + left.len <= right.position + right.len {
                        lhs.next();
                    } else {
                        rhs.next();
                    }
                    continue;
                }
                let len = end - start;
                let pair = GridRunPair {
                    position: start,
                    row: left.row + (start - left.position),
                    other_row: right.row + (start - right.position),
                    len,
                };
                // Consume the shared piece from both sides, keeping the remainder for the next
                // round.
                if left.position + left.len <= end {
                    lhs.next();
                }
                if right.position + right.len <= end {
                    rhs.next();
                }
                return Some(pair);
            }
        })
    }
}

/// A run of carried rows: `len` rows starting at absolute position `position` and compact
/// `row`, both of which advance by one per row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GridRun {
    /// Absolute grid position of the run's first row.
    pub position: usize,
    /// Compact row of the run's first row.
    pub row: usize,
    /// Number of rows in the run; never zero.
    pub len: usize,
}

/// A run shared by two axes, with the compact row of each.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GridRunPair {
    /// Absolute grid position of the shared run's first row.
    pub position: usize,
    /// Compact row of the first shared position in the axis `intersect_runs` was called on.
    pub row: usize,
    /// Compact row of the first shared position in the other axis.
    pub other_row: usize,
    /// Number of rows in the shared run; never zero.
    pub len: usize,
}

/// Lazily produced runs of one axis; see [`TimeAxis::runs`].
///
/// A contiguous axis holds its one run inline; a sparse axis walks its spans with a cursor.
pub struct GridRuns<'a> {
    one: Option<GridRun>,
    spans: &'a [GridSpan],
    span: usize,
    row: usize,
}

impl Iterator for GridRuns<'_> {
    type Item = GridRun;

    fn next(&mut self) -> Option<GridRun> {
        if let Some(run) = self.one.take() {
            return Some(run);
        }
        let span = *self.spans.get(self.span)?;
        self.span += 1;
        let run = GridRun {
            position: span.start,
            row: self.row,
            len: span.len,
        };
        self.row += span.len;
        Some(run)
    }
}

/// Walks every position at least one of `axes` carries, once, in increasing order.
///
/// A merge over one cursor per axis: each step costs one comparison per axis, and no position is
/// visited twice. This is how an operator covering several axes (a histogram's bucket series, an
/// aggregate over an input set) visits the positions that hold something, instead of scanning the
/// envelope from the earliest to the latest position and densifying every hole in between.
///
/// Every axis must be addressed on the same [`GridSpec`]: a position names a point of one shared
/// [`EvalGrid`], so merging axes of two different grids would interleave unrelated positions. An
/// explicit axis carries no grid and must not be passed at all -- its rows are not absolute
/// positions on any shared grid. Debug builds assert the precondition and therefore panic on a
/// violation; release builds trust the caller.
pub fn union_positions<'a>(axes: impl IntoIterator<Item = &'a TimeAxis>) -> UnionPositions<'a> {
    // Materialized once so that the debug assert above can see every axis' grid before the first
    // cursor is built; the cursors below borrow these references, not the list.
    let axes: Vec<&'a TimeAxis> = axes.into_iter().collect();
    debug_assert!(
        {
            let mut specs = axes.iter().map(|axis| axis.grid().map(|grid| grid.spec()));
            match specs.next() {
                // No axis at all: the merge yields nothing and reads no grid.
                None => true,
                Some(None) => false,
                Some(Some(first)) => specs.all(|spec| spec == Some(first)),
            }
        },
        "union_positions requires every axis on one grid, \
         but an explicit axis carries none and two grids only meet by coincidence",
    );
    let cursors: Vec<Positions<'a>> = axes.into_iter().map(TimeAxis::positions).collect();
    let pending = vec![None; cursors.len()];
    UnionPositions { cursors, pending }
}

/// The positions of several axes, merged; see [`union_positions`].
pub struct UnionPositions<'a> {
    cursors: Vec<Positions<'a>>,
    /// The next `(position, row)` of each cursor, buffered so that it can be peeked without a
    /// second cursor type.
    pending: Vec<Option<(usize, usize)>>,
}

impl Iterator for UnionPositions<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        let mut lowest: Option<usize> = None;
        for (cursor, pending) in self.cursors.iter_mut().zip(&mut self.pending) {
            if pending.is_none() {
                *pending = cursor.next();
            }
            if let Some((position, _)) = *pending {
                lowest = Some(lowest.map_or(position, |lowest| lowest.min(position)));
            }
        }
        let lowest = lowest?;
        // Every axis standing at this position advances; the others keep their buffered position.
        for pending in &mut self.pending {
            if matches!(*pending, Some((position, _)) if position == lowest) {
                *pending = None;
            }
        }
        Some(lowest)
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
