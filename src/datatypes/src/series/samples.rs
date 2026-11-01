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

use arrow::array::{Array, Float64Array, UInt32Array, UInt32Builder};
use arrow::compute::concat;
use snafu::OptionExt;

use crate::error::{InvalidSeriesSnafu, Result};
use crate::series::SeriesLabels;
use crate::series::histogram::{HistogramBatch, HistogramView};
use crate::series::time::{EvalGrid, TimeAxis};

#[cfg(test)]
mod tests;

fn invalid<T>(reason: impl Into<String>) -> Result<T> {
    Err(InvalidSeriesSnafu {
        reason: reason.into(),
    }
    .build())
}
fn arrow_result<T>(r: std::result::Result<T, arrow::error::ArrowError>) -> Result<T> {
    r.map_err(|e| {
        InvalidSeriesSnafu {
            reason: e.to_string(),
        }
        .build()
    })
}

/// Histogram references for each time position, backed by one immutable table.
#[derive(Clone, Debug)]
pub struct HistogramSamples {
    indices: UInt32Array,
    batch: Arc<HistogramBatch>,
}
impl HistogramSamples {
    /// Validate and bind reference indices to their immutable histogram table.
    pub fn try_new(indices: UInt32Array, batch: Arc<HistogramBatch>) -> Result<Self> {
        if indices.iter().flatten().any(|i| i as usize >= batch.len()) {
            return invalid("histogram sample index is out of bounds");
        }
        Ok(Self { indices, batch })
    }
    /// Read-only reference indices.
    pub fn indices(&self) -> &UInt32Array {
        &self.indices
    }
    /// Shared histogram table.
    pub fn batch(&self) -> &Arc<HistogramBatch> {
        &self.batch
    }
    /// Number of time positions.
    pub fn len(&self) -> usize {
        self.indices.len()
    }
    /// Whether this contains no positions.
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
    /// Returns a referenced histogram, or `None` for missing/out-of-range positions.
    pub fn view_at(&self, index: usize) -> Option<HistogramView<'_>> {
        if index >= self.indices.len() {
            return None;
        }
        if self.indices.is_valid(index) {
            self.batch.view(self.indices.value(index) as usize)
        } else {
            None
        }
    }
    fn slice(&self, offset: usize, len: usize) -> Self {
        Self {
            indices: self.indices.slice(offset, len),
            batch: self.batch.clone(),
        }
    }
}

/// Float and histogram values with mutually exclusive validity at each position.
#[derive(Clone, Debug)]
pub struct MixedSamples {
    floats: Float64Array,
    histograms: HistogramSamples,
}
impl MixedSamples {
    /// Read-only float values, including validity bitmap.
    pub fn floats(&self) -> &Float64Array {
        &self.floats
    }
    /// Read-only histogram references and their shared table.
    pub fn histograms(&self) -> &HistogramSamples {
        &self.histograms
    }
    /// Number of positions.
    pub fn len(&self) -> usize {
        self.floats.len()
    }
    /// Whether this contains no positions.
    pub fn is_empty(&self) -> bool {
        self.floats.is_empty()
    }
}

#[derive(Clone, Debug)]
enum SampleStorage {
    Float(Float64Array),
    Histogram(HistogramSamples),
    Mixed(MixedSamples),
    Chunks(Arc<[Samples]>, usize),
}

/// Typed Arrow-backed values for a time series.
#[derive(Clone, Debug)]
pub struct Samples {
    storage: SampleStorage,
}

/// A borrowed dense representation.
pub enum DenseSamplesRef<'a> {
    Float(&'a Float64Array),
    Histogram(&'a HistogramSamples),
    Mixed(&'a MixedSamples),
}

/// A sample value; ordinary NaN is a valid float, distinct from Missing.
pub enum SampleRef<'a> {
    Missing,
    Float(f64),
    Histogram(HistogramView<'a>),
}

impl Samples {
    /// Wrap float samples without copying; nulls are missing and NaNs remain values.
    pub fn from_float(values: Float64Array) -> Self {
        Self {
            storage: SampleStorage::Float(values),
        }
    }
    /// Validate histogram indices and retain their referenced table.
    pub fn try_from_histogram(indices: UInt32Array, batch: Arc<HistogramBatch>) -> Result<Self> {
        Ok(Self {
            storage: SampleStorage::Histogram(HistogramSamples::try_new(indices, batch)?),
        })
    }
    /// Validate lengths and ensure no position is simultaneously float and histogram.
    pub fn try_from_mixed(floats: Float64Array, histograms: HistogramSamples) -> Result<Self> {
        if floats.len() != histograms.len() {
            return invalid("mixed sample arrays have unequal lengths");
        }
        if (0..floats.len()).any(|i| floats.is_valid(i) && histograms.indices.is_valid(i)) {
            return invalid("mixed sample position has both float and histogram values");
        }
        Ok(Self {
            storage: SampleStorage::Mixed(MixedSamples { floats, histograms }),
        })
    }
    /// Retain immutable chunks, flattening nested descriptors and checking total length.
    pub fn try_from_chunks(chunks: Vec<Samples>) -> Result<Self> {
        let mut flat = Vec::new();
        for chunk in chunks {
            match chunk.storage {
                SampleStorage::Chunks(parts, _) => flat.extend(parts.iter().cloned()),
                _ => flat.push(chunk),
            }
        }
        let total = flat
            .iter()
            .try_fold(0usize, |n, s| n.checked_add(s.len()))
            .context(InvalidSeriesSnafu {
                reason: "sample chunk length overflow",
            })?;
        flat.retain(|s| !s.is_empty());
        if flat.is_empty() {
            return Ok(Self::from_float(Float64Array::new_null(0)));
        }
        if flat.len() == 1 {
            return flat.pop().ok_or_else(|| {
                InvalidSeriesSnafu {
                    reason: "single sample chunk disappeared".to_string(),
                }
                .build()
            });
        }
        Ok(Self {
            storage: SampleStorage::Chunks(flat.into(), total),
        })
    }
    /// Returns the dense segments in logical order without allocating, copying payloads, or
    /// densifying the samples.
    ///
    /// Chunked samples expose their retained segment boundaries; different fields may therefore
    /// have different boundaries. The logical start of each segment is the sum of the lengths of
    /// all preceding segments, and the total length is [`Self::len`]. For non-chunked samples,
    /// including empty samples, this returns a singleton slice containing `self`. Chunk
    /// construction flattens nested chunks and drops empty segments; an all-empty input becomes
    /// one empty float segment. Each segment is dense: [`Self::dense`] returns `Some` with its
    /// typed Arrow view. Clone the samples cheaply if they need to outlive this borrow.
    ///
    /// ```
    /// use arrow::array::Float64Array;
    /// use datatypes::series::samples::{DenseSamplesRef, Samples};
    ///
    /// let samples = Samples::try_from_chunks(vec![
    ///     Samples::from_float(Float64Array::from(vec![1.0, 2.0])),
    ///     Samples::from_float(Float64Array::from(vec![3.0])),
    /// ])
    /// .unwrap();
    ///
    /// let mut logical_start = 0;
    /// let mut starts = Vec::new();
    /// for segment in samples.segments() {
    ///     starts.push(logical_start);
    ///     match segment.dense().unwrap() {
    ///         DenseSamplesRef::Float(values) => assert_eq!(values.len(), segment.len()),
    ///         _ => unreachable!("these segments contain floats"),
    ///     }
    ///     logical_start += segment.len();
    /// }
    /// assert_eq!(starts, vec![0, 2]);
    /// assert_eq!(logical_start, samples.len());
    /// ```
    pub fn segments(&self) -> &[Samples] {
        match &self.storage {
            SampleStorage::Chunks(chunks, _) => chunks,
            _ => std::slice::from_ref(self),
        }
    }
    /// Number of sample positions.
    pub fn len(&self) -> usize {
        match &self.storage {
            SampleStorage::Float(a) => a.len(),
            SampleStorage::Histogram(a) => a.len(),
            SampleStorage::Mixed(a) => a.len(),
            SampleStorage::Chunks(_, len) => *len,
        }
    }
    /// Whether this has no positions.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Returns a dense typed representation, or `None` for chunks.
    pub fn dense(&self) -> Option<DenseSamplesRef<'_>> {
        match &self.storage {
            SampleStorage::Float(a) => Some(DenseSamplesRef::Float(a)),
            SampleStorage::Histogram(a) => Some(DenseSamplesRef::Histogram(a)),
            SampleStorage::Mixed(a) => Some(DenseSamplesRef::Mixed(a)),
            SampleStorage::Chunks(_, _) => None,
        }
    }
    /// Returns the sample, treating an out-of-range index as missing.
    ///
    /// A chunked field walks its descriptors from the start, so reading row after row with this
    /// costs a scan per row: a sequential reader should use [`Self::cursor`] instead.
    pub fn sample(&self, index: usize) -> SampleRef<'_> {
        if index >= self.len() {
            return SampleRef::Missing;
        }
        match &self.storage {
            SampleStorage::Float(a) => {
                if a.is_valid(index) {
                    SampleRef::Float(a.value(index))
                } else {
                    SampleRef::Missing
                }
            }
            SampleStorage::Histogram(a) => a
                .view_at(index)
                .map_or(SampleRef::Missing, SampleRef::Histogram),
            SampleStorage::Mixed(a) => {
                if a.floats.is_valid(index) {
                    SampleRef::Float(a.floats.value(index))
                } else {
                    a.histograms
                        .view_at(index)
                        .map_or(SampleRef::Missing, SampleRef::Histogram)
                }
            }
            SampleStorage::Chunks(chunks, _) => {
                let mut i = index;
                for part in chunks.iter() {
                    if i < part.len() {
                        return part.sample(i);
                    }
                    i -= part.len();
                }
                SampleRef::Missing
            }
        }
    }
    /// Iterate all positions sequentially without materializing per-sample objects.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = SampleRef<'_>> + '_ {
        let (chunks, remaining) = match &self.storage {
            SampleStorage::Chunks(chunks, len) => (Some(chunks.as_ref()), *len),
            _ => (None, self.len()),
        };
        SamplesIter {
            samples: self,
            chunks,
            chunk_index: 0,
            local_index: 0,
            remaining,
        }
    }
    /// A forward reader of this field addressed by compact row; see [`RowCursor`].
    pub fn cursor(&self) -> RowCursor<'_> {
        let (chunks, remaining) = match &self.storage {
            SampleStorage::Chunks(chunks, len) => (Some(chunks.as_ref()), *len),
            _ => (None, self.len()),
        };
        RowCursor {
            iter: SamplesIter {
                samples: self,
                chunks,
                chunk_index: 0,
                local_index: 0,
                remaining,
            },
            row: 0,
        }
    }
    /// Return a checked zero-copy slice (across chunks only intersected descriptors are retained).
    pub fn slice(&self, offset: usize, len: usize) -> Result<Self> {
        let end = offset.checked_add(len).context(InvalidSeriesSnafu {
            reason: "sample slice bounds overflow",
        })?;
        if end > self.len() {
            return invalid("sample slice is out of bounds");
        }
        match &self.storage {
            SampleStorage::Float(a) => Ok(Self::from_float(a.slice(offset, len))),
            SampleStorage::Histogram(a) => Ok(Self {
                storage: SampleStorage::Histogram(a.slice(offset, len)),
            }),
            SampleStorage::Mixed(a) => Ok(Self {
                storage: SampleStorage::Mixed(MixedSamples {
                    floats: a.floats.slice(offset, len),
                    histograms: a.histograms.slice(offset, len),
                }),
            }),
            SampleStorage::Chunks(chunks, _) => {
                let mut skip = offset;
                let mut remain = len;
                let mut out = Vec::new();
                for chunk in chunks.iter() {
                    if skip >= chunk.len() {
                        skip -= chunk.len();
                        continue;
                    }
                    let take = remain.min(chunk.len() - skip);
                    if take != 0 {
                        out.push(chunk.slice(skip, take)?);
                        remain -= take;
                    }
                    skip = 0;
                    if remain == 0 {
                        break;
                    }
                }
                Self::try_from_chunks(out)
            }
        }
    }
    /// Materialize chunks into one dense array. When all histogram-bearing chunks reference the
    /// same Arrow table view, reuse it; otherwise concatenate tables per chunk and remap indices.
    pub fn into_dense(self) -> Result<Self> {
        let SampleStorage::Chunks(chunks, total_len) = self.storage else {
            return Ok(self);
        };
        let all_float = chunks
            .iter()
            .all(|s| matches!(s.storage, SampleStorage::Float(_)));
        if all_float {
            let arrays: Vec<&dyn Array> = chunks
                .iter()
                .filter_map(|s| match &s.storage {
                    SampleStorage::Float(a) => Some(a as &dyn Array),
                    _ => None,
                })
                .collect();
            let array = arrow_result(concat(&arrays))?;
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    InvalidSeriesSnafu {
                        reason: "float concatenation returned a non-float array",
                    }
                    .build()
                })?;
            return Ok(Self::from_float(values.clone()));
        }

        let mut histogram_batches = chunks.iter().filter_map(|chunk| match &chunk.storage {
            SampleStorage::Histogram(histogram) => Some(&histogram.batch),
            SampleStorage::Mixed(mixed) => Some(&mixed.histograms.batch),
            SampleStorage::Float(_) | SampleStorage::Chunks(_, _) => None,
        });
        let mut first_data = None;
        let shared_batch = histogram_batches.next().filter(|first| {
            histogram_batches.all(|batch| {
                if Arc::ptr_eq(first, batch) {
                    return true;
                }
                first_data
                    .get_or_insert_with(|| first.as_arrow().to_data())
                    .ptr_eq(&batch.as_arrow().to_data())
            })
        });
        let needs_float = chunks.iter().any(|chunk| {
            matches!(
                chunk.storage,
                SampleStorage::Float(_) | SampleStorage::Mixed(_)
            )
        });
        let mut tables: Vec<&dyn Array> = Vec::new();
        let mut indices = UInt32Builder::with_capacity(total_len);
        let mut floats =
            needs_float.then(|| arrow::array::Float64Builder::with_capacity(total_len));
        let mut base = 0u32;
        for chunk in chunks.iter() {
            let (float_values, histogram) = match &chunk.storage {
                SampleStorage::Float(values) => (Some(values), None),
                SampleStorage::Histogram(histogram) => (None, Some(histogram)),
                SampleStorage::Mixed(mixed) => (Some(&mixed.floats), Some(&mixed.histograms)),
                SampleStorage::Chunks(_, _) => {
                    return invalid("nested sample chunks were not flattened");
                }
            };
            if shared_batch.is_none()
                && let Some(histogram) = histogram
            {
                tables.push(histogram.batch.as_arrow());
            }
            for position in 0..chunk.len() {
                if let Some(floats) = &mut floats {
                    match float_values {
                        Some(values) if values.is_valid(position) => {
                            floats.append_value(values.value(position));
                        }
                        _ => floats.append_null(),
                    }
                }
                match histogram {
                    Some(histogram) if histogram.indices.is_valid(position) => {
                        indices.append_value(
                            base.checked_add(histogram.indices.value(position))
                                .context(InvalidSeriesSnafu {
                                    reason: "histogram remapped index overflow",
                                })?,
                        );
                    }
                    _ => indices.append_null(),
                }
            }
            if shared_batch.is_none()
                && let Some(histogram) = histogram
            {
                base = base
                    .checked_add(u32::try_from(histogram.batch.len()).map_err(|_| {
                        InvalidSeriesSnafu {
                            reason: "histogram table too large",
                        }
                        .build()
                    })?)
                    .context(InvalidSeriesSnafu {
                        reason: "histogram table index overflow",
                    })?;
            }
        }
        let batch = if let Some(batch) = shared_batch {
            Arc::clone(batch)
        } else {
            let table = arrow_result(concat(&tables))?;
            let struct_array = table
                .as_any()
                .downcast_ref::<arrow::array::StructArray>()
                .ok_or_else(|| {
                    InvalidSeriesSnafu {
                        reason: "histogram concatenation returned a non-struct array",
                    }
                    .build()
                })?
                .clone();
            Arc::new(HistogramBatch::try_from_arrow(struct_array)?)
        };
        let histogram = HistogramSamples::try_new(indices.finish(), batch)?;
        if let Some(mut floats) = floats {
            Self::try_from_mixed(floats.finish(), histogram)
        } else {
            Ok(Self {
                storage: SampleStorage::Histogram(histogram),
            })
        }
    }
}

/// Sequential exact-size iterator over a dense series or its chunk descriptors.
struct SamplesIter<'a> {
    samples: &'a Samples,
    chunks: Option<&'a [Samples]>,
    chunk_index: usize,
    local_index: usize,
    remaining: usize,
}
impl<'a> Iterator for SamplesIter<'a> {
    type Item = SampleRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        if let Some(chunks) = self.chunks {
            while self.chunk_index < chunks.len()
                && self.local_index == chunks[self.chunk_index].len()
            {
                self.chunk_index += 1;
                self.local_index = 0;
            }
            let chunk = &chunks[self.chunk_index];
            let value = chunk.sample(self.local_index);
            self.local_index += 1;
            Some(value)
        } else {
            let index = self.samples.len() - self.remaining - 1;
            Some(self.samples.sample(index))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl ExactSizeIterator for SamplesIter<'_> {
    fn len(&self) -> usize {
        self.remaining
    }
}

/// A forward reader of one field's samples, addressed by compact row.
///
/// [`Samples::sample`] rescans the chunk descriptors from the start on every call, so reading a
/// chunked field with it row by row costs a scan per row. A cursor resumes where it stopped: the
/// rows asked for must not go backwards, and reading `n` rows in order costs one pass over the
/// field. That is what every operator walking a [`TimeAxis`](crate::series::time::TimeAxis) needs,
/// since the axis says which rows exist and the cursor turns them into values.
///
/// ```
/// use arrow::array::Float64Array;
/// use datatypes::series::samples::{SampleRef, Samples};
///
/// let samples = Samples::try_from_chunks(vec![
///     Samples::from_float(Float64Array::from(vec![1.0, 2.0])),
///     Samples::from_float(Float64Array::from(vec![3.0])),
/// ])
/// .unwrap();
/// let mut cursor = samples.cursor();
/// assert!(matches!(cursor.at(2), Some(SampleRef::Float(3.0))));
/// assert!(cursor.at(0).is_none(), "a cursor never goes back");
/// ```
pub struct RowCursor<'a> {
    iter: SamplesIter<'a>,
    /// Row the next [`RowCursor::at`] call must start from.
    row: usize,
}
impl<'a> RowCursor<'a> {
    /// Reads the sample at compact `row`, which must not be behind the previous read.
    ///
    /// Returns `None` when the row is behind the cursor's position or at or past the field's end.
    /// Such a call leaves the cursor where it was, so one out-of-range row does not consume the
    /// rows after it.
    pub fn at(&mut self, row: usize) -> Option<SampleRef<'a>> {
        if row < self.row {
            return None;
        }
        // Bound-check before advancing: the iterator is exact-size, so draining it would make it
        // answer `None` for every later row as well.
        if row - self.row >= self.iter.len() {
            return None;
        }
        let sample = self.iter.nth(row - self.row)?;
        self.row = row + 1;
        Some(sample)
    }
}

#[derive(Clone, Debug)]
enum Fields {
    One(Samples),
    Many(Box<[Samples]>),
}
/// Field values using inline storage for a single field.
#[derive(Clone, Debug)]
pub struct SeriesFields {
    storage: Fields,
}
impl SeriesFields {
    /// Create a one-field collection without allocating a field list.
    pub fn one(field: Samples) -> Self {
        Self {
            storage: Fields::One(field),
        }
    }
    /// Validate nonempty, equally sized fields.
    pub fn try_from_vec(fields: Vec<Samples>) -> Result<Self> {
        if fields.is_empty() {
            return invalid("series fields must not be empty");
        }
        if fields.iter().any(|f| f.len() != fields[0].len()) {
            return invalid("series fields have unequal lengths");
        }
        Ok(if fields.len() == 1 {
            let mut fields = fields.into_iter();
            let field = fields.next().ok_or_else(|| {
                InvalidSeriesSnafu {
                    reason: "single series field disappeared".to_string(),
                }
                .build()
            })?;
            Self::one(field)
        } else {
            Self {
                storage: Fields::Many(fields.into_boxed_slice()),
            }
        })
    }
    /// Borrow all fields.
    pub fn as_slice(&self) -> &[Samples] {
        match &self.storage {
            Fields::One(f) => std::slice::from_ref(f),
            Fields::Many(f) => f,
        }
    }
    /// Number of fields.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }
    /// Whether this has no fields.
    pub fn is_empty(&self) -> bool {
        false
    }
    /// Get a field by index.
    pub fn get(&self, index: usize) -> Option<&Samples> {
        self.as_slice().get(index)
    }
    /// Iterate fields.
    pub fn iter(&self) -> std::slice::Iter<'_, Samples> {
        self.as_slice().iter()
    }
}

/// Shared name and position of each output field.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct FieldSpec {
    pub name: Arc<str>,
}
/// Shared schema describing series fields.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Hash, Eq)]
pub struct SeriesSchema {
    pub fields: Arc<[FieldSpec]>,
    /// Shared grid for regular series; its `GridSpec` must match each series grid axis. Explicit axes require `None`.
    pub eval_grid: Option<Arc<EvalGrid>>,
}
/// A validated metric, time axis, and aligned fields.
#[derive(Clone, Debug)]
pub struct Series {
    pub metric: SeriesLabels,
    pub time: TimeAxis,
    pub fields: SeriesFields,
}
impl Series {
    /// Construct after validating schema, field lengths, and strictly increasing timestamps.
    pub fn try_new(
        schema: &SeriesSchema,
        metric: SeriesLabels,
        time: TimeAxis,
        fields: SeriesFields,
    ) -> Result<Self> {
        if fields.len() != schema.fields.len() {
            return invalid("series field count does not match schema");
        }
        let time_len = time.try_len()?;
        if fields.iter().any(|f| f.len() != time_len) {
            return invalid("series field length does not match time axis");
        }
        match (&schema.eval_grid, time.grid()) {
            (Some(expected), Some(grid)) if expected.spec() == grid.spec() => {}
            (Some(_), Some(_)) => {
                return invalid("series grid does not match schema evaluation grid");
            }
            (Some(_), None) => {
                return invalid("explicit series time axis cannot use an evaluation grid");
            }
            (None, Some(_)) => {
                return invalid("grid series requires a schema evaluation grid");
            }
            (None, None) => {}
        }
        Ok(Self {
            metric,
            time,
            fields,
        })
    }
}
