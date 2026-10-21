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

//! Building evaluated series that cover only their result range on a shared grid.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::builder::NullBufferBuilder;
use datafusion::arrow::array::{Float64Array, UInt32Array};
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{
    HistogramSamples, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridSpan, TimeAxis};

use crate::histogram_eval::float_histogram::FloatHistogram;

#[cfg(test)]
pub(crate) mod reference;
#[cfg(test)]
mod tests;

/// Builds one grid series from rows pushed at absolute grid indices.
///
/// A push names one position and the value each field holds there. Rows are accumulated
/// compactly: a row is kept only when at least one field holds a value at that position, so an
/// empty stretch costs one skipped push and no storage at all. Fields stay row-aligned, so a field
/// with no value at a retained row still gets a null there; the Arrow layout needs one row per
/// retained position, and the position does hold a value.
///
/// [`GridSeriesBuilder::finish`] hands out all the buffered rows as **one** [`Series`], whose
/// [`TimeAxis`] names the maximal runs of consecutive positions the rows make up. The rows are not
/// densified: the axis says where they sit, so a series over two far-apart positions costs two rows
/// and two spans, not a row per position between them.
///
/// A builder serves one series at a time. [`GridSeriesBuilder::begin_series`] starts the next one;
/// nothing else resets the series state, so [`GridSeriesBuilder::take_buffered`] can hand out the
/// rows gathered so far and let the same series go on.
pub struct GridSeriesBuilder {
    schema: Arc<SeriesSchema>,
    grid: Arc<EvalGrid>,
    /// Smallest index the next push may use.
    next: usize,
    /// Grid index of each retained row, ascending.
    ///
    /// This vector is the only record of where the gaps are, and it says only where rows sit:
    /// with `V` entries, `F` fields and `k` runs (`k <= V`) the whole series costs `8*F*V` value
    /// bytes plus validity bits, one `usize` per retained row and `O(k)` run descriptors. Nothing
    /// is proportional to the positions holding no value.
    indices: Vec<usize>,
    /// One row per retained row, per field; empty when nothing is buffered.
    fields: Vec<FieldBuilder>,
}

struct FieldBuilder {
    values: Vec<f64>,
    nulls: NullBufferBuilder,
    /// Created at the first histogram value; holds an index per row from then on.
    histograms: Option<HistogramColumn>,
}

struct HistogramColumn {
    batch: HistogramBatchBuilder,
    indices: Vec<Option<u32>>,
}

/// A value of [`GridSeriesBuilder::push_samples`].
pub(crate) enum GridSample {
    Float(f64),
    /// Boxed so that buffered float results stay as small as `Option<f64>`.
    Histogram(Box<FloatHistogram>),
}

const _: () = assert!(std::mem::size_of::<Option<GridSample>>() == 16);

/// `sample` as an owned value; `None` when missing.
pub(crate) fn owned(sample: SampleRef<'_>) -> Result<Option<GridSample>> {
    Ok(match sample {
        SampleRef::Missing => None,
        SampleRef::Float(value) => Some(GridSample::Float(value)),
        SampleRef::Histogram(view) => Some(GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&view)?,
        ))),
    })
}

impl GridSeriesBuilder {
    /// Creates a builder for series of `schema`, which must have an evaluation grid.
    pub fn try_new(schema: Arc<SeriesSchema>) -> Result<Self> {
        let grid = schema.eval_grid.clone().ok_or_else(|| {
            DataFusionError::Execution("grid series schema requires an evaluation grid".into())
        })?;
        Ok(Self {
            schema,
            grid,
            next: 0,
            indices: Vec::new(),
            fields: Vec::new(),
        })
    }

    /// Pushes the field values at absolute grid `index`; `None` is a missing value.
    ///
    /// Indices must be strictly increasing; skipped indices are positions that hold no value.
    pub fn push(&mut self, index: usize, values: &[Option<f64>]) -> Result<()> {
        if !self.start_row(index, values.len(), values.iter().all(Option::is_none))? {
            return Ok(());
        }
        for (field, value) in self.fields.iter_mut().zip(values) {
            field.push_float(*value);
        }
        Ok(())
    }

    /// Like [`GridSeriesBuilder::push`], with native histogram values.
    pub(crate) fn push_samples(
        &mut self,
        index: usize,
        values: &[Option<GridSample>],
    ) -> Result<()> {
        if !self.start_row(index, values.len(), values.iter().all(Option::is_none))? {
            return Ok(());
        }
        for (field, value) in self.fields.iter_mut().zip(values) {
            match value {
                Some(GridSample::Histogram(h)) => {
                    field.values.push(0.0);
                    field.nulls.append(false);
                    let rows = field.values.len() - 1;
                    let column = field.histograms.get_or_insert_with(|| HistogramColumn {
                        batch: HistogramBatchBuilder::new(),
                        indices: vec![None; rows],
                    });
                    column.indices.push(Some(h.push_to(&mut column.batch)?));
                }
                Some(GridSample::Float(value)) => field.push_float(Some(*value)),
                None => field.push_float(None),
            }
        }
        Ok(())
    }

    /// Validates the row and retains it; returns whether it holds a value to write.
    fn start_row(&mut self, index: usize, len: usize, empty: bool) -> Result<bool> {
        if len != self.schema.fields.len() {
            return Err(DataFusionError::Execution(format!(
                "grid series row has {len} fields, schema has {}",
                self.schema.fields.len()
            )));
        }
        if index < self.next || index >= self.grid.timestamps().len() {
            return Err(DataFusionError::Execution(format!(
                "grid series index {index} is not increasing or exceeds grid length {}",
                self.grid.timestamps().len()
            )));
        }
        self.next = index + 1;
        if empty {
            // A position holding no value anywhere is not a row: an index absent from `indices`
            // already says exactly that, so writing it down would only cost storage.
            return Ok(false);
        }
        if self.fields.is_empty() {
            self.fields = (0..len).map(|_| FieldBuilder::new()).collect();
        }
        self.indices.push(index);
        Ok(true)
    }

    /// Whether rows have been pushed that no caller has taken yet.
    ///
    /// Every hand-out drains the buffer, so "rows buffered" and "rows a caller has not taken" are
    /// the same thing: a `true` here means the next [`Self::finish`] or [`Self::take_buffered`] has
    /// something to return.
    pub fn has_pending(&self) -> bool {
        !self.indices.is_empty()
    }

    /// Starts the series the following pushes belong to.
    ///
    /// This is the only place that resets the series state, so it is the logical-series boundary:
    /// [`Self::finish`] hands the rows out but resets nothing, and [`Self::take_buffered`]
    /// deliberately does not reset anything either.
    ///
    /// Panics in debug builds when rows are still pending, which means the previous series was
    /// left unread.
    pub fn begin_series(&mut self) {
        debug_assert!(
            !self.has_pending(),
            "a new series starts with rows of the previous one still pending"
        );
        self.next = 0;
        self.indices.clear();
        self.fields.clear();
    }

    /// Hands out every row buffered so far as one series, without ending the series.
    ///
    /// This is a prefix flush: the rows pushed since the last hand-out become one [`Series`] whose
    /// axis names the runs they make up. `None` means nothing is buffered, and the caller may push
    /// more rows. Later pushes still belong to the same logical series, so the caller keeps the
    /// metric and reads the next hand-out as a continuation rather than as a new series.
    ///
    /// Nothing is reset: not the metric, not the index cursor, not the position order. A caller
    /// that wants a new series calls [`Self::begin_series`].
    pub fn take_buffered(&mut self, metric: MetricName) -> Result<Option<Series>> {
        self.collect(metric)
    }

    /// Returns the series the pushed rows make up: one [`Series`] over the runs of consecutive
    /// positions that hold a value, or `None` when no retained row holds one.
    ///
    /// The rows share the one buffer per field, so the result costs a span per run and not a row
    /// per position between the runs. This ends the logical series; the next one starts with
    /// [`Self::begin_series`], which is the only reset.
    pub fn finish(&mut self, metric: MetricName) -> Result<Option<Series>> {
        self.collect(metric)
    }

    /// Turns the buffered rows into one series over the runs they make up.
    fn collect(&mut self, metric: MetricName) -> Result<Option<Series>> {
        if self.indices.is_empty() {
            return Ok(None);
        }
        let fields = std::mem::take(&mut self.fields);
        let indices = std::mem::take(&mut self.indices);
        let samples = fields
            .into_iter()
            .map(|mut field| {
                let floats = Float64Array::new(field.values.into(), field.nulls.finish());
                let Some(column) = field.histograms else {
                    return Ok(Samples::from_float(floats));
                };
                let batch = column.batch.finish().map_err(external)?;
                let histograms =
                    HistogramSamples::try_new(UInt32Array::from(column.indices), batch)
                        .map_err(external)?;
                Samples::try_from_mixed(floats, histograms).map_err(external)
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = SeriesFields::try_from_vec(samples).map_err(external)?;
        // The runs are all the axis has to remember about the gaps, and the rows are already
        // compact, so no position holding no value is visited or stored.
        let spans = spans_of(&indices);
        let time = TimeAxis::from_grid_spans(self.grid.clone(), spans).map_err(external)?;
        Series::try_new(&self.schema, metric, time, fields)
            .map(Some)
            .map_err(external)
    }
}

/// Cuts the retained indices into maximal runs of consecutive positions.
fn spans_of(indices: &[usize]) -> Vec<GridSpan> {
    let mut spans: Vec<GridSpan> = Vec::new();
    let mut start = 0;
    while let Some(&position) = indices.get(start) {
        let mut len = 1;
        while indices.get(start + len) == Some(&(position + len)) {
            len += 1;
        }
        spans.push(GridSpan::new(position, len));
        start += len;
    }
    spans
}

impl FieldBuilder {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            nulls: NullBufferBuilder::new(0),
            histograms: None,
        }
    }

    fn push_float(&mut self, value: Option<f64>) {
        self.values.push(value.unwrap_or_default());
        self.nulls.append(value.is_some());
        if let Some(column) = &mut self.histograms {
            column.indices.push(None);
        }
    }
}

/// Prometheus `schema.IsMetadataLabel`: the labels removed with the metric name.
pub(crate) const METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];

pub(crate) type LabelSet = Vec<(Arc<str>, Arc<str>)>;

/// A label set as Prometheus prints one in errors: `{name="value", ...}`.
pub(crate) fn render_label_set(labels: &LabelSet) -> String {
    let labels: Vec<String> = labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect();
    format!("{{{}}}", labels.join(", "))
}

/// The label set Prometheus compares, which has no empty values.
pub(crate) fn label_set(metric: &MetricName) -> LabelSet {
    let name = metric
        .name()
        .filter(|name| !name.is_empty())
        .map(|name| (Arc::from("__name__"), Arc::from(name)));
    name.into_iter()
        .chain(
            metric
                .tags()
                .iter()
                .filter(|tag| !tag.value.is_empty())
                .map(|tag| (tag.name.clone(), tag.value.clone())),
        )
        .collect()
}

/// Merges series that share a label set into one, as Prometheus does after a function that
/// rewrites labels or drops `__name__`; two of them holding one field at one point fail, whether
/// the samples are floats or native histograms.
///
/// The merge rearranges the payloads instead of rewriting them: a merged row takes the value its
/// member carries at that position -- a float, a NaN, or a native histogram -- and a field another
/// field's row does not fill keeps a null there. The values keep their storage: a run of rows one
/// member holds in one dense segment is handed out as a [`Samples::slice`] of that segment, so a
/// histogram table is shared rather than copied. The result costs one row per position holding a
/// value, plus the rows the other fields fill, and nothing for a position of the grid holding none.
pub fn merge_same_labelset(schema: &Arc<SeriesSchema>, series: Vec<Series>) -> Result<Vec<Series>> {
    let mut groups: HashMap<LabelSet, Vec<usize>> = HashMap::with_capacity(series.len());
    let mut collided = false;
    for (index, series) in series.iter().enumerate() {
        match groups.entry(label_set(&series.metric)) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(index);
                collided = true;
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![index]);
            }
        }
    }
    if !collided {
        return Ok(series);
    }

    // The merged axis and the fields placed on it are the schema's grid: a member of another grid
    // would land its rows at positions naming other timestamps.
    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution("grid series schema requires an evaluation grid".into())
    })?;
    let mut series: Vec<Option<Series>> = series.into_iter().map(Some).collect();
    let mut output = Vec::with_capacity(groups.len());
    // The groups in the order their first member arrived: a `HashMap` would hand the merged series
    // back in an order that changes between runs, and the root's chunk stream exposes exactly this
    // order, so two executions of one query could report their series differently.
    let mut groups: Vec<(LabelSet, Vec<usize>)> = groups.into_iter().collect();
    groups.sort_unstable_by_key(|(_, members)| members[0]);
    // `(absolute position, member slot, compact row)`, sorted by position before merging. The slot
    // indexes the group's fragments, so a row names its member without a search.
    let mut rows: Vec<(usize, usize, usize)> = Vec::new();
    // The position of each merged row, ascending, with the positions several fragments share held
    // once.
    let mut positions: Vec<usize> = Vec::new();
    for (labels, members) in groups {
        if let [member] = members[..] {
            output.extend(series[member].take());
            continue;
        }
        // Validate every fragment before indexing it: the merge reads its fields and places its
        // rows on the schema's grid.
        let mut fragments: Vec<&Series> = Vec::with_capacity(members.len());
        for &member in &members {
            let fragment = series[member].as_ref().ok_or_else(|| {
                DataFusionError::Execution("each series is in exactly one label set".into())
            })?;
            let Some(fragment_grid) = fragment.time.grid() else {
                return Err(DataFusionError::Execution(
                    "merging requires evaluated series".into(),
                ));
            };
            if fragment_grid.spec() != grid.spec() {
                return Err(DataFusionError::Execution(
                    "merging requires one evaluation grid".into(),
                ));
            }
            if fragment.fields.len() != schema.fields.len() {
                return Err(DataFusionError::Execution(format!(
                    "merging requires {} fields, a fragment holds {}",
                    schema.fields.len(),
                    fragment.fields.len()
                )));
            }
            // The alignment is checked before a row is read: a field of another length would either
            // run off the axis or leave values the axis never reaches.
            let time_len = fragment.time.len();
            if let Some((field, samples)) = fragment
                .fields
                .iter()
                .enumerate()
                .find(|(_, samples)| samples.len() != time_len)
            {
                return Err(DataFusionError::Execution(format!(
                    "merging requires every field to hold the {time_len} rows of its axis, field \
                     {field} holds {}",
                    samples.len()
                )));
            }
            fragments.push(fragment);
        }
        rows.clear();
        for (member_slot, fragment) in fragments.iter().enumerate() {
            // One forward reader per field: the axis names the rows that exist, and a row holding
            // no value in any field is not a merged row.
            let mut readers: Vec<FieldReader<'_>> =
                fragment.fields.iter().map(FieldReader::new).collect();
            for (position, row) in fragment.time.positions() {
                let mut present = false;
                for reader in &mut readers {
                    let (segment, offset) = reader.locate(row)?;
                    present |= !matches!(segment.sample(offset), SampleRef::Missing);
                }
                if present {
                    rows.push((position, member_slot, row));
                }
            }
        }
        // By position, then by member slot: the slots follow the order the fragments were collected
        // in, so a member's rows only go forward and its readers never go back.
        rows.sort_unstable();
        positions.clear();
        positions.extend(rows.iter().map(|&(position, _, _)| position));
        positions.dedup();
        let mut fields = Vec::with_capacity(schema.fields.len());
        for field in 0..schema.fields.len() {
            let mut readers: Vec<FieldReader<'_>> = fragments
                .iter()
                .map(|fragment| FieldReader::new(&fragment.fields.as_slice()[field]))
                .collect();
            fields.push(merge_field(&mut readers, &rows, &labels)?);
        }
        // The runs are all the axis remembers about the holes, so a sparse merge stores one span
        // per run and no row for the positions between them.
        let time =
            TimeAxis::from_grid_spans(grid.clone(), spans_of(&positions)).map_err(external)?;
        let fields = SeriesFields::try_from_vec(fields).map_err(external)?;
        // The first fragment names the output, as it always has: a label set ignores empty values,
        // so the metric of the first member is the one the merged series keeps.
        let metric = fragments[0].metric.clone();
        output.push(Series::try_new(schema, metric, time, fields).map_err(external)?);
    }
    Ok(output)
}

/// Builds one field of the merged series, one sample per merged row.
///
/// The rows are walked in output order. A row holding a value of this field in one member continues
/// the run of rows that member holds in one dense segment, and a run is handed out as one
/// [`Samples::slice`]; the rows another field fills become one null run, so the field stays aligned
/// with the merged axis while sharing the values it carries.
fn merge_field(
    readers: &mut [FieldReader<'_>],
    rows: &[(usize, usize, usize)],
    labels: &LabelSet,
) -> Result<Samples> {
    let mut chunks: Vec<Samples> = Vec::new();
    // The run of values the last merged row continued, if any.
    let mut run: Option<FieldRun<'_>> = None;
    // Merged rows this field leaves empty, waiting for the null run that carries them.
    let mut missing = 0;
    let mut start = 0;
    while start < rows.len() {
        let position = rows[start].0;
        let mut end = start;
        // At most one member may hold this field here; the second value is the collision Prometheus
        // rejects, since the two label sets became one.
        let mut value: Option<(usize, &Samples, usize)> = None;
        while end < rows.len() && rows[end].0 == position {
            let (_, member, row) = rows[end];
            let (segment, offset) = readers[member].locate(row)?;
            if !matches!(segment.sample(offset), SampleRef::Missing) {
                if value.is_some() {
                    let labels: Vec<_> = labels
                        .iter()
                        .map(|(name, value)| format!("{name}=\"{value}\""))
                        .collect();
                    return Err(DataFusionError::Execution(format!(
                        "vector cannot contain metrics with the same labelset {{{}}}",
                        labels.join(", ")
                    )));
                }
                value = Some((member, segment, offset));
            }
            end += 1;
        }
        match value {
            Some((member, segment, offset)) => {
                if let Some(run) = &mut run
                    && run.continues(member, segment, offset)
                {
                    run.len += 1;
                } else {
                    if let Some(run) = run.take() {
                        run.push(&mut chunks)?;
                    }
                    push_missing(&mut chunks, &mut missing);
                    run = Some(FieldRun {
                        member,
                        segment,
                        offset,
                        len: 1,
                    });
                }
            }
            None => {
                if let Some(run) = run.take() {
                    run.push(&mut chunks)?;
                }
                missing += 1;
            }
        }
        start = end;
    }
    if let Some(run) = run {
        run.push(&mut chunks)?;
    }
    push_missing(&mut chunks, &mut missing);
    Samples::try_from_chunks(chunks).map_err(external)
}

/// A run of merged rows one member field fills out of one dense segment, without copying values.
///
/// The run grows only while the next merged row is the same member's next row in the same segment,
/// so handing it out is one [`Samples::slice`] of that segment: the payload stays where it is, and
/// the rows a hole skipped are not part of the run.
struct FieldRun<'a> {
    /// The member slot the run reads, which indexes the [`FieldReader`]s of the field.
    member: usize,
    /// The dense segment holding the run's values.
    segment: &'a Samples,
    /// Offset of the run's first row inside `segment`.
    offset: usize,
    /// How many rows of `segment` the run covers.
    len: usize,
}

impl FieldRun<'_> {
    /// Returns whether the value at `offset` of `segment` continues the run: the same member's next
    /// row in the same dense segment.
    fn continues(&self, member: usize, segment: &Samples, offset: usize) -> bool {
        self.member == member
            && std::ptr::eq(self.segment, segment)
            && self.offset + self.len == offset
    }

    /// Pushes the run as one slice of its dense segment.
    fn push(self, chunks: &mut Vec<Samples>) -> Result<()> {
        chunks.push(
            self.segment
                .slice(self.offset, self.len)
                .map_err(external)?,
        );
        Ok(())
    }
}

/// Reads one field's rows by locating the dense segment each row sits in.
///
/// [`Samples::sample`] rescans a chunked field from its first segment on every row, so reading a
/// field row by row would cost a scan over the segments before it. The rows a merge reads only go
/// forward, so this walks the segments once and returns the segment with the offset of the row
/// inside it, which is what a [`FieldRun`] slices.
struct FieldReader<'a> {
    segments: &'a [Samples],
    /// Index of the segment `start` belongs to.
    segment: usize,
    /// Logical row the current segment starts at.
    start: usize,
}

impl<'a> FieldReader<'a> {
    /// Creates a reader at the first row of `samples`.
    fn new(samples: &'a Samples) -> Self {
        Self {
            segments: samples.segments(),
            segment: 0,
            start: 0,
        }
    }

    /// Returns the dense segment holding `row` and the offset of `row` inside it.
    ///
    /// Rows must not go backwards. A row past the field is an error rather than a panic: a member is
    /// only read after its fields are validated to align with its axis.
    fn locate(&mut self, row: usize) -> Result<(&'a Samples, usize)> {
        while let Some(segment) = self.segments.get(self.segment) {
            if let Some(offset) = row
                .checked_sub(self.start)
                .filter(|offset| *offset < segment.len())
            {
                return Ok((segment, offset));
            }
            self.segment += 1;
            self.start += segment.len();
        }
        Err(DataFusionError::Execution(
            "merging requires each field to be row-aligned with its time axis".into(),
        ))
    }
}

/// Pushes the merged rows a field leaves empty as one null run.
fn push_missing(chunks: &mut Vec<Samples>, missing: &mut usize) {
    if *missing != 0 {
        chunks.push(Samples::from_float(Float64Array::new_null(*missing)));
        *missing = 0;
    }
}

pub(crate) fn external(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
