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

//! Vector–vector binary and set operations over evaluated series on one grid.
//!
//! Semantics follow Prometheus `VectorBinop`, `VectorAnd`, `VectorOr` and `VectorUnless`
//! evaluated at every grid point. Series are processed whole, over their own grid ranges, and
//! the cardinality rules are checked per point: series sharing a match group only conflict at
//! points where both are present. Each field is matched on its own, as if the operands held
//! only that field; the results of all fields with one label set form one series.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::histogram::{CounterResetHint, HistogramBatchBuilder, HistogramView};
use datatypes::series::samples::{FieldSpec, RowCursor, SampleRef, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridRunPair, GridSpan, TimeAxis};
use datatypes::series::{Label, MetricName};

use crate::grid_series::{GridSample, GridSeriesBuilder, METADATA_LABELS};
use crate::histogram_eval::float_histogram::FloatHistogram;

#[cfg(test)]
mod tests;

/// A PromQL binary operator between two instant vectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

impl BinaryOp {
    fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Ne | Self::Gt | Self::Lt | Self::Ge | Self::Le
        )
    }

    /// The value and whether a comparison holds, as Prometheus `vectorElemBinop` computes them.
    fn apply(self, lhs: f64, rhs: f64) -> (f64, bool) {
        match self {
            Self::Add => (lhs + rhs, true),
            Self::Sub => (lhs - rhs, true),
            Self::Mul => (lhs * rhs, true),
            Self::Div => (lhs / rhs, true),
            Self::Mod => (lhs % rhs, true),
            Self::Pow => (lhs.powf(rhs), true),
            Self::Atan2 => (lhs.atan2(rhs), true),
            Self::Eq => (lhs, lhs == rhs),
            Self::Ne => (lhs, lhs != rhs),
            Self::Gt => (lhs, lhs > rhs),
            Self::Lt => (lhs, lhs < rhs),
            Self::Ge => (lhs, lhs >= rhs),
            Self::Le => (lhs, lhs <= rhs),
        }
    }
}

/// The labels two series are matched by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchingLabels {
    /// `on(...)`: only these labels, `__name__` included when listed.
    On(Vec<Arc<str>>),
    /// `ignoring(...)`, and the default with no labels: every label except these and
    /// `__name__`.
    Ignoring(Vec<Arc<str>>),
}

/// How many series of each side may share a match group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cardinality {
    OneToOne,
    /// `group_left(...)`: the right side is the "one" side; the listed labels are copied from it.
    ManyToOne(Vec<Arc<str>>),
    /// `group_right(...)`: the left side is the "one" side; the listed labels are copied from it.
    OneToMany(Vec<Arc<str>>),
}

/// A vector–vector binary operation with its modifiers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryMatching {
    pub op: BinaryOp,
    /// `bool`: comparisons return 1 or 0 instead of filtering.
    pub return_bool: bool,
    pub labels: MatchingLabels,
    pub cardinality: Cardinality,
}

/// A PromQL set operator between two instant vectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    And,
    Or,
    Unless,
}

/// The schema of `binary_vectors` output: fields paired by position, named after the left side.
pub fn binary_output_schema(lhs: &SeriesSchema, rhs: &SeriesSchema) -> Result<Arc<SeriesSchema>> {
    let grid = same_grid(lhs, rhs)?;
    let fields = lhs.fields.len().min(rhs.fields.len());
    Ok(Arc::new(SeriesSchema {
        fields: lhs.fields[..fields].to_vec().into(),
        eval_grid: Some(grid),
    }))
}

/// Applies `matching` to every pair of matched series at every grid point.
///
/// Fields are paired by position. Arithmetic and `bool` comparisons drop `__name__`, `__type__`
/// and `__unit__`; comparisons without `bool` keep the left value where they hold. Native
/// histograms support `+`, `-`, `==` and `!=` with each other, `*` with a float on either side
/// and `/` by a float; Prometheus skips any other pairing of a histogram.
///
/// A caller that accounts for the workspace this allocates calls
/// [`binary_vectors_with_reservation`]; this form reserves nothing.
pub fn binary_vectors(
    matching: &BinaryMatching,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
) -> Result<Vec<Series>> {
    binary_vectors_with_reservation(
        matching,
        lhs_schema,
        lhs,
        rhs_schema,
        rhs,
        &mut |_: usize| -> Result<()> { Ok(()) },
    )
}

/// [`binary_vectors`], handing the workspace it needs to `reserve` as it allocates.
///
/// What the operation will allocate cannot be known before it allocates: which series match each
/// other, where their axes overlap and which rows the operator keeps all come out of the operands
/// as they are read. So `reserve` receives the **total** additional live workspace the operation
/// needs, never an increment: the first call covers what the operands' metadata bounds, and every
/// later call only grows it, because the rows the results keep are added as they are filed while a
/// step's scratch replaces the scratch of the step before it. A total `reserve` refuses ends the
/// operation with that error, and what it admitted stays the caller's to release.
pub(crate) fn binary_vectors_with_reservation(
    matching: &BinaryMatching,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
    reserve: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<Vec<Series>> {
    let output = binary_output_schema(lhs_schema, rhs_schema)?;
    let grid = output
        .eval_grid
        .clone()
        .expect("set by binary_output_schema");
    let mut budget = Budget::new(reserve);
    // The output schema's field list, one result table per field and the vector holding them.
    let descriptors = output
        .fields
        .len()
        .saturating_mul(std::mem::size_of::<Results>() + 2 * std::mem::size_of::<FieldSpec>())
        .saturating_add(std::mem::size_of::<SeriesSchema>());
    budget.retain(descriptors)?;
    let mut errors = Vec::new();
    let mut fields = Vec::with_capacity(output.fields.len());
    for field in 0..output.fields.len() {
        let lhs = Operand::try_new(lhs_schema, lhs, field)?;
        let rhs = Operand::try_new(rhs_schema, rhs, field)?;
        fields.push(binary_field(
            matching,
            &grid,
            &lhs,
            &rhs,
            &mut errors,
            &mut budget,
        )?);
    }
    earliest_error(errors)?;
    // The merge holds every field's results at once: the triples it files, the table it files them
    // in and the builder writing the series out are the last scratch of the operation.
    let sets = fields
        .iter()
        .map(|results| results.series.len())
        .fold(0usize, usize::saturating_add);
    let merge = merge_bytes(
        budget.rows,
        sets,
        output.fields.len(),
        budget.histograms,
        budget.histogram_heap,
    );
    budget.drop_scratch();
    budget.step(merge)?;
    merge_fields(fields, output)
}

/// One label of a match key, charged twice because `group_key` grows its `Vec` by doubling.
const KEY_LABEL_BYTES: usize = 2 * std::mem::size_of::<(Arc<str>, Arc<str>)>();

/// One entry of a match-group table, with the control byte and spare capacity it keeps.
const GROUP_SLOT_BYTES: usize = 2 * std::mem::size_of::<(GroupKey, Vec<usize>)>();

/// One entry of the one-to-one match set, charged the same way.
const MATCH_SLOT_BYTES: usize = 2 * std::mem::size_of::<(usize, usize)>();

/// One entry of a field's result table, charged the same way.
const RESULT_SLOT_BYTES: usize = 2 * std::mem::size_of::<(GroupKey, ResultRows)>();

/// One row of a field's results: its point, its match group and its value, charged twice for the
/// capacity each of the three vectors ends at.
const RESULT_ROW_BYTES: usize =
    2 * (2 * std::mem::size_of::<usize>() + std::mem::size_of::<Option<GridSample>>());

/// One row of the merge's table, charged the same way.
const MERGE_SLOT_BYTES: usize =
    2 * std::mem::size_of::<(GroupKey, (MetricName, Vec<(usize, usize, GridSample)>))>();

/// One row of the builder's histogram table: the scalar columns and the seven list offsets.
const HISTOGRAM_ROW_BYTES: usize = 4 * std::mem::size_of::<f64>()
    + 2 * std::mem::size_of::<i32>()
    + std::mem::size_of::<i64>()
    + 7 * std::mem::size_of::<i32>();

/// The fixed text of one error message, around the label set it renders.
const ERROR_TEXT_BYTES: usize = 256;

/// The workspace a binary operation admits as it allocates.
///
/// What the results retain -- rows, the label sets they are filed under, their histograms -- only
/// grows, while the scratch of a step replaces the scratch of the step before it. So the total
/// handed to the caller is the live peak: the retained bytes plus the largest scratch beside them,
/// never the sum of everything the operation allocated along the way.
struct Budget<'a> {
    /// Admits a total of additional live workspace, or refuses it.
    reserve: &'a mut dyn FnMut(usize) -> Result<()>,
    /// The bytes the results keep until the operation returns.
    retained: usize,
    /// The largest scratch needed beside them by the step being run.
    scratch: usize,
    /// The largest total admitted so far.
    total: usize,
    /// The result rows filed, which the merge holds until it writes them out.
    rows: usize,
    /// The result rows holding a histogram, and the heap those histograms hold.
    histograms: usize,
    histogram_heap: usize,
}

impl<'a> Budget<'a> {
    fn new(reserve: &'a mut dyn FnMut(usize) -> Result<()>) -> Self {
        Self {
            reserve,
            retained: 0,
            scratch: 0,
            total: 0,
            rows: 0,
            histograms: 0,
            histogram_heap: 0,
        }
    }

    /// Retains `bytes` of results and admits the total covering them.
    fn retain(&mut self, bytes: usize) -> Result<()> {
        self.retained = self.retained.saturating_add(bytes);
        self.admit()
    }

    /// Admits a step needing `bytes` beside what is retained, replacing the previous step's
    /// scratch.
    fn step(&mut self, bytes: usize) -> Result<()> {
        self.scratch = self.scratch.max(bytes);
        self.admit()
    }

    /// Forgets the last step's scratch: its tables and buffers are gone.
    fn drop_scratch(&mut self) {
        self.scratch = 0;
    }

    /// Retains one histogram result row: the value it boxes and the heap its buckets hold.
    fn retain_histogram(&mut self, heap: usize) -> Result<()> {
        self.histograms = self.histograms.saturating_add(1);
        self.histogram_heap = self.histogram_heap.saturating_add(heap);
        self.retain(std::mem::size_of::<FloatHistogram>().saturating_add(heap))
    }

    /// Hands the live total to `reserve` once it has grown. It never shrinks, so the caller never
    /// gives back what a later step no longer needs.
    fn admit(&mut self) -> Result<()> {
        let total = self.retained.saturating_add(self.scratch);
        if total > self.total {
            self.total = total;
            (self.reserve)(total)?;
        }
        Ok(())
    }
}

/// What one field's tables, flags and messages cost before it pairs anything, with the largest
/// label-set key it can file a row under.
///
/// The presence of each side and of the points both are present at are three flags per grid point;
/// the one side's groups are a key, the values `on(...)` matching copies and a member list per
/// series with the table holding them and the references indexing them; the duplicate scan
/// collects that side's present points; one series of the many side is looked up by a key of its
/// own; and a field reports at most three messages, each a fixed frame around one rendering.
/// Nothing here counts the pairing the field walks: which series match and where their axes
/// overlap is only known as the walk reaches them.
fn field_metadata_bytes(
    matching: &BinaryMatching,
    include: &[Arc<str>],
    points: usize,
    many: &Operand<'_>,
    one: &Operand<'_>,
) -> (usize, usize) {
    let mut bytes = points.saturating_mul(3);
    let mut largest_key = 0usize;
    for series in one.series {
        bytes = bytes
            .saturating_add(match_key_bytes(&series.metric, &matching.labels))
            .saturating_add(2 * std::mem::size_of::<usize>())
            .saturating_add(GROUP_SLOT_BYTES)
            .saturating_add(2 * std::mem::size_of::<&GroupKey>())
            .saturating_add(2 * std::mem::size_of::<(&GroupKey, usize)>())
            // The duplicate scan's point list, one point per row the side carries at most.
            .saturating_add(2 * std::mem::size_of::<usize>() * series.time.len());
        largest_key = largest_key.max(result_key_bytes(&series.metric));
    }
    let mut lookup = 0usize;
    for series in many.series {
        lookup = lookup.max(match_key_bytes(&series.metric, &matching.labels));
        largest_key = largest_key.max(result_key_bytes(&series.metric));
    }
    // A result label set carries the many side's labels and the `group_*` ones copied into it, so
    // the widest rendering over both operands bounds the text an error writes.
    let include_text = include.iter().fold(0usize, |bytes, name| {
        bytes.saturating_add(name.len()).saturating_add(6)
    });
    let text = many
        .series
        .iter()
        .chain(one.series)
        .fold(include_text, |bytes, series| {
            bytes.max(label_text_bytes(&series.metric).saturating_add(include_text))
        });
    let errors = 3usize.saturating_mul(ERROR_TEXT_BYTES.saturating_add(text));
    (
        bytes.saturating_add(lookup).saturating_add(errors),
        largest_key.saturating_add(include.len().saturating_mul(KEY_LABEL_BYTES)),
    )
}

/// The bytes one pairing allocates before it files a row: the result label set `result_metric`
/// rewrites, the `group_*` labels it copies in, and the key its rows are looked up by.
///
/// Each rewrite filters the tag list into a `Vec` and turns that into a shared slice, so the list
/// it replaces is alive while two copies of the new one are built, and an included label the many
/// side does not carry is inserted, which costs its descriptor too. This is one pairing's scratch:
/// the next pairing replaces it, and a pairing whose axes share no position still pays it, because
/// the label set is built before the axes are intersected.
fn pair_bytes(many: &MetricName, one: &MetricName, include: &[Arc<str>]) -> usize {
    // The label list each rewrite builds and the one it replaces, then the key `key_of` builds for
    // a row: every label with a value and the name, each charged twice for the growth of the key's
    // `Vec`, and the name string it copies.
    let labels = 3usize
        .saturating_mul(many.tags().len().saturating_add(include.len()))
        .saturating_mul(std::mem::size_of::<Label>());
    let key = many
        .tags()
        .len()
        .saturating_add(include.len())
        .saturating_add(1)
        .saturating_mul(KEY_LABEL_BYTES)
        .saturating_add(many.name().map_or(0, str::len));
    labels
        .saturating_add(copied_bytes(one, include))
        .saturating_add(key)
}

/// The label text `result_metric` copies from the one side: the value of every included label it
/// sets, and the name of a label it inserts, since `MetricName::set` copies that as well.
fn copied_bytes(one: &MetricName, include: &[Arc<str>]) -> usize {
    include
        .iter()
        .fold(0usize, |bytes, name| match one.get(name) {
            Some(value) if !value.is_empty() => {
                bytes.saturating_add(value.len()).saturating_add(name.len())
            }
            _ => bytes,
        })
}

/// The key `key_of` files a result label set under: every label with a value and the name, each
/// charged twice for the growth of the key's `Vec`, plus the name string it copies.
fn result_key_bytes(metric: &MetricName) -> usize {
    metric
        .tags()
        .len()
        .saturating_add(1)
        .saturating_mul(KEY_LABEL_BYTES)
        .saturating_add(metric.name().map_or(0, str::len))
}

/// The bytes one result label set keeps once a row is filed under it: the label list and the
/// copied strings `result_metric` leaves behind, its key and its entry in the result table.
fn result_set_bytes(metric: &MetricName, one: &MetricName, include: &[Arc<str>]) -> usize {
    metric
        .tags()
        .len()
        .saturating_mul(std::mem::size_of::<Label>())
        .saturating_add(copied_bytes(one, include))
        .saturating_add(result_key_bytes(metric))
        .saturating_add(RESULT_SLOT_BYTES)
}

/// The bytes the merge of every field's results and the builder writing them out allocate.
///
/// The merge files every kept triple under its label set and hands one series per label set to a
/// builder, so the triples, the table, the row it fills, the output vector and the builder's own
/// buffers are alive together: one index per retained row, one value and validity bit per field
/// and row, the span a run may start with, a row of the histogram table per histogram and the heap
/// its buckets are copied to. Every term counts a row or a label set a field actually filed, so
/// operands whose pairs keep nothing cost no output.
fn merge_bytes(
    rows: usize,
    sets: usize,
    fields: usize,
    histograms: usize,
    histogram_heap: usize,
) -> usize {
    // One kept triple, charged twice for the capacity its vector ends at, and one row of the
    // builder's columns: its index, its value and validity bit per field, the span it may start
    // and its row of the histogram table's index column.
    let values = 2 * fields.saturating_mul(std::mem::size_of::<f64>() + 1);
    let row = (2 * std::mem::size_of::<(usize, usize, GridSample)>())
        .saturating_add(2 * std::mem::size_of::<usize>())
        .saturating_add(2 * std::mem::size_of::<GridSpan>())
        .saturating_add(values)
        .saturating_add(std::mem::size_of::<Option<u32>>());
    rows.saturating_mul(row)
        // One label set's entry in the merge table and the series it is written out as.
        .saturating_add(
            sets.saturating_mul(MERGE_SLOT_BYTES.saturating_add(2 * std::mem::size_of::<Series>())),
        )
        // The row the builder reads its pushes from, one slot per field.
        .saturating_add(2 * fields.saturating_mul(std::mem::size_of::<Option<GridSample>>()))
        // The histogram table of every field that carries one: the builders of its columns, the
        // scalars and list offsets of its rows, and the buckets the batch copies beside the value
        // the results still hold.
        .saturating_add(fields.saturating_mul(std::mem::size_of::<HistogramBatchBuilder>()))
        .saturating_add(histograms.saturating_mul(2 * HISTOGRAM_ROW_BYTES))
        .saturating_add(2 * histogram_heap)
}

/// The bytes one match key holds: its label pairs and, for `on(...)`, a copy of each value.
///
/// `group_key` copies an `on(...)` value with `Arc::from` and shares the `Arc` of an
/// `ignoring(...)` label, so only `on(...)` adds string bytes. The pairs are charged twice because
/// the key is collected into a `Vec` that grows by doubling.
fn match_key_bytes(metric: &MetricName, labels: &MatchingLabels) -> usize {
    match labels {
        MatchingLabels::On(names) => {
            names
                .iter()
                .fold(0usize, |bytes, name| match metric.get(name) {
                    Some(value) if !value.is_empty() => bytes
                        .saturating_add(KEY_LABEL_BYTES)
                        .saturating_add(value.len()),
                    _ => bytes,
                })
        }
        MatchingLabels::Ignoring(names) => metric
            .tags()
            .iter()
            .filter(|tag| !tag.value.is_empty() && !names.contains(&tag.name))
            .count()
            .saturating_mul(KEY_LABEL_BYTES),
    }
}

/// The bytes `render` writes for a label set built from `metric`: each label as `name="value"`,
/// the braces and separators between them, and the metric's name.
fn label_text_bytes(metric: &MetricName) -> usize {
    let named = metric
        .name()
        .map_or(0, |name| name.len().saturating_add(10));
    metric.tags().iter().fold(named, |bytes, tag| {
        bytes
            .saturating_add(tag.name.len())
            .saturating_add(tag.value.len())
            .saturating_add(6)
    })
}

/// The bytes one owned copy of a histogram operand holds: its scalars inline, and on the heap the
/// buckets of both sides, the span layout of the view and its custom bounds.
fn owned_histogram_bytes(view: &HistogramView<'_>) -> usize {
    let sides = [view.positive, view.negative];
    let buckets = sides
        .iter()
        .map(|side| {
            side.buckets
                .len()
                .saturating_mul(std::mem::size_of::<(i32, f64)>())
        })
        .fold(0usize, usize::saturating_add);
    let spans = sides
        .iter()
        .map(|side| {
            side.span_offsets
                .len()
                .saturating_mul(std::mem::size_of::<(i32, i32)>())
        })
        .fold(0usize, usize::saturating_add);
    std::mem::size_of::<FloatHistogram>()
        .saturating_add(buckets)
        .saturating_add(spans)
        .saturating_add(
            view.custom_values
                .len()
                .saturating_mul(std::mem::size_of::<f64>()),
        )
}

/// The bytes `elem_binop` allocates to read one histogram pairing before it can hand back a value.
///
/// The operator copies both operands into owned [`FloatHistogram`]s that stay alive while the
/// result is built from them: their buckets, the span layout of the view each was read from and
/// their custom bounds. `+` and `-` then insert every bucket index of the right side into the left
/// copy, so the result holds the union of both sides' buckets -- subtracting disjoint buckets adds
/// the right side's -- in a list charged twice because it reallocates while the old buffer is still
/// alive, and a schema mismatch reduces a copy of each side before merging. The value the operator
/// boxes is charged as well. This is one row's scratch: a row the operator drops leaves only this
/// behind, and the next row replaces it.
fn histogram_pair_bytes(lhs: &SampleRef<'_>, rhs: &SampleRef<'_>) -> usize {
    // The owned copy of one operand, its buckets and its custom bounds; a float has none of them.
    let operand = |sample: &SampleRef<'_>| match sample {
        SampleRef::Histogram(view) => (
            owned_histogram_bytes(view),
            view.positive
                .buckets
                .len()
                .saturating_add(view.negative.buckets.len()),
            view.custom_values.len(),
        ),
        _ => (0, 0, 0),
    };
    let (lhs_owned, lhs_buckets, lhs_bounds) = operand(lhs);
    let (rhs_owned, rhs_buckets, rhs_bounds) = operand(rhs);
    if lhs_owned == 0 && rhs_owned == 0 {
        return 0;
    }
    lhs_owned
        .saturating_add(rhs_owned)
        .saturating_add(
            lhs_buckets
                .saturating_add(rhs_buckets)
                .saturating_mul(2 * std::mem::size_of::<(i32, f64)>()),
        )
        .saturating_add(
            lhs_bounds
                .max(rhs_bounds)
                .saturating_mul(2 * std::mem::size_of::<f64>()),
        )
        .saturating_add(2 * std::mem::size_of::<FloatHistogram>())
}

/// Matches one field of each side, collecting the first failure of each kind as
/// `(point, rank, message)`.
fn binary_field(
    matching: &BinaryMatching,
    grid: &EvalGrid,
    lhs: &Operand<'_>,
    rhs: &Operand<'_>,
    errors: &mut Vec<(usize, u8, String)>,
    budget: &mut Budget<'_>,
) -> Result<Results> {
    let (many, one, include, one_is_left) = match &matching.cardinality {
        Cardinality::OneToOne => (lhs, rhs, &[][..], false),
        Cardinality::ManyToOne(include) => (lhs, rhs, &include[..], false),
        Cardinality::OneToMany(include) => (rhs, lhs, &include[..], true),
    };
    let points = grid.timestamps().len();
    // Only the rows of the previous field are still live, so its tables are dropped and this
    // field's are reserved before they are built.
    budget.drop_scratch();
    let (metadata, largest_key) = field_metadata_bytes(matching, include, points, many, one);
    budget.step(metadata)?;
    // Prometheus short-circuits a point where either side is empty, before any check.
    let both_present: Vec<bool> = lhs
        .presence(points)
        .into_iter()
        .zip(rhs.presence(points))
        .map(|(lhs, rhs)| lhs && rhs)
        .collect();

    let one_groups = group(one, &matching.labels);
    let mut one_duplicate: Option<(usize, &GroupKey)> = None;
    for (key, members) in &one_groups {
        if let Some(point) = overlap(one, members, |point| both_present[point])
            && one_duplicate.is_none_or(|(first, _)| point < first)
        {
            one_duplicate = Some((point, key));
        }
    }

    let drop_name = !matching.op.is_comparison() || matching.return_bool;
    let mut results = Results::default();
    let group_keys: Vec<&GroupKey> = one_groups.keys().collect();
    let group_ids: HashMap<&GroupKey, usize> = group_keys
        .iter()
        .enumerate()
        .map(|(id, key)| (*key, id))
        .collect();
    // One-to-one: a match group may be matched once per point; Prometheus counts a filtered
    // comparison as a match too, but not an operation that failed.
    let rows_before = budget.rows;
    let mut matched: HashSet<(usize, usize)> = HashSet::new();
    let mut implicit_many_to_one: Option<(usize, &GroupKey)> = None;
    for (many_index, many_series) in many.series.iter().enumerate() {
        let key = group_key(&many_series.metric, &matching.labels);
        let Some(candidates) = one_groups.get(&key) else {
            continue;
        };
        let group_id = group_ids[&key];
        for &one_index in candidates {
            // One pairing's label set is built before the axes are intersected, so it is
            // reserved first and whatever it keeps is charged as the rows are filed. The next
            // pairing replaces this scratch instead of adding to it, and a pairing whose axes
            // share no position still paid for its label set.
            let pairing = pair_bytes(&many_series.metric, &one.series[one_index].metric, include);
            budget.step(metadata.saturating_add(pairing))?;
            let metric = result_metric(
                &many_series.metric,
                &one.series[one_index].metric,
                matching,
                include,
                drop_name,
            )?;
            let label_set = result_set_bytes(&metric, &one.series[one_index].metric, include);
            // Only the positions both operands carry are evaluated, and each keeps its own row:
            // the intersection is a merge of the two axes, never an envelope with offsets.
            // One cursor per side serves this whole pairing: `intersect_runs` walks both axes'
            // run lists forward, and each `GridRunPair` carries the row of each side, so the rows
            // asked for only increase from one shared run to the next -- the order a `RowCursor`
            // requires. A cursor per run would restart at row 0 and re-walk every row before it.
            let (mut many_cursor, mut one_cursor) =
                (many.cursor(many_index), one.cursor(one_index));
            for pair in many.shared_runs(many_index, one, one_index) {
                for offset in 0..pair.len {
                    let point = pair.position + offset;
                    let (many_value, one_value) = (
                        many_cursor
                            .at(pair.row + offset)
                            .expect("a row never goes back"),
                        one_cursor
                            .at(pair.other_row + offset)
                            .expect("a row never goes back"),
                    );
                    if matches!(many_value, SampleRef::Missing)
                        || matches!(one_value, SampleRef::Missing)
                    {
                        continue;
                    }
                    let (lhs_value, rhs_value) = if one_is_left {
                        (one_value, many_value)
                    } else {
                        (many_value, one_value)
                    };
                    // The operator's own scratch: an owned copy of each histogram operand and
                    // the union it grows into. It is replaced by the row after it, so a float
                    // pairing and the rows of the pairing before it cost nothing more.
                    budget.step(
                        metadata
                            .saturating_add(pairing)
                            .saturating_add(histogram_pair_bytes(&lhs_value, &rhs_value)),
                    )?;
                    let Some((result, keep)) = elem_binop(matching.op, lhs_value, rhs_value)?
                    else {
                        continue;
                    };
                    if matching.cardinality == Cardinality::OneToOne {
                        budget.retain(MATCH_SLOT_BYTES)?;
                        if !matched.insert((group_id, point))
                            && implicit_many_to_one.is_none_or(|(first, _)| point < first)
                        {
                            implicit_many_to_one = Some((point, group_keys[group_id]));
                        }
                    }
                    let value = if matching.return_bool {
                        Some(GridSample::Float(if keep { 1.0 } else { 0.0 }))
                    } else {
                        keep.then_some(result)
                    };
                    // Only a histogram the row keeps is retained: a filtered comparison drops the
                    // value it computed, and a `bool` comparison keeps a float.
                    if let Some(GridSample::Histogram(histogram)) = &value {
                        budget.retain_histogram(histogram.heap_bytes())?;
                    }
                    results.insert(&metric, group_id, point, value, label_set, budget)?;
                }
            }
        }
    }
    // The sort of a label set is the last scratch of the field: the row order it reads, the
    // vectors it rebuilds and the label set a duplicate report clones. Charged for every row the
    // field filed, which bounds one label set's worst case.
    let sort = budget
        .rows
        .saturating_sub(rows_before)
        .saturating_mul(
            4 * std::mem::size_of::<usize>() + 2 * std::mem::size_of::<Option<GridSample>>(),
        )
        .saturating_add(largest_key);
    budget.step(metadata.saturating_add(sort))?;
    // Prometheus fails at the first failing step. Within a step it checks the one side, then
    // matches, and only after the operator checks that result label sets are unique.
    if let Some((point, key)) = one_duplicate {
        let side = if one_is_left { "left" } else { "right" };
        errors.push((
            point,
            0,
            format!(
                "found duplicate series for the match group {} on the {side} hand-side of the \
                 operation at {} ms; many-to-many matching not allowed: matching labels must be \
                 unique on one side",
                render(key),
                grid.timestamps()[point]
            ),
        ));
    }
    if let Some((point, key)) = implicit_many_to_one {
        errors.push((
            point,
            1,
            format!(
                "multiple matches for labels {} at {} ms: many-to-one matching must be explicit \
                 (group_left/group_right)",
                render(key),
                grid.timestamps()[point]
            ),
        ));
    }
    if let Some(duplicate) = results.sort_rows() {
        let grouping = duplicate.within_group && matching.cardinality != Cardinality::OneToOne;
        errors.push(if grouping {
            (
                duplicate.point,
                1,
                format!(
                    "multiple matches for labels {} at {} ms: grouping labels must ensure \
                     unique matches",
                    render(&duplicate.key),
                    grid.timestamps()[duplicate.point]
                ),
            )
        } else {
            (
                duplicate.point,
                2,
                same_labelset(&duplicate.key, grid.timestamps()[duplicate.point]),
            )
        });
    }
    Ok(results)
}

fn earliest_error(errors: Vec<(usize, u8, String)>) -> Result<()> {
    match errors
        .into_iter()
        .min_by_key(|(point, rank, _)| (*point, *rank))
    {
        Some((_, _, error)) => Err(DataFusionError::Execution(error)),
        None => Ok(()),
    }
}

/// Applies a set operator at every grid point. Series keep their labels and values; fields are
/// paired by position.
pub fn set_vectors(
    op: SetOp,
    labels: &MatchingLabels,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
) -> Result<Vec<Series>> {
    let grid = same_grid(lhs_schema, rhs_schema)?;
    if lhs_schema.fields.len() != rhs_schema.fields.len() {
        return Err(DataFusionError::NotImplemented(
            "set operators between vectors with different field counts".into(),
        ));
    }
    let output = Arc::new(SeriesSchema {
        fields: lhs_schema.fields.clone(),
        eval_grid: Some(grid.clone()),
    });
    let mut errors = Vec::new();
    let mut fields = Vec::with_capacity(output.fields.len());
    for field in 0..output.fields.len() {
        let lhs = Operand::try_new(lhs_schema, lhs, field)?;
        let rhs = Operand::try_new(rhs_schema, rhs, field)?;
        let mut results = set_field(op, labels, &lhs, &rhs)?;
        if let Some(duplicate) = results.sort_rows() {
            let timestamp = grid.timestamps()[duplicate.point];
            errors.push((duplicate.point, 0, same_labelset(&duplicate.key, timestamp)));
        }
        fields.push(results);
    }
    earliest_error(errors)?;
    merge_fields(fields, output)
}

fn set_field(
    op: SetOp,
    labels: &MatchingLabels,
    lhs: &Operand<'_>,
    rhs: &Operand<'_>,
) -> Result<Results> {
    let (kept, filter, keep_matched) = match op {
        SetOp::And => (lhs, rhs, true),
        SetOp::Unless => (lhs, rhs, false),
        SetOp::Or => (rhs, lhs, false),
    };
    let filter_groups = group(filter, labels);
    let mut results = Results::default();
    // A set operator hands on the label sets of its operands and no caller accounts for its
    // workspace, so it admits every total and charges no label set of its own.
    let mut unbounded = |_: usize| -> Result<()> { Ok(()) };
    let mut budget = Budget::new(&mut unbounded);
    if op == SetOp::Or {
        for index in 0..lhs.series.len() {
            let mut cursor = lhs.cursor(index);
            for (point, row) in lhs.positions(index) {
                if let Some(value) = owned(cursor.at(row).expect("a row never goes back"))? {
                    results.insert(
                        &lhs.series[index].metric,
                        0,
                        point,
                        Some(value),
                        0,
                        &mut budget,
                    )?;
                }
            }
        }
    }
    for (index, series) in kept.series.iter().enumerate() {
        let candidates = filter_groups.get(&group_key(&series.metric, labels));
        let candidate_indices: &[usize] = candidates.map_or(&[], Vec::as_slice);
        // One forward cursor per candidate: `kept`'s positions only increase, so the row a
        // candidate is read at only increases as well.
        let mut candidate_cursors: Vec<RowCursor<'_>> = candidate_indices
            .iter()
            .map(|&candidate| filter.cursor(candidate))
            .collect();
        let mut cursor = kept.cursor(index);
        for (point, row) in kept.positions(index) {
            let sample = cursor.at(row).expect("a row never goes back");
            if matches!(sample, SampleRef::Missing) {
                continue;
            }
            let mut matched = false;
            for (&candidate, candidate_cursor) in
                candidate_indices.iter().zip(&mut candidate_cursors)
            {
                if filter.present(candidate, point, candidate_cursor) {
                    matched = true;
                    break;
                }
            }
            if matched == keep_matched {
                results.insert(&series.metric, 0, point, owned(sample)?, 0, &mut budget)?;
            }
        }
    }
    Ok(results)
}

/// Joins the per-field results into series with all fields, by label set.
fn merge_fields(fields: Vec<Results>, schema: Arc<SeriesSchema>) -> Result<Vec<Series>> {
    let width = schema.fields.len();
    // Per label set: the metric and its `(point, field, value)` samples.
    type Merged = (MetricName, Vec<(usize, usize, GridSample)>);
    let mut merged: HashMap<GroupKey, Merged> = HashMap::new();
    for (field, results) in fields.into_iter().enumerate() {
        for (key, rows) in results.series {
            let (_, samples) = merged
                .entry(key)
                .or_insert_with(|| (rows.metric.clone(), Vec::new()));
            for (&point, value) in rows.points.iter().zip(rows.values) {
                if let Some(value) = value {
                    samples.push((point, field, value));
                }
            }
        }
    }
    let mut builder = GridSeriesBuilder::try_new(schema)?;
    let mut row: Vec<Option<GridSample>> = Vec::with_capacity(width);
    let mut output = Vec::with_capacity(merged.len());
    for (metric, mut samples) in merged.into_values() {
        // One builder serves every label set, so each result series starts explicitly.
        builder.begin_series();
        samples.sort_unstable_by_key(|(point, field, _)| (*point, *field));
        let mut samples = samples.into_iter().peekable();
        while let Some((point, field, value)) = samples.next() {
            row.clear();
            row.resize_with(width, || None);
            row[field] = Some(value);
            while let Some((_, field, value)) = samples.next_if(|next| next.0 == point) {
                row[field] = Some(value);
            }
            builder.push_samples(point, &row)?;
        }
        output.extend(builder.finish(metric)?);
    }
    Ok(output)
}

fn same_grid(lhs: &SeriesSchema, rhs: &SeriesSchema) -> Result<Arc<EvalGrid>> {
    match (&lhs.eval_grid, &rhs.eval_grid) {
        (Some(lhs), Some(rhs)) if lhs.spec() == rhs.spec() => Ok(lhs.clone()),
        _ => Err(DataFusionError::Execution(
            "vector operands must be evaluated on the same grid".into(),
        )),
    }
}

/// Label pairs identifying a match group or a result; empty values are absent, as in
/// Prometheus.
type GroupKey = Vec<(Arc<str>, Arc<str>)>;

fn group_key(metric: &MetricName, labels: &MatchingLabels) -> GroupKey {
    let mut key: GroupKey = match labels {
        MatchingLabels::On(names) => names
            .iter()
            .filter_map(|name| {
                let value = metric.get(name)?;
                (!value.is_empty()).then(|| (name.clone(), Arc::from(value)))
            })
            .collect(),
        MatchingLabels::Ignoring(names) => metric
            .tags()
            .iter()
            .filter(|tag| !tag.value.is_empty() && !names.contains(&tag.name))
            .map(|tag| (tag.name.clone(), tag.value.clone()))
            .collect(),
    };
    key.sort();
    key.dedup();
    key
}

fn key_of(metric: &MetricName) -> GroupKey {
    let mut key = group_key(metric, &MatchingLabels::Ignoring(Vec::new()));
    if let Some(name) = metric.name() {
        key.insert(0, (Arc::from("__name__"), Arc::from(name)));
    }
    key
}

fn render(key: &GroupKey) -> String {
    let labels: Vec<_> = key
        .iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect();
    format!("{{{}}}", labels.join(", "))
}

/// Groups the series of `operand` by their match group.
fn group(operand: &Operand<'_>, labels: &MatchingLabels) -> HashMap<GroupKey, Vec<usize>> {
    let mut groups: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    for (index, series) in operand.series.iter().enumerate() {
        groups
            .entry(group_key(&series.metric, labels))
            .or_default()
            .push(index);
    }
    groups
}

/// The first point where two `members` are present and `relevant` holds.
fn overlap(
    operand: &Operand<'_>,
    members: &[usize],
    relevant: impl Fn(usize) -> bool,
) -> Option<usize> {
    if members.len() < 2 {
        return None;
    }
    // Sorting the present points keeps the cost proportional to them, not to the span.
    let mut points = Vec::new();
    for &member in members {
        let mut cursor = operand.cursor(member);
        for (point, row) in operand.positions(member) {
            if !matches!(
                cursor.at(row).expect("a row never goes back"),
                SampleRef::Missing
            ) {
                points.push(point);
            }
        }
    }
    points.sort_unstable();
    for pair in points.windows(2) {
        if pair[0] == pair[1] && relevant(pair[0]) {
            return Some(pair[0]);
        }
    }
    None
}

fn result_metric(
    many: &MetricName,
    one: &MetricName,
    matching: &BinaryMatching,
    include: &[Arc<str>],
    drop_name: bool,
) -> Result<MetricName> {
    let mut metric = many.clone();
    if drop_name {
        metric.remove_all(&METADATA_LABELS);
    }
    if matching.cardinality == Cardinality::OneToOne {
        match &matching.labels {
            MatchingLabels::On(names) => {
                metric.keep_only(&names.iter().map(|name| &**name).collect::<Vec<_>>())
            }
            MatchingLabels::Ignoring(names) => {
                metric.remove_all(&names.iter().map(|name| &**name).collect::<Vec<_>>())
            }
        }
    }
    for name in include {
        match one.get(name) {
            Some(value) if !value.is_empty() => metric
                .set(name, Arc::from(value))
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            _ => metric.remove(name),
        }
    }
    Ok(metric)
}

/// Validates one operand of [`binary_vectors`]: every series must sit on the schema's grid, carry
/// the schema's fields, and hold one sample for every row its own axis places.
///
/// [`Series`] keeps its fields public, so a series built by struct literal can disagree with the
/// schema it is handed in with, and then the row the axis names has nothing to read. The kernel
/// cannot read such an operand, and it must say so before it allocates anything for it, so the
/// check lives here and runs before a field reserves its tables.
pub(crate) fn validate_binary_operand(schema: &SeriesSchema, series: &[Series]) -> Result<()> {
    let grid = schema.eval_grid.as_ref().map(|grid| grid.spec());
    for item in series {
        // Both grid axes address the grid; the explicit ones cannot be indexed by position.
        let Some(item_grid) = item.time.grid() else {
            return Err(DataFusionError::Execution(
                "vector operand series must be on the evaluation grid".into(),
            ));
        };
        if Some(item_grid.spec()) != grid
            || item.fields.len() != schema.fields.len()
            || item
                .fields
                .iter()
                .any(|field| field.len() != item.time.len())
        {
            return Err(DataFusionError::Execution(
                "vector operand series does not match its schema".into(),
            ));
        }
    }
    Ok(())
}

/// One field of one side's series, with verified grid axes.
///
/// An operand's series are addressed on the evaluation grid, and each may skip positions. Every
/// accessor therefore reads the axis: a point lookup asks the axis for the row of a position
/// (`row_of`, a binary search), and a traversal walks the rows the axis carries
/// (`positions`). Nothing here assumes a series covers one contiguous range, so a sparse operand
/// never reads a hole as a row.
struct Operand<'a> {
    series: &'a [Series],
    field: usize,
}

impl<'a> Operand<'a> {
    fn try_new(schema: &SeriesSchema, series: &'a [Series], field: usize) -> Result<Self> {
        validate_binary_operand(schema, series)?;
        Ok(Self { series, field })
    }

    fn axis(&self, index: usize) -> &'a TimeAxis {
        &self.series[index].time
    }

    /// Every absolute position this series carries, with its compact row, in order.
    fn positions(&self, index: usize) -> datatypes::series::time::Positions<'a> {
        self.series[index].time.positions()
    }

    /// A forward reader of `series[index]`'s field, for a walk in axis order.
    ///
    /// The rows of one walk only increase, so a cursor reads the field once; reading each row
    /// with [`Samples::sample`](datatypes::series::samples::Samples::sample) would rescan the
    /// chunk descriptors per row.
    fn cursor(&self, index: usize) -> RowCursor<'a> {
        self.series[index].fields.as_slice()[self.field].cursor()
    }

    /// Whether `series[index]` holds a value at absolute grid `point`, read through `cursor`.
    ///
    /// The points one walk asks about only increase, so its cursor never goes back: a candidate
    /// read per point through [`Samples::sample`](datatypes::series::samples::Samples::sample)
    /// would rescan the chunk descriptors from the start every time.
    fn present(&self, index: usize, point: usize, cursor: &mut RowCursor<'a>) -> bool {
        self.axis(index).row_of(point).is_some_and(|row| {
            !matches!(
                cursor.at(row).expect("a row never goes back"),
                SampleRef::Missing
            )
        })
    }

    /// The positions both series carry, as shared runs with a compact row on each side.
    ///
    /// A merge over the two axes' runs, so the cost is the runs and not the positions between
    /// them, and neither side's row is derived by subtracting a start.
    fn shared_runs(
        &self,
        own: usize,
        other: &Operand<'a>,
        theirs: usize,
    ) -> impl Iterator<Item = GridRunPair> + 'a {
        self.axis(own).intersect_runs(other.axis(theirs))
    }

    /// Whether any series is present at each grid point.
    fn presence(&self, points: usize) -> Vec<bool> {
        let mut presence = vec![false; points];
        for index in 0..self.series.len() {
            let mut cursor = self.cursor(index);
            for (point, row) in self.positions(index) {
                if !presence[point]
                    && !matches!(
                        cursor.at(row).expect("a row never goes back"),
                        SampleRef::Missing
                    )
                {
                    presence[point] = true;
                }
            }
        }
        presence
    }
}

/// `sample` as an owned value; `None` when missing.
fn owned(sample: SampleRef<'_>) -> Result<Option<GridSample>> {
    Ok(match sample {
        SampleRef::Missing => None,
        SampleRef::Float(value) => Some(GridSample::Float(value)),
        SampleRef::Histogram(view) => Some(GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&view)?,
        ))),
    })
}

/// Prometheus `vectorElemBinop` over present samples: the value and whether to keep it, or
/// `None` when Prometheus reports the pair as an annotation and skips it without counting the
/// match: an operator the sample types do not support, or an addition or subtraction of
/// histograms that mix exponential and custom buckets or whose counter reset hints collide.
fn elem_binop(
    op: BinaryOp,
    lhs: SampleRef<'_>,
    rhs: SampleRef<'_>,
) -> Result<Option<(GridSample, bool)>> {
    let histogram = |view: &HistogramView<'_>| FloatHistogram::try_from_view(view);
    let mut result = match (lhs, rhs) {
        (SampleRef::Float(lhs), SampleRef::Float(rhs)) => {
            let (value, keep) = op.apply(lhs, rhs);
            return Ok(Some((GridSample::Float(value), keep)));
        }
        (SampleRef::Float(factor), SampleRef::Histogram(h))
        | (SampleRef::Histogram(h), SampleRef::Float(factor))
            if op == BinaryOp::Mul =>
        {
            let mut h = histogram(&h)?;
            h.mul(factor);
            h
        }
        (SampleRef::Histogram(h), SampleRef::Float(divisor)) if op == BinaryOp::Div => {
            let mut h = histogram(&h)?;
            h.div(divisor);
            h
        }
        (SampleRef::Histogram(lhs), SampleRef::Histogram(rhs)) => {
            let (mut lhs, rhs) = (histogram(&lhs)?, histogram(&rhs)?);
            match op {
                BinaryOp::Eq | BinaryOp::Ne => {
                    let keep = lhs.equals(&rhs) == (op == BinaryOp::Eq);
                    return Ok(Some((GridSample::Histogram(Box::new(lhs)), keep)));
                }
                BinaryOp::Add | BinaryOp::Sub => {
                    if lhs.uses_custom_buckets() != rhs.uses_custom_buckets() {
                        return Ok(None);
                    }
                    let outcome = if op == BinaryOp::Add {
                        lhs.add(&rhs)?
                    } else {
                        lhs.sub(&rhs)?
                    };
                    if outcome.counter_reset_collision {
                        return Ok(None);
                    }
                    if op == BinaryOp::Sub {
                        lhs.reset_hint = CounterResetHint::Gauge;
                    }
                    lhs
                }
                _ => return Ok(None),
            }
        }
        _ => return Ok(None),
    };
    result.compact();
    Ok(Some((GridSample::Histogram(Box::new(result)), true)))
}

/// The result rows of one field by result label set.
#[derive(Default)]
struct Results {
    series: HashMap<GroupKey, ResultRows>,
}

/// Rows in arrival order: their points, the match group each came from, and their values;
/// `None` is a match the operator filtered.
struct ResultRows {
    metric: MetricName,
    points: Vec<usize>,
    groups: Vec<usize>,
    values: Vec<Option<GridSample>>,
}

/// Two rows of one result label set at one point.
struct Duplicate {
    point: usize,
    key: GroupKey,
    /// Whether two of the rows came from the same match group.
    within_group: bool,
}

fn same_labelset(key: &GroupKey, timestamp: i64) -> String {
    format!(
        "vector cannot contain metrics with the same labelset {} at {timestamp} ms",
        render(key)
    )
}

impl Results {
    /// Files one row of one field, retaining the slots it takes.
    ///
    /// The label set the row is filed under is only built when the table does not hold it yet, so
    /// `label_set` -- the bytes that set keeps -- is charged on the vacant entry alone. The value
    /// is charged by the caller, which knows whether the row keeps a histogram.
    fn insert(
        &mut self,
        metric: &MetricName,
        group: usize,
        point: usize,
        value: Option<GridSample>,
        label_set: usize,
        budget: &mut Budget<'_>,
    ) -> Result<()> {
        let rows = match self.series.entry(key_of(metric)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                budget.retain(label_set)?;
                entry.insert(ResultRows {
                    metric: metric.clone(),
                    points: Vec::new(),
                    groups: Vec::new(),
                    values: Vec::new(),
                })
            }
        };
        budget.retain(RESULT_ROW_BYTES)?;
        rows.points.push(point);
        rows.groups.push(group);
        rows.values.push(value);
        budget.rows = budget.rows.saturating_add(1);
        Ok(())
    }

    /// Sorts the rows of each label set by point and returns the earliest point where a label
    /// set has two rows, preferring one within a match group at that point. Prometheus
    /// evaluates step by step and fails at the first such step.
    fn sort_rows(&mut self) -> Option<Duplicate> {
        let mut first: Option<(usize, bool, &GroupKey)> = None;
        for (key, rows) in &mut self.series {
            if !rows.points.is_sorted() {
                let mut order: Vec<usize> = (0..rows.points.len()).collect();
                order.sort_by_key(|&row| rows.points[row]);
                rows.points = order.iter().map(|&row| rows.points[row]).collect();
                rows.groups = order.iter().map(|&row| rows.groups[row]).collect();
                let mut values = std::mem::take(&mut rows.values);
                rows.values = order.iter().map(|&row| values[row].take()).collect();
            }
            let mut start = 0;
            while start < rows.points.len() {
                let point = rows.points[start];
                let end = start + rows.points[start..].partition_point(|&p| p == point);
                if end - start > 1 {
                    let mut groups = rows.groups[start..end].to_vec();
                    groups.sort_unstable();
                    let within_group = groups.windows(2).any(|pair| pair[0] == pair[1]);
                    if first.is_none_or(|(first_point, first_within, _)| {
                        (point, !within_group) < (first_point, !first_within)
                    }) {
                        first = Some((point, within_group, key));
                    }
                    break;
                }
                start = end;
            }
        }
        first.map(|(point, within_group, key)| Duplicate {
            point,
            key: key.clone(),
            within_group,
        })
    }
}
