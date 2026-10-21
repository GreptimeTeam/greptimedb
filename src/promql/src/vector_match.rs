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

use crate::grid_series::{GridSample, GridSeriesBuilder, METADATA_LABELS, external, owned};
use crate::histogram_eval::float_histogram::FloatHistogram;

#[cfg(test)]
mod tests;

// How many candidate pairs the binary kernel visited on this thread.
//
// Test-only, and per thread: the pairs a query considers are what says whether a series delivered in
// many chunks is still one candidate, and that is not visible in the result. A thread of its own
// keeps one test's chunks out of another test's figure, and the counter is compiled out of the
// kernel otherwise.
#[cfg(test)]
thread_local! {
    static VISITED_PAIRS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Resets the candidate-pair counter of this thread to zero.
#[cfg(test)]
pub(crate) fn reset_visited_pairs() {
    VISITED_PAIRS.with(|visited| visited.set(0));
}

/// The candidate pairs the binary kernel has visited on this thread since the last
/// [`reset_visited_pairs`].
///
/// A pair is visited once per one-side candidate the kernel looks up for a many-side series, before
/// the pairing's label set and axis intersection are built, so the count is what the kernel actually
/// does rather than what its operands' sizes imply it would.
#[cfg(test)]
pub(crate) fn visited_pairs() -> usize {
    VISITED_PAIRS.with(std::cell::Cell::get)
}

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
    pub fill: FillValues,
}

/// The values of `fill`, `fill_left` and `fill_right`, which stand in for a sample that has no
/// match on the other side.
///
/// Prometheus applies them after it swaps the operands of `group_right`, so there `fill_left`
/// stands in for a missing right-hand sample and `fill_right` for a missing left-hand one.
#[derive(Clone, Copy, Debug, Default)]
pub struct FillValues {
    pub lhs: Option<f64>,
    pub rhs: Option<f64>,
}

// Compared by bits: a NaN fill equals itself, and `fill(-0)` differs from `fill(0)` (`1 / fill`).
impl PartialEq for FillValues {
    fn eq(&self, other: &Self) -> bool {
        let bits = |value: Option<f64>| value.map(f64::to_bits);
        bits(self.lhs) == bits(other.lhs) && bits(self.rhs) == bits(other.rhs)
    }
}

impl Eq for FillValues {}

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
/// and `/` by a float; Prometheus skips any other pairing of a histogram. A fill value pairs a
/// sample that has no match with a float sample labelled with its match group.
pub fn binary_vectors(
    matching: &BinaryMatching,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
) -> Result<Vec<Series>> {
    binary_vectors_inner(matching, lhs_schema, lhs, rhs_schema, rhs, None)
}

/// [`binary_vectors`], handing the live workspace it holds to `reserve` as it allocates.
///
/// What the operation will allocate cannot be known before it allocates: which series match each
/// other, where their axes overlap and which rows the operator keeps all come out of the operands
/// as they are read. So `reserve` receives the **total** live workspace the operation holds, and it
/// is asked whenever that total changes: it grows as rows are kept and falls as a step's scratch --
/// one field's tables, one pairing's label set, an owned copy of a histogram -- is replaced or
/// dropped. A total `reserve` refuses ends the operation with that error, and what it admitted stays
/// the caller's to release.
pub(crate) fn binary_vectors_with_reservation(
    matching: &BinaryMatching,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
    reserve: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<Vec<Series>> {
    let mut budget = Budget::new(reserve);
    binary_vectors_inner(
        matching,
        lhs_schema,
        lhs,
        rhs_schema,
        rhs,
        Some(&mut budget),
    )
}

/// The one binary kernel: one walk, one lookup and one error order, whether or not a caller
/// accounts for the workspace. Every figure is reserved only when `budget` is held.
fn binary_vectors_inner(
    matching: &BinaryMatching,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Vec<Series>> {
    // The grid both operands are evaluated on, checked before anything is reserved for them.
    let grid = same_grid(lhs_schema, rhs_schema)?;
    // The fields paired by position and the descriptors they are handed back in: one result table
    // per field, the field list both the results and the output schema keep, and the schema itself.
    // The schema is what these bytes cover, so they are reserved before it is built.
    let width = lhs_schema.fields.len().min(rhs_schema.fields.len());
    if let Some(budget) = budget.as_deref_mut() {
        budget.retain(output_bytes(width))?;
    }
    let output = binary_output_schema(lhs_schema, rhs_schema)?;
    let mut errors = Vec::new();
    let mut fields = Vec::with_capacity(width);
    for field in 0..width {
        let lhs = Operand::try_new(lhs_schema, lhs, field)?;
        let rhs = Operand::try_new(rhs_schema, rhs, field)?;
        let results = binary_field(
            matching,
            &grid,
            &lhs,
            &rhs,
            &mut errors,
            budget.as_deref_mut(),
        )?;
        // The field's tables and sort are destroyed with its frame, so their release is admitted
        // here: one figure for what the field kept -- its messages and rows -- once they are gone.
        if let Some(budget) = budget.as_deref_mut() {
            budget.drop_scratch()?;
        }
        fields.push(results);
    }
    earliest_error(errors)?;
    // The merge holds every field's results at once: the triples it files, the table it files them
    // in and the builder writing the series out are the last workspace of the operation.
    if let Some(budget) = budget.as_deref_mut() {
        let sets = fields
            .iter()
            .map(|results| results.series.len())
            .fold(0usize, usize::saturating_add);
        let merge = merge_bytes(
            budget.kept,
            sets,
            width,
            budget.histograms,
            budget.histogram_heap,
        );
        budget.step(merge)?;
    }
    merge_fields(fields, output, budget)
}

/// The capacity a fresh `Vec` holds: a first push allocates four entries, and a full one doubles.
const VEC_MIN_CAPACITY: usize = 4;

/// One label of a match key, charged twice because `group_key` grows its `Vec` by doubling.
const KEY_LABEL_BYTES: usize = 2 * std::mem::size_of::<(Arc<str>, Arc<str>)>();

/// One name of [`MatchingLabels`], which `result_metric` collects into a reference vector. The
/// vector is collected from an exact-size iterator, so its buffer holds one `&str` per name.
const MATCHING_NAME_BYTES: usize = std::mem::size_of::<&str>();

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

/// One row while a label set is sorted: the old and the new copy of its point, group and value
/// vectors, each charged twice for the capacity it ends at, and the row order the sort reads.
const SORT_ROW_BYTES: usize =
    4 * std::mem::size_of::<usize>() + 2 * std::mem::size_of::<Option<GridSample>>();

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

/// The bytes the operation's own descriptors cost: one result table per paired field, the field
/// list the results and the output schema each keep, and the schema struct.
fn output_bytes(fields: usize) -> usize {
    fields
        .saturating_mul(std::mem::size_of::<Results>() + 2 * std::mem::size_of::<FieldSpec>())
        .saturating_add(std::mem::size_of::<SeriesSchema>())
}

/// The live workspace a binary operation admits as it allocates.
///
/// What the results keep -- rows, the label sets they are filed under, their histograms -- stays
/// live until the operation returns, while the scratch of a step is what one step holds beside it.
/// [`Budget::step`] replaces that scratch instead of adding to it, so the total handed to the caller
/// is what is retained plus the step being run: it rises when a wider step begins and falls when a
/// narrower one replaces it, and it is never the sum of every step the operation has run.
struct Budget<'a> {
    /// Admits the live workspace, or refuses it.
    reserve: &'a mut dyn FnMut(usize) -> Result<()>,
    /// The bytes the results keep until the operation returns.
    retained: usize,
    /// The bytes of the step being run, beside what is retained.
    scratch: usize,
    /// The total handed to `reserve` last, so an unchanged one is not asked for twice.
    requested: Option<usize>,
    /// The result rows filed, rows the operator filtered included: what the sort walks.
    rows: usize,
    /// The result rows holding a value: what the merge files as triples and writes out.
    kept: usize,
    /// The retained rows holding a histogram, and the heap those histograms hold.
    histograms: usize,
    histogram_heap: usize,
}

impl<'a> Budget<'a> {
    fn new(reserve: &'a mut dyn FnMut(usize) -> Result<()>) -> Self {
        Self {
            reserve,
            retained: 0,
            scratch: 0,
            requested: None,
            rows: 0,
            kept: 0,
            histograms: 0,
            histogram_heap: 0,
        }
    }

    /// Retains `bytes` of results and admits the total covering them.
    fn retain(&mut self, bytes: usize) -> Result<()> {
        self.retained = self.retained.saturating_add(bytes);
        self.admit()
    }

    /// Admits `bytes` as the whole workspace of the step being run, replacing the step before it.
    fn step(&mut self, bytes: usize) -> Result<()> {
        self.scratch = bytes;
        self.admit()
    }

    /// Forgets the last step's scratch: its tables and buffers are gone.
    fn drop_scratch(&mut self) -> Result<()> {
        self.scratch = 0;
        self.admit()
    }

    /// Adds `bytes` to what the results retain without admitting it, for a figure the next
    /// [`Budget::step`] admits as one total.
    fn keep(&mut self, bytes: usize) {
        self.retained = self.retained.saturating_add(bytes);
    }

    /// Moves one kept histogram into what the results retain: the value it boxes and the heap its
    /// buckets and bounds hold. The [`Budget::step`] that follows admits the transfer together with
    /// the released operator temporary, so the figure never holds the histogram twice.
    fn keep_histogram(&mut self, histogram: &FloatHistogram) {
        self.histograms = self.histograms.saturating_add(1);
        self.histogram_heap = self.histogram_heap.saturating_add(histogram.heap_bytes());
        self.keep(result_histogram_bytes(histogram));
    }

    /// Counts one filed row: every row is what the sort walks, a row holding a value is what the
    /// merge files and writes out.
    fn file_row(&mut self, kept: bool) {
        self.rows = self.rows.saturating_add(1);
        if kept {
            self.kept = self.kept.saturating_add(1);
        }
    }

    /// Hands the live total to `reserve` when it differs from the last one. It falls as well as
    /// rises, and only a repeated figure is left out.
    fn admit(&mut self) -> Result<()> {
        let total = self.retained.saturating_add(self.scratch);
        if self.requested != Some(total) {
            self.requested = Some(total);
            (self.reserve)(total)?;
        }
        Ok(())
    }
}

/// What one field's tables, flags and messages cost before it pairs anything.
#[derive(Default)]
struct FieldMetadata {
    /// The presence flags and the one side's match groups: what the field holds beside the pairing
    /// it walks, and the figure a step of that walk is charged on top of.
    tables: usize,
    /// The allowance for the messages the field can report, which it reserves for its sort before
    /// it knows which of them it appends; only the ones it writes are kept.
    errors: usize,
    /// The largest key a row of the field is filed under.
    largest_key: usize,
}

/// The bytes one field's tables, flags and messages cost.
///
/// The presence of each side and of the points both are present at are three flags per grid point;
/// the one side's groups are a key, the values `on(...)` matching copies and a member list per
/// series with the table holding them and the references indexing them; one series of the many side
/// is looked up by a key of its own; and a field reports at most three messages, each a fixed frame
/// around one rendering. Nothing here counts the pairing the field walks: which series match, where
/// their axes overlap and which rows the operator keeps is only known as the walk reaches them.
/// The duplicate scan's list of present points is not part of these tables either: a group of one
/// member collects nothing, and a wider group's list is charged by [`overlap`] as it is pushed to.
fn field_metadata_bytes(
    matching: &BinaryMatching,
    include: &[Arc<str>],
    points: usize,
    many: &Operand<'_>,
    one: &Operand<'_>,
) -> FieldMetadata {
    let mut bytes = points.saturating_mul(3);
    let mut largest_key = 0usize;
    for series in one.series {
        bytes = bytes
            .saturating_add(match_key_bytes(&series.metric, &matching.labels))
            .saturating_add(2 * std::mem::size_of::<usize>())
            .saturating_add(GROUP_SLOT_BYTES)
            .saturating_add(2 * std::mem::size_of::<&GroupKey>())
            .saturating_add(2 * std::mem::size_of::<(&GroupKey, usize)>());
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
    FieldMetadata {
        tables: bytes.saturating_add(lookup),
        errors: 3usize.saturating_mul(ERROR_TEXT_BYTES.saturating_add(text)),
        largest_key: largest_key.saturating_add(include.len().saturating_mul(KEY_LABEL_BYTES)),
    }
}

/// The workspace one row of a pairing holds: the field's tables, the match slots it holds, the
/// pairing's label set and -- while the operator reads one -- its histogram temporaries.
fn row_workspace(
    metadata: &FieldMetadata,
    matched: usize,
    pairing: usize,
    histograms: usize,
) -> usize {
    metadata
        .tables
        .saturating_add(matched)
        .saturating_add(pairing)
        .saturating_add(histograms)
}

/// The bytes one pairing allocates before it files a row: the result label set `result_metric`
/// rewrites, the `group_*` labels it copies in, the reference vector it collects the matching label
/// names into, and the key its rows are looked up by.
///
/// Each rewrite filters the tag list into a `Vec` and turns that into a shared slice, so the list it
/// replaces is alive while two copies of the new one are built, and an included label the many side
/// does not carry is inserted, which costs its descriptor too. The matching names are collected into
/// a reference vector of their own before the rewrite, for `on(...)` and `ignoring(...)` alike and
/// whether or not the series carries them. This is one pairing's scratch: the next pairing replaces
/// it, and a pairing whose axes share no position still pays it, because the label set is built
/// before the axes are intersected.
fn pair_bytes(
    many: &MetricName,
    one: &MetricName,
    matching: &BinaryMatching,
    include: &[Arc<str>],
) -> usize {
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
    // Only a one-to-one match rewrites the label list by its matching labels, and it collects their
    // names first; `group_left` and `group_right` copy the included labels instead.
    let names = match (&matching.cardinality, &matching.labels) {
        (Cardinality::OneToOne, MatchingLabels::On(names))
        | (Cardinality::OneToOne, MatchingLabels::Ignoring(names)) => {
            names.len().saturating_mul(MATCHING_NAME_BYTES)
        }
        _ => 0,
    };
    labels
        .saturating_add(copied_bytes(one, include))
        .saturating_add(key)
        .saturating_add(names)
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
/// buffers are alive together: one triple of a label set's samples, the span a run may start with, a
/// row of the histogram table per histogram and the heap its buckets are copied to. Every term counts
/// a value a field actually kept or a label set it filed, so a pair the operator filtered costs its
/// label set -- the table holds one without a value -- and no row at all. What grows with the output
/// rows is charged by [`merge_fields`] as they are pushed: the value and validity column of every
/// field, and the index column of a field whose first histogram backfills the rows before it.
fn merge_bytes(
    kept: usize,
    sets: usize,
    fields: usize,
    histograms: usize,
    histogram_heap: usize,
) -> usize {
    // One kept triple, charged twice for the capacity its vector ends at, one slot of the row the
    // builder reads and the span it may start.
    let row = (2 * std::mem::size_of::<(usize, usize, GridSample)>())
        .saturating_add(2 * std::mem::size_of::<usize>())
        .saturating_add(2 * std::mem::size_of::<GridSpan>());
    kept.saturating_mul(row)
        // One label set's entry in the merge table and the series it is written out as.
        .saturating_add(
            sets.saturating_mul(MERGE_SLOT_BYTES.saturating_add(2 * std::mem::size_of::<Series>())),
        )
        // The row the builder reads its pushes from and the per-field ledger of the capacity each
        // histogram index column has reached, one slot per field.
        .saturating_add(2 * fields.saturating_mul(std::mem::size_of::<Option<GridSample>>()))
        .saturating_add(fields.saturating_mul(std::mem::size_of::<usize>()))
        // The histogram table of every field that carries one: the builders of its columns, the
        // scalars and list offsets of its rows, and the buckets the batch copies beside the value
        // the results still hold.
        .saturating_add(fields.saturating_mul(std::mem::size_of::<HistogramBatchBuilder>()))
        .saturating_add(histograms.saturating_mul(2 * HISTOGRAM_ROW_BYTES))
        .saturating_add(2 * histogram_heap)
}

/// The bytes one output row of the merge adds to the builder's value columns: the value and the
/// validity bit of every field of the schema, charged twice for the capacity each column grows to.
///
/// A row holds a value only in the fields the results filed at its point, and the builder writes a
/// null into every other column, so this figure counts the schema's fields rather than the row's
/// values: fields whose samples sit at different positions still write a column each.
fn merge_row_bytes(fields: usize) -> usize {
    2 * fields.saturating_mul(std::mem::size_of::<f64>() + 1)
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

/// The bytes one boxed histogram value holds: its scalars inline and the heap its buckets, bounds
/// and span layout own.
fn result_histogram_bytes(histogram: &FloatHistogram) -> usize {
    std::mem::size_of::<FloatHistogram>().saturating_add(histogram.heap_bytes())
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
///
/// The field's tables and the match set it builds are its own workspace: they replace what the field
/// before held and are gone when it returns, which is why the caller admits that release after the
/// call. The messages it appended and the rows it filed are what stays live.
fn binary_field(
    matching: &BinaryMatching,
    grid: &EvalGrid,
    lhs: &Operand<'_>,
    rhs: &Operand<'_>,
    errors: &mut Vec<(usize, u8, String)>,
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Results> {
    let (many, one, include, one_is_left) = match &matching.cardinality {
        Cardinality::OneToOne => (lhs, rhs, &[][..], false),
        Cardinality::ManyToOne(include) => (lhs, rhs, &include[..], false),
        Cardinality::OneToMany(include) => (rhs, lhs, &include[..], true),
    };
    let points = grid.timestamps().len();
    let errors_before = errors.len();
    // This field's tables, before it pairs anything: nothing of the field before is still live.
    let metadata = match budget.as_deref_mut() {
        Some(budget) => {
            let metadata = field_metadata_bytes(matching, include, points, many, one);
            budget.step(metadata.tables)?;
            metadata
        }
        None => FieldMetadata::default(),
    };
    let fill = matching.fill;
    // Prometheus short-circuits a point where either side is empty, before any check; with a fill
    // value only a point where both are empty.
    let filling = fill.lhs.is_some() || fill.rhs.is_some();
    let evaluated: Vec<bool> = lhs
        .presence(points)
        .into_iter()
        .zip(rhs.presence(points))
        .map(|(lhs, rhs)| if filling { lhs || rhs } else { lhs && rhs })
        .collect();

    let one_groups = group(one, &matching.labels);
    let mut one_duplicate: Option<(usize, &GroupKey)> = None;
    for (key, members) in &one_groups {
        // The group's list of present points is charged as this scan grows it, beside the tables
        // the field keeps from here on.
        let duplicate = overlap(
            one,
            members,
            |point| evaluated[point],
            metadata.tables,
            budget.as_deref_mut(),
        )?;
        if let Some(point) = duplicate
            && one_duplicate.is_none_or(|(first, _)| point < first)
        {
            one_duplicate = Some((point, key));
        }
    }

    let drop_name = !matching.op.is_comparison() || matching.return_bool;
    let rows_before = budget.as_deref().map_or(0, |budget| budget.rows);
    let mut pairs = Pairs {
        matching,
        one_is_left,
        results: Results::default(),
        group_keys: Vec::new(),
        group_ids: HashMap::new(),
        // One-to-one: a match group may be matched once per point; Prometheus counts a filtered
        // comparison as a match too, but not an operation that failed. Filling the many side
        // consults the same set.
        matched: (matching.cardinality == Cardinality::OneToOne || fill.lhs.is_some())
            .then(HashSet::new),
        // The slots the match set holds are part of this field's workspace, not of what a row keeps.
        matched_bytes: 0,
        implicit_many_to_one: None,
    };
    for (many_index, many_series) in many.series.iter().enumerate() {
        let key = group_key(&many_series.metric, &matching.labels);
        let candidates = one_groups.get(&key).map_or(&[][..], Vec::as_slice);
        if candidates.is_empty() && fill.rhs.is_none() {
            continue;
        }
        let group = pairs.group_id(&key);
        for &one_index in candidates {
            // One candidate of the one side is paired with this series, whether or not the pairing
            // turns out to hold a row: the visit is what the test-only counter reports, before the
            // pairing's label set and axis intersection are built.
            #[cfg(test)]
            VISITED_PAIRS.with(|visited| visited.set(visited.get() + 1));
            // One pairing's label set is built before the axes are intersected, so it is reserved
            // first and the next pairing replaces it.
            let mut pairing = 0usize;
            if let Some(budget) = budget.as_deref_mut() {
                pairing = pair_bytes(
                    &many_series.metric,
                    &one.series[one_index].metric,
                    matching,
                    include,
                );
                budget.step(row_workspace(&metadata, pairs.matched_bytes, pairing, 0))?;
            }
            let metric = result_metric(
                &many_series.metric,
                &one.series[one_index].metric,
                matching,
                include,
                drop_name,
            )?;
            let label_set = if budget.is_some() {
                result_set_bytes(&metric, &one.series[one_index].metric, include)
            } else {
                0
            };
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
                    pairs.apply(
                        group,
                        &metric,
                        point,
                        many_value,
                        one_value,
                        &metadata,
                        pairing,
                        label_set,
                        budget.as_deref_mut(),
                    )?;
                }
            }
        }
        // A many-side sample no one-side series matched is answered with the fill value, at the
        // label set the match group of the many-side series gives it.
        if let Some(value) = fill.rhs {
            let metric = result_metric(
                &many_series.metric,
                &metric_of(&key)?,
                matching,
                include,
                drop_name,
            )?;
            let label_set = if budget.is_some() {
                result_set_bytes(&metric, &many_series.metric, include)
            } else {
                0
            };
            let mut cursor = many.cursor(many_index);
            let mut candidate_cursors: Vec<RowCursor<'_>> =
                candidates.iter().map(|&index| one.cursor(index)).collect();
            for (point, row) in many.positions(many_index) {
                let sample = cursor.at(row).expect("a row never goes back");
                let matched = candidates
                    .iter()
                    .zip(&mut candidate_cursors)
                    .any(|(&index, cursor)| one.present(index, point, cursor));
                if !matched {
                    // A fill row pairs no two series, so it builds no pairing label set.
                    pairs.apply(
                        group,
                        &metric,
                        point,
                        sample,
                        SampleRef::Float(value),
                        &metadata,
                        0,
                        label_set,
                        budget.as_deref_mut(),
                    )?;
                }
            }
        }
    }
    // Prometheus fills the many side after every match of the step, for the samples of the one
    // side that no operation matched.
    if let Some(value) = fill.lhs {
        for (one_index, one_series) in one.series.iter().enumerate() {
            let key = group_key(&one_series.metric, &matching.labels);
            let group = pairs.group_id(&key);
            let metric = result_metric(
                &metric_of(&key)?,
                &one_series.metric,
                matching,
                include,
                drop_name,
            )?;
            let label_set = if budget.is_some() {
                result_set_bytes(&metric, &one_series.metric, include)
            } else {
                0
            };
            let mut cursor = one.cursor(one_index);
            for (point, row) in one.positions(one_index) {
                if !pairs
                    .matched
                    .as_ref()
                    .is_some_and(|matched| matched.contains(&(group, point)))
                {
                    let sample = cursor.at(row).expect("a row never goes back");
                    // A fill row pairs no two series, so it builds no pairing label set: what it
                    // reserves is the workspace of one row and the label set it is filed under.
                    pairs.apply(
                        group,
                        &metric,
                        point,
                        SampleRef::Float(value),
                        sample,
                        &metadata,
                        0,
                        label_set,
                        budget.as_deref_mut(),
                    )?;
                }
            }
        }
    }
    // The match set is a step of the walk, not something the field keeps: taking the pairing state
    // apart here drops it, so the sort and the messages are what is live from here on.
    let Pairs {
        mut results,
        group_keys,
        implicit_many_to_one,
        ..
    } = pairs;
    // The messages the field reports and the sort of its rows are its last workspace, with the
    // messages it may append charged before they exist.
    if let Some(budget) = budget.as_deref_mut() {
        let rows = budget.rows.saturating_sub(rows_before);
        budget.step(
            metadata
                .tables
                .saturating_add(metadata.errors)
                .saturating_add(metadata.largest_key)
                .saturating_add(rows.saturating_mul(SORT_ROW_BYTES)),
        )?;
    }
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
    if let Some((point, group)) = implicit_many_to_one {
        let key = &group_keys[group];
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
    // Only the messages this field actually appended stay live until the earliest one is reported.
    // They join what the results retain, and the caller admits them once the field's tables and
    // sort -- still live here -- are gone.
    if let Some(budget) = budget {
        let appended = errors[errors_before..]
            .iter()
            .fold(0usize, |bytes, (_, _, text)| {
                bytes
                    .saturating_add(std::mem::size_of::<String>())
                    .saturating_add(text.capacity())
            });
        budget.keep(appended);
    }
    Ok(results)
}

/// The matched pairs of one field and the state Prometheus checks them against.
struct Pairs<'a> {
    matching: &'a BinaryMatching,
    one_is_left: bool,
    results: Results,
    group_keys: Vec<GroupKey>,
    group_ids: HashMap<GroupKey, usize>,
    /// The match groups an operation matched at a point, kept where Prometheus consults them:
    /// one-to-one matching, and filling the many side. A filtered comparison counts as a match,
    /// an operation that failed does not.
    matched: Option<HashSet<(usize, usize)>>,
    /// The bytes the match set holds: its slots are part of the field's workspace, not of what a
    /// row keeps, so they are charged beside the tables and given back with the set.
    matched_bytes: usize,
    implicit_many_to_one: Option<(usize, usize)>,
}

impl Pairs<'_> {
    /// The id of the match group `key`, which stands for it in `matched` and in the results.
    fn group_id(&mut self, key: &GroupKey) -> usize {
        if let Some(&id) = self.group_ids.get(key) {
            return id;
        }
        let id = self.group_keys.len();
        self.group_keys.push(key.clone());
        self.group_ids.insert(key.clone(), id);
        id
    }

    /// Applies the operator to a sample of each side at `point`; a missing sample is no pair.
    ///
    /// `metadata` is what the field keeps between rows, `pairing` the label set and axis
    /// intersection this pairing built, and `label_set` the bytes the result label set keeps: the
    /// workspace is reserved through `budget` as each step of the row replaces the one before.
    #[allow(clippy::too_many_arguments)]
    fn apply(
        &mut self,
        group: usize,
        metric: &MetricName,
        point: usize,
        many_value: SampleRef<'_>,
        one_value: SampleRef<'_>,
        metadata: &FieldMetadata,
        pairing: usize,
        label_set: usize,
        mut budget: Option<&mut Budget<'_>>,
    ) -> Result<()> {
        if matches!(many_value, SampleRef::Missing) || matches!(one_value, SampleRef::Missing) {
            return Ok(());
        }
        let (lhs_value, rhs_value) = if self.one_is_left {
            (one_value, many_value)
        } else {
            (many_value, one_value)
        };
        // The operator's temporaries: an owned copy of each histogram operand and the union it
        // grows into, live while the row it reads is computed.
        if let Some(budget) = budget.as_deref_mut() {
            let histograms = histogram_pair_bytes(&lhs_value, &rhs_value);
            budget.step(row_workspace(
                metadata,
                self.matched_bytes,
                pairing,
                histograms,
            ))?;
        }
        let Some((result, keep)) = elem_binop(self.matching.op, lhs_value, rhs_value)? else {
            // The operator skipped the pair: its temporaries are gone and nothing of it was filed.
            if let Some(budget) = budget.as_deref_mut() {
                budget.step(row_workspace(metadata, self.matched_bytes, pairing, 0))?;
            }
            return Ok(());
        };
        // The operator has returned, so the copies it read its operands into are gone and what it
        // still owns -- possibly on the heap -- is the value it handed back. That value is live
        // until it is dropped or filed, so the figure it costs replaces the operator's temporary and
        // is held across the steps below.
        let owned = match &result {
            GridSample::Histogram(histogram) => result_histogram_bytes(histogram),
            GridSample::Float(_) => 0,
        };
        if let Some(budget) = budget.as_deref_mut() {
            budget.step(row_workspace(metadata, self.matched_bytes, pairing, owned))?;
        }
        // One-to-one: a match group may be matched once per point; Prometheus counts a filtered
        // comparison as a match too, but not an operation that failed.
        if let Some(matched) = &mut self.matched {
            if !matched.insert((group, point)) {
                if self.matching.cardinality == Cardinality::OneToOne
                    && self
                        .implicit_many_to_one
                        .is_none_or(|(first, _)| point < first)
                {
                    self.implicit_many_to_one = Some((point, group));
                }
            } else if let Some(budget) = budget.as_deref_mut() {
                // Only a point the group has not been matched at yet costs an entry, charged with
                // the value that is still owned beside it.
                self.matched_bytes = self.matched_bytes.saturating_add(MATCH_SLOT_BYTES);
                budget.step(row_workspace(metadata, self.matched_bytes, pairing, owned))?;
            }
        }
        // The value the row keeps. A `bool` comparison keeps a float and a filtered comparison
        // keeps nothing, so what the operator built is dropped there before its charge is given up;
        // a kept value is moved into the row.
        let value = if self.matching.return_bool {
            drop(result);
            Some(GridSample::Float(if keep { 1.0 } else { 0.0 }))
        } else if keep {
            Some(result)
        } else {
            drop(result);
            None
        };
        // One total replaces the operator's temporary: a kept histogram moves into the results as it
        // goes, and in every other case the value is already dropped.
        if let Some(budget) = budget.as_deref_mut() {
            if let Some(GridSample::Histogram(histogram)) = &value {
                budget.keep_histogram(histogram);
            }
            budget.step(row_workspace(metadata, self.matched_bytes, pairing, 0))?;
        }
        self.results
            .insert(metric, group, point, value, label_set, budget)
    }
}

/// The labels Prometheus gives a fill sample: the match group of the sample it pairs with.
fn metric_of(key: &GroupKey) -> Result<MetricName> {
    let mut name = None;
    let mut labels = Vec::with_capacity(key.len());
    for (label, value) in key {
        if &**label == "__name__" {
            name = Some(value.clone());
        } else {
            labels.push(Label {
                name: label.clone(),
                value: value.clone(),
            });
        }
    }
    MetricName::try_new(name, labels).map_err(external)
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
    // A set operator accounts for no workspace, so its merge is charged nothing either.
    merge_fields(fields, output, None)
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
    // A set operator hands on the label sets of its operands and no caller accounts for its
    // workspace, so it files its rows without charging them.
    let mut results = Results::default();
    if op == SetOp::Or {
        for index in 0..lhs.series.len() {
            let mut cursor = lhs.cursor(index);
            for (point, row) in lhs.positions(index) {
                if let Some(value) = owned(cursor.at(row).expect("a row never goes back"))? {
                    results.insert(&lhs.series[index].metric, 0, point, Some(value), 0, None)?;
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
                results.insert(&series.metric, 0, point, owned(sample)?, 0, None)?;
            }
        }
    }
    Ok(results)
}

/// The capacity a field's histogram index column reaches when `pushed` output rows have been pushed
/// and the row being pushed holds `histogram`; `None` when the column does not grow.
///
/// [`GridSeriesBuilder::push_samples`] creates the column at the first histogram with one entry per
/// row before it and then pushes, so that push allocates `max(2 * pushed, 4)`; every later row adds
/// an entry, and a full column doubles.
fn index_column_growth(pushed: usize, capacity: usize, histogram: bool) -> Option<usize> {
    if capacity == 0 {
        return histogram.then(|| (2 * pushed).max(VEC_MIN_CAPACITY));
    }
    (pushed == capacity).then(|| 2 * capacity)
}

/// Joins the per-field results into series with all fields, by label set.
///
/// The builder's columns grow with the output rows, which only exist once the triples are being
/// grouped, so they are reserved per row -- one row's worth before the builder takes it -- when a
/// caller accounts for the workspace: the value and validity column of every field, and the
/// histogram index column of a field that holds one.
fn merge_fields(
    fields: Vec<Results>,
    schema: Arc<SeriesSchema>,
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Vec<Series>> {
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
        // The capacity each field's histogram index column has reached, `0` until its first
        // histogram. The columns are handed out inside the series, so what they are charged stays
        // charged; only the growth of one series is charged at a time. Only a caller accounting for
        // the workspace reads the ledger, so an unaccounted merge allocates nothing for it.
        let mut index_capacity = if budget.is_some() {
            vec![0usize; width]
        } else {
            Vec::new()
        };
        samples.sort_unstable_by_key(|(point, field, _)| (*point, *field));
        let mut samples = samples.into_iter().peekable();
        let mut pushed = 0usize;
        while let Some((point, field, value)) = samples.next() {
            row.clear();
            row.resize_with(width, || None);
            row[field] = Some(value);
            while let Some((_, field, value)) = samples.next_if(|next| next.0 == point) {
                row[field] = Some(value);
            }
            // The columns of this row, admitted before the builder grows them.
            if let Some(budget) = budget.as_deref_mut() {
                budget.retain(merge_row_bytes(width))?;
                let mut index = 0usize;
                for (capacity, value) in index_capacity.iter_mut().zip(&row) {
                    let histogram = matches!(value, Some(GridSample::Histogram(_)));
                    if let Some(grown) = index_column_growth(pushed, *capacity, histogram) {
                        index = index.saturating_add(
                            grown
                                .saturating_sub(*capacity)
                                .saturating_mul(std::mem::size_of::<Option<u32>>()),
                        );
                        *capacity = grown;
                    }
                }
                budget.retain(index)?;
            }
            builder.push_samples(point, &row)?;
            pushed += 1;
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
///
/// The list of the group's present points is the only thing this scan allocates, and it is charged
/// as a slot is pushed: the figure follows the list's capacity, so a group of one member and a group
/// whose members carry no sample both cost nothing, however long their axes are. The list is given
/// back once it is dropped.
fn overlap(
    operand: &Operand<'_>,
    members: &[usize],
    relevant: impl Fn(usize) -> bool,
    tables: usize,
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Option<usize>> {
    if members.len() < 2 {
        return Ok(None);
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
                // The push that starts a new allocation: the capacity the list reaches is what is
                // live beside the field's tables, charged twice for the buffer it replaces.
                if let Some(budget) = budget.as_deref_mut()
                    && points.len() == points.capacity()
                {
                    let capacity = points.capacity().saturating_mul(2).max(VEC_MIN_CAPACITY);
                    let list = capacity.saturating_mul(2 * std::mem::size_of::<usize>());
                    budget.step(tables.saturating_add(list))?;
                }
                points.push(point);
            }
        }
    }
    points.sort_unstable();
    let duplicate = points
        .windows(2)
        .find(|pair| pair[0] == pair[1] && relevant(pair[0]))
        .map(|pair| pair[0]);
    // The list belongs to this scan alone: it is dropped -- a duplicate found or not -- before its
    // bytes are given back.
    drop(points);
    if let Some(budget) = budget {
        budget.step(tables)?;
    }
    Ok(duplicate)
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
/// cannot read such an operand, so the check lives here and runs before a field builds its matching
/// tables: the output schema is reserved before it, but nothing of the walk is.
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
    /// `label_set` -- the bytes that set keeps -- is charged on the vacant entry alone. A row whose
    /// value the operator filtered is still a row: it keeps its slots and its label set, which the
    /// merge carries as a set without a value. The value itself is charged by the caller, which
    /// knows whether the row keeps a histogram.
    fn insert(
        &mut self,
        metric: &MetricName,
        group: usize,
        point: usize,
        value: Option<GridSample>,
        label_set: usize,
        mut budget: Option<&mut Budget<'_>>,
    ) -> Result<()> {
        let kept = value.is_some();
        let rows = match self.series.entry(key_of(metric)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if let Some(budget) = budget.as_deref_mut() {
                    budget.retain(label_set)?;
                }
                entry.insert(ResultRows {
                    metric: metric.clone(),
                    points: Vec::new(),
                    groups: Vec::new(),
                    values: Vec::new(),
                })
            }
        };
        if let Some(budget) = budget {
            budget.retain(RESULT_ROW_BYTES)?;
            budget.file_row(kept);
        }
        rows.points.push(point);
        rows.groups.push(group);
        rows.values.push(value);
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
