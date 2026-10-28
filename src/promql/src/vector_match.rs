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
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, RowCursor, SampleRef, Series, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRunPair, GridSpan, TimeAxis};
use datatypes::series::{Label, SeriesLabels};

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
/// match on the other side: `lhs` for a missing left-hand sample and `rhs` for a missing
/// right-hand one, as the query writes them, `group_right` included (prometheus/prometheus#19939).
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
/// is asked at the three granularities [`Budget`] describes: where a long-lived result buffer grows,
/// once per phase, and in the blocks a growing result buffer is reserved in. It is never asked per
/// grid point or per row. A total `reserve` refuses ends the operation with that error, and what it
/// admitted stays the caller's to release.
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
        budget.hold(output_bytes(width))?;
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
        // The field's flags, tables and sort are destroyed with its frame, so its phase ends here:
        // what the field filed is what stays live, and the next field's phase is charged on its own.
        if let Some(budget) = budget.as_deref_mut() {
            budget.phase(0)?;
        }
        fields.push(results);
    }
    earliest_error(errors)?;
    // The merge is the operation's last phase: it enters its own figure from the results the fields
    // filed, which is where every term of it is read.
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

/// One row of a field's results: its point, its match group and its value. The three vectors grow
/// together, so one slot of each is what a row takes, and a block of the vectors is charged a row's
/// bytes per slot it grew by.
const RESULT_ROW_BYTES: usize =
    2 * std::mem::size_of::<usize>() + std::mem::size_of::<Option<GridSample>>();

/// One row while a label set is sorted: the old and the new copy of its point, group and value
/// vectors, and the row order the sort reads. The sort copies a label set's rows when they are not
/// already in order, so this is what one row costs while it runs.
const SORT_ROW_BYTES: usize =
    4 * std::mem::size_of::<usize>() + 2 * std::mem::size_of::<Option<GridSample>>();

/// One row of the merge's table, charged the same way.
const MERGE_SLOT_BYTES: usize =
    2 * std::mem::size_of::<(GroupKey, (SeriesLabels, Vec<(usize, usize, GridSample)>))>();

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

/// The live workspace a binary operation admits as it allocates, at three granularities.
///
/// A resource limit belongs at an allocation boundary, not at a value:
///
/// - [`Budget::hold`]: what the results keep until the operation returns -- rows, the label sets they
///   are filed under, the values boxed along them, the messages an error reports. Each is charged
///   where its container grows.
/// - [`Budget::phase`]: one figure for the workspace of one walk, read from the operands before it
///   allocates and replacing the phase before it, so the total is the phase being run beside what is
///   held and never the sum of every phase.
/// - [`Budget::grow`]: a block a container of that phase took when it outgrew what it held.
///
/// It exists because the caller is handed a *total*, and this turns those figures into that total.
struct Budget<'a> {
    /// Admits the live workspace, or refuses it.
    reserve: &'a mut dyn FnMut(usize) -> Result<()>,
    /// The bytes the results keep until the operation returns.
    held: usize,
    /// The bytes of the phase being run, beside what is held.
    phase: usize,
    /// The total handed to `reserve` last, so an unchanged one is not asked for twice.
    requested: Option<usize>,
}

impl<'a> Budget<'a> {
    fn new(reserve: &'a mut dyn FnMut(usize) -> Result<()>) -> Self {
        Self {
            reserve,
            held: 0,
            phase: 0,
            requested: None,
        }
    }

    /// Retains `bytes` of results, which stay live until the operation returns.
    fn hold(&mut self, bytes: usize) -> Result<()> {
        self.held = self.held.saturating_add(bytes);
        self.admit()
    }

    /// Admits `bytes` as the whole workspace of the phase being run, replacing the phase before it.
    fn phase(&mut self, bytes: usize) -> Result<()> {
        self.phase = bytes;
        self.admit()
    }

    /// Adds `bytes` to the phase for a container of it that has outgrown what was reserved.
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.phase = self.phase.saturating_add(bytes);
        self.admit()
    }

    /// Hands the live total to `reserve` when it differs from the last one. It falls as well as
    /// rises, and only a repeated figure is left out.
    fn admit(&mut self) -> Result<()> {
        let total = self.held.saturating_add(self.phase);
        if self.requested != Some(total) {
            self.requested = Some(total);
            (self.reserve)(total)?;
        }
        Ok(())
    }
}

/// The figures one field's phases are charged from, read once from its operands.
#[derive(Default)]
struct FieldMetadata {
    /// The presence flags and the one side's match groups.
    tables: usize,
    /// The allowance for the messages the field can report, which its sort reserves before it knows
    /// which of them it appends; only the ones it writes are kept.
    errors: usize,
    /// The largest key a row of the field is filed under.
    largest_key: usize,
    /// The widest pairing's label set and the histogram workspace one row of the walk reads.
    pairing: usize,
}

/// The figures one field's phases are charged from.
///
/// The presence of each side and of the points both are present at are three flags per grid point;
/// the one side's groups are a key, the values `on(...)` matching copies and a member list per
/// series; one series of the many side is looked up by a key of its own; and a field reports at most
/// three messages, each a fixed frame around one rendering. The duplicate scan's list is not part of
/// these tables: a group of one member collects nothing, and a wider group's list is charged by
/// [`overlap`] as it grows. The pairing's own terms are read from the widest series of each side
/// rather than from every pair, since one pairing's label set replaces the one before it.
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
    // The widest series of each side: one pairing's label set replaces the one before it, so the
    // widest is what the phase holds at its peak.
    let pairing = widest_metric(many)
        .zip(widest_metric(one))
        .map_or(0, |(many, one)| pair_bytes(many, one, matching, include));
    FieldMetadata {
        tables: bytes.saturating_add(lookup),
        errors: 3usize.saturating_mul(ERROR_TEXT_BYTES.saturating_add(text)),
        largest_key: largest_key.saturating_add(include.len().saturating_mul(KEY_LABEL_BYTES)),
        pairing: pairing.saturating_add(histogram_phase_bytes(many, one)),
    }
}

/// The series of `operand` with the most labels: the widest label set one pairing of its field can
/// build.
fn widest_metric<'a>(operand: &'a Operand<'_>) -> Option<&'a SeriesLabels> {
    operand
        .series
        .iter()
        .map(|series| &series.metric)
        .max_by_key(|metric| metric.tags().len())
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
    many: &SeriesLabels,
    one: &SeriesLabels,
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
/// sets, and the name of a label it inserts, since `SeriesLabels::set` copies that as well.
fn copied_bytes(one: &SeriesLabels, include: &[Arc<str>]) -> usize {
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
fn result_key_bytes(metric: &SeriesLabels) -> usize {
    metric
        .tags()
        .len()
        .saturating_add(1)
        .saturating_mul(KEY_LABEL_BYTES)
        .saturating_add(metric.name().map_or(0, str::len))
}

/// The bytes one result label set keeps once a row is filed under it: the label list and the
/// copied strings `result_metric` leaves behind, its key and its entry in the result table.
fn result_set_bytes(metric: &SeriesLabels, one: &SeriesLabels, include: &[Arc<str>]) -> usize {
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
/// rows is the builder's columns, which [`merge_fields`] reserves in blocks as it pushes them.
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
        // The row the builder reads its pushes from.
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
fn match_key_bytes(metric: &SeriesLabels, labels: &MatchingLabels) -> usize {
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
fn label_text_bytes(metric: &SeriesLabels) -> usize {
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

/// The bytes one owned copy of a histogram view holds: its scalars inline, and on the heap the
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

/// The bytes one row of a histogram operator holds at its peak, read once from the widest histogram
/// of each side of a field: an owned copy of each operand, the union `+` and `-` grow into -- both
/// sides' buckets, charged twice because the union reallocates while the old buffer is alive -- the
/// custom bounds the wider side copies, and the value the operator boxes. A phase figure, not one per
/// row: a row that pairs two histograms is bounded by the widest of each side.
fn histogram_phase_bytes(many: &Operand<'_>, one: &Operand<'_>) -> usize {
    // What one owned copy of a view takes, the buckets the union grows by and its custom bounds.
    let operand = |operand: &Operand<'_>| match operand.widest_histogram() {
        Some(view) => (
            owned_histogram_bytes(&view),
            view.positive
                .buckets
                .len()
                .saturating_add(view.negative.buckets.len()),
            view.custom_values.len(),
        ),
        None => (0, 0, 0),
    };
    let (lhs_owned, lhs_buckets, lhs_bounds) = operand(many);
    let (rhs_owned, rhs_buckets, rhs_bounds) = operand(one);
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
    let points = grid.spec().len as usize;
    let timestamp_at = |point: usize| {
        (grid.spec().start_ms as i128 + point as i128 * grid.spec().step_ms as i128) as i64
    };
    let errors_before = errors.len();
    // The pairing phase is entered once, before the field allocates anything: its presence flags,
    // its match groups, the widest label set a pairing builds and the histogram payload one row of
    // the walk reads. Nothing of the field before it is still live.
    let metadata = match budget.as_deref_mut() {
        Some(budget) => {
            let metadata = field_metadata_bytes(matching, include, points, many, one);
            budget.phase(metadata.tables.saturating_add(metadata.pairing))?;
            metadata
        }
        None => FieldMetadata::default(),
    };
    // The walk below fills `rhs` for a missing one-side sample and `lhs` for a missing many-side
    // one, so under `group_right`, whose one side is the left, the two trade places.
    let fill = if one_is_left {
        FillValues {
            lhs: matching.fill.rhs,
            rhs: matching.fill.lhs,
        }
    } else {
        matching.fill
    };
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
        // Scanning one group is its own phase: the list of its present points is the only thing it
        // allocates, it is charged by the block it takes as it grows, and it is dropped with the
        // scan, so the next group's list replaces it.
        let duplicate = overlap(
            one,
            members,
            |point| evaluated[point],
            metadata.tables.saturating_add(metadata.pairing),
            budget.as_deref_mut(),
        )?;
        if let Some(point) = duplicate
            && one_duplicate.is_none_or(|(first, _)| point < first)
        {
            one_duplicate = Some((point, key));
        }
    }

    let drop_name = !matching.op.is_comparison() || matching.return_bool;
    let mut results = Results::default();
    let mut pairs = Pairs {
        matching,
        one_is_left,
        group_keys: Vec::new(),
        group_ids: HashMap::new(),
        // One-to-one: a match group may be matched once per point; Prometheus counts a filtered
        // comparison as a match too, but not an operation that failed. Filling the many side
        // consults the same set.
        matched: (matching.cardinality == Cardinality::OneToOne || fill.lhs.is_some())
            .then(HashSet::new),
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
            // One pairing's label set is built before the axes are intersected, and the next
            // pairing replaces it: its bytes are part of the phase entered above, not a figure
            // reserved per pair.
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
            // The pairing's row is opened by the first point it files and reused by every later
            // one, so a pairing whose axes do not intersect never builds its key.
            let mut slot = RowSlot::Unfiled(&mut results.series);
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
                    slot = pairs.apply(
                        slot,
                        &mut results.totals,
                        &metric,
                        label_set,
                        group,
                        point,
                        many_value,
                        one_value,
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
            // One fill label set for this many-side series, opened by the first point it files.
            let mut slot = RowSlot::Unfiled(&mut results.series);
            for (point, row) in many.positions(many_index) {
                let sample = cursor.at(row).expect("a row never goes back");
                let matched = candidates
                    .iter()
                    .zip(&mut candidate_cursors)
                    .any(|(&index, cursor)| one.present(index, point, cursor));
                if !matched {
                    // A fill row pairs no two series, so it builds no pairing label set.
                    slot = pairs.apply(
                        slot,
                        &mut results.totals,
                        &metric,
                        label_set,
                        group,
                        point,
                        sample,
                        SampleRef::Float(value),
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
            let mut slot = RowSlot::Unfiled(&mut results.series);
            for (point, row) in one.positions(one_index) {
                if !pairs
                    .matched
                    .as_ref()
                    .is_some_and(|matched| matched.contains(&(group, point)))
                {
                    let sample = cursor.at(row).expect("a row never goes back");
                    // A fill row pairs no two series, so it builds no pairing label set: what it
                    // keeps is the label set it is filed under.
                    slot = pairs.apply(
                        slot,
                        &mut results.totals,
                        &metric,
                        label_set,
                        group,
                        point,
                        SampleRef::Float(value),
                        sample,
                        budget.as_deref_mut(),
                    )?;
                }
            }
        }
    }
    // The match set, the groups and the pairing state are gone with the walk: taking them apart
    // here drops them, so the sort and the messages are what is live from here on.
    let Pairs {
        group_keys,
        implicit_many_to_one,
        ..
    } = pairs;
    // The sort phase: one figure for the tables still live beside it, the messages it may append
    // and one slot per row it sorts -- entered once, before it allocates any of them.
    if let Some(budget) = budget.as_deref_mut() {
        budget.phase(
            metadata
                .tables
                .saturating_add(metadata.errors)
                .saturating_add(metadata.largest_key)
                .saturating_add(results.totals.rows.saturating_mul(SORT_ROW_BYTES)),
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
                timestamp_at(point)
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
                timestamp_at(point)
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
                    timestamp_at(duplicate.point)
                ),
            )
        } else {
            (
                duplicate.point,
                2,
                same_labelset(&duplicate.key, timestamp_at(duplicate.point)),
            )
        });
    }
    // Only the messages this field actually appended stay live until the earliest one is reported.
    // They join what the results retain, once the field's tables and sort are gone.
    if let Some(budget) = budget {
        let appended = errors[errors_before..]
            .iter()
            .fold(0usize, |bytes, (_, _, text)| {
                bytes
                    .saturating_add(std::mem::size_of::<String>())
                    .saturating_add(text.capacity())
            });
        budget.hold(appended)?;
    }
    Ok(results)
}

/// The matched pairs of one field and the state Prometheus checks them against.
///
/// The results are filed beside it, not in it: a pairing's open row borrows the table it files into
/// while the match state stays free to move with the point.
struct Pairs<'a> {
    matching: &'a BinaryMatching,
    one_is_left: bool,
    group_keys: Vec<GroupKey>,
    group_ids: HashMap<GroupKey, usize>,
    /// The match groups an operation matched at a point, kept where Prometheus consults them:
    /// one-to-one matching, and filling the many side. A filtered comparison counts as a match,
    /// an operation that failed does not.
    matched: Option<HashSet<(usize, usize)>>,
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
    /// Nothing here is charged per point: the operator's histograms are part of the phase, and the
    /// only figures a row can charge are the two allocation boundaries it crosses -- a block the match
    /// set grows into, and `label_set`, the bytes a result label set keeps when this row is the first
    /// filed under it, or the value the results retain. The row a pairing files under its label set
    /// is opened by [`RowSlot`] on the first point the pairing files, and the slot is handed back to
    /// file the pairing's next point with.
    #[allow(clippy::too_many_arguments)]
    fn apply<'s>(
        &mut self,
        slot: RowSlot<'s>,
        totals: &mut ResultTotals,
        metric: &SeriesLabels,
        label_set: usize,
        group: usize,
        point: usize,
        many_value: SampleRef<'_>,
        one_value: SampleRef<'_>,
        mut budget: Option<&mut Budget<'_>>,
    ) -> Result<RowSlot<'s>> {
        if matches!(many_value, SampleRef::Missing) || matches!(one_value, SampleRef::Missing) {
            return Ok(slot);
        }
        let (lhs_value, rhs_value) = if self.one_is_left {
            (one_value, many_value)
        } else {
            (many_value, one_value)
        };
        let Some((result, keep)) = elem_binop(self.matching.op, lhs_value, rhs_value)? else {
            // The operator skipped the pair and nothing of it was filed.
            return Ok(slot);
        };
        // One-to-one: a match group may be matched once per point; Prometheus counts a filtered
        // comparison as a match too, but not an operation that failed.
        if let Some(matched) = &mut self.matched {
            // The match set is the one container this walk grows point by point, so it is charged by
            // the block it takes when it outgrows what it holds -- before the insert allocates it, and
            // never per point of a set that has room.
            if matched.len() + 1 > matched.capacity()
                && let Some(budget) = budget.as_deref_mut()
            {
                let grown = (2 * matched.capacity()).max(VEC_MIN_CAPACITY);
                budget.grow(
                    grown
                        .saturating_sub(matched.capacity())
                        .saturating_mul(MATCH_SLOT_BYTES),
                )?;
            }
            if !matched.insert((group, point))
                && self.matching.cardinality == Cardinality::OneToOne
                && self
                    .implicit_many_to_one
                    .is_none_or(|(first, _)| point < first)
            {
                self.implicit_many_to_one = Some((point, group));
            }
        }
        // The value the row keeps. A `bool` comparison keeps a float and a filtered comparison
        // keeps nothing; a kept value is moved into the row.
        let value = if self.matching.return_bool {
            drop(result);
            Some(GridSample::Float(if keep { 1.0 } else { 0.0 }))
        } else if keep {
            Some(result)
        } else {
            drop(result);
            None
        };
        slot.file(totals, metric, label_set, group, point, value, budget)
    }
}

/// The labels Prometheus gives a fill sample: the match group of the sample it pairs with.
fn metric_of(key: &GroupKey) -> Result<SeriesLabels> {
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
    SeriesLabels::try_new(name, labels).map_err(external)
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
    set_vectors_inner(op, labels, lhs_schema, lhs, rhs_schema, rhs, None)
}

/// [`set_vectors`], handing the live workspace it holds to `reserve` as it allocates.
///
/// The figures follow [`binary_vectors_with_reservation`]: what the results keep is held as their
/// containers grow, each field's walk and sort is a phase that replaces the one before it, and the
/// merge is the last phase. A total `reserve` refuses ends the operation with that error.
pub(crate) fn set_vectors_with_reservation(
    op: SetOp,
    labels: &MatchingLabels,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
    reserve: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<Vec<Series>> {
    let mut budget = Budget::new(reserve);
    set_vectors_inner(
        op,
        labels,
        lhs_schema,
        lhs,
        rhs_schema,
        rhs,
        Some(&mut budget),
    )
}

/// The one set kernel, whether or not a caller accounts for its workspace.
fn set_vectors_inner(
    op: SetOp,
    labels: &MatchingLabels,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Vec<Series>> {
    let grid = same_grid(lhs_schema, rhs_schema)?;
    if lhs_schema.fields.len() != rhs_schema.fields.len() {
        return Err(DataFusionError::NotImplemented(
            "set operators between vectors with different field counts".into(),
        ));
    }
    let width = lhs_schema.fields.len();
    if let Some(budget) = budget.as_deref_mut() {
        budget.hold(output_bytes(width))?;
    }
    let output = Arc::new(SeriesSchema {
        fields: lhs_schema.fields.clone(),
        eval_grid: Some(grid.clone()),
    });
    let mut errors = Vec::new();
    let mut fields = Vec::with_capacity(width);
    for field in 0..width {
        let lhs = Operand::try_new(lhs_schema, lhs, field)?;
        let rhs = Operand::try_new(rhs_schema, rhs, field)?;
        let results = set_field(
            op,
            labels,
            &grid,
            &lhs,
            &rhs,
            &mut errors,
            budget.as_deref_mut(),
        )?;
        // The field's group table and sort are gone with its frame; what it filed stays live.
        if let Some(budget) = budget.as_deref_mut() {
            budget.phase(0)?;
        }
        fields.push(results);
    }
    earliest_error(errors)?;
    merge_fields(fields, output, budget)
}

/// The figures one set field's phases are charged from, read once from its operands.
struct SetFieldMetadata {
    /// The filter side's match-group table, without the presence bitmaps it holds.
    tables: usize,
    /// The widest key one row builds: the match key a kept series is looked up by, or the key a row
    /// is filed under.
    scratch: usize,
    /// The one message the field can report, a fixed frame around one rendering.
    error: usize,
}

fn set_field_metadata(
    labels: &MatchingLabels,
    kept: &Operand<'_>,
    filter: &Operand<'_>,
) -> SetFieldMetadata {
    let mut tables = 0usize;
    for series in filter.series {
        tables = tables
            .saturating_add(match_key_bytes(&series.metric, labels))
            .saturating_add(GROUP_SLOT_BYTES);
    }
    // `or` files the filter side's rows too, so both sides' label sets bound a row's keys and the
    // text of the message.
    let (mut scratch, mut text) = (0usize, 0usize);
    for series in kept.series.iter().chain(filter.series) {
        scratch = scratch
            .max(match_key_bytes(&series.metric, labels))
            .max(result_key_bytes(&series.metric));
        text = text.max(label_text_bytes(&series.metric));
    }
    SetFieldMetadata {
        tables,
        scratch,
        error: ERROR_TEXT_BYTES.saturating_add(text),
    }
}

/// Filters one field of the kept side by the presence of its match group on the filter side, and
/// appends the first point where two kept rows share a label set as `(point, rank, message)`.
fn set_field(
    op: SetOp,
    labels: &MatchingLabels,
    grid: &EvalGrid,
    lhs: &Operand<'_>,
    rhs: &Operand<'_>,
    errors: &mut Vec<(usize, u8, String)>,
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Results> {
    let (kept, filter, keep_matched) = match op {
        SetOp::And => (lhs, rhs, true),
        SetOp::Unless => (lhs, rhs, false),
        SetOp::Or => (rhs, lhs, false),
    };
    // The walk is one phase, entered before the group table exists; each group's bitmap joins it
    // as the group is first met.
    let metadata = match budget.as_deref_mut() {
        Some(budget) => {
            let metadata = set_field_metadata(labels, kept, filter);
            budget.phase(metadata.tables.saturating_add(metadata.scratch))?;
            Some(metadata)
        }
        None => None,
    };
    let presence = group_presence(
        filter,
        labels,
        grid.spec().len as usize,
        budget.as_deref_mut(),
    )?;
    // A set operator hands on its operands' label sets unchanged, so a row is filed under the label
    // set of the series it came from.
    let charged = budget.is_some();
    let label_set = |metric: &SeriesLabels| {
        if charged {
            result_set_bytes(metric, metric, &[])
        } else {
            0
        }
    };
    let mut results = Results::default();
    if op == SetOp::Or {
        for index in 0..lhs.series.len() {
            let metric = &lhs.series[index].metric;
            let label_set = label_set(metric);
            let mut cursor = lhs.cursor(index);
            // The series' row is opened by the first point it files, and every later point of the
            // series is pushed to it.
            let mut slot = RowSlot::Unfiled(&mut results.series);
            for (point, row) in lhs.positions(index) {
                if let Some(value) = owned(cursor.at(row).expect("a row never goes back"))? {
                    slot = slot.file(
                        &mut results.totals,
                        metric,
                        label_set,
                        0,
                        point,
                        Some(value),
                        budget.as_deref_mut(),
                    )?;
                }
            }
        }
    }
    for (index, series) in kept.series.iter().enumerate() {
        let label_set = label_set(&series.metric);
        let bits = presence.get(&group_key(&series.metric, labels));
        let mut cursor = kept.cursor(index);
        let mut slot = RowSlot::Unfiled(&mut results.series);
        for (point, row) in kept.positions(index) {
            let sample = cursor.at(row).expect("a row never goes back");
            if matches!(sample, SampleRef::Missing) {
                continue;
            }
            let matched = bits.is_some_and(|bits| bits[point / 64] & (1 << (point % 64)) != 0);
            if matched == keep_matched {
                let value = owned(sample)?;
                slot = slot.file(
                    &mut results.totals,
                    &series.metric,
                    label_set,
                    0,
                    point,
                    value,
                    budget.as_deref_mut(),
                )?;
            }
        }
    }
    // The group table is gone before the sort phase, which is charged without it: the message the
    // sort may append and one slot per row it sorts, entered before it allocates either.
    drop(presence);
    if let (Some(budget), Some(metadata)) = (budget.as_deref_mut(), &metadata) {
        budget.phase(
            metadata
                .error
                .saturating_add(metadata.scratch)
                .saturating_add(results.totals.rows.saturating_mul(SORT_ROW_BYTES)),
        )?;
    }
    if let Some(duplicate) = results.sort_rows() {
        let timestamp = (grid.spec().start_ms as i128
            + duplicate.point as i128 * grid.spec().step_ms as i128) as i64;
        let message = same_labelset(&duplicate.key, timestamp);
        // The message stays live until the earliest one is reported.
        if let Some(budget) = budget {
            budget.hold(std::mem::size_of::<String>().saturating_add(message.capacity()))?;
        }
        errors.push((duplicate.point, 0, message));
    }
    Ok(results)
}

/// The points at which each match group of `operand` holds a sample, one bit per grid point.
///
/// Prometheus decides a set operator at a step from the signatures present on the filter side, so
/// a kept sample needs only whether its group is present at its point: built once from the filter
/// side's rows, that is one lookup per kept sample however many series share the group. A group's
/// bitmap is charged as the group is first met, before it is allocated.
fn group_presence(
    operand: &Operand<'_>,
    labels: &MatchingLabels,
    points: usize,
    mut budget: Option<&mut Budget<'_>>,
) -> Result<HashMap<GroupKey, Vec<u64>>> {
    let words = points.div_ceil(64);
    let mut presence: HashMap<GroupKey, Vec<u64>> = HashMap::new();
    for (index, series) in operand.series.iter().enumerate() {
        let bits = match presence.entry(group_key(&series.metric, labels)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if let Some(budget) = budget.as_deref_mut() {
                    budget.grow(words.saturating_mul(std::mem::size_of::<u64>()))?;
                }
                entry.insert(vec![0; words])
            }
        };
        let mut cursor = operand.cursor(index);
        for (point, row) in operand.positions(index) {
            if !matches!(
                cursor.at(row).expect("a row never goes back"),
                SampleRef::Missing
            ) {
                bits[point / 64] |= 1 << (point % 64);
            }
        }
    }
    Ok(presence)
}

/// Joins the per-field results into series with all fields, by label set.
///
/// The merge is one phase, entered from the results the fields filed: the triples it groups, the
/// table it groups them in and the builder's own buffers. Its columns are reserved once per result
/// series, before the series pushes its first row, rather than once per row.
fn merge_fields(
    fields: Vec<Results>,
    schema: Arc<SeriesSchema>,
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Vec<Series>> {
    let width = schema.fields.len();
    // What the merge holds before it allocates any of it, read from the results themselves: the
    // kept triples, the label sets they are filed under and the histogram payload they carry.
    if let Some(budget) = budget.as_deref_mut() {
        let mut sets = 0usize;
        let mut merge = (0usize, 0usize, 0usize);
        for results in &fields {
            sets = sets.saturating_add(results.series.len());
            merge = (
                merge.0.saturating_add(results.totals.kept),
                merge.1.saturating_add(results.totals.histograms),
                merge.2.saturating_add(results.totals.histogram_heap),
            );
        }
        budget.phase(merge_bytes(merge.0, sets, width, merge.1, merge.2))?;
    }
    // Per label set: the metric and its `(point, field, value)` samples.
    type Merged = (SeriesLabels, Vec<(usize, usize, GridSample)>);
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
        // This series' columns, in one reservation before it writes the first row of them. They stay
        // live inside the series it is finished as, so the phase keeps them.
        if let Some(budget) = budget.as_deref_mut() {
            budget.grow(merge_series_columns(&samples, width))?;
        }
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

/// The bytes one result series' columns take: the rows it writes -- one per distinct point of its
/// samples, which is what the builder writes a row for -- times a row's columns.
///
/// A row is the value and validity column of every field, charged twice for the capacity each column
/// grows to, and the histogram index column of every field. The index columns are counted for every
/// field rather than only the ones that happen to carry a histogram, so the figure over-reserves
/// rather than missing a column, and nothing here follows how a column grows.
fn merge_series_columns(samples: &[(usize, usize, GridSample)], fields: usize) -> usize {
    let row = 2 * fields.saturating_mul(std::mem::size_of::<f64>() + 1)
        + fields.saturating_mul(std::mem::size_of::<Option<u32>>());
    let mut rows = 0usize;
    let mut consumed: Option<usize> = None;
    for (point, ..) in samples {
        if consumed != Some(*point) {
            rows = rows.saturating_add(1);
            consumed = Some(*point);
        }
    }
    rows.saturating_mul(row)
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

fn group_key(metric: &SeriesLabels, labels: &MatchingLabels) -> GroupKey {
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

fn key_of(metric: &SeriesLabels) -> GroupKey {
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
/// The group's list of present points is the only thing this scan allocates, and scanning one group
/// is its own phase: the block the list takes is charged when it outgrows what it holds, and the next
/// group's scan enters `base` again rather than adding a second list to it. A group of one member and
/// a group whose members carry no sample both cost nothing, however long their axes are.
fn overlap(
    operand: &Operand<'_>,
    members: &[usize],
    relevant: impl Fn(usize) -> bool,
    base: usize,
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
                // A new block of the list, reserved before the push that allocates it.
                if let Some(budget) = budget.as_deref_mut()
                    && points.len() == points.capacity()
                {
                    let capacity = points.capacity().saturating_mul(2).max(VEC_MIN_CAPACITY);
                    let list = capacity.saturating_mul(std::mem::size_of::<usize>());
                    budget.phase(base.saturating_add(list))?;
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
    // The list is dropped with this scan, so the field's own phase figure is live from here on.
    if let Some(budget) = budget {
        budget.phase(base)?;
    }
    Ok(duplicate)
}

fn result_metric(
    many: &SeriesLabels,
    one: &SeriesLabels,
    matching: &BinaryMatching,
    include: &[Arc<str>],
    drop_name: bool,
) -> Result<SeriesLabels> {
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

    /// The widest histogram this operand's field holds.
    ///
    /// Read once before the rows are matched, so the workspace of a histogram row is reserved for
    /// the field rather than per row. A field of floats is answered without reading a sample: its
    /// segments say what it holds, so the numeric path pays nothing for this figure.
    fn widest_histogram(&self) -> Option<HistogramView<'a>> {
        let mut widest: Option<(HistogramView<'a>, usize)> = None;
        for series in self.series {
            let field = &series.fields.as_slice()[self.field];
            let holds_histograms = field.segments().iter().any(|segment| {
                matches!(
                    segment.dense(),
                    Some(DenseSamplesRef::Histogram(_) | DenseSamplesRef::Mixed(_))
                )
            });
            if !holds_histograms {
                continue;
            }
            for sample in field.iter() {
                let SampleRef::Histogram(view) = sample else {
                    continue;
                };
                if widest.is_none_or(|(_, widest)| owned_histogram_bytes(&view) > widest) {
                    widest = Some((view, owned_histogram_bytes(&view)));
                }
            }
        }
        widest.map(|(view, _)| view)
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
///
/// A pairing opens one [`RowSlot`] and looks its label set up on the first row it files, so the key
/// is built, hashed and compared once per pairing that files -- not once per point.
#[derive(Default)]
struct Results {
    series: HashMap<GroupKey, ResultRows>,
    /// The figures a filed row advances; see [`ResultTotals`].
    totals: ResultTotals,
}

/// The rows filed, the rows holding a value, the rows holding a histogram and the heap those
/// histograms hold: counted as each row is filed, so neither later phase walks them again.
///
/// Held beside the table rather than in it, so a pairing's open row borrows the table it files into
/// while these figures stay free to move with the point.
#[derive(Default)]
struct ResultTotals {
    rows: usize,
    kept: usize,
    histograms: usize,
    histogram_heap: usize,
}

/// Rows in arrival order: their points, the match group each came from, and their values;
/// `None` is a match the operator filtered.
struct ResultRows {
    metric: SeriesLabels,
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

/// One pairing's result row, opened by the first point the pairing files.
///
/// A pairing builds its label set once, but the row it files under is looked up, created and
/// charged only when that pairing files the first of its points: a pairing whose samples never
/// pair, or whose axes do not intersect, costs what it cost before -- nothing. Once open, the row
/// is pushed to directly, so the points of one pairing build no key, hash nothing and clone no
/// metric.
enum RowSlot<'a> {
    /// No point filed yet, and the table the first one is filed in.
    Unfiled(&'a mut HashMap<GroupKey, ResultRows>),
    /// The row every point of this pairing files under.
    Row(&'a mut ResultRows),
}

impl<'a> RowSlot<'a> {
    /// Files one row of one field, opening the pairing's row on its first filed point, and returns
    /// the slot to file the pairing's next point with.
    ///
    /// A row whose value the operator filtered is still a row: it keeps its slots and its label
    /// set, which the merge carries as a set without a value. A kept histogram is boxed beside its
    /// row, so its payload is charged here, where the value is retained, and never per grid point
    /// of a float.
    #[allow(clippy::too_many_arguments)]
    fn file(
        self,
        totals: &mut ResultTotals,
        metric: &SeriesLabels,
        label_set: usize,
        group: usize,
        point: usize,
        value: Option<GridSample>,
        mut budget: Option<&mut Budget<'_>>,
    ) -> Result<Self> {
        let rows = match self {
            Self::Row(rows) => rows,
            Self::Unfiled(series) => match series.entry(key_of(metric)) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    // The label set the row is filed under is only built when the table does not
                    // hold it yet, so these bytes are charged on the vacant entry alone.
                    if let Some(budget) = budget.as_deref_mut() {
                        budget.hold(label_set)?;
                    }
                    entry.insert(ResultRows {
                        metric: metric.clone(),
                        points: Vec::new(),
                        groups: Vec::new(),
                        values: Vec::new(),
                    })
                }
            },
        };
        // The row vectors grow together, so one block of them is charged once when the point,
        // group and value vectors outgrow the capacity they hold.
        if rows.points.len() == rows.points.capacity()
            && let Some(budget) = budget.as_deref_mut()
        {
            let grown = (2 * rows.points.capacity()).max(VEC_MIN_CAPACITY);
            budget.hold(
                grown
                    .saturating_sub(rows.points.capacity())
                    .saturating_mul(RESULT_ROW_BYTES),
            )?;
        }
        totals.rows = totals.rows.saturating_add(1);
        if let Some(value) = &value {
            totals.kept = totals.kept.saturating_add(1);
            if let GridSample::Histogram(histogram) = value {
                totals.histograms = totals.histograms.saturating_add(1);
                totals.histogram_heap =
                    totals.histogram_heap.saturating_add(histogram.heap_bytes());
                if let Some(budget) = budget {
                    budget.hold(result_histogram_bytes(histogram))?;
                }
            }
        }
        rows.points.push(point);
        rows.groups.push(group);
        rows.values.push(value);
        Ok(Self::Row(rows))
    }
}

impl Results {
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
