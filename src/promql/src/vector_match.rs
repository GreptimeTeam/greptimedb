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
use datatypes::series::MetricName;
use datatypes::series::histogram::{CounterResetHint, HistogramView};
use datatypes::series::samples::{RowCursor, SampleRef, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridRunPair, TimeAxis};

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
pub fn binary_vectors(
    matching: &BinaryMatching,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
) -> Result<Vec<Series>> {
    let output = binary_output_schema(lhs_schema, rhs_schema)?;
    let grid = output
        .eval_grid
        .clone()
        .expect("set by binary_output_schema");
    let mut errors = Vec::new();
    let mut fields = Vec::with_capacity(output.fields.len());
    for field in 0..output.fields.len() {
        let lhs = Operand::try_new(lhs_schema, lhs, field)?;
        let rhs = Operand::try_new(rhs_schema, rhs, field)?;
        fields.push(binary_field(matching, &grid, &lhs, &rhs, &mut errors)?);
    }
    earliest_error(errors)?;
    merge_fields(fields, output)
}

/// Matches one field of each side, collecting the first failure of each kind as
/// `(point, rank, message)`.
fn binary_field(
    matching: &BinaryMatching,
    grid: &EvalGrid,
    lhs: &Operand<'_>,
    rhs: &Operand<'_>,
    errors: &mut Vec<(usize, u8, String)>,
) -> Result<Results> {
    let (many, one, include, one_is_left) = match &matching.cardinality {
        Cardinality::OneToOne => (lhs, rhs, &[][..], false),
        Cardinality::ManyToOne(include) => (lhs, rhs, &include[..], false),
        Cardinality::OneToMany(include) => (rhs, lhs, &include[..], true),
    };
    let points = grid.timestamps().len();
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
    let mut matched: HashSet<(usize, usize)> = HashSet::new();
    let mut implicit_many_to_one: Option<(usize, &GroupKey)> = None;
    for (many_index, many_series) in many.series.iter().enumerate() {
        let key = group_key(&many_series.metric, &matching.labels);
        let Some(candidates) = one_groups.get(&key) else {
            continue;
        };
        let group_id = group_ids[&key];
        for &one_index in candidates {
            let metric = result_metric(
                &many_series.metric,
                &one.series[one_index].metric,
                matching,
                include,
                drop_name,
            )?;
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
                    let Some((result, keep)) = elem_binop(matching.op, lhs_value, rhs_value)?
                    else {
                        continue;
                    };
                    if matching.cardinality == Cardinality::OneToOne
                        && !matched.insert((group_id, point))
                        && implicit_many_to_one.is_none_or(|(first, _)| point < first)
                    {
                        implicit_many_to_one = Some((point, group_keys[group_id]));
                    }
                    let value = if matching.return_bool {
                        Some(GridSample::Float(if keep { 1.0 } else { 0.0 }))
                    } else {
                        keep.then_some(result)
                    };
                    results.insert(&metric, group_id, point, value);
                }
            }
        }
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
    if op == SetOp::Or {
        for index in 0..lhs.series.len() {
            let mut cursor = lhs.cursor(index);
            for (point, row) in lhs.positions(index) {
                if let Some(value) = owned(cursor.at(row).expect("a row never goes back"))? {
                    results.insert(&lhs.series[index].metric, 0, point, Some(value));
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
                results.insert(&series.metric, 0, point, owned(sample)?);
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
    fn insert(
        &mut self,
        metric: &MetricName,
        group: usize,
        point: usize,
        value: Option<GridSample>,
    ) {
        let rows = match self.series.entry(key_of(metric)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(ResultRows {
                metric: metric.clone(),
                points: Vec::new(),
                groups: Vec::new(),
                values: Vec::new(),
            }),
        };
        rows.points.push(point);
        rows.groups.push(group);
        rows.values.push(value);
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
