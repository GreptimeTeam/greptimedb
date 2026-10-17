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
//! points where both are present.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::samples::{DenseSamplesRef, SampleRef, Samples, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, TimeAxis};

use crate::grid_series::GridSeriesBuilder;

mod go_math;
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
            Self::Pow => (go_math::pow(lhs, rhs), true),
            Self::Atan2 => (go_math::atan2(lhs, rhs), true),
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
/// Fields are paired by position and computed independently; a point is present in a series
/// when any of its fields has a value. Arithmetic and `bool` comparisons drop `__name__`;
/// comparisons without `bool` keep the left value where they hold.
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
    let lhs = Operand::try_new(lhs_schema, lhs)?;
    let rhs = Operand::try_new(rhs_schema, rhs)?;
    let (many, one, include, one_is_left) = match &matching.cardinality {
        Cardinality::OneToOne => (&lhs, &rhs, &[][..], false),
        Cardinality::ManyToOne(include) => (&lhs, &rhs, &include[..], false),
        Cardinality::OneToMany(include) => (&rhs, &lhs, &include[..], true),
    };
    let points = grid.timestamps().len();
    // Prometheus short-circuits a point where either side is empty, before any check.
    let both_present: Vec<bool> = lhs
        .presence(points)?
        .into_iter()
        .zip(rhs.presence(points)?)
        .map(|(lhs, rhs)| lhs && rhs)
        .collect();

    let one_groups = group(one, &matching.labels);
    let mut one_duplicate: Option<(usize, &GroupKey)> = None;
    for (key, members) in &one_groups {
        if let Some(point) = overlap(one, members, |point| Ok(both_present[point]))?
            && one_duplicate.is_none_or(|(first, _)| point < first)
        {
            one_duplicate = Some((point, key));
        }
    }

    let fields = output.fields.len();
    let drop_name = !matching.op.is_comparison() || matching.return_bool;
    let mut results = Results::default();
    let mut matched_groups: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    let mut values = vec![None; fields];
    for (many_index, many_series) in many.series.iter().enumerate() {
        let key = group_key(&many_series.metric, &matching.labels);
        let Some(candidates) = one_groups.get(&key) else {
            continue;
        };
        if matches!(matching.cardinality, Cardinality::OneToOne) {
            matched_groups.entry(key).or_default().push(many_index);
        }
        for &one_index in candidates {
            let metric = result_metric(
                &many_series.metric,
                &one.series[one_index].metric,
                matching,
                include,
                drop_name,
            )?;
            let (start, end) = one.overlap_range(many, one_index, many_index);
            for point in start..end {
                if !(many.present(many_index, point)? && one.present(one_index, point)?) {
                    continue;
                }
                for (field, value) in values.iter_mut().enumerate() {
                    let (lhs_value, rhs_value) = if one_is_left {
                        (
                            one.value(one_index, field, point)?,
                            many.value(many_index, field, point)?,
                        )
                    } else {
                        (
                            many.value(many_index, field, point)?,
                            one.value(one_index, field, point)?,
                        )
                    };
                    *value = match (lhs_value, rhs_value) {
                        (Some(lhs_value), Some(rhs_value)) => {
                            let (result, holds) = matching.op.apply(lhs_value, rhs_value);
                            if !matching.op.is_comparison() {
                                Some(result)
                            } else if matching.return_bool {
                                Some(if holds { 1.0 } else { 0.0 })
                            } else {
                                holds.then_some(result)
                            }
                        }
                        _ => None,
                    };
                }
                results.insert(&metric, point, &values);
            }
        }
    }
    // One-to-one: a match group may be matched once per point; Prometheus counts a filtered
    // comparison as a match too.
    let mut implicit_many_to_one: Option<(usize, &GroupKey)> = None;
    for (key, members) in &matched_groups {
        let matched = |point: usize| -> Result<bool> {
            let Some(candidates) = one_groups.get(key) else {
                return Ok(false);
            };
            for &candidate in candidates {
                if one.covers(candidate, point) && one.present(candidate, point)? {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        if let Some(point) = overlap(many, members, matched)?
            && implicit_many_to_one.is_none_or(|(first, _)| point < first)
        {
            implicit_many_to_one = Some((point, key));
        }
    }
    let grouping_duplicate = results.sort_rows();
    // Prometheus fails at the first failing step, and checks the one side before matching.
    let error = match (one_duplicate, implicit_many_to_one, grouping_duplicate) {
        (Some((point, key)), many_to_one, grouping)
            if many_to_one.is_none_or(|(first, _)| point <= first)
                && grouping.as_ref().is_none_or(|(first, _)| point <= *first) =>
        {
            let side = if one_is_left { "left" } else { "right" };
            Some(format!(
                "found duplicate series for the match group {} on the {side} hand-side of the \
                 operation at {} ms; many-to-many matching not allowed: matching labels must be \
                 unique on one side",
                render(key),
                grid.timestamps()[point]
            ))
        }
        (_, Some((point, key)), _) => Some(format!(
            "multiple matches for labels {} at {} ms: many-to-one matching must be explicit \
             (group_left/group_right)",
            render(key),
            grid.timestamps()[point]
        )),
        (_, _, Some((point, key))) => Some(format!(
            "multiple matches for labels {} at {} ms: grouping labels must ensure unique matches",
            render(&key),
            grid.timestamps()[point]
        )),
        _ => None,
    };
    if let Some(error) = error {
        return Err(DataFusionError::Execution(error));
    }
    results.build(output)
}

/// Applies a set operator at every grid point. Series keep their labels and values.
pub fn set_vectors(
    op: SetOp,
    labels: &MatchingLabels,
    lhs_schema: &SeriesSchema,
    lhs: &[Series],
    rhs_schema: &SeriesSchema,
    rhs: &[Series],
) -> Result<Vec<Series>> {
    let grid = same_grid(lhs_schema, rhs_schema)?;
    if op == SetOp::Or && lhs_schema.fields.len() != rhs_schema.fields.len() {
        return Err(DataFusionError::NotImplemented(
            "`or` between vectors with different field counts".into(),
        ));
    }
    let output = Arc::new(SeriesSchema {
        fields: lhs_schema.fields.clone(),
        eval_grid: Some(grid.clone()),
    });
    let lhs = Operand::try_new(lhs_schema, lhs)?;
    let rhs = Operand::try_new(rhs_schema, rhs)?;
    let (kept, filter, keep_matched) = match op {
        SetOp::And => (&lhs, &rhs, true),
        SetOp::Unless => (&lhs, &rhs, false),
        SetOp::Or => (&rhs, &lhs, false),
    };
    let filter_groups = group(filter, labels);
    let mut results = Results::default();
    let fields = output.fields.len();
    let mut values = vec![None; fields];
    if op == SetOp::Or {
        for index in 0..lhs.series.len() {
            for point in lhs.range(index) {
                if lhs.present(index, point)? {
                    lhs.row(index, point, &mut values)?;
                    results.insert(&lhs.series[index].metric, point, &values);
                }
            }
        }
    }
    for (index, series) in kept.series.iter().enumerate() {
        let candidates = filter_groups.get(&group_key(&series.metric, labels));
        for point in kept.range(index) {
            if !kept.present(index, point)? {
                continue;
            }
            let mut matched = false;
            for &candidate in candidates.into_iter().flatten() {
                if filter.covers(candidate, point) && filter.present(candidate, point)? {
                    matched = true;
                    break;
                }
            }
            if matched == keep_matched {
                kept.row(index, point, &mut values)?;
                results.insert(&series.metric, point, &values);
            }
        }
    }
    if let Some((point, key)) = results.sort_rows() {
        return Err(DataFusionError::Execution(format!(
            "vector cannot contain metrics with the same labelset {} at {} ms",
            render(&key),
            grid.timestamps()[point]
        )));
    }
    results.build(output)
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
    mut relevant: impl FnMut(usize) -> Result<bool>,
) -> Result<Option<usize>> {
    if members.len() < 2 {
        return Ok(None);
    }
    let start = members.iter().map(|&m| operand.start(m)).min().unwrap_or(0);
    let end = members.iter().map(|&m| operand.end(m)).max().unwrap_or(0);
    let mut seen = vec![false; end.saturating_sub(start)];
    let mut first = None;
    for &member in members {
        for point in operand.range(member) {
            if first.is_some_and(|first| point >= first) {
                break;
            }
            if !operand.present(member, point)? {
                continue;
            }
            if std::mem::replace(&mut seen[point - start], true) && relevant(point)? {
                first = Some(point);
            }
        }
    }
    Ok(first)
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
        metric.remove("__name__");
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

/// One side's series with checked grid ranges.
struct Operand<'a> {
    series: &'a [Series],
    starts: Vec<usize>,
}

impl<'a> Operand<'a> {
    fn try_new(schema: &SeriesSchema, series: &'a [Series]) -> Result<Self> {
        let grid = schema.eval_grid.as_ref().map(|grid| grid.spec());
        let mut starts = Vec::with_capacity(series.len());
        for item in series {
            let TimeAxis::Grid(range) = &item.time else {
                return Err(DataFusionError::Execution(
                    "vector operand series must be on the evaluation grid".into(),
                ));
            };
            if Some(range.grid().spec()) != grid
                || item.fields.len() != schema.fields.len()
                || item.fields.iter().any(|field| field.len() != range.len())
            {
                return Err(DataFusionError::Execution(
                    "vector operand series does not match its schema".into(),
                ));
            }
            starts.push(range.start());
        }
        Ok(Self { series, starts })
    }

    fn start(&self, index: usize) -> usize {
        self.starts[index]
    }

    fn end(&self, index: usize) -> usize {
        self.starts[index] + self.series[index].time.len()
    }

    fn range(&self, index: usize) -> std::ops::Range<usize> {
        self.start(index)..self.end(index)
    }

    fn covers(&self, index: usize, point: usize) -> bool {
        self.range(index).contains(&point)
    }

    /// The shared points of `self[own]` and `other[theirs]`.
    fn overlap_range(&self, other: &Operand<'_>, own: usize, theirs: usize) -> (usize, usize) {
        let start = self.start(own).max(other.start(theirs));
        let end = self.end(own).min(other.end(theirs));
        (start, end.max(start))
    }

    fn value(&self, index: usize, field: usize, point: usize) -> Result<Option<f64>> {
        let samples = &self.series[index].fields.as_slice()[field];
        sample(samples, point - self.starts[index])
    }

    fn present(&self, index: usize, point: usize) -> Result<bool> {
        for field in 0..self.series[index].fields.len() {
            if self.value(index, field, point)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn row(&self, index: usize, point: usize, values: &mut [Option<f64>]) -> Result<()> {
        for (field, value) in values.iter_mut().enumerate() {
            *value = self.value(index, field, point)?;
        }
        Ok(())
    }

    /// Whether any series is present at each grid point.
    fn presence(&self, points: usize) -> Result<Vec<bool>> {
        let mut presence = vec![false; points];
        for index in 0..self.series.len() {
            for point in self.range(index) {
                if !presence[point] && self.present(index, point)? {
                    presence[point] = true;
                }
            }
        }
        Ok(presence)
    }
}

fn sample(samples: &Samples, row: usize) -> Result<Option<f64>> {
    if let Some(DenseSamplesRef::Float(values)) = samples.dense() {
        return Ok(float_at(values, row));
    }
    match samples.sample(row) {
        SampleRef::Missing => Ok(None),
        SampleRef::Float(value) => Ok(Some(value)),
        SampleRef::Histogram(_) => Err(DataFusionError::NotImplemented(
            "vector matching of histogram samples".into(),
        )),
    }
}

fn float_at(values: &Float64Array, row: usize) -> Option<f64> {
    values.is_valid(row).then(|| values.value(row))
}

/// Result rows by result label set, assembled into trimmed grid series.
#[derive(Default)]
struct Results {
    series: HashMap<GroupKey, ResultRows>,
}

/// Rows in arrival order: their points, and their field values laid out row after row.
struct ResultRows {
    metric: MetricName,
    points: Vec<usize>,
    values: Vec<Option<f64>>,
}

impl Results {
    fn insert(&mut self, metric: &MetricName, point: usize, values: &[Option<f64>]) {
        let rows = match self.series.entry(key_of(metric)) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(ResultRows {
                metric: metric.clone(),
                points: Vec::new(),
                values: Vec::new(),
            }),
        };
        rows.points.push(point);
        rows.values.extend_from_slice(values);
    }

    /// Sorts the rows of each label set by point and returns the earliest point where a label
    /// set has two rows. Prometheus evaluates step by step and fails at the first such step.
    fn sort_rows(&mut self) -> Option<(usize, GroupKey)> {
        let mut first: Option<(usize, &GroupKey)> = None;
        for (key, rows) in &mut self.series {
            if !rows.points.is_sorted() {
                let fields = rows.values.len() / rows.points.len();
                let mut order: Vec<usize> = (0..rows.points.len()).collect();
                order.sort_by_key(|&row| rows.points[row]);
                rows.points = order.iter().map(|&row| rows.points[row]).collect();
                rows.values = order
                    .iter()
                    .flat_map(|&row| {
                        rows.values[row * fields..(row + 1) * fields]
                            .iter()
                            .copied()
                    })
                    .collect();
            }
            if let Some(pair) = rows.points.windows(2).find(|pair| pair[0] == pair[1])
                && first.is_none_or(|(point, _)| pair[0] < point)
            {
                first = Some((pair[0], key));
            }
        }
        first.map(|(point, key)| (point, key.clone()))
    }

    /// Builds the result series from rows sorted by [`Results::sort_rows`] without duplicates.
    fn build(self, schema: Arc<SeriesSchema>) -> Result<Vec<Series>> {
        let fields = schema.fields.len();
        let mut builder = GridSeriesBuilder::try_new(schema)?;
        let mut output = Vec::with_capacity(self.series.len());
        for rows in self.series.into_values() {
            for (row, &point) in rows.points.iter().enumerate() {
                builder.push(point, &rows.values[row * fields..(row + 1) * fields])?;
            }
            if let Some(series) = builder.finish(rows.metric)? {
                output.push(series);
            }
        }
        Ok(output)
    }
}
