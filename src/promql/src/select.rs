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

//! Operators that depend on the whole set of evaluated series at each grid point: `topk`,
//! `bottomk`, `scalar`, `absent` and the instant-result sorts.
//!
//! Semantics follow Prometheus (`aggregationK`, `funcScalar`, `funcAbsent`,
//! `createLabelsForAbsentFunction`, `funcSort*`). A scalar is a grid series without labels that
//! has a value at every point.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};
use datatypes::series::time::TimeAxis;
use datatypes::series::{Label, MetricName};

use crate::go_sort;
use crate::grid_series::{GridSeriesBuilder, external};

#[cfg(test)]
mod tests;

/// The labels that form an aggregation group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Grouping {
    /// `by(...)`: only these labels, `__name__` included when listed.
    By(Vec<Arc<str>>),
    /// `without(...)`: every label except these and `__name__`.
    Without(Vec<Arc<str>>),
}

/// The order `sort_instant` arranges an instant result in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortOrder {
    /// `sort`: ascending values, NaN last.
    Asc,
    /// `sort_desc`: descending values, NaN last.
    Desc,
    /// `sort_by_label`: natural order of the listed label values, then the label sets.
    ByLabel(Vec<Arc<str>>),
    /// `sort_by_label_desc`.
    ByLabelDesc(Vec<Arc<str>>),
}

/// `topk(k, ...)` (`top`) or `bottomk(k, ...)` with a constant `k`.
///
/// At every point each group keeps the `k` largest (or smallest) values, NaN ranking last for
/// both; the selected samples keep their series' labels and values. Prometheus breaks ties by
/// the order its storage returns series in, which is not a property of the series; here ties
/// go to the series whose label set sorts first.
pub fn topk(
    k: f64,
    top: bool,
    grouping: &Grouping,
    schema: &SeriesSchema,
    series: &[Series],
) -> Result<Vec<Series>> {
    single_field(schema, "topk/bottomk")?;
    let points = grid_points(schema, series)?;
    // Prometheus returns nothing for k below 1, rejects NaN and values out of int64, then
    // truncates k and caps it at the number of input series.
    if k < 1.0 {
        return Ok(Vec::new());
    }
    if k.is_nan() {
        return Err(DataFusionError::Execution("Parameter value is NaN".into()));
    }
    // Prometheus' `maxInt64`, the largest float64 below 2^63.
    if k >= 9_223_372_036_854_774_784.0 {
        return Err(DataFusionError::Execution(format!(
            "Scalar value {k} overflows int64"
        )));
    }
    let k = k.trunc().min(series.len() as f64) as usize;
    if k == 0 {
        return Ok(Vec::new());
    }

    let mut order: Vec<usize> = (0..series.len()).collect();
    order.sort_by(|&a, &b| compare_labels(&series[a].metric, &series[b].metric));
    let mut groups: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    for index in order {
        groups
            .entry(group_key(&series[index].metric, grouping))
            .or_default()
            .push(index);
    }

    let mut selected: Vec<Vec<usize>> = vec![Vec::new(); series.len()];
    let mut candidates: Vec<(usize, f64)> = Vec::new();
    for members in groups.values() {
        for point in 0..points {
            candidates.clear();
            for &member in members {
                if let Some(value) = value_at(&series[member], point)? {
                    candidates.push((member, value));
                }
            }
            // Members are in label order, so a stable sort hands ties to the earlier label set.
            candidates.sort_by(|a, b| rank(a.1, b.1, top));
            for &(member, _) in candidates.iter().take(k) {
                selected[member].push(point);
            }
        }
    }

    let output = Arc::new(schema.clone());
    let mut builder = GridSeriesBuilder::try_new(output)?;
    let mut result = Vec::new();
    for (index, points) in selected.into_iter().enumerate() {
        if points.is_empty() {
            continue;
        }
        let mut sorted = points;
        sorted.sort_unstable();
        for point in sorted {
            builder.push(point, &[value_at(&series[index], point)?])?;
        }
        if let Some(built) = builder.finish(series[index].metric.clone())? {
            result.push(built);
        }
    }
    Ok(result)
}

/// `scalar(v)`: the value of the only series present at each point, NaN otherwise.
pub fn scalar(schema: &SeriesSchema, series: &[Series]) -> Result<Series> {
    single_field(schema, "scalar")?;
    let points = grid_points(schema, series)?;
    let mut values: Vec<Option<f64>> = vec![None; points];
    let mut counts = vec![0u8; points];
    for item in series {
        let TimeAxis::Grid(range) = &item.time else {
            unreachable!("checked by grid_points");
        };
        for point in range.start()..range.start() + range.len() {
            if let Some(value) = value_at(item, point)? {
                counts[point] = counts[point].saturating_add(1);
                values[point] = Some(value);
            }
        }
    }
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone()))?;
    for (point, (value, count)) in values.into_iter().zip(counts).enumerate() {
        let value = if count == 1 { value } else { Some(GO_NAN) };
        builder.push(point, &[value])?;
    }
    let empty = MetricName::try_new(None, Vec::new()).map_err(external)?;
    Ok(builder
        .finish(empty)?
        .expect("every point of a scalar has a value"))
}

/// `absent(v)`: 1 with `labels` at every point where no series is present.
pub fn absent(
    schema: &SeriesSchema,
    series: &[Series],
    labels: MetricName,
) -> Result<Option<Series>> {
    let points = grid_points(schema, series)?;
    let fields = schema.fields.len();
    // Each field is absent on its own.
    let mut present = vec![false; points * fields];
    for item in series {
        let TimeAxis::Grid(range) = &item.time else {
            unreachable!("checked by grid_points");
        };
        for (field, samples) in item.fields.iter().enumerate() {
            for row in 0..range.len() {
                if !matches!(samples.sample(row), SampleRef::Missing) {
                    present[(range.start() + row) * fields + field] = true;
                }
            }
        }
    }
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone()))?;
    let mut row = vec![None; fields];
    for point in 0..points {
        for (field, value) in row.iter_mut().enumerate() {
            *value = (!present[point * fields + field]).then_some(1.0);
        }
        builder.push(point, &row)?;
    }
    builder.finish(labels)
}

/// A label matcher of the selector `absent` is applied to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LabelMatcher {
    pub name: Arc<str>,
    pub op: MatchOp,
    pub value: Arc<str>,
    /// Whether the matcher accepts an empty value, i.e. a missing label; the caller evaluates
    /// regular expressions.
    pub matches_empty: bool,
}

/// The operator of a [`LabelMatcher`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchOp {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

/// The labels of an `absent` result, as Prometheus `createLabelsForAbsentFunction` derives them:
/// the first equality matcher of a name sets that label, any other matcher of the name removes
/// it, and `__name__` is skipped.
///
/// Prometheus reads the matchers after TSDB's `PostingsForMatchers` has reordered them in place,
/// moving the matchers that narrow the result before those that only subtract from it; that is
/// the order users see whenever the storage holds data, and the order used here. It changes the
/// result only when a name has both an equality and another matcher.
pub fn absent_labels(matchers: &[LabelMatcher]) -> Result<MetricName> {
    let must_be_set: Vec<&str> = matchers
        .iter()
        .filter(|matcher| !matcher.matches_empty)
        .map(|matcher| &*matcher.name)
        .collect();
    let subtracting = |matcher: &LabelMatcher| {
        !must_be_set.contains(&&*matcher.name)
            || (matches!(matcher.op, MatchOp::NotEqual | MatchOp::NotRegex)
                && matcher.matches_empty)
    };
    let ordered = matchers
        .iter()
        .filter(|matcher| !subtracting(matcher))
        .chain(matchers.iter().filter(|matcher| subtracting(matcher)));

    let mut labels: Vec<Label> = Vec::new();
    let mut set: Vec<&str> = Vec::new();
    for matcher in ordered {
        if &*matcher.name == "__name__" {
            continue;
        }
        if matcher.op == MatchOp::Equal && !set.contains(&&*matcher.name) {
            labels.retain(|label| label.name != matcher.name);
            labels.push(Label {
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            });
            set.push(&matcher.name);
        } else {
            labels.retain(|label| label.name != matcher.name);
        }
    }
    labels.retain(|label| !label.value.is_empty());
    MetricName::try_new(None, labels).map_err(external)
}

/// Orders the series of an instant result. A range result is returned unchanged: Prometheus
/// does not sort matrices.
pub fn sort_instant(
    order: &SortOrder,
    schema: &SeriesSchema,
    mut series: Vec<Series>,
) -> Result<Vec<Series>> {
    let points = grid_points(schema, &series)?;
    if points != 1 {
        return Ok(series);
    }
    match order {
        SortOrder::Asc | SortOrder::Desc => {
            single_field(schema, "sort")?;
            let mut keyed = Vec::with_capacity(series.len());
            for item in series {
                let value = value_at(&item, 0)?;
                keyed.push((value.unwrap_or(f64::NAN), item));
            }
            let descending = *order == SortOrder::Desc;
            // `sort.Sort(sort.Reverse(h))` with Prometheus' heap `Less`, which holds whenever
            // its first value is NaN.
            go_sort::sort_by(&mut keyed, |(a, _), (b, _)| {
                b.is_nan() || if descending { b < a } else { b > a }
            });
            Ok(keyed.into_iter().map(|(_, item)| item).collect())
        }
        SortOrder::ByLabel(labels) | SortOrder::ByLabelDesc(labels) => {
            let descending = matches!(order, SortOrder::ByLabelDesc(_));
            go_sort::sort_by(&mut series, |a, b| {
                for label in labels {
                    let (a, b) = (
                        a.metric.get(label).unwrap_or(""),
                        b.metric.get(label).unwrap_or(""),
                    );
                    if a == b {
                        continue;
                    }
                    return natural_less(a, b) != descending;
                }
                let order = compare_labels(&a.metric, &b.metric);
                if descending {
                    order == Ordering::Greater
                } else {
                    order == Ordering::Less
                }
            });
            Ok(series)
        }
    }
}

/// Go's `math.NaN()` bit pattern, which Prometheus returns for an undefined scalar.
const GO_NAN: f64 = f64::from_bits(0x7FF8_0000_0000_0001);

/// `true` when `a` ranks before `b` for `topk` (`top`) or `bottomk`; NaN ranks last for both.
fn rank(a: f64, b: f64, top: bool) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ if top => b.partial_cmp(&a).expect("not NaN"),
        _ => a.partial_cmp(&b).expect("not NaN"),
    }
}

/// Label pairs identifying a group; empty values are absent, as in Prometheus.
type GroupKey = Vec<(Arc<str>, Arc<str>)>;

fn group_key(metric: &MetricName, grouping: &Grouping) -> GroupKey {
    let mut key: Vec<_> = labels_with_name(metric)
        .into_iter()
        .filter(|(name, value)| {
            !value.is_empty()
                && match grouping {
                    Grouping::By(names) => names.contains(name),
                    Grouping::Without(names) => &**name != "__name__" && !names.contains(name),
                }
        })
        .collect();
    key.sort();
    key
}

/// The label pairs of `metric` with `__name__` among them, sorted by name.
fn labels_with_name(metric: &MetricName) -> Vec<(Arc<str>, Arc<str>)> {
    let mut labels: Vec<_> = metric
        .tags()
        .iter()
        .map(|tag| (tag.name.clone(), tag.value.clone()))
        .collect();
    if let Some(name) = metric.name() {
        labels.push((Arc::from("__name__"), Arc::from(name)));
    }
    labels.sort();
    labels
}

/// Prometheus `labels.Compare`: label pairs in name order, then the shorter set first.
fn compare_labels(a: &MetricName, b: &MetricName) -> Ordering {
    labels_with_name(a).cmp(&labels_with_name(b))
}

/// `github.com/facette/natsort.Compare`, which `sort_by_label` uses: digit runs compare as
/// integers when both parse, other runs as strings.
fn natural_less(a: &str, b: &str) -> bool {
    let (chunks_a, chunks_b) = (chunks(a), chunks(b));
    for (i, chunk_a) in chunks_a.iter().enumerate() {
        let Some(chunk_b) = chunks_b.get(i) else {
            return false;
        };
        let equal = match (chunk_a.parse::<i64>(), chunk_b.parse::<i64>()) {
            (Ok(a), Ok(b)) if a != b => return a < b,
            (Ok(_), Ok(_)) => true,
            _ => chunk_a == chunk_b,
        };
        if !equal {
            return chunk_a < chunk_b;
        }
        if i == chunks_a.len() - 1 {
            return true;
        } else if i == chunks_b.len() - 1 {
            return false;
        }
    }
    false
}

/// Maximal runs of ASCII digits and of other characters, like the regexp `(\d+|\D+)`.
fn chunks(s: &str) -> Vec<&str> {
    let mut chunks = Vec::new();
    let mut start = 0;
    let bytes = s.as_bytes();
    for i in 1..=bytes.len() {
        if i == bytes.len() || bytes[i].is_ascii_digit() != bytes[start].is_ascii_digit() {
            chunks.push(&s[start..i]);
            start = i;
        }
    }
    chunks
}

fn single_field(schema: &SeriesSchema, operator: &str) -> Result<()> {
    if schema.fields.len() != 1 {
        return Err(DataFusionError::NotImplemented(format!(
            "{operator} over series with {} fields",
            schema.fields.len()
        )));
    }
    Ok(())
}

/// Checks every series against `schema` and returns the grid length.
fn grid_points(schema: &SeriesSchema, series: &[Series]) -> Result<usize> {
    let grid = schema.eval_grid.as_ref().ok_or_else(|| {
        DataFusionError::Execution("select operators need an evaluation grid".into())
    })?;
    for item in series {
        match &item.time {
            TimeAxis::Grid(range)
                if range.grid().spec() == grid.spec()
                    && item.fields.len() == schema.fields.len() => {}
            _ => {
                return Err(DataFusionError::Execution(
                    "series does not match the select operator schema".into(),
                ));
            }
        }
    }
    Ok(grid.timestamps().len())
}

/// The first field's value of `series` at grid `point`.
fn value_at(series: &Series, point: usize) -> Result<Option<f64>> {
    let TimeAxis::Grid(range) = &series.time else {
        unreachable!("checked by grid_points");
    };
    if !(range.start()..range.start() + range.len()).contains(&point) {
        return Ok(None);
    }
    match series.fields.as_slice()[0].sample(point - range.start()) {
        SampleRef::Missing => Ok(None),
        SampleRef::Float(value) => Ok(Some(value)),
        SampleRef::Histogram(_) => Err(DataFusionError::NotImplemented(
            "select operators over histogram samples".into(),
        )),
    }
}
