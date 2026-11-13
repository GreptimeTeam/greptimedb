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
//! `bottomk`, `limitk`, `limit_ratio`, `scalar`, `absent` and the instant-result sorts.
//!
//! Semantics follow Prometheus (`aggregationK`, `HashRatioSampler`, `funcScalar`, `funcAbsent`,
//! `createLabelsForAbsentFunction`, `funcSort*`). A scalar is a grid series without labels that
//! has a value at every point, so `vector(s)` is `s` itself and has no kernel.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{RowCursor, SampleRef, Series, SeriesSchema};
use datatypes::series::time::union_positions;
use datatypes::series::{Label, SeriesLabels};

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
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
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
/// both; the selected samples keep their series' labels and values. Histogram samples are not
/// ranked, as Prometheus ignores them here. Prometheus breaks ties by
/// the order its storage returns series in, which is not a property of the series; here ties
/// go to the series whose label set sorts first.
pub fn topk(
    k: f64,
    top: bool,
    grouping: &Grouping,
    schema: &SeriesSchema,
    series: &[Series],
) -> Result<Vec<Series>> {
    let k = k_param(k, series.len())?;
    topk_at(|_| Ok(k), top, grouping, schema, series)
}

/// `topk`/`bottomk` with `k` evaluated independently at each grid point.
pub fn topk_at(
    k_at: impl Fn(usize) -> Result<Option<usize>>,
    top: bool,
    grouping: &Grouping,
    schema: &SeriesSchema,
    series: &[Series],
) -> Result<Vec<Series>> {
    single_field(schema, "topk/bottomk")?;
    // Validates the series against the schema and its grid; the walk below visits the carried
    // positions, not the whole grid.
    grid_points(schema, series)?;
    let mut order: Vec<usize> = (0..series.len()).collect();
    // The label pairs of a series are built once: a comparator that compared two metrics would
    // rebuild both lists per comparison, and the groups read the same lists again.
    let mut label_sets: Vec<Vec<(Arc<str>, Arc<str>)>> = series
        .iter()
        .map(|item| labels_with_name(&item.metric))
        .collect();
    order.sort_by(|&a, &b| label_sets[a].cmp(&label_sets[b]));
    let mut groups: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    for index in order {
        groups
            .entry(group_key_from(&label_sets[index], grouping))
            .or_default()
            .push(index);
    }
    // The groups hold what they need, so the label lists are freed before the point walk.
    label_sets.clear();

    let mut selected: Vec<Vec<(usize, usize)>> = vec![Vec::new(); series.len()];
    let mut candidates: Vec<(usize, usize, f64)> = Vec::new();
    // The values of one point, for the partial selection below: ranking them there leaves the
    // candidates in member order, which is what hands ties to the earlier label set.
    let mut values: Vec<f64> = Vec::new();
    for members in groups.values() {
        // A point no member carries holds no candidate, so only the union of the members'
        // positions is visited: a hole in the grid never reaches the ranking. Each member reads
        // through one forward cursor, whose rows only increase as the union walk advances.
        let mut cursors: Vec<RowCursor<'_>> = members
            .iter()
            .map(|&member| series[member].fields.as_slice()[0].cursor())
            .collect();
        for point in union_positions(members.iter().map(|&member| &series[member].time)) {
            candidates.clear();
            for (slot, &member) in members.iter().enumerate() {
                let Some(row) = series[member].time.row_of(point) else {
                    continue;
                };
                if let SampleRef::Float(value) = cursors[slot]
                    .at(row)
                    .expect("the axis and the fields are aligned")
                {
                    candidates.push((member, row, value));
                }
            }
            let Some(k) = k_at(point)? else {
                continue;
            };
            // Which candidates win is the result; the order they were found in is not. The ranking
            // is a partial selection of the k best values rather than an ordering of the whole
            // group, and it runs on a copy of the values so that the candidates keep the member
            // order the ties are resolved in.
            let k = k.min(candidates.len());
            if k == 0 {
                continue;
            }
            let boundary = (k < candidates.len()).then(|| {
                values.clear();
                values.extend(candidates.iter().map(|candidate| candidate.2));
                values.select_nth_unstable_by(k, |a, b| rank(*a, *b, top));
                values[k]
            });
            // The partition splits the run of values equal to its boundary, while ties go to the
            // earlier label set: the ones equal to the boundary inside the front are the slots left
            // for them, and the scan below fills those slots in member order, which is label order.
            let mut tied = match boundary {
                Some(boundary) => values[..k]
                    .iter()
                    .filter(|&&value| rank(value, boundary, top) == Ordering::Equal)
                    .count(),
                None => 0,
            };
            for &(member, row, value) in &candidates {
                if let Some(boundary) = boundary {
                    match rank(value, boundary, top) {
                        Ordering::Less => {}
                        Ordering::Equal if tied > 0 => tied -= 1,
                        _ => continue,
                    }
                }
                selected[member].push((point, row));
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
        // One builder serves every result series, so each one starts explicitly: only
        // `begin_series` resets the positions it accepts.
        builder.begin_series();
        // The selected positions of one series only increase, so one cursor reads their rows.
        let mut cursor = series[index].fields.as_slice()[0].cursor();
        for (point, row) in sorted {
            builder.push(point, &[cursor_value(&mut cursor, row)?])?;
        }
        result.extend(builder.finish(series[index].metric.clone())?);
    }
    Ok(result)
}

/// `limitk(k, ...)` with a constant `k`.
///
/// At every point each group keeps the first `k` series in input order that hold a sample there,
/// float or histogram, with their labels and values. Prometheus takes them in the order its
/// storage returns series: label order once a query reads several queriers (the head and a
/// block), creation order when the TSDB head alone answers it. A caller that wants Prometheus'
/// result over persisted data passes the series in label order.
pub fn limitk(
    k: f64,
    grouping: &Grouping,
    schema: &SeriesSchema,
    series: &[Series],
) -> Result<Vec<Series>> {
    single_field(schema, "limitk")?;
    grid_points(schema, series)?;
    let Some(k) = k_param(k, series.len())? else {
        return Ok(Vec::new());
    };
    let mut groups: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    for (index, item) in series.iter().enumerate() {
        groups
            .entry(group_key(&item.metric, grouping))
            .or_default()
            .push(index);
    }
    let mut selected: Vec<Vec<(usize, usize)>> = vec![Vec::new(); series.len()];
    for members in groups.values() {
        let mut cursors: Vec<RowCursor<'_>> = members
            .iter()
            .map(|&member| series[member].fields.as_slice()[0].cursor())
            .collect();
        for point in union_positions(members.iter().map(|&member| &series[member].time)) {
            let mut taken = 0;
            for (slot, &member) in members.iter().enumerate() {
                if taken == k {
                    break;
                }
                let Some(row) = series[member].time.row_of(point) else {
                    continue;
                };
                if !matches!(
                    cursors[slot]
                        .at(row)
                        .expect("the axis and the fields are aligned"),
                    SampleRef::Missing
                ) {
                    selected[member].push((point, row));
                    taken += 1;
                }
            }
        }
    }

    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone()))?;
    let mut result = Vec::new();
    for (index, points) in selected.into_iter().enumerate() {
        if points.is_empty() {
            continue;
        }
        builder.begin_series();
        // A series belongs to one group, whose points are visited in order.
        let mut cursor = series[index].fields.as_slice()[0].cursor();
        for (point, row) in points {
            // The sample is moved, not computed, so it goes straight into the result.
            let sample = cursor.at(row).expect("the axis and the fields are aligned");
            builder.push_refs(point, &[sample])?;
        }
        result.extend(builder.finish(series[index].metric.clone())?);
    }
    Ok(result)
}

/// `limit_ratio(r, ...)` with a constant `r`.
///
/// Keeps whole series: Prometheus derives an offset in `[0, 1)` from the hash of a series' labels
/// and keeps the series when the offset is below `r`, or, for a negative `r`, at least `1 + r`.
/// Which series are kept does not depend on the grouping, so it is not a parameter. The hash is
/// the one of Prometheus' default `stringlabels` build; the other label builds hash differently.
pub fn limit_ratio(ratio: f64, schema: &SeriesSchema, series: &[Series]) -> Result<Vec<Series>> {
    grid_points(schema, series)?;
    if ratio.is_nan() {
        return Err(DataFusionError::Execution("Ratio value is NaN".into()));
    }
    let ratio = ratio.clamp(-1.0, 1.0);
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone()))?;
    let mut result = Vec::new();
    let mut row: Vec<SampleRef<'_>> = Vec::with_capacity(schema.fields.len());
    for item in series {
        let offset = sample_offset(&item.metric);
        let kept = if ratio >= 0.0 {
            offset < ratio
        } else {
            offset >= 1.0 + ratio
        };
        if !kept {
            continue;
        }
        // Rebuilt rather than cloned: an input may carry rows without a value, a result may not.
        builder.begin_series();
        let mut cursors: Vec<RowCursor<'_>> =
            item.fields.iter().map(|field| field.cursor()).collect();
        for (point, index) in item.time.positions() {
            row.clear();
            // The samples are moved, not computed, so the row is read and pushed as it stands.
            for cursor in &mut cursors {
                row.push(
                    cursor
                        .at(index)
                        .expect("the axis and the fields are aligned"),
                );
            }
            builder.push_refs(point, &row)?;
        }
        result.extend(builder.finish(item.metric.clone())?);
    }
    Ok(result)
}

/// Prometheus `HashRatioSampler.SampleOffset`: the xxHash64 of the labels as `stringlabels`
/// encodes them, each name and value prefixed by its length, over `u64::MAX`.
fn sample_offset(metric: &SeriesLabels) -> f64 {
    let mut data = Vec::new();
    for (name, value) in labels_with_name(metric) {
        // Prometheus label sets hold no empty value.
        if value.is_empty() {
            continue;
        }
        for text in [&name, &value] {
            let len = text.len();
            if len < 255 {
                data.push(len as u8);
            } else {
                data.extend([255, len as u8, (len >> 8) as u8, (len >> 16) as u8]);
            }
            data.extend_from_slice(text.as_bytes());
        }
    }
    twox_hash::XxHash64::oneshot(0, &data) as f64 / u64::MAX as f64
}

/// `scalar(v)`: the value of the only series present at each point, NaN otherwise.
pub fn scalar(schema: &SeriesSchema, series: &[Series]) -> Result<Series> {
    single_field(schema, "scalar")?;
    let points = grid_points(schema, series)?;
    let mut values: Vec<Option<f64>> = vec![None; points];
    let mut counts = vec![0u8; points];
    for item in series {
        // Walk the rows this series carries: a sparse axis must not be read as its envelope, and
        // a forward cursor keeps the read proportional to them.
        let mut field = item.fields.as_slice()[0].cursor();
        for (point, row) in item.time.positions() {
            if let Some(value) = cursor_value(&mut field, row)? {
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
    let empty = SeriesLabels::try_new(None, Vec::new()).map_err(external)?;
    // Every grid point is written above, with a value or a NaN, so the result is one run over the
    // grid and never empty. A different shape would mean this invariant broke, and returning it
    // silently would hide it.
    let series = builder
        .finish(empty)?
        .ok_or_else(|| DataFusionError::Execution("scalar holds no point of the grid".into()))?;
    if !series.time.is_contiguous() {
        return Err(DataFusionError::Execution(
            "scalar produced several runs, expected exactly one over the grid".into(),
        ));
    }
    Ok(series)
}

/// `absent(v)`: 1 with `labels` at every point where no series is present.
///
/// `absent_over_time(v[r])` is this over the result of `present_over_time(v[r])`. When `v[r]` is
/// a range selector, `labels` come from its matchers as for `absent(v)`; for a subquery
/// Prometheus derives no labels.
///
/// All of it is one label set, so the result is one series. A point where one field is present and
/// another is absent keeps its row, with a null in the field that is present; a point where every
/// field is present contributes no row, and the axis names the runs of points that do. `None` means
/// no point is absent in any field, so no row survives.
pub fn absent(
    schema: &SeriesSchema,
    series: &[Series],
    labels: SeriesLabels,
) -> Result<Option<Series>> {
    let points = grid_points(schema, series)?;
    let fields = schema.fields.len();
    // Each field is absent on its own.
    let mut present = vec![false; points * fields];
    for item in series {
        for (field, samples) in item.fields.iter().enumerate() {
            // One cursor per field, driven by the positions the axis carries.
            let mut cursor = samples.cursor();
            for (point, row) in item.time.positions() {
                if !matches!(
                    cursor.at(row).expect("the axis and the fields are aligned"),
                    SampleRef::Missing
                ) {
                    present[point * fields + field] = true;
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize, Hash)]
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
pub fn absent_labels(matchers: &[LabelMatcher]) -> Result<SeriesLabels> {
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
    SeriesLabels::try_new(None, labels).map_err(external)
}

/// Orders the series of an instant result. A range result keeps its order: Prometheus does not
/// sort matrices.
///
/// `sort` and `sort_desc` drop histogram samples, in a range result too: Prometheus filters them
/// out at every step (`filterFloats`) before it sorts. The label sorts keep every sample.
pub fn sort_instant(
    order: &SortOrder,
    schema: &SeriesSchema,
    mut series: Vec<Series>,
) -> Result<Vec<Series>> {
    let points = grid_points(schema, &series)?;
    if matches!(order, SortOrder::Asc | SortOrder::Desc) {
        single_field(schema, "sort")?;
        let mut floats = Vec::with_capacity(series.len());
        for item in series {
            floats.extend(float_samples(schema, item)?);
        }
        series = floats;
    }
    if points != 1 {
        return Ok(series);
    }
    match order {
        SortOrder::Asc | SortOrder::Desc => {
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

/// `series` without its histogram samples, `None` when nothing else is left. One field, as `sort`
/// reads.
fn float_samples(schema: &SeriesSchema, series: Series) -> Result<Option<Series>> {
    let field = &series.fields.as_slice()[0];
    if !field
        .iter()
        .any(|sample| matches!(sample, SampleRef::Histogram(_)))
    {
        return Ok(Some(series));
    }
    let mut builder = GridSeriesBuilder::try_new(Arc::new(schema.clone()))?;
    let mut cursor = field.cursor();
    for (point, row) in series.time.positions() {
        let value = match cursor.at(row).expect("the axis and the fields are aligned") {
            SampleRef::Float(value) => Some(value),
            SampleRef::Missing | SampleRef::Histogram(_) => None,
        };
        builder.push(point, &[value])?;
    }
    builder.finish(series.metric)
}

/// Go's `math.NaN()` bit pattern, which Prometheus returns for an undefined scalar.
const GO_NAN: f64 = f64::from_bits(0x7FF8_0000_0000_0001);

/// The order of `a` and `b` for `topk` (`top`) or `bottomk`; NaN ranks last for both.
fn rank(a: f64, b: f64, top: bool) -> Ordering {
    #[cfg(test)]
    calls::record_rank();
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ if top => b.partial_cmp(&a).expect("not NaN"),
        _ => a.partial_cmp(&b).expect("not NaN"),
    }
}

/// The `k` of `topk`, `bottomk` and `limitk` as Prometheus reads it: nothing for `k` below 1, an
/// error for NaN or a value out of int64, otherwise `k` truncated and capped at the number of
/// input series.
pub(crate) fn k_param(k: f64, series: usize) -> Result<Option<usize>> {
    if k < 1.0 {
        return Ok(None);
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
    Ok(Some(k.trunc().min(series as f64) as usize))
}

/// Validates the complete parameter series for Prometheus topk/bottomk/limitk semantics.
///
/// The values are read where the node holds them, in one pass. The caller keeps both the series the
/// parameter arrived as and the values it was read into, so a second copy of the present ones would
/// be memory neither of them is charged for.
pub(crate) fn validate_topk_parameters(values: &[Option<f64>]) -> Result<()> {
    // A position the series decided nothing at is no parameter value, so only the present ones are
    // read. NaN is tracked rather than folded into the bounds: it is a value Prometheus refuses
    // outright, not the largest one, and it is refused even where every value is below 1.
    let (mut max, mut min, mut nan) = (f64::NEG_INFINITY, f64::INFINITY, false);
    for value in values.iter().flatten() {
        if value.is_nan() {
            nan = true;
        } else {
            max = max.max(*value);
            min = min.min(*value);
        }
    }
    if nan {
        return Err(DataFusionError::Execution("Parameter value is NaN".into()));
    }
    // A `k` below 1 selects nothing, so a value that would not fit an int64 is never read.
    if max < 1.0 {
        return Ok(());
    }
    if min <= -9_223_372_036_854_775_808.0 {
        return Err(DataFusionError::Execution(format!(
            "Scalar value {min} underflows int64"
        )));
    }
    if max >= 9_223_372_036_854_774_784.0 {
        return Err(DataFusionError::Execution(format!(
            "Scalar value {max} overflows int64"
        )));
    }
    Ok(())
}

/// Label pairs identifying a group; empty values are absent, as in Prometheus.
type GroupKey = Vec<(Arc<str>, Arc<str>)>;

fn group_key(metric: &SeriesLabels, grouping: &Grouping) -> GroupKey {
    group_key_from(&labels_with_name(metric), grouping)
}

/// The group `labels` belong to, for callers that already hold the series' label pairs.
fn group_key_from(labels: &[(Arc<str>, Arc<str>)], grouping: &Grouping) -> GroupKey {
    let mut key: Vec<_> = labels
        .iter()
        .filter(|(name, value)| {
            !value.is_empty()
                && match grouping {
                    Grouping::By(names) => names.contains(name),
                    Grouping::Without(names) => &**name != "__name__" && !names.contains(name),
                }
        })
        .cloned()
        .collect();
    key.sort();
    key
}

/// The label pairs of `metric` with `__name__` among them, sorted by name.
fn labels_with_name(metric: &SeriesLabels) -> Vec<(Arc<str>, Arc<str>)> {
    #[cfg(test)]
    calls::record_label_list();
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
fn compare_labels(a: &SeriesLabels, b: &SeriesLabels) -> Ordering {
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
        // Both grid axes are addressed on a grid; what matters is that it is this schema's.
        let matches = match item.time.grid() {
            Some(item_grid) => {
                item_grid.spec() == grid.spec() && item.fields.len() == schema.fields.len()
            }
            None => false,
        };
        if !matches {
            return Err(DataFusionError::Execution(
                "series does not match the select operator schema".into(),
            ));
        }
    }
    Ok(grid.spec().len as usize)
}

/// The first field's value of `series` at grid `point`, looked up by position.
///
/// A point read: its only caller, `sort_instant`, reads one point of each instant series, so there
/// is no walk for a cursor to follow. The row of a position is searched for, not counted from the
/// first one, and a position the axis does not carry answers `None` rather than a row read from
/// another offset.
fn value_at(series: &Series, point: usize) -> Result<Option<f64>> {
    let Some(row) = series.time.row_of(point) else {
        return Ok(None);
    };
    // `Samples::sample` walks the chunk descriptors, not the rows, which is what a random read
    // costs; a cursor would have to replay every row before this one.
    into_value(series.fields.as_slice()[0].sample(row))
}

/// A sample as a value, rejecting histograms as the select operators do.
fn into_value(sample: SampleRef<'_>) -> Result<Option<f64>> {
    match sample {
        SampleRef::Missing => Ok(None),
        SampleRef::Float(value) => Ok(Some(value)),
        SampleRef::Histogram(_) => Err(DataFusionError::NotImplemented(
            "select operators over histogram samples".into(),
        )),
    }
}

/// The value a forward cursor reads at compact `row`; the rows asked for only increase.
fn cursor_value(cursor: &mut RowCursor<'_>, row: usize) -> Result<Option<f64>> {
    into_value(cursor.at(row).expect("the axis and the fields are aligned"))
}

/// How often this thread built a series' label list or compared two candidates, so a test can hold
/// `topk` to the work it should not do rather than to the result, which every shape produces.
/// Test-only: a decorated sort and a partial selection select the same candidates.
#[cfg(test)]
pub(crate) mod calls {
    use std::cell::Cell;

    thread_local! {
        /// Label lists [`super::labels_with_name`] built on this thread.
        static LABEL_LISTS: Cell<usize> = const { Cell::new(0) };
        /// Candidate pairs [`super::rank`] looked at on this thread.
        static RANKS: Cell<usize> = const { Cell::new(0) };
    }

    /// Records one label list.
    pub(crate) fn record_label_list() {
        LABEL_LISTS.with(|lists| lists.set(lists.get() + 1));
    }

    /// Records one comparison of two candidates.
    pub(crate) fn record_rank() {
        RANKS.with(|ranks| ranks.set(ranks.get() + 1));
    }

    /// The label lists built on this thread since the last call, which starts a new count.
    pub(crate) fn take_label_lists() -> usize {
        LABEL_LISTS.with(|lists| lists.replace(0))
    }

    /// The candidate comparisons made on this thread since the last call, which starts a new count.
    pub(crate) fn take_ranks() -> usize {
        RANKS.with(|ranks| ranks.replace(0))
    }
}
