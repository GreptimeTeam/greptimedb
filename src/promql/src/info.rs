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

//! `info(v, {selector})`: the series of `v` with the data labels of the info series that share
//! their identifying labels, following Prometheus `combineWithInfoSeries` and
//! `combineWithInfoVector`.
//!
//! Selecting the info series is the caller's: the series whose name and data labels match the
//! selector, evaluated at every point of the grid like an instant selector, with the value
//! replaced by the original timestamp of the selected sample, as Prometheus' `evalSeries` records
//! it for `info`. This kernel only combines.

#[cfg(test)]
mod tests;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};
use datatypes::series::{Label, SeriesLabels};
use regex::Regex;

use crate::grid_series::{
    GridSample, GridSeriesBuilder, LabelSet, external, label_set, owned, render_label_set,
};
use crate::select::MatchOp;
use crate::vector_match::{Budget, HISTOGRAM_ROW_BYTES, owned_histogram_bytes};

/// The labels an info series is joined on, as in Prometheus.
const IDENTIFYING_LABELS: [&str; 2] = ["instance", "job"];

/// The second argument of `info`, split the way Prometheus uses it.
#[derive(Clone, Debug)]
pub struct InfoSelector {
    /// The `__name__` matchers, which choose the info metrics; none means `target_info`.
    pub name_matchers: Vec<(MatchOp, Arc<str>)>,
    /// The names of the other matchers: only these labels are copied. None copies every label
    /// of the info series.
    pub data_labels: Vec<Arc<str>>,
    /// Whether every one of those other matchers matches the empty string, in which case a series
    /// without a matching info series is kept as it is.
    pub keep_unmatched: bool,
}

/// Combines `base` with the data labels of `info`.
///
/// `info` holds the info series the selector chooses, one float field each, whose value at a
/// point is the original timestamp of the sample selected there. For each info metric a base
/// series takes the info series with the same identifying labels. Several of them at one point are
/// compared in input order, as Prometheus compares them in the order its storage returns them: a
/// newer one replaces the one kept so far, and one exactly as new is an error. Labels
/// the base series already has are kept, and two info metrics giving one label different values
/// are an error. A base series without identifying labels matches no info series, since
/// Prometheus fetches none for it, and a base series that is itself an info metric is passed
/// through. Results that share a label set merge; a field held twice at one point is an error.
pub fn info(
    schema: &Arc<SeriesSchema>,
    base: &[Series],
    info: &[Series],
    selector: &InfoSelector,
) -> Result<Vec<Series>> {
    info_inner(schema, base, info, selector, None)
}

/// [`info()`], handing the live workspace it holds to `reserve` before it allocates it.
///
/// What the index of the info series and the rows of the results keep is held until the operation
/// returns; the merge of one label set's rows is a phase, and the builder's columns join the phase
/// of the output they are finished as. A total `reserve` refuses ends the operation with that error.
pub(crate) fn info_with_reservation(
    schema: &Arc<SeriesSchema>,
    base: &[Series],
    info: &[Series],
    selector: &InfoSelector,
    reserve: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<Vec<Series>> {
    let mut budget = Budget::new(reserve);
    info_inner(schema, base, info, selector, Some(&mut budget))
}

/// The one info kernel, whether or not a caller accounts for its workspace.
fn info_inner(
    schema: &Arc<SeriesSchema>,
    base: &[Series],
    info: &[Series],
    selector: &InfoSelector,
    mut budget: Option<&mut Budget<'_>>,
) -> Result<Vec<Series>> {
    let grid = schema
        .eval_grid
        .as_ref()
        .ok_or_else(|| DataFusionError::Execution("info requires an evaluated grid".into()))?;
    let points = grid.spec().len as usize;
    for series in base {
        if series.time.grid().map(|grid| grid.spec()) != Some(grid.spec())
            || series.fields.len() != schema.fields.len()
        {
            return Err(DataFusionError::Execution(
                "info base series does not match its schema".into(),
            ));
        }
    }
    for series in info {
        if series.time.grid().map(|grid| grid.spec()) != Some(grid.spec())
            || series.fields.len() != 1
        {
            return Err(DataFusionError::Execution(
                "info series must be single-field series on the base grid".into(),
            ));
        }
    }
    let name_matchers = effective_name_matchers(&selector.name_matchers)?;
    let is_info_metric = |name: &str| name_matchers.iter().all(|matcher| matcher.matches(name));
    let width = schema.fields.len();

    // The presence of the base at each point and the identifying values of every base series
    // stay live until the operation returns.
    if let Some(budget) = budget.as_deref_mut() {
        budget.hold(points.saturating_add(base.len().saturating_mul(PATTERN_BYTES)))?;
    }
    let mut base_present = vec![false; points];
    let mut sampled = vec![false; base.len()];
    for (series, sampled) in base.iter().zip(&mut sampled) {
        for field in series.fields.iter() {
            for ((point, _), sample) in series.time.positions().zip(field.iter()) {
                if !matches!(sample, SampleRef::Missing) {
                    base_present[point] = true;
                    *sampled = true;
                }
            }
        }
    }

    let mut error = FirstError::default();

    // Prometheus fetches info series per presence pattern of the identifying labels among the
    // base series it enriches: a present label matches any value the pattern's base series have,
    // an absent one matches only absence. An info series no pattern admits is never fetched, so
    // it neither enriches nor conflicts.
    // A base series without a sample is not in Prometheus' base matrix at all, so it selects no
    // info series.
    let mut patterns: HashMap<Vec<bool>, Vec<BTreeSet<Arc<str>>>> = HashMap::new();
    for (series, _) in base.iter().zip(&sampled).filter(|(_, sampled)| **sampled) {
        if is_info_metric(series.metric.name().unwrap_or("")) {
            continue;
        }
        let values = identifying_values(&series.metric);
        let presence: Vec<bool> = values.iter().map(Option::is_some).collect();
        if !presence.contains(&true) {
            continue;
        }
        let sets = patterns
            .entry(presence)
            .or_insert_with(|| vec![BTreeSet::new(); IDENTIFYING_LABELS.len()]);
        for (set, value) in sets.iter_mut().zip(values) {
            set.extend(value);
        }
    }

    // The info series kept for each signature at each point.
    let mut by_signature: HashMap<Signature, BTreeMap<usize, (f64, usize)>> = HashMap::new();
    let mut info_names: BTreeSet<Arc<str>> = BTreeSet::new();
    for (index, series) in info.iter().enumerate() {
        let values = identifying_values(&series.metric);
        let presence: Vec<bool> = values.iter().map(Option::is_some).collect();
        let fetched = patterns.get(&presence).is_some_and(|sets| {
            sets.iter()
                .zip(&values)
                .all(|(set, value)| value.as_ref().is_none_or(|value| set.contains(value)))
        });
        if !fetched {
            continue;
        }
        // The entry of a signature and the newest series it keeps at each point the series
        // holds a sample, charged before they are allocated.
        if let Some(budget) = budget.as_deref_mut() {
            budget.hold(
                series
                    .time
                    .len()
                    .saturating_mul(KEPT_POINT_BYTES)
                    .saturating_add(SIGNATURE_BYTES),
            )?;
        }
        let name: Arc<str> = Arc::from(series.metric.name().unwrap_or(""));
        info_names.insert(name.clone());
        let kept = by_signature.entry((name, values)).or_default();
        let mut cursor = series.fields.as_slice()[0].cursor();
        for (point, row) in series.time.positions() {
            let timestamp = match cursor.at(row).expect("the axis and the fields are aligned") {
                SampleRef::Missing => continue,
                SampleRef::Float(timestamp) => timestamp,
                SampleRef::Histogram(_) => {
                    return Err(DataFusionError::Internal(
                        "info series must hold the original sample timestamps as floats".into(),
                    ));
                }
            };
            match kept.entry(point) {
                Entry::Vacant(entry) => {
                    entry.insert((timestamp, index));
                }
                Entry::Occupied(mut entry) => {
                    let (existing, existing_index) = *entry.get();
                    if timestamp > existing {
                        entry.insert((timestamp, index));
                    } else if timestamp == existing && base_present[point] {
                        error.record(point, 0, || {
                            format!(
                                "found duplicate series for info metric: existing {} @ {}, new \
                                 {} @ {}",
                                render_label_set(&label_set(&info[existing_index].metric)),
                                existing as i64,
                                render_label_set(&label_set(&series.metric)),
                                timestamp as i64
                            )
                        });
                    }
                }
            }
        }
    }

    let mut result_index: HashMap<LabelSet, usize> = HashMap::new();
    let mut results: Vec<(SeriesLabels, Vec<Row>)> = Vec::new();
    let mut added: Vec<(Arc<str>, Arc<str>)> = Vec::new();
    let mut chosen: Vec<usize> = Vec::new();
    for series in base {
        let ignored = is_info_metric(series.metric.name().unwrap_or(""));
        // The signature of each info metric is fixed for a series, so its kept series are looked
        // up once; a point then only reads which of them is kept there.
        let candidates: Vec<&BTreeMap<usize, (f64, usize)>> = if ignored {
            Vec::new()
        } else {
            let values = identifying_values(&series.metric);
            info_names
                .iter()
                .filter_map(|name| by_signature.get(&(name.clone(), values.clone())))
                .collect()
        };
        // The result of the previous point, reused while the same info series are kept.
        let mut previous: Option<Choice> = None;
        // A row of this series is kept by the result it goes to, so every row it can have is held
        // before the walk; a histogram is held as it is copied.
        if let Some(budget) = budget.as_deref_mut() {
            budget.hold(series.time.len().saturating_mul(row_bytes(width)))?;
        }
        let mut cursors: Vec<_> = series.fields.iter().map(|field| field.cursor()).collect();
        for (point, row) in series.time.positions() {
            let mut values = Vec::with_capacity(width);
            for cursor in &mut cursors {
                let sample = cursor.at(row).expect("the axis and the fields are aligned");
                if let (Some(budget), SampleRef::Histogram(view)) = (budget.as_deref_mut(), &sample)
                {
                    budget.hold(owned_histogram_bytes(view))?;
                }
                values.push(owned(sample)?);
            }
            if values.iter().all(Option::is_none) {
                continue;
            }
            chosen.clear();
            chosen.extend(
                candidates
                    .iter()
                    .filter_map(|kept| kept.get(&point).map(|&(_, index)| index)),
            );
            let reuse = previous
                .as_ref()
                .is_some_and(|previous| previous.info == chosen);
            if !reuse {
                added.clear();
                let mut conflict = None;
                for &index in &chosen {
                    if let Err(label) =
                        add_data_labels(&info[index].metric, &series.metric, selector, &mut added)
                    {
                        conflict = Some(label);
                        break;
                    }
                }
                let result = if chosen.is_empty() && !selector.keep_unmatched && !ignored {
                    None
                } else {
                    let mut metric = series.metric.clone();
                    for (name, value) in &added {
                        metric.set(name, value.clone()).map_err(external)?;
                    }
                    Some(match result_index.entry(label_set(&metric)) {
                        std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            if let Some(budget) = budget.as_deref_mut() {
                                budget.hold(result_set_bytes(&metric))?;
                            }
                            results.push((metric, Vec::new()));
                            *entry.insert(results.len() - 1)
                        }
                    })
                };
                previous = Some(Choice {
                    info: chosen.clone(),
                    result,
                    conflict,
                });
            }
            let choice = previous.as_ref().expect("set above");
            if let Some(label) = &choice.conflict {
                error.record(point, 1, || format!("conflicting label: {label}"));
            }
            if let Some(result) = choice.result {
                results[result].1.push((point, values));
            }
        }
    }

    // Rows of one label set arrive per base series; sorted by point, a field held twice at one
    // point is a collision.
    for (metric, rows) in &mut results {
        // The sort's buffer and the merged rows, while the rows they replace are still alive.
        if let Some(budget) = budget.as_deref_mut() {
            budget.phase(rows.len().saturating_mul(2 * std::mem::size_of::<Row>()))?;
        }
        rows.sort_by_key(|(point, _)| *point);
        let mut merged: Vec<Row> = Vec::with_capacity(rows.len());
        for (point, values) in rows.drain(..) {
            match merged.last_mut() {
                Some((last, slots)) if *last == point => {
                    for (slot, value) in slots.iter_mut().zip(values) {
                        if value.is_none() {
                            continue;
                        }
                        if slot.is_some() {
                            error.record(point, 2, || {
                                format!(
                                    "vector cannot contain metrics with the same labelset {}",
                                    render_label_set(&label_set(metric))
                                )
                            });
                        }
                        *slot = value;
                    }
                }
                _ => merged.push((point, values)),
            }
        }
        *rows = merged;
    }
    if let Some((_, _, message)) = error.0 {
        return Err(DataFusionError::Execution(message));
    }

    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut output = Vec::with_capacity(results.len());
    if let Some(budget) = budget.as_deref_mut() {
        budget.phase(0)?;
    }
    for (metric, rows) in results {
        builder.begin_series();
        // This series' columns, reserved before its first row. They stay live inside the series
        // it is finished as, so the phase keeps them.
        if let Some(budget) = budget.as_deref_mut() {
            budget.grow(column_bytes(&rows, width))?;
        }
        builder.reserve_rows(rows.len())?;
        for (point, row) in rows {
            builder.push_samples(point, &row)?;
        }
        output.extend(builder.finish(metric)?);
    }
    Ok(output)
}

/// The samples of one result series at one grid point, one per field.
type Row = (usize, Vec<Option<GridSample>>);

/// The identifying values one base series adds to its presence pattern, charged twice for the
/// spare room of the sets they go to.
const PATTERN_BYTES: usize = 2 * IDENTIFYING_LABELS.len() * std::mem::size_of::<Arc<str>>();

/// The entry of one info series' signature: its key, its map of kept series, and the name the
/// info metric list keeps, charged twice for the spare room of the tables.
const SIGNATURE_BYTES: usize = 2
    * (std::mem::size_of::<(Signature, BTreeMap<usize, (f64, usize)>)>()
        + IDENTIFYING_LABELS.len() * std::mem::size_of::<Option<Arc<str>>>()
        + std::mem::size_of::<Arc<str>>());

/// The series one signature keeps at one point, charged twice for the B-tree's spare room.
const KEPT_POINT_BYTES: usize = 2 * std::mem::size_of::<(usize, (f64, usize))>();

/// One row a result keeps: its entry in the result's row list, charged twice for the list's
/// growth, and its samples, allocated at their exact count.
fn row_bytes(width: usize) -> usize {
    (2 * std::mem::size_of::<Row>())
        .saturating_add(width.saturating_mul(std::mem::size_of::<Option<GridSample>>()))
}

/// The bytes a result label set keeps: its metric, rebuilt by each label it is given, the label set
/// it is filed under with the copy of the name that set makes, and its entries in the result list
/// and index.
fn result_set_bytes(metric: &SeriesLabels) -> usize {
    let labels = metric.tags().len().saturating_add(1);
    labels
        .saturating_mul(
            2 * std::mem::size_of::<Label>() + std::mem::size_of::<(Arc<str>, Arc<str>)>(),
        )
        .saturating_add(metric.name().map_or(0, str::len))
        .saturating_add(
            metric
                .tags()
                .iter()
                .map(|tag| tag.name.len())
                .sum::<usize>(),
        )
        .saturating_add(2 * std::mem::size_of::<(SeriesLabels, Vec<Row>)>())
        .saturating_add(2 * std::mem::size_of::<(LabelSet, usize)>())
}

/// The bytes one result series' columns take: per row, the grid index, the value and validity
/// column of every field, charged twice for the capacity each grows to, and a histogram index; per
/// histogram, a row of the builder's histogram table and the buckets copied into it.
fn column_bytes(rows: &[Row], width: usize) -> usize {
    let row = std::mem::size_of::<usize>()
        + 2 * width.saturating_mul(std::mem::size_of::<f64>() + 1)
        + width.saturating_mul(std::mem::size_of::<Option<u32>>());
    rows.iter().flat_map(|(_, values)| values).fold(
        rows.len().saturating_mul(row),
        |bytes, value| match value {
            Some(GridSample::Histogram(histogram)) => bytes
                .saturating_add(HISTOGRAM_ROW_BYTES)
                .saturating_add(histogram.heap_bytes()),
            _ => bytes,
        },
    )
}

/// What the info series kept at a point make of one base series.
struct Choice {
    /// The info series, one per info metric that has one.
    info: Vec<usize>,
    /// The result series the point goes to, `None` when the point is dropped.
    result: Option<usize>,
    /// A label two of the info series set to different values.
    conflict: Option<Arc<str>>,
}

/// The error Prometheus reports: the one of the first failing step, and within a step the first
/// of its checks (the info series, then each base series, then the result label sets).
#[derive(Default)]
struct FirstError(Option<(usize, u8, String)>);

impl FirstError {
    /// Records an error of check `rank` at `point`, formatting it only if it comes first.
    fn record(&mut self, point: usize, rank: u8, message: impl FnOnce() -> String) {
        if self
            .0
            .as_ref()
            .is_none_or(|(first, first_rank, _)| (point, rank) < (*first, *first_rank))
        {
            self.0 = Some((point, rank, message()));
        }
    }
}

/// An info metric name and the identifying label values present on a series.
type Signature = (Arc<str>, Vec<Option<Arc<str>>>);

/// The value of each identifying label, `None` where the series does not have it.
fn identifying_values(metric: &SeriesLabels) -> Vec<Option<Arc<str>>> {
    IDENTIFYING_LABELS
        .iter()
        .map(|label| {
            metric
                .get(label)
                .filter(|value| !value.is_empty())
                .map(Arc::from)
        })
        .collect()
}

/// Adds the data labels `info` gives a series with labels `base` to `added`; returns the name of
/// a label another info metric already set to a different value.
fn add_data_labels(
    info: &SeriesLabels,
    base: &SeriesLabels,
    selector: &InfoSelector,
    added: &mut Vec<(Arc<str>, Arc<str>)>,
) -> std::result::Result<(), Arc<str>> {
    for tag in info.tags() {
        if tag.value.is_empty()
            || (!selector.data_labels.is_empty() && !selector.data_labels.contains(&tag.name))
        {
            continue;
        }
        if let Some((_, value)) = added.iter().find(|(name, _)| *name == tag.name) {
            if *value != tag.value {
                return Err(tag.name.clone());
            }
            continue;
        }
        if base.get(&tag.name).is_some_and(|value| !value.is_empty()) {
            continue;
        }
        added.push((tag.name.clone(), tag.value.clone()));
    }
    Ok(())
}

struct NameMatcher {
    op: MatchOp,
    value: Arc<str>,
    regex: Option<Regex>,
}

impl NameMatcher {
    fn try_new(op: MatchOp, value: Arc<str>) -> Result<Self> {
        let regex = match op {
            MatchOp::Regex | MatchOp::NotRegex => {
                Some(Regex::new(&format!("^(?s:{value})$")).map_err(|error| {
                    DataFusionError::Plan(format!("invalid regular expression {value}: {error}"))
                })?)
            }
            MatchOp::Equal | MatchOp::NotEqual => None,
        };
        Ok(Self { op, value, regex })
    }

    fn matches(&self, name: &str) -> bool {
        match (self.op, &self.regex) {
            (MatchOp::Equal, _) => name == &*self.value,
            (MatchOp::NotEqual, _) => name != &*self.value,
            (MatchOp::Regex, Some(regex)) => regex.is_match(name),
            (MatchOp::NotRegex, Some(regex)) => !regex.is_match(name),
            _ => unreachable!("regex matchers compile their regex"),
        }
    }
}

/// Prometheus `effectiveInfoNameMatchers`: the name matchers as given when one of them selects,
/// prefixed with `.+_info` when all of them only exclude, and `target_info` when there are none.
fn effective_name_matchers(matchers: &[(MatchOp, Arc<str>)]) -> Result<Vec<NameMatcher>> {
    let mut effective = Vec::with_capacity(matchers.len() + 1);
    if matchers.is_empty() {
        effective.push(NameMatcher::try_new(
            MatchOp::Equal,
            Arc::from("target_info"),
        )?);
    } else if matchers
        .iter()
        .all(|(op, _)| matches!(op, MatchOp::NotEqual | MatchOp::NotRegex))
    {
        effective.push(NameMatcher::try_new(MatchOp::Regex, Arc::from(".+_info"))?);
    }
    for (op, value) in matchers {
        effective.push(NameMatcher::try_new(*op, value.clone())?);
    }
    Ok(effective)
}
