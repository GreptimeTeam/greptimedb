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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::{
    Array, AsArray, Float64Array, ListArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::BooleanBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::cmp::distinct;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
use datatypes::arrow_array::string_array_value_at_index;
use futures::{StreamExt, TryStreamExt, stream};
use promql_parser::parser::EvalStmt;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{Expr, parse};

use crate::research_series_grid::evaluate::SelectorKey;
use crate::research_series_grid::evaluate::VectorResult;
use crate::research_series_grid::grid::TimeGrid;
use crate::research_series_grid::online_execution::OnlineQuery;
use crate::research_series_grid::online_input::selector_requests;
use crate::research_series_grid::online_plan::OnlineNode;
use crate::research_series_grid::online_scan::CollectedSeries;
use crate::research_series_grid::plan::millis;
use crate::research_series_grid::vector::{Labels, SeriesVector};

pub(super) mod join_sum;

pub(crate) const PACKED_POINTS: &str = "__research_series_points";

#[derive(Debug, Clone)]
pub(crate) struct FragmentInput {
    pub(crate) key: SelectorKey,
    pub(crate) time: String,
    pub(crate) value: String,
    pub(crate) tags: Vec<String>,
    pub(crate) metric: Option<String>,
    pub(crate) packed: bool,
    pub(crate) dense: Option<bool>,
    pub(crate) partial_max: bool,
}

impl FragmentInput {
    pub(crate) fn collect_series(
        &self,
        series: Vec<crate::research_series_grid::scan_range::SeriesGrid>,
        grid: &TimeGrid,
        cancelled: &AtomicBool,
        memory: &datafusion::execution::memory_pool::MemoryReservation,
        rows: &mut BTreeMap<Labels, Vec<Option<f64>>>,
    ) -> Result<()> {
        use datafusion_common::ScalarValue;

        for series in series {
            if cancelled.load(Ordering::Relaxed) {
                return Err(DataFusionError::Execution(
                    "Fragment query cancelled".into(),
                ));
            }
            if series.labels.len() != self.tags.len() || series.values.len() != grid.instants.len()
            {
                return Err(DataFusionError::Execution(
                    "Native fragment label or grid length mismatch".into(),
                ));
            }
            if series.values.iter().all(Option::is_none) {
                continue;
            }
            let mut labels = BTreeMap::new();
            for (name, mut value) in self.tags.iter().zip(series.labels) {
                if let ScalarValue::Dictionary(_, inner) = value {
                    value = *inner;
                }
                let value = match value {
                    ScalarValue::Utf8(value)
                    | ScalarValue::LargeUtf8(value)
                    | ScalarValue::Utf8View(value) => value,
                    _ => {
                        return Err(DataFusionError::Execution(
                            "Native fragment label type mismatch".into(),
                        ));
                    }
                };
                if let Some(value) = value {
                    labels.insert(name.clone(), value);
                }
            }
            if let Some(metric) = &self.metric {
                labels.insert("__name__".into(), metric.clone());
            }
            match rows.entry(labels) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let bytes = series
                        .values
                        .capacity()
                        .checked_mul(size_of::<Option<f64>>())
                        .and_then(|n| n.checked_add(256))
                        .and_then(|n| {
                            entry.key().iter().try_fold(n, |n, (name, value)| {
                                n.checked_add(name.capacity())?
                                    .checked_add(value.capacity())?
                                    .checked_add(256)
                            })
                        })
                        .ok_or_else(|| {
                            DataFusionError::Execution("Native fragment size overflow".into())
                        })?;
                    memory.try_grow(bytes)?;
                    entry.insert(series.values);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    for (index, (output, value)) in
                        entry.get_mut().iter_mut().zip(series.values).enumerate()
                    {
                        if index % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                            return Err(DataFusionError::Execution(
                                "Fragment query cancelled".into(),
                            ));
                        }
                        if let Some(value) = value {
                            self.merge_point(output, value)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn merge_point(&self, output: &mut Option<f64>, value: f64) -> Result<()> {
        if self.dense == Some(true) {
            *output = Some(output.map_or(value, |partial| partial + value));
        } else if self.partial_max {
            if output.is_none_or(|partial| {
                matches!(
                    value.partial_cmp(&partial),
                    Some(std::cmp::Ordering::Greater) | None
                )
            }) {
                *output = Some(value);
            }
        } else if output.replace(value).is_some() {
            return Err(DataFusionError::Execution(
                "Duplicate packed fragment series timestamp".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn build(
    stmt: &EvalStmt,
    planned: Vec<(SelectorKey, Expr, LogicalPlan)>,
) -> Result<LogicalPlan> {
    let root_selector = matches!(&stmt.expr, Expr::VectorSelector(_))
        && planned.len() == 1
        && needs_series_execution(&planned[0].1, true)
        && !needs_fragment_execution(&planned[0].1);
    let mut specs = Vec::new();
    let mut children = Vec::new();
    let mut output_tags = std::collections::BTreeSet::new();
    for (key, expr, plan) in planned {
        let fields = plan.schema().fields();
        let times: Vec<_> = fields
            .iter()
            .filter(|field| {
                matches!(
                    field.data_type(),
                    DataType::Timestamp(TimeUnit::Millisecond, _)
                )
            })
            .collect();
        let values: Vec<_> = fields
            .iter()
            .filter(|field| {
                field.data_type().is_integer()
                    || matches!(field.data_type(), DataType::Float32 | DataType::Float64)
            })
            .collect();
        if times.len() != 1 || values.len() != 1 {
            if root_selector {
                return Ok(plan);
            }
            return Err(DataFusionError::Plan(
                "Fragment requires one timestamp and one numeric value".into(),
            ));
        }
        let time = times[0].name().clone();
        let value = values[0].name().clone();
        let tags: Vec<_> = fields
            .iter()
            .filter(|field| field.name() != &time && field.name() != &value)
            .map(|field| field.name().clone())
            .collect();
        for field in fields.iter().filter(|field| tags.contains(field.name())) {
            let valid = match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
                DataType::Dictionary(key, value) => key.is_integer() && value.is_string(),
                _ => false,
            };
            if !valid {
                if root_selector {
                    return Ok(plan);
                }
                return Err(DataFusionError::Plan(format!(
                    "Unsupported fragment label {}: {}",
                    field.name(),
                    field.data_type()
                )));
            }
        }
        let mut named_expr = &expr;
        while let Expr::Paren(paren) = named_expr {
            named_expr = &paren.expr;
        }
        let metric = match named_expr {
            Expr::VectorSelector(selector) => selector.name.clone(),
            _ => None,
        };
        output_tags.extend(tags.iter().cloned());
        let packed = std::env::var_os("GT_RESEARCH_PACKED_SERIES").is_some();
        let mut child = remove_fragment_output_sort(plan);
        if std::env::var_os("GT_RESEARCH_SERIES_RANGE").is_some() {
            child = crate::research_series_grid::SeriesRange::rewrite(child)?;
        }
        let mut dense = None;
        let mut partial_max = false;
        if packed && std::env::var_os("GT_RESEARCH_SERIES_FRAGMENT").is_some() {
            if metric.is_some()
                && let Some(instant) = crate::research_series_grid::SeriesRange::recognize_instant(
                    &child, &tags, &time, &value,
                )?
            {
                child = instant.plan();
            }
            if let Some(fragment) = crate::research_series_grid::SeriesFragment::recognize(
                &child, &tags, &time, &value,
            )? {
                dense = Some(fragment.partial_sum());
                child = fragment.plan();
            }
        }
        if packed
            && dense.is_none()
            && std::env::var_os("GT_RESEARCH_SERIES_FRAGMENT").is_some()
            && let Some(fragment) = crate::research_series_grid::SeriesInstantMax::recognize(
                &child, &tags, &time, &value,
            )?
        {
            partial_max = true;
            child = fragment.plan();
        }
        if packed && dense.is_none() && !partial_max {
            if tags.iter().any(|tag| tag == PACKED_POINTS) {
                return Err(DataFusionError::Plan(
                    "Packed fragment label collision".into(),
                ));
            }
            if std::env::var_os("GT_RESEARCH_SERIES_PACK").is_some() {
                child = crate::research_series_grid::SeriesPack::plan(child, &tags, &time, &value)?;
            } else {
                let point = datafusion::functions::core::named_struct().call(vec![
                    lit("time"),
                    col(&time),
                    lit("value"),
                    col(&value),
                ]);
                child = LogicalPlanBuilder::from(child)
                    .aggregate(
                        tags.iter().map(col).collect::<Vec<_>>(),
                        vec![
                            datafusion::functions_aggregate::array_agg::array_agg_udaf()
                                .call(vec![point])
                                .alias(PACKED_POINTS),
                        ],
                    )?
                    .build()?;
            }
        }
        specs.push(FragmentInput {
            key,
            time,
            value,
            tags,
            metric,
            packed,
            dense,
            partial_max,
        });
        children.push(child);
    }
    if output_tags.iter().any(|name| {
        matches!(
            name.as_str(),
            "__research_grid_time" | "__research_grid_value"
        )
    }) {
        return Err(DataFusionError::Plan(
            "Fragment label conflicts with output schema".into(),
        ));
    }
    let mut fields = vec![
        Field::new(
            "__research_grid_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("__research_grid_value", DataType::Float64, false),
    ];
    fields.extend(
        output_tags
            .into_iter()
            .filter(|name| name != "__name__")
            .map(|name| Field::new(name, DataType::Utf8, true)),
    );
    if stmt.interval.subsec_nanos() % 1_000_000 != 0 {
        return Err(DataFusionError::Plan(
            "Fragment requires millisecond interval".into(),
        ));
    }
    let step = i64::try_from(stmt.interval.as_millis())
        .map_err(|_| DataFusionError::Plan("Fragment interval overflow".into()))?;
    let grid = TimeGrid::new(millis(stmt.start)?, millis(stmt.end)?, step)
        .map_err(|error| DataFusionError::Plan(error.into()))?;
    OnlineNode::plan(
        OnlineQuery {
            fragments: Some(Arc::new(specs)),
            expr: stmt.expr.clone(),
            grid: Arc::new(grid),
            specs: Arc::new(Vec::new()),
            tag_orders: Arc::new(BTreeMap::new()),
            schema: Arc::new(Schema::new(fields)),
            lookback: 0,
            workers: 8,
        },
        children,
    )
}

// Mirrors the HTTP PromQL boundary: only presentation order is discarded.
// Limits, fetched sorts and ordering consumed by extension nodes remain intact.
fn remove_fragment_output_sort(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => Arc::unwrap_or_clone(sort.input),
        LogicalPlan::Projection(mut projection) => {
            projection.input = Arc::new(remove_fragment_output_sort(Arc::unwrap_or_clone(
                projection.input,
            )));
            LogicalPlan::Projection(projection)
        }
        plan => plan,
    }
}

fn collect_packed(
    spec: &FragmentInput,
    batch: &datafusion::arrow::record_batch::RecordBatch,
    grid: &TimeGrid,
    cancelled: &AtomicBool,
    memory: &datafusion::execution::memory_pool::MemoryReservation,
    rows: &mut BTreeMap<Labels, Vec<Option<f64>>>,
) -> Result<()> {
    let invalid = |message: &str| DataFusionError::Execution(message.into());
    let packed = batch
        .column_by_name(PACKED_POINTS)
        .and_then(|array| array.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| invalid("Packed fragment requires a list of points"))?;
    for row in 0..batch.num_rows() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(invalid("Fragment query cancelled"));
        }
        if packed.is_null(row) {
            return Err(invalid("Null packed fragment"));
        }
        let points = packed.value(row);
        let (times, values) = if spec.dense.is_some() {
            if points.len() != grid.instants.len() || points.data_type() != &DataType::Float64 {
                return Err(invalid("Dense fragment grid length or type mismatch"));
            }
            (None, points.clone())
        } else {
            let points = points
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("Packed fragment point type mismatch"))?;
            let times = points
                .column_by_name("time")
                .and_then(|array| array.as_any().downcast_ref::<TimestampMillisecondArray>())
                .ok_or_else(|| invalid("Packed fragment timestamp type mismatch"))?;
            let values = points
                .column_by_name("value")
                .ok_or_else(|| invalid("Missing packed fragment values"))?;
            if points.null_count() != 0 || times.null_count() != 0 {
                return Err(invalid("Null packed point or timestamp"));
            }
            (Some(times), cast(values, &DataType::Float64)?)
        };
        let values = values
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| invalid("Packed fragment value type mismatch"))?;
        if values.null_count() == values.len() {
            continue;
        }
        let mut labels = BTreeMap::new();
        for name in &spec.tags {
            let column = batch
                .column_by_name(name)
                .ok_or_else(|| invalid("Missing packed fragment label"))?;
            if let Some(value) = string_array_value_at_index(column, row) {
                labels.insert(name.clone(), value.to_owned());
            }
        }
        if let Some(metric) = &spec.metric {
            labels.insert("__name__".into(), metric.clone());
        }
        if !rows.contains_key(&labels) {
            let bytes = grid
                .instants
                .len()
                .checked_mul(size_of::<Option<f64>>())
                .and_then(|n| n.checked_add(256))
                .and_then(|n| {
                    labels.iter().try_fold(n, |n, (name, value)| {
                        n.checked_add(name.len())?
                            .checked_add(value.len())?
                            .checked_add(256)
                    })
                })
                .ok_or_else(|| invalid("Packed fragment size overflow"))?;
            memory.try_grow(bytes)?;
            rows.insert(labels.clone(), vec![None; grid.instants.len()]);
        }
        let output = rows
            .get_mut(&labels)
            .ok_or_else(|| invalid("Missing packed fragment series"))?;
        let dense = times.is_none_or(|times| times.values().as_ref() == grid.instants.as_slice());
        for index in 0..values.len() {
            if index % 1024 == 0 && cancelled.load(Ordering::Relaxed) {
                return Err(invalid("Fragment query cancelled"));
            }
            if values.is_null(index) {
                continue;
            }
            let position = if dense {
                index
            } else {
                grid.instants
                    .binary_search(
                        &times
                            .ok_or_else(|| invalid("Missing sparse fragment timestamps"))?
                            .value(index),
                    )
                    .map_err(|_| invalid("Packed fragment timestamp outside grid"))?
            };
            spec.merge_point(&mut output[position], values.value(index))?;
        }
    }
    Ok(())
}

pub(crate) async fn collect(
    specs: Arc<Vec<FragmentInput>>,
    children: Vec<Arc<dyn ExecutionPlan>>,
    grid: Arc<TimeGrid>,
    context: Arc<TaskContext>,
    cancelled: Arc<AtomicBool>,
) -> Result<CollectedSeries> {
    let reservation =
        Arc::new(MemoryConsumer::new("ResearchOnlineFragments").register(context.memory_pool()));
    let mut streams = Vec::new();
    for (index, child) in children.iter().enumerate() {
        for partition in 0..child.output_partitioning().partition_count() {
            streams.push(
                child
                    .execute(partition, context.clone())?
                    .map_ok(move |batch| (index, batch))
                    .boxed(),
            );
        }
    }
    let mut stream = stream::select_all(streams);
    let mut rows: Vec<BTreeMap<Labels, Vec<Option<f64>>>> =
        specs.iter().map(|_| BTreeMap::new()).collect();
    while let Some((index, batch)) = stream.try_next().await? {
        let spec = specs[index].clone();
        let grid = grid.clone();
        let cancelled = cancelled.clone();
        let retained = reservation.clone();
        let temporary = reservation.new_empty();
        temporary.try_grow(
            batch
                .get_array_memory_size()
                .checked_add(batch.num_rows().saturating_mul(32))
                .ok_or_else(|| DataFusionError::Execution("Fragment scratch overflow".into()))?,
        )?;
        rows = common_runtime::spawn_blocking_query(move || {
            let _temporary = temporary;
            if cancelled.load(Ordering::Relaxed) {
                return Err(DataFusionError::Execution(
                    "Fragment query cancelled".into(),
                ));
            }
            if spec.packed {
                collect_packed(
                    &spec,
                    &batch,
                    &grid,
                    &cancelled,
                    &retained,
                    &mut rows[index],
                )?;
                return Ok::<_, DataFusionError>(rows);
            }
            let time = batch
                .column_by_name(&spec.time)
                .and_then(|column| column.as_any().downcast_ref::<TimestampMillisecondArray>())
                .ok_or_else(|| {
                    DataFusionError::Execution("Fragment timestamp type mismatch".into())
                })?;
            let values = batch
                .column_by_name(&spec.value)
                .ok_or_else(|| DataFusionError::Execution("Missing fragment value".into()))?;
            let values = cast(values, &DataType::Float64)?;
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("Fragment value type mismatch".into()))?;
            if time.null_count() != 0 {
                return Err(DataFusionError::Execution("Null fragment timestamp".into()));
            }
            // Match the response layer's adjacent-label semantics without requiring sorted output.
            let mut boundaries: Option<BooleanBuffer> = None;
            if batch.num_rows() >= 2 {
                for name in &spec.tags {
                    let column = batch.column_by_name(name).ok_or_else(|| {
                        DataFusionError::Execution("Missing fragment label".into())
                    })?;
                    // Equal keys within one dictionary imply equal labels. Different keys
                    // may split equal labels; the exact label map below merges those runs.
                    let column = column
                        .as_any_dictionary_opt()
                        .map_or(column.as_ref(), |dictionary| dictionary.keys());
                    let changed = distinct(
                        &column.slice(0, batch.num_rows() - 1),
                        &column.slice(1, batch.num_rows() - 1),
                    )?;
                    boundaries = Some(match boundaries {
                        Some(previous) => &previous | changed.values(),
                        None => changed.values().clone(),
                    });
                }
            }
            let mut start = 0;
            let ends: Vec<_> = boundaries
                .iter()
                .flat_map(BooleanBuffer::set_indices)
                .map(|index| index + 1)
                .chain(std::iter::once(batch.num_rows()))
                .collect();
            for end in ends {
                let run = start..end;
                start = end;
                if run.clone().all(|row| values.is_null(row)) {
                    continue;
                }
                if cancelled.load(Ordering::Relaxed) {
                    return Err(DataFusionError::Execution(
                        "Fragment query cancelled".into(),
                    ));
                }
                let mut labels = BTreeMap::new();
                for name in &spec.tags {
                    let column = batch.column_by_name(name).ok_or_else(|| {
                        DataFusionError::Execution("Missing fragment label".into())
                    })?;
                    if let Some(value) = string_array_value_at_index(column, run.start) {
                        labels.insert(name.clone(), value.to_owned());
                    }
                }
                if let Some(metric) = &spec.metric {
                    labels.insert("__name__".into(), metric.clone());
                }
                if !rows[index].contains_key(&labels) {
                    let bytes = grid
                        .instants
                        .len()
                        .checked_mul(size_of::<Option<f64>>())
                        .and_then(|bytes| {
                            bytes.checked_add(
                                labels
                                    .iter()
                                    .map(|(name, value)| name.len() + value.len() + 256)
                                    .sum::<usize>(),
                            )
                        })
                        .ok_or_else(|| {
                            DataFusionError::Execution("Fragment size overflow".into())
                        })?;
                    retained.try_grow(bytes.checked_add(256).ok_or_else(|| {
                        DataFusionError::Execution("Fragment size overflow".into())
                    })?)?;
                    rows[index].insert(labels.clone(), vec![None; grid.instants.len()]);
                }
                let points = rows[index]
                    .get_mut(&labels)
                    .ok_or_else(|| DataFusionError::Internal("Missing fragment series".into()))?;
                for row in run {
                    if values.is_null(row) {
                        continue;
                    }
                    let point = grid.instants.binary_search(&time.value(row)).map_err(|_| {
                        DataFusionError::Execution("Fragment timestamp outside grid".into())
                    })?;
                    if points[point].replace(values.value(row)).is_some() {
                        return Err(DataFusionError::Execution(
                            "Duplicate fragment series timestamp".into(),
                        ));
                    }
                }
            }
            Ok::<_, DataFusionError>(rows)
        })
        .await
        .map_err(|error| DataFusionError::Execution(error.to_string()))??;
    }
    let inputs = specs
        .iter()
        .zip(rows)
        .map(|(spec, rows)| {
            (
                spec.key.clone(),
                VectorResult {
                    series: rows
                        .into_iter()
                        .map(|(labels, values)| SeriesVector { labels, values })
                        .collect(),
                    tag_order: spec.tags.clone(),
                    reservation: None,
                },
            )
        })
        .collect();
    Ok(CollectedSeries {
        inputs,
        reservation,
    })
}

/// Keeps matching and ranking in the native evaluator and delegates maximal lower subtrees.
pub(crate) fn split(expr: &Expr) -> Result<(Expr, Vec<(SelectorKey, Expr)>)> {
    split_with_boundary(expr, false)
}

/// Leaves all upper operators in the native evaluator, including scalar arithmetic.
pub(crate) fn split_selectors(expr: &Expr) -> Result<(Expr, Vec<(SelectorKey, Expr)>)> {
    split_with_boundary(expr, true)
}

fn split_with_boundary(
    expr: &Expr,
    selectors_only: bool,
) -> Result<(Expr, Vec<(SelectorKey, Expr)>)> {
    selector_requests(expr)?;
    let mut splitter = Splitter {
        selectors_only,
        ..Default::default()
    };
    let upper = splitter.rewrite(expr.clone())?;
    Ok((upper, splitter.inputs))
}

#[derive(Default)]
struct Splitter {
    selectors_only: bool,
    inputs: Vec<(SelectorKey, Expr)>,
    expressions: BTreeMap<String, Expr>,
}

impl Splitter {
    fn rewrite(&mut self, mut expr: Expr) -> Result<Expr> {
        let boundary = if self.selectors_only {
            matches!(&expr, Expr::VectorSelector(_))
                || matches!(&expr, Expr::Call(call) if matches!(
                    call.func.name, "rate" | "increase" | "irate" | "max_over_time"
                ))
        } else {
            expr.value_type() == ValueType::Vector && !contains_native(&expr)
        };
        if boundary {
            let identity = expr.to_string();
            if let Some(selector) = self.expressions.get(&identity) {
                return Ok(selector.clone());
            }
            let selector = parse(&format!("__research_fragment_{}", self.inputs.len()))
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
            let Expr::VectorSelector(vector) = &selector else {
                return Err(DataFusionError::Internal(
                    "Invalid fragment identity".into(),
                ));
            };
            self.inputs.push((SelectorKey::new(vector, None), expr));
            self.expressions.insert(identity, selector.clone());
            return Ok(selector);
        }
        match &mut expr {
            Expr::Aggregate(aggregate) => {
                aggregate.expr = Box::new(self.rewrite(*aggregate.expr.clone())?);
                if let Some(param) = &mut aggregate.param {
                    **param = self.rewrite(*param.clone())?;
                }
            }
            Expr::Binary(binary) => {
                binary.lhs = Box::new(self.rewrite(*binary.lhs.clone())?);
                binary.rhs = Box::new(self.rewrite(*binary.rhs.clone())?);
            }
            Expr::Paren(paren) => paren.expr = Box::new(self.rewrite(*paren.expr.clone())?),
            Expr::Unary(unary) => unary.expr = Box::new(self.rewrite(*unary.expr.clone())?),
            Expr::Call(call) => {
                for arg in &mut call.args.args {
                    **arg = self.rewrite(*arg.clone())?;
                }
            }
            Expr::NumberLiteral(_) => {}
            _ => return Err(DataFusionError::Plan("Unsupported native frontier".into())),
        }
        Ok(expr)
    }
}

/// Whether `expr` joins two instant vectors where one side is not an aggregation,
/// so each of its input series is retained by the join.
pub(crate) fn has_series_level_join(expr: &Expr) -> bool {
    fn unwrap(expr: &Expr) -> &Expr {
        match expr {
            Expr::Paren(paren) => unwrap(&paren.expr),
            expr => expr,
        }
    }
    fn is_vector(expr: &Expr) -> bool {
        !matches!(
            unwrap(expr),
            Expr::NumberLiteral(_) | Expr::StringLiteral(_)
        )
    }
    match expr {
        Expr::Binary(binary) => {
            (is_vector(&binary.lhs)
                && is_vector(&binary.rhs)
                && (!matches!(unwrap(&binary.lhs), Expr::Aggregate(_))
                    || !matches!(unwrap(&binary.rhs), Expr::Aggregate(_)))
                && binary
                    .modifier
                    .as_ref()
                    .is_some_and(|m| m.matching.is_some()))
                || has_series_level_join(&binary.lhs)
                || has_series_level_join(&binary.rhs)
        }
        Expr::Aggregate(aggregate) => has_series_level_join(&aggregate.expr),
        Expr::Paren(paren) => has_series_level_join(&paren.expr),
        Expr::Unary(unary) => has_series_level_join(&unary.expr),
        Expr::Call(call) => call.args.args.iter().any(|arg| has_series_level_join(arg)),
        _ => false,
    }
}

/// A series-level join keeps a dense grid per input series until matched; over long
/// ranges it runs on the row-streaming plan unless the planner admits the selection.
pub(crate) fn exceeds_join_grid(expr: &Expr, points: usize) -> bool {
    points > super::dense_budget::DENSE_GRID_POINTS && has_series_level_join(expr)
}

pub(crate) fn needs_series_execution(expr: &Expr, root_selectors: bool) -> bool {
    let mut root = expr;
    while let Expr::Paren(paren) = root {
        root = &paren.expr;
    }
    (root_selectors
        && matches!(root, Expr::VectorSelector(selector) if selector.name.is_some() && selector.at.is_none() && selector.matchers.or_matchers.is_empty()))
        || needs_fragment_execution(expr)
}

fn needs_fragment_execution(expr: &Expr) -> bool {
    if contains_native(expr) {
        return true;
    }
    // Counter windows can form a whole region fragment; they must not force
    // the splitter to move their surrounding scalar/sum operators upstairs.
    match expr {
        Expr::Call(call) => {
            matches!(call.func.name, "irate" | "increase")
                || call
                    .args
                    .args
                    .iter()
                    .any(|arg| needs_fragment_execution(arg))
        }
        Expr::Aggregate(aggregate) => needs_fragment_execution(&aggregate.expr),
        Expr::Binary(binary) => {
            needs_fragment_execution(&binary.lhs) || needs_fragment_execution(&binary.rhs)
        }
        Expr::Paren(paren) => needs_fragment_execution(&paren.expr),
        Expr::Unary(unary) => needs_fragment_execution(&unary.expr),
        Expr::Extension(extension) => extension
            .expr
            .children()
            .into_iter()
            .any(needs_fragment_execution),
        _ => false,
    }
}

pub(crate) fn contains_native(expr: &Expr) -> bool {
    fn unparen(mut expr: &Expr) -> &Expr {
        while let Expr::Paren(paren) = expr {
            expr = &paren.expr;
        }
        expr
    }

    match expr {
        Expr::Aggregate(aggregate) => {
            aggregate.op.to_string() == "topk"
                || (aggregate.op.to_string() == "sum"
                    && matches!(unparen(&aggregate.expr), Expr::Aggregate(inner)
                        if inner.op.to_string() == "max"
                            && matches!(unparen(&inner.expr), Expr::VectorSelector(_))))
                || contains_native(&aggregate.expr)
                || aggregate.param.as_deref().is_some_and(contains_native)
        }
        Expr::Binary(binary) => {
            (binary.lhs.value_type() == ValueType::Vector
                && binary.rhs.value_type() == ValueType::Vector)
                || contains_native(&binary.lhs)
                || contains_native(&binary.rhs)
        }
        Expr::Extension(extension) => extension.expr.children().into_iter().any(contains_native),
        Expr::Paren(paren) => contains_native(&paren.expr),
        Expr::Unary(unary) => contains_native(&unary.expr),
        Expr::Call(call) => {
            call.func.name == "histogram_quantile"
                || call.args.args.iter().any(|arg| contains_native(arg))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{DictionaryArray, Int64Array, StringArray, UInt32Array};
    use datafusion::arrow::datatypes::UInt32Type;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_expr::{LogicalPlanBuilder, Sort, col, lit};

    use super::*;

    #[tokio::test]
    async fn series_pack_preserves_partitioned_points_and_duplicate_detection() {
        use crate::research_series_grid::SeriesPack;
        use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
        use datafusion::physical_planner::DefaultPhysicalPlanner;
        use datafusion_common::ToDFSchema;
        use datafusion_expr::EmptyRelation;
        for duplicate in [false, true] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, true),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, true),
            ]));
            let batch = |ts: i64, value: f64| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec![Some("a"), None])),
                        Arc::new(TimestampMillisecondArray::from(vec![ts, ts])),
                        Arc::new(Float64Array::from(vec![Some(value), Some(-value)])),
                    ],
                )
                .unwrap()
            };
            let partitions = vec![
                vec![batch(0, 3.0)],
                vec![batch(if duplicate { 0 } else { 10 }, 5.0)],
            ];
            let source = Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&partitions, schema.clone(), None).unwrap(),
            )));
            let logical = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.to_dfschema_ref().unwrap(),
            });
            let plan = SeriesPack::plan(logical, &["host".into()], "ts", "value").unwrap();
            let LogicalPlan::Extension(extension) = plan else {
                unreachable!()
            };
            let node = extension
                .node
                .as_any()
                .downcast_ref::<SeriesPack>()
                .unwrap();
            let context = SessionContext::new();
            let packed = node
                .physical(
                    source,
                    &DefaultPhysicalPlanner::default(),
                    &context.state(),
                    &PhysicalPlanningContext::default(),
                )
                .unwrap();
            assert_eq!(packed.output_partitioning().partition_count(), 2);
            assert_eq!(packed.schema().as_ref(), extension.node.schema().as_arrow());
            let key = SelectorKey {
                selector: "fragment".into(),
                function: None,
            };
            let result = collect(
                Arc::new(vec![FragmentInput {
                    key: key.clone(),
                    time: "ts".into(),
                    value: "value".into(),
                    tags: vec!["host".into()],
                    metric: None,
                    packed: true,
                    dense: None,
                    partial_max: false,
                }]),
                vec![packed],
                Arc::new(TimeGrid::new(0, 10, 10).unwrap()),
                context.task_ctx(),
                Arc::new(AtomicBool::new(false)),
            )
            .await;
            if duplicate {
                assert!(
                    result
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("Duplicate packed")
                );
            } else {
                let result = result.unwrap();
                let series = &result.inputs[&key].series;
                assert_eq!(series.len(), 2);
                for item in series {
                    assert_eq!(
                        item.values,
                        if item.labels.contains_key("host") {
                            vec![Some(3.0), Some(5.0)]
                        } else {
                            vec![Some(-3.0), Some(-5.0)]
                        }
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn series_pack_ignores_empty_global_partial_states() {
        use crate::research_series_grid::SeriesPack;
        use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
        use datafusion::physical_planner::DefaultPhysicalPlanner;
        use datafusion_common::ToDFSchema;
        use datafusion_expr::EmptyRelation;

        for all_empty in [false, true] {
            let schema = Arc::new(Schema::new(vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Float64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(vec![0, 10])),
                    Arc::new(Float64Array::from(vec![3.0, 5.0])),
                ],
            )
            .unwrap();
            let partitions = vec![vec![], if all_empty { vec![] } else { vec![batch] }, vec![]];
            let source = Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&partitions, schema.clone(), None).unwrap(),
            )));
            let input = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: schema.to_dfschema_ref().unwrap(),
            });
            let plan = SeriesPack::plan(input, &[], "ts", "value").unwrap();
            let LogicalPlan::Extension(extension) = plan else {
                unreachable!()
            };
            let node = extension
                .node
                .as_any()
                .downcast_ref::<SeriesPack>()
                .unwrap();
            let context = SessionContext::new();
            let packed = node
                .physical(
                    source,
                    &DefaultPhysicalPlanner::default(),
                    &context.state(),
                    &PhysicalPlanningContext::default(),
                )
                .unwrap();
            let key = SelectorKey {
                selector: "fragment".into(),
                function: None,
            };
            let result = collect(
                Arc::new(vec![FragmentInput {
                    key: key.clone(),
                    time: "ts".into(),
                    value: "value".into(),
                    tags: vec![],
                    metric: None,
                    packed: true,
                    dense: None,
                    partial_max: false,
                }]),
                vec![packed],
                Arc::new(TimeGrid::new(0, 10, 10).unwrap()),
                context.task_ctx(),
                Arc::new(AtomicBool::new(false)),
            )
            .await
            .unwrap();
            let series = &result.inputs[&key].series;
            if all_empty {
                assert!(series.is_empty());
            } else {
                assert_eq!(series.len(), 1);
                assert!(series[0].labels.is_empty());
                assert_eq!(series[0].values, vec![Some(3.0), Some(5.0)]);
            }
        }
    }

    #[test]
    fn counter_entry_keeps_region_scalar_and_sum_fragments() {
        for query in [
            "sum by(pod)(8 * irate(x[5m]))",
            "sum by(container)(increase(x[5m]))",
            "irate(x[5m])",
        ] {
            let expr = parse(query).unwrap();
            assert!(needs_series_execution(&expr, false));
            let (upper, inputs) = split(&expr).unwrap();
            assert!(matches!(upper, Expr::VectorSelector(_)));
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs[0].1, expr);
        }
        assert!(!needs_series_execution(
            &parse("sum(rate(x[5m]))").unwrap(),
            false
        ));
        assert!(!needs_series_execution(&parse("x").unwrap(), false));
    }

    #[test]
    fn root_selector_entry_preserves_nested_fragment_boundaries() {
        for query in ["x", "(x{host=\"a\"} offset 1m)", "x offset -1m"] {
            let expr = parse(query).unwrap();
            assert!(needs_series_execution(&expr, true));
            assert!(!needs_series_execution(&expr, false));
            let (upper, inputs) = split(&expr).unwrap();
            assert!(matches!(upper, Expr::VectorSelector(_)));
            assert_eq!(inputs.len(), 1);
            assert_eq!(inputs[0].1, expr);
        }
        for query in [
            "sum(x)",
            "x * 2",
            "abs(x)",
            "sum(rate(x[5m]))",
            "{host=\"a\"}",
            "x @ 100",
            "(x @ start())",
        ] {
            assert!(
                !needs_series_execution(&parse(query).unwrap(), true),
                "{query}"
            );
        }
        let mut disjunction = parse("x{host=\"a\"}").unwrap();
        let Expr::VectorSelector(selector) = &mut disjunction else {
            unreachable!();
        };
        selector
            .matchers
            .or_matchers
            .push(selector.matchers.matchers.clone());
        assert!(!needs_series_execution(&disjunction, true));
        for query in ["sum(irate(x[5m]))", "topk(2, x)", "x / y"] {
            let expr = parse(query).unwrap();
            assert_eq!(
                needs_series_execution(&expr, true),
                needs_series_execution(&expr, false)
            );
            assert!(needs_series_execution(&expr, true));
        }
    }

    #[test]
    fn root_selector_keeps_original_plan_for_unsupported_payloads() {
        use std::time::{Duration, UNIX_EPOCH};

        use datafusion_common::ToDFSchema;
        use datafusion_expr::EmptyRelation;

        let expr = parse("metric").unwrap();
        let (upper, fragments) = split(&expr).unwrap();
        let stmt = EvalStmt {
            expr: upper,
            start: UNIX_EPOCH + Duration::from_secs(1000),
            end: UNIX_EPOCH + Duration::from_secs(2000),
            interval: Duration::from_secs(10),
            lookback_delta: Duration::from_secs(300),
        };
        for numeric in [false, true] {
            let mut fields = vec![
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("greptime_native_histogram", DataType::Binary, true),
            ];
            if numeric {
                fields.push(Field::new("greptime_value", DataType::Float64, true));
            }
            let original = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: Schema::new(fields).to_dfschema_ref().unwrap(),
            });
            let actual = build(
                &stmt,
                vec![(fragments[0].0.clone(), expr.clone(), original.clone())],
            )
            .unwrap();
            assert_eq!(actual, original);
        }
    }

    #[test]
    fn packed_fragments_preserve_alignment_and_reject_duplicate_points() {
        use datafusion::arrow::buffer::OffsetBuffer;

        let fields = vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
        ];
        let points = StructArray::new(
            fields.into(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![20, 0, 10])),
                Arc::new(Float64Array::from(vec![Some(3.0), Some(1.0), None])),
            ],
            None,
        );
        let item = Arc::new(Field::new("item", points.data_type().clone(), false));
        let list = ListArray::new(
            item,
            OffsetBuffer::new(vec![0, 3].into()),
            Arc::new(points),
            None,
        );
        let batch = RecordBatch::try_from_iter(vec![
            (
                PACKED_POINTS,
                Arc::new(list) as datafusion::arrow::array::ArrayRef,
            ),
            ("host", Arc::new(StringArray::from(vec![Some("a")]))),
        ])
        .unwrap();
        let spec = FragmentInput {
            key: SelectorKey {
                selector: "fragment".into(),
                function: None,
            },
            time: "ts".into(),
            value: "value".into(),
            tags: vec!["host".into()],
            metric: None,
            packed: true,
            dense: None,
            partial_max: false,
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let memory = MemoryConsumer::new("packed-collect-test").register(&pool);
        let grid = TimeGrid::new(0, 20, 10).unwrap();
        let mut rows = BTreeMap::new();
        collect_packed(
            &spec,
            &batch,
            &grid,
            &AtomicBool::new(false),
            &memory,
            &mut rows,
        )
        .unwrap();
        assert_eq!(
            rows.values().next().unwrap(),
            &vec![Some(1.0), None, Some(3.0)]
        );
        assert!(
            collect_packed(
                &spec,
                &batch,
                &grid,
                &AtomicBool::new(false),
                &memory,
                &mut rows
            )
            .unwrap_err()
            .to_string()
            .contains("Duplicate packed")
        );
        assert!(
            collect_packed(
                &spec,
                &batch,
                &grid,
                &AtomicBool::new(true),
                &memory,
                &mut BTreeMap::new()
            )
            .unwrap_err()
            .to_string()
            .contains("cancelled")
        );
        let tiny_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let tiny = MemoryConsumer::new("packed-budget-test").register(&tiny_pool);
        assert!(
            collect_packed(
                &spec,
                &batch,
                &grid,
                &AtomicBool::new(false),
                &tiny,
                &mut BTreeMap::new()
            )
            .is_err()
        );
        assert!(
            collect_packed(
                &spec,
                &batch,
                &TimeGrid::new(0, 10, 10).unwrap(),
                &AtomicBool::new(false),
                &memory,
                &mut BTreeMap::new()
            )
            .unwrap_err()
            .to_string()
            .contains("outside grid")
        );
    }

    async fn collect_fixture(
        limit: usize,
        duplicate: bool,
        integer: bool,
    ) -> (Result<CollectedSeries>, Arc<GreedyMemoryPool>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "value",
                if integer {
                    DataType::Int64
                } else {
                    DataType::Float64
                },
                false,
            ),
            Field::new("host", DataType::Utf8, true),
        ]));
        let batch = |times: Vec<i64>, values: Vec<f64>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondArray::from(times)),
                    if integer {
                        Arc::new(Int64Array::from(
                            values.into_iter().map(|v| v as i64).collect::<Vec<_>>(),
                        ))
                    } else {
                        Arc::new(Float64Array::from(values))
                    },
                    Arc::new(StringArray::from(vec![Some("a"), None])),
                ],
            )
            .unwrap()
        };
        let partitions = vec![
            vec![batch(vec![10, 0], vec![4.0, 3.0])],
            vec![batch(
                vec![if duplicate { 10 } else { 0 }, 10],
                vec![2.0, 5.0],
            )],
        ];
        let child = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema, None).unwrap(),
        )));
        let pool = Arc::new(GreedyMemoryPool::new(limit));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .build()
            .unwrap();
        let context = Arc::new(TaskContext::default().with_runtime(Arc::new(runtime)));
        let spec = FragmentInput {
            key: SelectorKey {
                selector: "fragment".into(),
                function: None,
            },
            time: "ts".into(),
            value: "value".into(),
            tags: vec!["host".into()],
            metric: None,
            packed: false,
            dense: None,
            partial_max: false,
        };
        let result = collect(
            Arc::new(vec![spec]),
            vec![child],
            Arc::new(TimeGrid::new(0, 10, 10).unwrap()),
            context,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        (result, pool)
    }

    #[tokio::test]
    async fn dictionary_runs_merge_by_values_across_batches() {
        for duplicate in [false, true] {
            let batch = |keys: Vec<Option<u32>>,
                         dictionary: Vec<Option<&str>>,
                         times: Vec<i64>,
                         values: Vec<f64>| {
                let labels = DictionaryArray::<UInt32Type>::try_new(
                    UInt32Array::from(keys),
                    Arc::new(StringArray::from(dictionary)),
                )
                .unwrap();
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new(
                            "ts",
                            DataType::Timestamp(TimeUnit::Millisecond, None),
                            false,
                        ),
                        Field::new("value", DataType::Float64, false),
                        Field::new("host", labels.data_type().clone(), true),
                    ])),
                    vec![
                        Arc::new(TimestampMillisecondArray::from(times)),
                        Arc::new(Float64Array::from(values)),
                        Arc::new(labels),
                    ],
                )
                .unwrap()
            };
            let first = batch(
                vec![Some(0), Some(1), Some(2), None],
                vec![Some("a"), Some("a"), None],
                vec![0, if duplicate { 0 } else { 10 }, 0, 10],
                vec![1.0, 2.0, 3.0, 4.0],
            );
            // Key 0 denotes a different label in this batch; null values and null keys
            // must both map to the absent label, without losing a timestamp.
            let second = batch(
                vec![Some(0), Some(1), Some(2)],
                vec![Some("b"), Some("a"), None],
                vec![0, 20, 20],
                vec![5.0, 6.0, 7.0],
            );
            let schema = first.schema();
            let child = Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(&[vec![first, second]], schema, None).unwrap(),
            )));
            let pool = Arc::new(GreedyMemoryPool::new(1 << 20));
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(pool.clone())
                .build()
                .unwrap();
            let result = collect(
                Arc::new(vec![FragmentInput {
                    key: SelectorKey {
                        selector: "fragment".into(),
                        function: None,
                    },
                    time: "ts".into(),
                    value: "value".into(),
                    tags: vec!["host".into()],
                    metric: None,
                    packed: false,
                    dense: None,
                    partial_max: false,
                }]),
                vec![child],
                Arc::new(TimeGrid::new(0, 20, 10).unwrap()),
                Arc::new(TaskContext::default().with_runtime(Arc::new(runtime))),
                Arc::new(AtomicBool::new(false)),
            )
            .await;
            if duplicate {
                assert!(
                    result
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("Duplicate fragment")
                );
            } else {
                let result = result.unwrap();
                let series = &result.inputs.values().next().unwrap().series;
                assert_eq!(series.len(), 3);
                assert!(series[0].labels.is_empty());
                assert_eq!(series[0].values, vec![Some(3.0), Some(4.0), Some(7.0)]);
                assert_eq!(series[1].labels["host"], "a");
                assert_eq!(series[1].values, vec![Some(1.0), Some(2.0), Some(6.0)]);
                assert_eq!(series[2].labels["host"], "b");
                assert_eq!(series[2].values, vec![Some(5.0), None, None]);
                drop(result);
            }
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn merges_partitioned_grid_and_releases_budget() {
        for integer in [false, true] {
            let (result, pool) = collect_fixture(1 << 20, false, integer).await;
            let result = result.unwrap();
            let series = &result.inputs.values().next().unwrap().series;
            assert_eq!(series.len(), 2);
            assert!(series[0].labels.is_empty());
            assert_eq!(series[0].values, vec![Some(3.0), Some(5.0)]);
            assert_eq!(series[1].labels["host"], "a");
            assert_eq!(series[1].values, vec![Some(2.0), Some(4.0)]);
            assert!(pool.reserved() > 0);
            drop(result);
            assert_eq!(pool.reserved(), 0);
        }
        let (result, pool) = collect_fixture(1, false, false).await;
        assert!(result.is_err());
        assert_eq!(pool.reserved(), 0);
        let (result, pool) = collect_fixture(1 << 20, true, false).await;
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("Duplicate fragment")
        );
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn fragment_sort_removal_preserves_schema_and_row_selection() {
        let input =
            LogicalPlanBuilder::values(vec![vec![lit(3_i64)], vec![lit(1_i64)], vec![lit(2_i64)]])
                .unwrap()
                .build()
                .unwrap();
        let sort = Sort {
            expr: vec![col("column1").sort(true, false)],
            input: Arc::new(input),
            fetch: None,
        };
        let projected = LogicalPlanBuilder::from(LogicalPlan::Sort(sort.clone()))
            .project(vec![col("column1").alias("sample")])
            .unwrap()
            .project(vec![col("sample")])
            .unwrap()
            .build()
            .unwrap();
        let limited = LogicalPlanBuilder::from(LogicalPlan::Sort(sort.clone()))
            .limit(0, Some(2))
            .unwrap()
            .sort(vec![col("column1").sort(false, false)])
            .unwrap()
            .build()
            .unwrap();
        let fetched = LogicalPlan::Sort(Sort {
            fetch: Some(2),
            ..sort.clone()
        });
        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for (plan, expected) in [
            (LogicalPlan::Sort(sort), vec![3, 1, 2]),
            (projected, vec![3, 1, 2]),
            (limited, vec![1, 2]),
            (fetched, vec![1, 2]),
        ] {
            let schema = plan.schema().clone();
            let plan = remove_fragment_output_sort(plan);
            assert_eq!(plan.schema(), &schema);
            let batches = context
                .execute_logical_plan(plan)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let values = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, expected);
        }
    }

    #[test]
    fn selector_boundary_keeps_upper_operators_and_window_identity() {
        let expr = parse("topk(10, sum by(host)(8 * rate(x[5m] offset 1h))) / on(host) group_left(zone) max by(host,zone)(y)").unwrap();
        let (upper, inputs) = split_selectors(&expr).unwrap();
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].1, parse("rate(x[5m] offset 1h)").unwrap());
        assert_eq!(inputs[1].1, parse("y").unwrap());
        assert_eq!(upper, parse("topk(10, sum by(host)(8 * __research_fragment_0)) / on(host) group_left(zone) max by(host,zone)(__research_fragment_1)").unwrap());

        let (_, inputs) = split_selectors(&parse("sum(rate(x[5m])) + sum(rate(x[5m])) + sum(rate(x[10m])) + sum(rate(x[5m] offset 1h))").unwrap()).unwrap();
        assert_eq!(inputs.len(), 3);
        assert!(split_selectors(&parse("sum_over_time(x[1h:1m])").unwrap()).is_err());
        assert!(split_selectors(&parse("x @ 10").unwrap()).is_err());
    }

    #[test]
    fn keeps_instant_max_below_nested_sum_frontier() {
        for expression in [
            "sum by(namespace)(max by(namespace,pod)(x))",
            "sum without(pod)((max without(instance)((x offset 1h))))",
        ] {
            let expr = parse(expression).unwrap();
            assert!(contains_native(&expr));
            let (upper, inputs) = split(&expr).unwrap();
            assert_eq!(inputs.len(), 1);
            let Expr::Aggregate(sum) = upper else {
                panic!("Outer sum must consume merged partial maxima");
            };
            assert_eq!(sum.op.to_string(), "sum");
            let (fragment, nested) = split(&inputs[0].1).unwrap();
            assert!(matches!(fragment, Expr::VectorSelector(_)));
            assert_eq!(nested[0].1, inputs[0].1);
            assert!(inputs[0].1.to_string().contains("max"));
        }
        for expression in ["sum(max_over_time(x[5m]))", "sum(max(rate(x[5m])))"] {
            let expr = parse(expression).unwrap();
            assert!(!contains_native(&expr));
            let (_, inputs) = split(&expr).unwrap();
            assert_eq!(inputs[0].1, expr);
        }
    }

    #[test]
    fn preserves_scalar_rate_aggregation_below_frontier() {
        let expr = parse("sum by(namespace)(8 * rate(x[5m]))").unwrap();
        let (upper, inputs) = split(&expr).unwrap();
        assert!(matches!(upper, Expr::VectorSelector(_)));
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].1, expr);
    }

    #[test]
    fn long_range_guard_only_targets_joins_that_keep_input_series() {
        let parse = |query| promql_parser::parser::parse(query).unwrap();
        let q09 = parse(
            r#"sum by (pod) ((8 * rate(m{ns="a"}[5m])) * on(cluster, ns, pod) group_left() topk by (cluster, ns, pod) (1, max by (cluster, ns, pod) (info{host="false"})))"#,
        );
        let q04 = parse("sum(max by (ns) (a)) by (ns) / sum(b) by (ns)");
        let scalar = parse("8 * rate(m[5m])");
        assert!(exceeds_join_grid(&q09, 2017));
        assert!(!exceeds_join_grid(&q09, 1024));
        assert!(!exceeds_join_grid(&q04, 2017));
        assert!(!exceeds_join_grid(&scalar, 2017));
    }

    #[test]
    fn keeps_matching_modifiers_and_window_offsets() {
        let expr = parse("topk(10, sum by(host)(rate(x[5m] offset 1h))) / on(host) group_left(zone) max by(host,zone)(y)").unwrap();
        let (upper, inputs) = split(&expr).unwrap();
        assert_eq!(inputs.len(), 2);
        assert_eq!(
            inputs[0].1,
            parse("sum by(host)(rate(x[5m] offset 1h))").unwrap()
        );
        assert_eq!(inputs[1].1, parse("max by(host,zone)(y)").unwrap());
        assert_eq!(
            upper,
            parse(
                "topk(10, __research_fragment_0) / on(host) group_left(zone) __research_fragment_1"
            )
            .unwrap()
        );
    }

    #[test]
    fn shares_only_identical_fragments() {
        let (_, inputs) = split(&parse("sum(rate(x[5m])) / sum(rate(x[5m]))").unwrap()).unwrap();
        assert_eq!(inputs.len(), 1);
        let (_, inputs) = split(&parse("sum(rate(x[5m])) / sum(rate(x[10m]))").unwrap()).unwrap();
        assert_eq!(inputs.len(), 2);
        assert!(split(&parse("sum_over_time(x[1h:1m])").unwrap()).is_err());
    }

    #[test]
    fn dense_fragments_merge_partial_sums_and_validate_grid_and_uniqueness() {
        use datafusion::arrow::array::ArrayRef;
        use datafusion::arrow::buffer::OffsetBuffer;
        let batch = |values: Vec<Option<f64>>| {
            let end = values.len() as i32;
            let list = ListArray::new(
                Arc::new(Field::new("item", DataType::Float64, true)),
                OffsetBuffer::new(vec![0, end].into()),
                Arc::new(Float64Array::from(values)),
                None,
            );
            RecordBatch::try_from_iter(vec![
                (PACKED_POINTS, Arc::new(list) as ArrayRef),
                ("host", Arc::new(StringArray::from(vec![Some("a")]))),
            ])
            .unwrap()
        };
        let mut spec = FragmentInput {
            key: SelectorKey {
                selector: "fragment".into(),
                function: None,
            },
            time: "ts".into(),
            value: "value".into(),
            tags: vec!["host".into()],
            metric: None,
            packed: true,
            dense: Some(true),
            partial_max: false,
        };
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let memory = MemoryConsumer::new("dense-collect-test").register(&pool);
        let grid = TimeGrid::new(0, 20, 10).unwrap();
        let cancelled = AtomicBool::new(false);
        let mut rows = BTreeMap::new();
        collect_packed(
            &spec,
            &batch(vec![Some(2.0), None, Some(f64::NAN)]),
            &grid,
            &cancelled,
            &memory,
            &mut rows,
        )
        .unwrap();
        collect_packed(
            &spec,
            &batch(vec![Some(3.0), Some(-0.0), None]),
            &grid,
            &cancelled,
            &memory,
            &mut rows,
        )
        .unwrap();
        let values = rows.values().next().unwrap();
        assert_eq!(values[0], Some(5.0));
        assert_eq!(values[1].unwrap().to_bits(), (-0.0f64).to_bits());
        assert!(values[2].unwrap().is_nan());
        assert!(
            collect_packed(
                &spec,
                &batch(vec![Some(1.0)]),
                &grid,
                &cancelled,
                &memory,
                &mut rows
            )
            .is_err()
        );
        spec.dense = Some(false);
        assert!(
            collect_packed(
                &spec,
                &batch(vec![Some(1.0), None, None]),
                &grid,
                &cancelled,
                &memory,
                &mut rows
            )
            .unwrap_err()
            .to_string()
            .contains("Duplicate packed")
        );
        let mut empty = BTreeMap::new();
        collect_packed(
            &spec,
            &batch(vec![None, None, None]),
            &grid,
            &cancelled,
            &memory,
            &mut empty,
        )
        .unwrap();
        assert!(empty.is_empty());
        spec.partial_max = true;
        let mut maxima = BTreeMap::new();
        collect_packed(
            &spec,
            &batch(vec![Some(2.0), Some(-0.0), None]),
            &grid,
            &cancelled,
            &memory,
            &mut maxima,
        )
        .unwrap();
        collect_packed(
            &spec,
            &batch(vec![Some(1.0), Some(0.0), Some(f64::NAN)]),
            &grid,
            &cancelled,
            &memory,
            &mut maxima,
        )
        .unwrap();
        let values = maxima.values().next().unwrap();
        assert_eq!(values[0], Some(2.0));
        assert_eq!(values[1].unwrap().to_bits(), (-0.0f64).to_bits());
        assert!(values[2].unwrap().is_nan());
    }

    #[test]
    fn native_fragments_preserve_labels_merge_semantics_and_resource_limits() {
        use crate::research_series_grid::scan_range::SeriesGrid;
        use datafusion_common::ScalarValue;

        let mut spec = FragmentInput {
            key: SelectorKey {
                selector: "fragment".into(),
                function: None,
            },
            time: "ts".into(),
            value: "value".into(),
            tags: vec!["host".into(), "missing".into(), "empty".into()],
            metric: Some("metric".into()),
            packed: true,
            dense: Some(true),
            partial_max: false,
        };
        let series = |values: Vec<Option<f64>>| SeriesGrid {
            labels: vec![
                ScalarValue::Dictionary(
                    Box::new(DataType::UInt32),
                    Box::new(ScalarValue::Utf8(Some("a".into()))),
                ),
                ScalarValue::LargeUtf8(None),
                ScalarValue::Utf8View(Some(String::new())),
            ],
            values,
        };
        let grid = TimeGrid::new(0, 20, 10).unwrap();
        let cancelled = AtomicBool::new(false);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let memory = MemoryConsumer::new("native-fragments-test").register(&pool);
        for partial_max in [false, true] {
            spec.dense = Some(!partial_max);
            spec.partial_max = partial_max;
            let mut rows = BTreeMap::new();
            spec.collect_series(
                vec![
                    series(vec![Some(2.0), Some(-0.0), None]),
                    series(vec![Some(3.0), None, Some(f64::NAN)]),
                ],
                &grid,
                &cancelled,
                &memory,
                &mut rows,
            )
            .unwrap();
            let (labels, values) = rows.first_key_value().unwrap();
            assert_eq!(
                labels,
                &BTreeMap::from([
                    ("__name__".into(), "metric".into()),
                    ("host".into(), "a".into()),
                    ("empty".into(), String::new()),
                ])
            );
            assert_eq!(values[0], Some(if partial_max { 3.0 } else { 5.0 }));
            assert_eq!(values[1].unwrap().to_bits(), (-0.0f64).to_bits());
            assert!(values[2].unwrap().is_nan());
        }
        spec.dense = Some(false);
        spec.partial_max = false;
        let mut rows = BTreeMap::new();
        spec.collect_series(
            vec![series(vec![None; 3])],
            &grid,
            &cancelled,
            &memory,
            &mut rows,
        )
        .unwrap();
        assert!(rows.is_empty());
        spec.collect_series(
            vec![
                series(vec![Some(1.0), None, None]),
                series(vec![None, Some(2.0), None]),
            ],
            &grid,
            &cancelled,
            &memory,
            &mut rows,
        )
        .unwrap();
        assert_eq!(
            rows.values().next().unwrap(),
            &vec![Some(1.0), Some(2.0), None]
        );
        assert!(
            spec.collect_series(
                vec![series(vec![Some(1.0), None, None])],
                &grid,
                &cancelled,
                &memory,
                &mut rows
            )
            .unwrap_err()
            .to_string()
            .contains("Duplicate packed")
        );
        assert!(
            spec.collect_series(
                vec![series(vec![None])],
                &grid,
                &cancelled,
                &memory,
                &mut rows
            )
            .is_err()
        );
        let mut invalid = series(vec![Some(1.0), None, None]);
        invalid.labels.pop();
        assert!(
            spec.collect_series(vec![invalid], &grid, &cancelled, &memory, &mut rows)
                .is_err()
        );
        let mut invalid = series(vec![Some(1.0), None, None]);
        invalid.labels[0] = ScalarValue::Int64(Some(1));
        assert!(
            spec.collect_series(vec![invalid], &grid, &cancelled, &memory, &mut rows)
                .is_err()
        );
        let tiny_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let tiny = MemoryConsumer::new("native-fragments-limit").register(&tiny_pool);
        let mut empty = BTreeMap::new();
        assert!(
            spec.collect_series(
                vec![series(vec![Some(1.0); 3])],
                &grid,
                &cancelled,
                &tiny,
                &mut empty
            )
            .is_err()
        );
        assert!(empty.is_empty());
        cancelled.store(true, Ordering::Relaxed);
        assert!(
            spec.collect_series(
                vec![series(vec![Some(1.0); 3])],
                &grid,
                &cancelled,
                &memory,
                &mut empty
            )
            .is_err()
        );
        drop(rows);
        drop(memory);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn nested_sum_merges_partition_maxima_before_reducing_groups() {
        use crate::research_series_grid::vector::{Aggregate, aggregate};
        use datafusion::arrow::array::Float64Builder;
        use datafusion::arrow::array::ListBuilder;

        let batch = |host: &str, values: [Option<f64>; 3]| {
            let mut points = ListBuilder::new(Float64Builder::new());
            points.values().extend(values);
            points.append(true);
            RecordBatch::try_from_iter(vec![
                (
                    PACKED_POINTS,
                    Arc::new(points.finish()) as datafusion::arrow::array::ArrayRef,
                ),
                ("host", Arc::new(StringArray::from(vec![host]))),
            ])
            .unwrap()
        };
        let first = batch("a", [Some(2.0), None, Some(1.0)]);
        let schema = first.schema();
        let partitions = vec![
            vec![first, batch("b", [Some(4.0), Some(2.0), None])],
            vec![
                batch("a", [Some(3.0), Some(5.0), None]),
                batch("b", [Some(1.0), None, None]),
            ],
        ];
        let input = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&partitions, schema, None).unwrap(),
        )));
        let collected = collect(
            Arc::new(vec![FragmentInput {
                key: SelectorKey {
                    selector: "fragment".into(),
                    function: None,
                },
                time: "ts".into(),
                value: "value".into(),
                tags: vec!["host".into()],
                metric: None,
                packed: true,
                dense: Some(false),
                partial_max: true,
            }]),
            vec![input],
            Arc::new(TimeGrid::new(0, 20, 10).unwrap()),
            Arc::new(TaskContext::default()),
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();
        let series = collected.inputs.values().next().unwrap().series.clone();
        let result = aggregate(series, &[], false, Aggregate::Sum, 3).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values, vec![Some(7.0), Some(7.0), Some(1.0)]);
    }
}
