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

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::Range;

use ahash::{HashMap as AHashMap, HashMapExt};
use api::v1::{ColumnSchema, RowInsertRequest, RowInsertRequests, Rows, SemanticType};
use common_base::regex_pattern::NAME_PATTERN_REG;
use common_query::native_histogram::native_histogram_value_type;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::{KeyValue, any_value};
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{Metric, NumberDataPoint, metric};
use session::protocol_ctx::OtlpMetricCtx;
use snafu::{OptionExt, ensure};
use table::requests::{
    METADATA_QUALITY_DECLARED, SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_METRIC_TYPE,
};

use crate::error::{self, Result};
use crate::metrics::OTLP_EXPONENTIAL_HISTOGRAM_REJECTED_DATA_POINTS;
use crate::otlp::metrics::{
    MetricMetadata, MetricsConversion, OTEL_RESOURCE_INFO_TABLE_NAME, ResourceInfoData,
    add_accepted_data_points, encode_metrics, from_metric_type, has_no_recorded_value,
    process_resource_attrs, process_scope_attrs, reject_data_points, validate_sample_kind,
};
use crate::query_handler::MetricsIngestOutcome;
use crate::row_writer::{MultiTableData, TableData};
use crate::semantic::SemanticIndex;

/// Rows share exact input schemas while retaining their source-point boundaries.
pub struct PreparedMetrics {
    /// Destination/schema batches. Normalization must preserve batch and row order/counts.
    pub requests: Vec<RowInsertRequest>,
    points: Vec<PreparedPoint>,
    row_locations: Vec<(usize, usize)>,
    metrics: Vec<MetricMetadata>,
    resources: Vec<Vec<KeyValue>>,
    outcome: MetricsIngestOutcome,
}

struct PreparedPoint {
    metric: usize,
    resource: usize,
    timestamp: i64,
    rows: Range<usize>,
}

#[derive(Default)]
struct MetricWriter {
    tables: MultiTableData,
    // TableData schemas only grow. A new column invalidates the cached batch.
    batches: AHashMap<String, (usize, usize)>,
}

pub fn prepare_metrics(
    request: ExportMetricsServiceRequest,
    metric_ctx: &mut OtlpMetricCtx,
) -> Result<PreparedMetrics> {
    let mut prepared = PreparedMetrics {
        requests: Vec::new(),
        points: Vec::new(),
        row_locations: Vec::new(),
        metrics: Vec::new(),
        resources: Vec::new(),
        outcome: MetricsIngestOutcome::default(),
    };
    let mut schema_batches = AHashMap::<u64, Vec<usize>>::new();
    for resource in request.resource_metrics {
        let raw_attrs = resource.resource.map(|r| r.attributes).unwrap_or_default();
        let mut attrs = raw_attrs.clone();
        process_resource_attrs(&mut attrs, metric_ctx);
        let resource_index = prepared.resources.len();
        prepared.resources.push(if metric_ctx.resource_info {
            raw_attrs
        } else {
            Vec::new()
        });
        for scope in resource.scope_metrics {
            let scope_attrs = process_scope_attrs(&scope, metric_ctx);
            for mut metric in scope.metrics {
                let Some(data) = metric.data.as_ref() else {
                    continue;
                };
                metric_ctx.set_metric_type(from_metric_type(data));
                let metric_index = prepared.metrics.len();
                prepared
                    .metrics
                    .push(MetricMetadata::new(&metric, metric_ctx)?);
                let mut writer = MetricWriter::default();
                let mut layout: (Vec<String>, bool) = (Vec::new(), false);
                let mut writers = AHashMap::new();
                // Move each payload once; never clone the accumulated request.
                macro_rules! prepare_points {
                    ($data:ident, $variant:ident, $omits_value:expr) => {
                        for point in std::mem::take(&mut $data.data_points) {
                            // Omissions must not become explicit nulls in a retained schema.
                            let omits_value = ($omits_value)(&point);
                            if omits_value != layout.1
                                || !scalar_attribute_names(&point.attributes)
                                    .eq(layout.0.iter().map(String::as_str))
                            {
                                let next_layout = (
                                    scalar_attribute_names(&point.attributes)
                                        .map(str::to_owned)
                                        .collect(),
                                    omits_value,
                                );
                                let next_writer = writers.remove(&next_layout).unwrap_or_default();
                                let previous = std::mem::replace(&mut writer, next_writer);
                                let previous_layout = std::mem::replace(&mut layout, next_layout);
                                if !previous.batches.is_empty() {
                                    writers.insert(previous_layout, previous);
                                }
                            }
                            let timestamp = point.time_unix_nano;
                            let start = point.start_time_unix_nano;
                            $data.data_points.push(point);
                            metric.data = Some(metric::Data::$variant($data));
                            if !prepared.push_point(
                                &metric,
                                metric_index,
                                timestamp,
                                start,
                                &attrs,
                                scope_attrs.as_ref(),
                                resource_index,
                                metric_ctx,
                                &mut writer,
                                &mut schema_batches,
                            )? {
                                writer = MetricWriter::default();
                            }
                            let Some(metric::Data::$variant(mut remaining)) = metric.data.take()
                            else {
                                unreachable!()
                            };
                            remaining.data_points.clear();
                            $data = remaining;
                        }
                    };
                }
                let Some(data) = metric.data.take() else {
                    continue;
                };
                match data {
                    metric::Data::Gauge(mut data) => {
                        prepare_points!(data, Gauge, |point: &NumberDataPoint| point
                            .value
                            .is_none())
                    }
                    metric::Data::Sum(mut data) => {
                        prepare_points!(data, Sum, |point: &NumberDataPoint| point.value.is_none()
                            && !has_no_recorded_value(point.flags))
                    }
                    metric::Data::Histogram(mut data) => {
                        prepare_points!(data, Histogram, |_| false)
                    }
                    metric::Data::Summary(mut data) => prepare_points!(data, Summary, |_| false),
                    metric::Data::ExponentialHistogram(mut data) => {
                        prepare_points!(data, ExponentialHistogram, |_| false)
                    }
                }
            }
        }
    }
    Ok(prepared)
}

fn scalar_attribute_names(attrs: &[KeyValue]) -> impl Iterator<Item = &str> {
    attrs.iter().filter_map(|attr| {
        matches!(
            attr.value.as_ref()?.value.as_ref()?,
            any_value::Value::StringValue(_)
                | any_value::Value::BoolValue(_)
                | any_value::Value::IntValue(_)
                | any_value::Value::DoubleValue(_)
        )
        .then_some(attr.key.as_str())
    })
}

impl PreparedMetrics {
    #[allow(clippy::too_many_arguments)]
    fn push_point(
        &mut self,
        metric: &Metric,
        metric_index: usize,
        timestamp: u64,
        start: u64,
        resource_attrs: &Vec<KeyValue>,
        scope_attrs: Option<&Vec<KeyValue>>,
        resource: usize,
        ctx: &OtlpMetricCtx,
        writer: &mut MetricWriter,
        schema_batches: &mut AHashMap<u64, Vec<usize>>,
    ) -> Result<bool> {
        let metadata = &self.metrics[metric_index];
        let exponential = metadata.native_schema.is_some();
        // OTLP gauges ignore StartTimeUnixNano.
        let invalid_interval =
            !matches!(metric.data, Some(metric::Data::Gauge(_))) && start > timestamp;
        let mut outcome = MetricsIngestOutcome::default();
        let encoded = if !exponential && (timestamp > i64::MAX as u64 || invalid_interval) {
            Err(error::InvalidOtlpMetricInputSnafu {
                reason: "invalid OTLP data-point timestamp or interval",
            }
            .build())
        } else {
            encode_metrics(
                &mut writer.tables,
                metric,
                metadata,
                Some(resource_attrs),
                scope_attrs,
                ctx,
                &mut outcome,
            )
        };
        if let Err(err) = encoded {
            match err {
                error::Error::InvalidOtlpMetricInput { .. }
                | error::Error::IncompatibleSchema { .. }
                | error::Error::TimestampOverflow { .. }
                | error::Error::TimePrecision { .. }
                | error::Error::InvalidParameter { .. } => {
                    self.reject(exponential, err.to_string())?;
                    return Ok(false);
                }
                _ => return Err(err),
            }
        }
        if outcome.rejected_data_points != 0 {
            // Payload encoders already counted histogram-specific rejections.
            reject_data_points(&mut self.outcome, 1, || {
                outcome.error_message.unwrap_or_default()
            })?;
            return Ok(false);
        }
        if let Some((name, _)) = writer.tables.iter().find(|(name, table)| {
            table.num_rows() != 0
                && !writer.batches.contains_key(*name)
                && !NAME_PATTERN_REG.is_match(name)
        }) {
            self.reject(exponential, format!("invalid output table name `{name}`"))?;
            return Ok(false);
        }
        let first_row = self.row_locations.len();
        for (name, schema, rows) in writer.tables.drain_rows() {
            let batch = match writer.batches.get(name) {
                Some(&(columns, batch)) if columns == schema.len() => batch,
                _ => {
                    let mut hash = schema_batches.hasher().build_hasher();
                    name.hash(&mut hash);
                    for column in schema {
                        (&column.column_name, column.datatype, column.semantic_type)
                            .hash(&mut hash);
                    }
                    let candidates = schema_batches.entry(hash.finish()).or_default();
                    let batch = candidates.iter().copied().find(|&index| {
                        self.requests[index].table_name == name
                            && self.requests[index]
                                .rows
                                .as_ref()
                                .is_some_and(|rows| rows.schema == schema)
                    });
                    let batch = if let Some(batch) = batch {
                        batch
                    } else {
                        validate_sample_kind(name, schema)?;
                        let batch = self.requests.len();
                        self.requests.push(RowInsertRequest {
                            table_name: name.to_owned(),
                            rows: Some(Rows {
                                schema: schema.to_vec(),
                                rows: Vec::new(),
                            }),
                        });
                        candidates.push(batch);
                        batch
                    };
                    writer
                        .batches
                        .insert(name.to_owned(), (schema.len(), batch));
                    batch
                }
            };
            let output = self.requests[batch]
                .rows
                .as_mut()
                .context(error::InternalSnafu {
                    err_msg: "missing prepared batch",
                })?;
            for row in rows {
                self.row_locations.push((batch, output.rows.len()));
                output.rows.push(row);
            }
        }
        self.points.push(PreparedPoint {
            metric: metric_index,
            resource,
            timestamp: timestamp as i64,
            rows: first_row..self.row_locations.len(),
        });
        Ok(true)
    }

    fn reject(&mut self, exponential: bool, reason: String) -> Result<()> {
        reject_data_points(&mut self.outcome, 1, || reason)?;
        if exponential {
            OTLP_EXPONENTIAL_HISTOGRAM_REJECTED_DATA_POINTS
                .with_label_values(&["invalid_data_point"])
                .inc();
        }
        Ok(())
    }

    /// Number of source points that survived payload conversion.
    pub fn num_points(&self) -> usize {
        self.points.len()
    }

    fn validate_batches(&self) -> Result<()> {
        let mut counts = vec![0; self.requests.len()];
        for &(batch, row) in &self.row_locations {
            let count = counts.get_mut(batch).context(error::InternalSnafu {
                err_msg: "missing prepared batch",
            })?;
            ensure!(
                row == *count,
                error::InternalSnafu {
                    err_msg: "invalid prepared row order",
                }
            );
            *count += 1;
        }
        ensure!(
            self.requests.iter().zip(counts).all(|(request, count)| {
                count != 0
                    && request
                        .rows
                        .as_ref()
                        .is_some_and(|rows| rows.rows.len() == count)
            }),
            error::InternalSnafu {
                err_msg: "OTLP prepared batch row counts changed"
            }
        );
        Ok(())
    }

    /// Rejects a source point if any row in its fan-out failed destination validation.
    pub fn point_rejections(&self, rows: Vec<Vec<Option<String>>>) -> Result<Vec<Option<String>>> {
        self.validate_batches()?;
        ensure!(
            rows.len() == self.requests.len()
                && rows.iter().zip(&self.requests).all(|(errors, request)| {
                    request
                        .rows
                        .as_ref()
                        .is_some_and(|rows| errors.len() == rows.rows.len())
                }),
            error::InternalSnafu {
                err_msg: "OTLP batch rejections must match prepared rows"
            }
        );
        Ok(self
            .points
            .iter()
            .map(|point| {
                self.row_locations[point.rows.clone()]
                    .iter()
                    .find_map(|&(batch, row)| rows[batch][row].clone())
            })
            .collect())
    }

    /// Complete authoritative validation before calling this method. Fresh
    /// logical and physical conflicts are then collected from one survivor set.
    pub fn finish(
        mut self,
        mut rejections: Vec<Option<String>>,
        physical_tables: &HashMap<String, String>,
    ) -> Result<MetricsConversion> {
        ensure!(
            rejections.len() == self.points.len(),
            error::InternalSnafu {
                err_msg: "OTLP prepared points and rejections must have equal lengths",
            }
        );
        self.validate_batches()?;
        let conflicts = conflicting_points(
            &self.requests,
            &self.points,
            &self.row_locations,
            &rejections,
            physical_tables,
        );
        for index in conflicts {
            rejections[index] =
                Some("incompatible metric sample kinds or column definitions".to_string());
        }
        let reuse_batches = rejections.iter().all(Option::is_none) && {
            let mut tables = HashSet::new();
            self.requests.len() <= 1 || self.requests.iter().all(|r| tables.insert(&r.table_name))
        };
        let mut outputs: Vec<(String, TableData)> = Vec::new();
        let mut output_indices = AHashMap::new();
        let mut mappings: Vec<Option<(usize, Vec<usize>, bool)>> = if reuse_batches {
            Vec::new()
        } else {
            vec![None; self.requests.len()]
        };
        let mut semantic_index = SemanticIndex::default();
        let mut resource_info = ResourceInfoData::default();
        let mut resource_times = vec![Vec::new(); self.resources.len()];
        for (point, rejection) in std::mem::take(&mut self.points).into_iter().zip(rejections) {
            if let Some(reason) = rejection {
                let metadata = &self.metrics[point.metric];
                self.reject(
                    metadata.native_schema.is_some(),
                    format!("metric `{}`: {reason}", metadata.original_name),
                )?;
                continue;
            }
            for &(batch, row) in &self.row_locations[point.rows] {
                let request = &mut self.requests[batch];
                semantic_index.merge_table_from(
                    &mut self.metrics[point.metric].semantics,
                    &request.table_name,
                );
                if reuse_batches {
                    continue;
                }
                let rows = request.rows.as_mut().context(error::InternalSnafu {
                    err_msg: "missing prepared batch",
                })?;
                if mappings[batch].is_none() {
                    let output = *output_indices
                        .entry(request.table_name.clone())
                        .or_insert_with(|| {
                            let index = outputs.len();
                            outputs.push((
                                request.table_name.clone(),
                                TableData::new(rows.schema.len(), rows.rows.len()),
                            ));
                            index
                        });
                    let columns = std::mem::take(&mut rows.schema)
                        .into_iter()
                        .map(|column| outputs[output].1.ensure_column(column))
                        .collect::<Result<Vec<_>>>()?;
                    let identity = columns
                        .iter()
                        .enumerate()
                        .all(|(index, &column)| index == column);
                    mappings[batch] = Some((output, columns, identity));
                }
                let (output, columns, identity) =
                    mappings[batch].as_ref().context(error::InternalSnafu {
                        err_msg: "missing output column mapping",
                    })?;
                let table = &mut outputs[*output].1;
                let values = std::mem::take(&mut rows.rows[row].values);
                if *identity {
                    table.add_row(values);
                } else {
                    let mut output = table.alloc_one_row();
                    for (value, &column) in values.into_iter().zip(columns) {
                        output[column] = value;
                    }
                    table.add_row(output);
                }
            }
            add_accepted_data_points(&mut self.outcome, 1)?;
            if !self.resources[point.resource].is_empty() {
                resource_times[point.resource].push(point.timestamp);
            }
        }
        for (attrs, times) in self.resources.iter().zip(resource_times) {
            if !times.is_empty() {
                resource_info.observe_times(attrs, times);
            }
        }
        let requests = RowInsertRequests {
            inserts: if reuse_batches {
                std::mem::take(&mut self.requests)
            } else {
                outputs
                    .into_iter()
                    .map(|(table_name, table)| RowInsertRequest {
                        table_name,
                        rows: Some(table.into_rows()),
                    })
                    .collect()
            },
        };
        let rows = requests
            .inserts
            .iter()
            .filter_map(|r| r.rows.as_ref())
            .map(|rows| rows.rows.len())
            .sum();
        let resource_info = if !resource_info.is_empty()
            && requests
                .inserts
                .iter()
                .any(|r| r.table_name == OTEL_RESOURCE_INFO_TABLE_NAME)
        {
            common_telemetry::warn!(
                "Skipping OTLP resource descriptor synthesis: a metric uses its table name"
            );
            None
        } else {
            resource_info.into_row_insert_requests()?
        };
        if resource_info.is_some() {
            semantic_index.record_scalar(
                OTEL_RESOURCE_INFO_TABLE_NAME,
                SEMANTIC_METRIC_TYPE,
                crate::semantic::METRIC_TYPE_INFO,
            );
            semantic_index.record_scalar(
                OTEL_RESOURCE_INFO_TABLE_NAME,
                SEMANTIC_METRIC_METADATA_QUALITY,
                METADATA_QUALITY_DECLARED,
            );
        }
        Ok(MetricsConversion {
            requests,
            rows,
            semantic_index,
            resource_info,
            outcome: self.outcome,
        })
    }
}

fn conflicting_points(
    requests: &[RowInsertRequest],
    points: &[PreparedPoint],
    row_locations: &[(usize, usize)],
    rejections: &[Option<String>],
    physical_tables: &HashMap<String, String>,
) -> HashSet<usize> {
    let mut contributors = vec![Vec::new(); requests.len()];
    for (index, point) in points
        .iter()
        .enumerate()
        .filter(|(index, _)| rejections[*index].is_none())
    {
        for &(batch, _) in &row_locations[point.rows.clone()] {
            if contributors[batch].last() != Some(&index) {
                contributors[batch].push(index);
            }
        }
    }
    let mut columns = AHashMap::<_, Vec<(usize, &ColumnSchema)>>::new();
    let mut kinds: AHashMap<&str, Vec<(usize, bool)>> = AHashMap::new();
    for (batch, request) in requests
        .iter()
        .enumerate()
        .filter(|(batch, _)| !contributors[*batch].is_empty())
    {
        let Some(rows) = &request.rows else { continue };
        for column in &rows.schema {
            columns
                .entry((false, &request.table_name, &column.column_name))
                .or_default()
                .push((batch, column));
            if let Some(physical) = physical_tables.get(&request.table_name) {
                columns
                    .entry((true, physical, &column.column_name))
                    .or_default()
                    .push((batch, column));
            }
            if column.semantic_type == SemanticType::Field as i32 {
                let native = api::helper::is_column_type_value_eq(
                    column.datatype,
                    column.datatype_extension.clone(),
                    native_histogram_value_type(),
                );
                kinds
                    .entry(&request.table_name)
                    .or_default()
                    .push((batch, native));
            }
        }
    }
    let mut rejected = HashSet::new();
    for group in columns.values() {
        let first = group[0].1;
        if group.iter().any(|(_, column)| {
            column.datatype != first.datatype
                || column.datatype_extension != first.datatype_extension
                || column.semantic_type != first.semantic_type
        }) {
            for (batch, _) in group {
                rejected.extend(contributors[*batch].iter().copied());
            }
        }
    }
    for group in kinds.values() {
        if group.iter().any(|(_, native)| *native) && group.iter().any(|(_, native)| !*native) {
            for (batch, _) in group {
                rejected.extend(contributors[*batch].iter().copied());
            }
        }
    }
    rejected
}
