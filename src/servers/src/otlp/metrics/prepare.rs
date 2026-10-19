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

use api::v1::{ColumnSchema, RowInsertRequests, SemanticType};
use common_base::regex_pattern::NAME_PATTERN_REG;
use common_query::native_histogram::native_histogram_value_type;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::KeyValue;
use otel_arrow_rust::proto::opentelemetry::metrics::v1::{Metric, metric};
use session::protocol_ctx::OtlpMetricCtx;
use table::requests::{
    METADATA_QUALITY_DECLARED, SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_METRIC_TYPE,
};

use crate::error::{self, Result};
use crate::metrics::OTLP_EXPONENTIAL_HISTOGRAM_REJECTED_DATA_POINTS;
use crate::otlp::metrics::{
    MetricsConversion, OTEL_RESOURCE_INFO_TABLE_NAME, ResourceInfoData, add_accepted_data_points,
    emitted_semantic_tables, encode_metrics, from_metric_type, legacy_normalize_otlp_name,
    process_resource_attrs, process_scope_attrs, record_metric_semantics, reject_data_points,
    translate_metric_name, validate_sample_kinds,
};
use crate::query_handler::MetricsIngestOutcome;
use crate::row_writer::{self, MultiTableData};
use crate::semantic::SemanticIndex;

/// Points stay separate until destination validation has resolved omissions and
/// rejected complete fan-outs. Vector order is the original source order.
pub struct PreparedMetrics {
    pub requests: Vec<RowInsertRequests>,
    points: Vec<PreparedPoint>,
    resources: Vec<Vec<KeyValue>>,
    outcome: MetricsIngestOutcome,
}

struct PreparedPoint {
    original_name: String,
    exponential: bool,
    semantics: SemanticIndex,
    resource: usize,
    timestamp: i64,
}

pub fn prepare_metrics(
    request: ExportMetricsServiceRequest,
    metric_ctx: &mut OtlpMetricCtx,
) -> Result<PreparedMetrics> {
    let mut prepared = PreparedMetrics {
        requests: Vec::new(),
        points: Vec::new(),
        resources: Vec::new(),
        outcome: MetricsIngestOutcome::default(),
    };
    for resource in request.resource_metrics {
        let raw_attrs = resource.resource.map(|r| r.attributes).unwrap_or_default();
        let mut attrs = raw_attrs.clone();
        process_resource_attrs(&mut attrs, metric_ctx);
        let resource_index = prepared.resources.len();
        prepared
            .resources
            .push(if metric_ctx.resource_info && !metric_ctx.is_legacy {
                raw_attrs
            } else {
                Vec::new()
            });
        for scope in resource.scope_metrics {
            let scope_attrs = process_scope_attrs(&scope, metric_ctx);
            for mut metric in scope.metrics {
                let Some(data) = metric.data.take() else {
                    continue;
                };
                metric_ctx.set_metric_type(from_metric_type(&data));
                // Move each payload once; never clone the accumulated request.
                macro_rules! prepare_points {
                    ($data:ident, $variant:ident) => {
                        for point in std::mem::take(&mut $data.data_points) {
                            let timestamp = point.time_unix_nano;
                            let start = point.start_time_unix_nano;
                            $data.data_points.push(point);
                            metric.data = Some(metric::Data::$variant($data));
                            prepared.push_point(
                                &metric,
                                timestamp,
                                start,
                                &attrs,
                                scope_attrs.as_ref(),
                                resource_index,
                                metric_ctx,
                            )?;
                            let Some(metric::Data::$variant(mut remaining)) = metric.data.take()
                            else {
                                unreachable!()
                            };
                            remaining.data_points.clear();
                            $data = remaining;
                        }
                    };
                }
                match data {
                    metric::Data::Gauge(mut data) => prepare_points!(data, Gauge),
                    metric::Data::Sum(mut data) => prepare_points!(data, Sum),
                    metric::Data::Histogram(mut data) => prepare_points!(data, Histogram),
                    metric::Data::Summary(mut data) => prepare_points!(data, Summary),
                    metric::Data::ExponentialHistogram(mut data) => {
                        prepare_points!(data, ExponentialHistogram)
                    }
                }
            }
        }
    }
    Ok(prepared)
}

impl PreparedMetrics {
    #[allow(clippy::too_many_arguments)]
    fn push_point(
        &mut self,
        metric: &Metric,
        timestamp: u64,
        start: u64,
        resource_attrs: &Vec<KeyValue>,
        scope_attrs: Option<&Vec<KeyValue>>,
        resource: usize,
        ctx: &OtlpMetricCtx,
    ) -> Result<()> {
        let exponential = matches!(metric.data, Some(metric::Data::ExponentialHistogram(_)));
        // OTLP gauges ignore StartTimeUnixNano.
        let invalid_interval =
            !matches!(metric.data, Some(metric::Data::Gauge(_))) && start > timestamp;
        let mut writer = MultiTableData::default();
        let mut outcome = MetricsIngestOutcome::default();
        let encoded = if !exponential && (timestamp > i64::MAX as u64 || invalid_interval) {
            Err(error::InvalidOtlpMetricInputSnafu {
                reason: "invalid OTLP data-point timestamp or interval",
            }
            .build())
        } else {
            encode_metrics(
                &mut writer,
                metric,
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
                    return Ok(());
                }
                _ => return Err(err),
            }
        }
        if outcome.rejected_data_points != 0 {
            // Payload encoders already counted histogram-specific rejections.
            reject_data_points(&mut self.outcome, 1, || {
                outcome.error_message.unwrap_or_default()
            })?;
            return Ok(());
        }
        let (mut requests, _) = writer.into_row_insert_requests();
        requests
            .inserts
            .retain(|request| request.rows.as_ref().is_some_and(|r| !r.rows.is_empty()));
        if let Some(request) = requests
            .inserts
            .iter()
            .find(|r| !NAME_PATTERN_REG.is_match(&r.table_name))
        {
            self.reject(
                exponential,
                format!("invalid output table name `{}`", request.table_name),
            )?;
            return Ok(());
        }
        validate_sample_kinds(&requests)?;
        let name = if ctx.is_legacy {
            legacy_normalize_otlp_name(&metric.name)
        } else {
            translate_metric_name(metric, &ctx.metric_type, ctx.metric_translation_strategy)
        };
        let mut emitted = emitted_semantic_tables(&ctx.metric_type, ctx.is_legacy, &name);
        emitted.retain(|(name, _)| requests.inserts.iter().any(|r| &r.table_name == name));
        let mut semantics = SemanticIndex::default();
        record_metric_semantics(&mut semantics, metric, &name, &emitted);
        self.points.push(PreparedPoint {
            original_name: metric.name.clone(),
            exponential,
            semantics,
            resource,
            timestamp: timestamp as i64,
        });
        self.requests.push(requests);
        Ok(())
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

    /// Complete authoritative validation before calling this method. Fresh
    /// logical and physical conflicts are then collected from one survivor set.
    pub fn finish(
        mut self,
        mut rejections: Vec<Option<String>>,
        physical_tables: &HashMap<String, String>,
    ) -> Result<MetricsConversion> {
        let conflicts = conflicting_points(&self.requests, &rejections, physical_tables);
        for index in conflicts {
            rejections[index] =
                Some("incompatible metric sample kinds or column definitions".to_string());
        }
        let mut writer = MultiTableData::default();
        let mut semantic_index = SemanticIndex::default();
        let mut resource_info = ResourceInfoData::default();
        let mut input_names = HashSet::new();
        for ((requests, point), rejection) in std::mem::take(&mut self.requests)
            .into_iter()
            .zip(std::mem::take(&mut self.points))
            .zip(rejections)
        {
            if let Some(reason) = rejection {
                self.reject(
                    point.exponential,
                    format!("metric `{}`: {reason}", point.original_name),
                )?;
                continue;
            }
            for request in requests.inserts {
                let Some(rows) = request.rows else { continue };
                let table = writer.get_or_default_table_data(
                    request.table_name,
                    rows.schema.len(),
                    rows.rows.len(),
                );
                for row in rows.rows {
                    let mut output = table.alloc_one_row();
                    row_writer::write_by_schema(
                        table,
                        rows.schema
                            .iter()
                            .cloned()
                            .zip(row.values.into_iter().map(|v| v.value_data)),
                        &mut output,
                    )?;
                    table.add_row(output);
                }
            }
            add_accepted_data_points(&mut self.outcome, 1)?;
            input_names.insert(point.original_name);
            semantic_index.merge_from(&point.semantics);
            resource_info.observe_times(&self.resources[point.resource], [point.timestamp]);
        }
        let (requests, rows) = writer.into_row_insert_requests();
        let resource_info = if requests
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
            input_names: input_names.into_iter().collect(),
        })
    }
}

fn conflicting_points(
    requests: &[RowInsertRequests],
    rejections: &[Option<String>],
    physical_tables: &HashMap<String, String>,
) -> HashSet<usize> {
    let mut columns = HashMap::<_, Vec<(usize, &ColumnSchema)>>::new();
    let mut kinds: HashMap<&str, Vec<(usize, bool)>> = HashMap::new();
    for (index, point) in requests
        .iter()
        .enumerate()
        .filter(|(i, _)| rejections[*i].is_none())
    {
        for request in &point.inserts {
            let Some(rows) = &request.rows else { continue };
            for column in &rows.schema {
                columns
                    .entry((false, &request.table_name, &column.column_name))
                    .or_default()
                    .push((index, column));
                if let Some(physical) = physical_tables.get(&request.table_name) {
                    columns
                        .entry((true, physical, &column.column_name))
                        .or_default()
                        .push((index, column));
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
                        .push((index, native));
                }
            }
        }
    }
    let mut rejected = HashSet::new();
    for group in columns.values() {
        let first = group[0].1;
        if group.iter().any(|(_, c)| {
            c.datatype != first.datatype
                || c.datatype_extension != first.datatype_extension
                || c.semantic_type != first.semantic_type
        }) {
            rejected.extend(group.iter().map(|(i, _)| i));
        }
    }
    for group in kinds.values() {
        if group.iter().any(|(_, native)| *native) && group.iter().any(|(_, native)| !*native) {
            rejected.extend(group.iter().map(|(i, _)| i));
        }
    }
    rejected
}
