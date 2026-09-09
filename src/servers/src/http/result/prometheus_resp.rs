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

//! prom supply the prometheus HTTP API Server compliance
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasher;

use arrow::array::{Array, ArrayRef, AsArray, StructArray};
use arrow::datatypes::{Float64Type, TimestampMillisecondType};
use arrow_schema::DataType;
use axum::Json;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::native_histogram::{
    NativeHistogram, is_native_histogram_value_type, read_histogram,
};
use common_query::prometheus::{format_prometheus_float, is_prometheus_stale_nan};
use common_query::promql_annotations::{
    PromqlAnnotationCollector, get_promql_annotation_collector,
};
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datatypes::arrow_array::string_array_value_at_index;
use datatypes::prelude::ConcreteDataType;
use indexmap::IndexMap;
use indexmap::map::RawEntryApiV1;
use indexmap::map::raw_entry_v1::RawEntryMut;
use itertools::Either;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::value::ValueType;
use ryu::Buffer;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ArrowSnafu, CollectRecordbatchSnafu, DataFusionSnafu, Result, UnexpectedResultSnafu,
    status_code_to_http_status,
};
use crate::http::header::{GREPTIME_DB_HEADER_METRICS, collect_plan_metrics};
use crate::http::prometheus::{
    PromData, PromNativeHistogram, PromQueryResult, PromSeriesMatrix, PromSeriesVector,
    PrometheusResponse,
};

#[derive(Default)]
struct PromSeriesSamples {
    values: Vec<(f64, PromSampleValue)>,
    histograms: Vec<(f64, PromNativeHistogram)>,
}

/// A sample value of the Prometheus HTTP API JSON format.
///
/// Samples read out of a query result are kept as `f64` and formatted while the
/// response is serialized, which avoids one `String` per sample. Samples parsed
/// from a JSON body keep their original spelling, so a response that is
/// deserialized and serialized again is unchanged.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PromSampleValue {
    #[serde(skip_deserializing)]
    Number(f64),
    Text(String),
}

impl Serialize for PromSampleValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Number(value) if value.is_finite() => {
                serializer.serialize_str(Buffer::new().format_finite(*value))
            }
            Self::Number(value) => serializer.collect_str(value),
            Self::Text(value) => serializer.serialize_str(value),
        }
    }
}

impl PromSampleValue {
    fn into_string(self) -> String {
        match self {
            Self::Number(value) => format_prometheus_sample_value(value),
            Self::Text(value) => value,
        }
    }
}

fn prometheus_native_histogram(histogram: &NativeHistogram) -> Result<PromNativeHistogram> {
    Ok(PromNativeHistogram {
        count: format_prometheus_float(histogram.count),
        sum: format_prometheus_float(histogram.sum),
        buckets: histogram
            .to_prometheus_buckets()
            .context(UnexpectedResultSnafu {
                reason: "native histogram cannot be converted to Prometheus buckets",
            })?,
    })
}

/// Formats a sample value for the Prometheus HTTP API.
///
/// Finite sample strings use ryu's shortest-roundtrip representation and may
/// differ textually from previous Rust/Prometheus wire formatting; values parse
/// identically. Non-finite values retain Rust's `f64::to_string()` output.
fn format_prometheus_sample_value(value: f64) -> String {
    if value.is_finite() {
        Buffer::new().format_finite(value).to_string()
    } else {
        value.to_string()
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PrometheusJsonResponse {
    pub status: String,
    #[serde(skip_serializing_if = "PrometheusResponse::is_none")]
    #[serde(default)]
    pub data: PrometheusResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "errorType")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warnings: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infos: Option<Vec<String>>,

    #[serde(skip)]
    pub status_code: Option<StatusCode>,
    // placeholder for header value
    #[serde(skip)]
    #[serde(default)]
    pub resp_metrics: HashMap<String, Value>,
}

impl IntoResponse for PrometheusJsonResponse {
    fn into_response(self) -> Response {
        let metrics = if self.resp_metrics.is_empty() {
            None
        } else {
            serde_json::to_string(&self.resp_metrics).ok()
        };

        let http_code = self.status_code.map(|c| status_code_to_http_status(&c));

        let mut resp = Json(self).into_response();

        if let Some(http_code) = http_code {
            *resp.status_mut() = http_code;
        }

        if let Some(m) = metrics.and_then(|m| HeaderValue::from_str(&m).ok()) {
            resp.headers_mut().insert(&GREPTIME_DB_HEADER_METRICS, m);
        }

        resp
    }
}

impl PrometheusJsonResponse {
    pub fn error<S1>(error_type: StatusCode, reason: S1) -> Self
    where
        S1: Into<String>,
    {
        PrometheusJsonResponse {
            status: "error".to_string(),
            data: PrometheusResponse::None,
            error: Some(reason.into()),
            error_type: Some(error_type.to_string()),
            warnings: None,
            infos: None,
            resp_metrics: Default::default(),
            status_code: Some(error_type),
        }
    }

    pub fn success(data: PrometheusResponse) -> Self {
        PrometheusJsonResponse {
            status: "success".to_string(),
            data,
            error: None,
            error_type: None,
            warnings: None,
            infos: None,
            resp_metrics: Default::default(),
            status_code: None,
        }
    }

    /// Adds collected PromQL warnings and infos to the response.
    fn append_promql_annotations(&mut self, collector: &PromqlAnnotationCollector) {
        let mut warnings = self.warnings.take().unwrap_or_default();
        let mut infos = self.infos.take().unwrap_or_default();
        collector.append_to(&mut warnings, &mut infos);
        self.warnings = (!warnings.is_empty()).then_some(warnings);
        self.infos = (!infos.is_empty()).then_some(infos);
    }

    /// Merges data and annotations from another expanded PromQL query response.
    pub(crate) fn append_query_response(&mut self, mut other: Self) {
        self.data.append(other.data);
        merge_annotations(&mut self.warnings, other.warnings.take());
        merge_annotations(&mut self.infos, other.infos.take());
    }

    /// Convert from `Result<Output>`
    pub async fn from_query_result(
        result: Result<Output>,
        metric_name: Option<String>,
        result_type: ValueType,
        query_id: Option<&str>,
    ) -> Self {
        // Hold the collector while a streaming result is consumed.
        let collector = query_id.and_then(get_promql_annotation_collector);
        let response: Result<Self> = try {
            let result = result?;
            let mut resp =
                match result.data {
                    OutputData::RecordBatches(batches) => Self::success(
                        Self::record_batches_to_data(batches, metric_name, result_type)?,
                    ),
                    OutputData::Stream(stream) => {
                        let record_batches = RecordBatches::try_collect(stream)
                            .await
                            .context(CollectRecordbatchSnafu)?;
                        Self::success(Self::record_batches_to_data(
                            record_batches,
                            metric_name,
                            result_type,
                        )?)
                    }
                    OutputData::AffectedRows(_) => Self::error(
                        StatusCode::Unexpected,
                        "expected data result, but got affected rows",
                    ),
                };

            if let Some(physical_plan) = result.meta.plan {
                let mut result_map = HashMap::new();
                let mut tmp = vec![&mut result_map];
                collect_plan_metrics(&physical_plan, &mut tmp);

                let re = result_map
                    .into_iter()
                    .map(|(k, v)| (k, Value::from(v)))
                    .collect();
                resp.resp_metrics = re;
            }

            resp
        };

        let result_type_string = result_type.to_string();

        let mut response = match response {
            Ok(resp) => resp,
            Err(err) => {
                // Prometheus won't report error if querying nonexist label and metric
                if err.status_code() == StatusCode::TableNotFound
                    || err.status_code() == StatusCode::TableColumnNotFound
                {
                    Self::success(PrometheusResponse::PromData(PromData {
                        result_type: result_type_string,
                        ..Default::default()
                    }))
                } else {
                    Self::error(err.status_code(), err.output_msg())
                }
            }
        };
        if let Some(collector) = collector {
            response.append_promql_annotations(&collector);
        }
        response
    }

    /// Convert [RecordBatches] to [PromData]
    fn record_batches_to_data(
        batches: RecordBatches,
        metric_name: Option<String>,
        result_type: ValueType,
    ) -> Result<PrometheusResponse> {
        // Return empty result if no batches
        if batches.iter().next().is_none() {
            return Ok(PrometheusResponse::PromData(PromData {
                result_type: result_type.to_string(),
                ..Default::default()
            }));
        }

        // infer semantic type of each column from schema.
        // TODO(ruihang): wish there is a better way to do this.
        let mut timestamp_column_index = None;
        let mut tag_column_indices = Vec::new();
        let mut first_field_column_index = None;
        let mut native_histogram_column_index = None;

        let mut num_label_columns = 0;

        for (i, column) in batches.schema().column_schemas().iter().enumerate() {
            match column.data_type {
                ConcreteDataType::Timestamp(datatypes::types::TimestampType::Millisecond(_))
                    if timestamp_column_index.is_none() =>
                {
                    timestamp_column_index = Some(i);
                }
                // Treat all value types as field
                ConcreteDataType::Float32(_)
                | ConcreteDataType::Float64(_)
                | ConcreteDataType::Int8(_)
                | ConcreteDataType::Int16(_)
                | ConcreteDataType::Int32(_)
                | ConcreteDataType::Int64(_)
                | ConcreteDataType::UInt8(_)
                | ConcreteDataType::UInt16(_)
                | ConcreteDataType::UInt32(_)
                | ConcreteDataType::UInt64(_)
                    if first_field_column_index.is_none() =>
                {
                    first_field_column_index = Some(i);
                }
                _ if native_histogram_column_index.is_none()
                    && is_native_histogram_value_type(&column.data_type) =>
                {
                    native_histogram_column_index = Some(i);
                }
                ConcreteDataType::String(_) => {
                    tag_column_indices.push(i);
                    num_label_columns += 1;
                }
                _ => {}
            }
        }

        let timestamp_column_index = timestamp_column_index.context(UnexpectedResultSnafu {
            reason: "no timestamp column found".to_string(),
        })?;
        if first_field_column_index.is_none() && native_histogram_column_index.is_none() {
            return UnexpectedResultSnafu {
                reason: "no value column found".to_string(),
            }
            .fail();
        }

        // Preserves the order of output tags.
        // Tag order matters, e.g., after sorc and sort_desc, the output order must be kept.
        let mut buffer = IndexMap::<Vec<(&str, &str)>, PromSeriesSamples>::new();

        // Only a series that is new to `buffer` needs its own key vector.
        let mut tags = Vec::with_capacity(num_label_columns + 1);

        let schema = batches.schema();
        for batch in batches.iter() {
            // prepare things...
            let tag_columns = tag_column_indices
                .iter()
                .map(|i| batch.column(*i))
                .collect::<Vec<_>>();
            let tag_names = tag_column_indices
                .iter()
                .map(|c| schema.column_name_by_index(*c))
                .collect::<Vec<_>>();
            let timestamp_column = batch
                .column(timestamp_column_index)
                .as_primitive::<TimestampMillisecondType>();

            let field_array = first_field_column_index
                .map(|index| arrow::compute::cast(batch.column(index), &DataType::Float64))
                .transpose()
                .context(ArrowSnafu)?;
            let field_column = field_array
                .as_ref()
                .map(|array| array.as_primitive::<Float64Type>());
            let native_histogram_column = native_histogram_column_index
                .map(|index| {
                    batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .with_context(|| UnexpectedResultSnafu {
                            reason: "native histogram column is not a struct array",
                        })
                })
                .transpose()?;

            // Read the labels once per run of rows that share them instead of
            // once per row. `partition` marks every boundary, so the probe only
            // decides whether looking for runs is worth its cost.
            let label_runs = if !prefer_label_runs(&tag_columns, batch.num_rows()) {
                Either::Left((0..batch.num_rows()).map(|row| row..row + 1))
            } else {
                let runs = if tag_columns.is_empty() {
                    // Without labels the whole batch is a single series.
                    std::iter::once(0..batch.num_rows()).collect()
                } else {
                    let columns = tag_columns
                        .iter()
                        .map(|column| (*column).clone())
                        .collect::<Vec<_>>();
                    arrow::compute::partition(&columns)
                        .context(ArrowSnafu)?
                        .ranges()
                };
                Either::Right(runs.into_iter())
            };

            // assemble rows
            for run in label_runs {
                let mut run_entry_index = None;
                for row_index in run {
                    let value = field_column.and_then(|field_column| {
                        if !field_column.is_valid(row_index) {
                            return None;
                        }
                        let value = field_column.value(row_index);
                        (!is_prometheus_stale_nan(value))
                            .then_some((timestamp_column.value(row_index), value))
                    });
                    let histogram = native_histogram_column
                        .and_then(|column| {
                            read_histogram(column, row_index)
                                .context(DataFusionSnafu)
                                .transpose()
                        })
                        .transpose()?
                        .filter(|histogram| !is_prometheus_stale_nan(histogram.sum))
                        .map(|histogram| {
                            prometheus_native_histogram(&histogram)
                                .map(|histogram| (timestamp_column.value(row_index), histogram))
                        })
                        .transpose()?;

                    if value.is_none() && histogram.is_none() {
                        continue;
                    }

                    let entry_index = match run_entry_index {
                        Some(index) => index,
                        None => {
                            // retrieve tags
                            tags.clear();
                            if let Some(metric_name) = &metric_name {
                                tags.push((METRIC_NAME, metric_name.as_str()));
                            }
                            for (tag_column, tag_name) in tag_columns.iter().zip(tag_names.iter()) {
                                if let Some(tag_value) =
                                    string_array_value_at_index(tag_column, row_index)
                                {
                                    tags.push((tag_name, tag_value));
                                }
                            }

                            let hash = buffer.hasher().hash_one(&tags);
                            let entry = buffer
                                .raw_entry_mut_v1()
                                .from_key_hashed_nocheck(hash, &tags);
                            let index = entry.index();
                            if let RawEntryMut::Vacant(entry) = entry {
                                // Hand the key buffer over with the hash that was
                                // just computed for the lookup.
                                let key = std::mem::replace(
                                    &mut tags,
                                    Vec::with_capacity(num_label_columns + 1),
                                );
                                entry.insert_hashed_nocheck(
                                    hash,
                                    key,
                                    PromSeriesSamples::default(),
                                );
                            }
                            run_entry_index = Some(index);
                            index
                        }
                    };
                    let samples = &mut buffer[entry_index];
                    if let Some((timestamp_millis, histogram)) = histogram {
                        samples
                            .histograms
                            .push((timestamp_millis as f64 / 1000.0, histogram));
                    } else if let Some((timestamp_millis, value)) = value {
                        samples.values.push((
                            timestamp_millis as f64 / 1000.0,
                            PromSampleValue::Number(value),
                        ));
                    }
                }
            }
        }

        // initialize result to return
        let mut result = match result_type {
            ValueType::Vector => PromQueryResult::Vector(vec![]),
            ValueType::Matrix => PromQueryResult::Matrix(vec![]),
            ValueType::Scalar => PromQueryResult::Scalar(None),
            ValueType::String => PromQueryResult::String(None),
        };

        // accumulate data into result
        buffer.into_iter().for_each(|(tags, mut samples)| {
            let metric = tags
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>();
            match result {
                PromQueryResult::Vector(ref mut v) => {
                    let histogram = samples.histograms.pop();
                    let value = if histogram.is_none() {
                        samples
                            .values
                            .pop()
                            .map(|(timestamp, value)| (timestamp, value.into_string()))
                    } else {
                        None
                    };
                    v.push(PromSeriesVector {
                        metric,
                        value,
                        histogram,
                    });
                }
                PromQueryResult::Matrix(ref mut v) => {
                    // sort values by timestamp
                    if !samples.values.is_sorted_by(|a, b| a.0 <= b.0) {
                        samples
                            .values
                            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                    }
                    if !samples.histograms.is_sorted_by(|a, b| a.0 <= b.0) {
                        samples
                            .histograms
                            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                    }

                    v.push(PromSeriesMatrix {
                        metric,
                        values: samples.values,
                        histograms: samples.histograms,
                    });
                }
                PromQueryResult::Scalar(ref mut v) => {
                    *v = samples
                        .values
                        .pop()
                        .map(|(timestamp, value)| (timestamp, value.into_string()));
                }
                PromQueryResult::String(ref mut _v) => {
                    // TODO(ruihang): Not supported yet
                }
            }
        });

        // sort matrix by metric
        // see: https://prometheus.io/docs/prometheus/3.5/querying/api/#range-vectors
        if let PromQueryResult::Matrix(ref mut v) = result {
            v.sort_by(|a, b| a.metric.cmp(&b.metric));
        }

        let result_type_string = result_type.to_string();
        let data = PrometheusResponse::PromData(PromData {
            result_type: result_type_string,
            result,
        });

        Ok(data)
    }
}

/// Decides whether to group the rows of a batch into runs that share their
/// label values.
///
/// Sampling adjacent row pairs keeps the decision independent of the batch
/// size. Probe positions come from a xorshift sequence instead of a fixed
/// stride, which would alias with periodic series layouts. A wrong guess only
/// costs time: `partition` still validates every boundary, and the row-by-row
/// path builds the labels of every row.
fn prefer_label_runs(columns: &[&ArrayRef], rows: usize) -> bool {
    if columns.is_empty() {
        return true;
    }
    if rows < 2 {
        return false;
    }
    let mut position = 0x9e37_79b9_u32;
    let mut changes = 0;
    for _ in 0..8 {
        position ^= position << 13;
        position ^= position >> 17;
        position ^= position << 5;
        let row = position as usize % (rows - 1);
        if columns.iter().any(|column| {
            string_array_value_at_index(column, row) != string_array_value_at_index(column, row + 1)
        }) {
            changes += 1;
            if changes > 2 {
                return false;
            }
        }
    }
    true
}

fn merge_annotations(target: &mut Option<Vec<String>>, source: Option<Vec<String>>) {
    let Some(source) = source else {
        return;
    };
    let target = target.get_or_insert_default();
    target.extend(source);
    target.sort();
    target.dedup();
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringViewArray;
    use common_query::native_histogram::{
        CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, Span, build_histogram_array,
        native_histogram_value_type,
    };
    use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
    use common_query::promql_annotations::{
        PromqlAnnotationCollector, promql_annotation_collector,
    };
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{
        Float64Vector, StringVector, StructVector, TimestampMillisecondVector, VectorRef,
    };

    use super::*;

    #[tokio::test]
    async fn query_response_preserves_and_merges_promql_annotations() {
        let query_id = "query_response_preserves_and_merges_promql_annotations";
        let left = promql_annotation_collector(query_id);
        left.record_warning("shared warning");
        left.record_info("left info");
        let mut response = PrometheusJsonResponse::from_query_result(
            Ok(Output::new_with_record_batches(RecordBatches::empty())),
            None,
            ValueType::Vector,
            Some(query_id),
        )
        .await;

        let right = PromqlAnnotationCollector::default();
        right.record_warning("shared warning");
        right.record_info("right info");
        let mut other = PrometheusJsonResponse::success(PrometheusResponse::None);
        other.append_promql_annotations(&right);
        response.append_query_response(other);

        assert_eq!(response.warnings, Some(vec!["shared warning".to_string()]));
        assert_eq!(
            response.infos,
            Some(vec!["left info".to_string(), "right info".to_string()])
        );
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json["warnings"], serde_json::json!(["shared warning"]));
        assert_eq!(
            json["infos"],
            serde_json::json!(["left info", "right info"])
        );
    }

    fn sample_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            zero_threshold: 0.001,
            sum: 3.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: Some(0),
            custom_values: vec![],
            positive_spans: vec![Span {
                offset: 0,
                length: 1,
            }],
            negative_spans: vec![],
            count: 2.0,
            zero_count: 1.0,
            positive_buckets: vec![1.0],
            negative_buckets: vec![],
        }
    }

    fn histogram_vector(values: &[Option<NativeHistogram>]) -> VectorRef {
        let histogram_array = build_histogram_array(values);
        let histogram_array = histogram_array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        let ConcreteDataType::Struct(histogram_type) = native_histogram_value_type().clone() else {
            unreachable!("native histogram type must be a struct")
        };
        Arc::new(StructVector::try_new(histogram_type, histogram_array).unwrap())
    }

    #[test]
    fn format_prometheus_sample_value_uses_ryu_for_finite_values() {
        let values = [
            1.5,
            0.1,
            1.0,
            0.0,
            -0.0,
            100.0,
            1e-6,
            1e-7,
            1e21,
            1e30,
            f64::MAX,
            f64::MIN_POSITIVE,
        ];

        for value in values {
            let output = format_prometheus_sample_value(value);
            assert_eq!(output.parse::<f64>().unwrap().to_bits(), value.to_bits());
        }

        // Representative integral values use ryu's explicit .0 form.
        assert_eq!(format_prometheus_sample_value(1.0), "1.0");
        assert_eq!(format_prometheus_sample_value(-0.0), "-0.0");
        assert_eq!(format_prometheus_sample_value(100.0), "100.0");
        assert_eq!(format_prometheus_sample_value(1e-6), "1e-6");
        assert_eq!(format_prometheus_sample_value(1e-7), "1e-7");
        assert_eq!(format_prometheus_sample_value(1e21), "1e21");

        // These known shortest-roundtrip tie cases have different text but
        // remain numerically equivalent to Rust's representation.
        for value in [
            f64::from_bits(0x42374876e8000400),
            f64::from_bits(0x3ff0000800000000),
            f64::from_bits(0x430a8e5672bc7312),
        ] {
            let ryu_output = format_prometheus_sample_value(value);
            let std_output = value.to_string();
            assert_ne!(ryu_output, std_output);
            assert_eq!(ryu_output.parse::<f64>().unwrap(), value);
            assert_eq!(std_output.parse::<f64>().unwrap(), value);
        }
    }

    #[test]
    fn format_prometheus_sample_value_preserves_nonfinite_values() {
        assert_eq!(format_prometheus_sample_value(f64::NAN), "NaN");
        assert_eq!(format_prometheus_sample_value(f64::INFINITY), "inf");
        assert_eq!(format_prometheus_sample_value(f64::NEG_INFINITY), "-inf");

        // Parsing a NaN does not preserve its payload bits, so NaN is checked
        // by its required semantic spelling rather than by to_bits().
        assert!(
            format_prometheus_sample_value(f64::NAN)
                .parse::<f64>()
                .unwrap()
                .is_nan()
        );
    }

    #[test]
    fn sample_value_serialization_matches_eager_formatting() {
        let mut values = vec![
            0.0,
            -0.0,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            1e-7,
            1e21,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        let mut bits = 0x1234_5678_9876_5432_u64;
        for _ in 0..1000 {
            bits ^= bits << 13;
            bits ^= bits >> 7;
            bits ^= bits << 17;
            values.push(f64::from_bits(bits));
        }
        for value in values {
            assert_eq!(
                serde_json::to_string(&PromSampleValue::Number(value)).unwrap(),
                serde_json::to_string(&format_prometheus_sample_value(value)).unwrap()
            );
        }
        for value in ["1.00", "+Inf", "-0", "NaN", "not-a-number", "", "\"\\\n"] {
            let json = serde_json::to_string(value).unwrap();
            let parsed: PromSampleValue = serde_json::from_str(&json).unwrap();
            assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
        }
        for value in ["1", "null", "true", "[]", "{}"] {
            assert!(serde_json::from_str::<PromSampleValue>(value).is_err());
        }
    }

    #[tokio::test]
    async fn matrix_response_body_matches_eagerly_formatted_json() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
        ]));
        let batches = RecordBatches::try_new(
            schema.clone(),
            vec![
                RecordBatch::new(
                    schema,
                    vec![
                        Arc::new(TimestampMillisecondVector::from_values([
                            1000, 2000, 3000, 4000, 5000,
                        ])) as _,
                        Arc::new(StringVector::from(vec![Some("a"); 5])) as _,
                        Arc::new(Float64Vector::from_values([
                            -0.0,
                            f64::NAN,
                            1e-7,
                            f64::INFINITY,
                            f64::NEG_INFINITY,
                        ])) as _,
                    ],
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let actual = PrometheusJsonResponse::from_query_result(
            Ok(Output::new_with_record_batches(batches)),
            None,
            ValueType::Matrix,
            None,
        )
        .await;
        // Deserializing the expectation yields `PromSampleValue::Text`, so this
        // compares the deferred numeric encoding against eagerly built strings.
        let expected: PrometheusJsonResponse = serde_json::from_value(serde_json::json!({
            "status": "success",
            "data": {"resultType": "matrix", "result": [{
                "metric": {"host": "a"},
                "values": [[1.0, "-0.0"], [2.0, "NaN"], [3.0, "1e-7"], [4.0, "inf"], [5.0, "-inf"]]
            }]}
        }))
        .unwrap();
        assert_eq!(
            serde_json::to_string(&actual).unwrap(),
            serde_json::to_string(&expected).unwrap()
        );
    }

    #[test]
    fn matrix_response_preserves_ordinary_nan_and_filters_stale_markers() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_vec(vec![
                    1_000, 2_000, 3_000, 4_000,
                ])) as _,
                Arc::new(Float64Vector::from(vec![
                    Some(1.0),
                    Some(f64::from_bits(0x7ff8_0000_0000_0000)),
                    Some(f64::from_bits(0x7ff0_0000_0000_0002)),
                    None,
                ])) as _,
            ],
        )
        .unwrap();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();

        let response =
            PrometheusJsonResponse::record_batches_to_data(batches, None, ValueType::Matrix)
                .unwrap();
        let PrometheusResponse::PromData(data) = response else {
            panic!("expected Prometheus data response");
        };
        let PromQueryResult::Matrix(series) = data.result else {
            panic!("expected matrix result");
        };

        assert_eq!(series.len(), 1);
        assert_eq!(
            serde_json::to_value(&series[0].values).unwrap(),
            serde_json::json!([[1.0, "1.0"], [2.0, "NaN"]])
        );
    }

    #[test]
    fn record_batches_to_data_formats_values_with_ryu() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_vec(vec![
                    1_000, 2_000, 3_000, 4_000, 5_000, 6_000,
                ])) as _,
                Arc::new(Float64Vector::from(vec![
                    Some(0.0),
                    Some(-0.0),
                    Some(1.25),
                    Some(1e30),
                    Some(1e-7),
                    Some(f64::MAX),
                ])) as _,
            ],
        )
        .unwrap();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();

        let response =
            PrometheusJsonResponse::record_batches_to_data(batches, None, ValueType::Matrix)
                .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Matrix(series),
            ..
        }) = response
        else {
            panic!("expected matrix response");
        };

        assert_eq!(series.len(), 1);
        let input_values = [0.0, -0.0, 1.25, 1e30, 1e-7, f64::MAX];
        // Keep this expected-value generation independent from the production
        // formatter while still asserting the Arrow/batch-to-Prometheus path.
        let expected = input_values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let expected_value = if value.is_finite() {
                    let mut buffer = Buffer::new();
                    buffer.format_finite(value).to_string()
                } else {
                    value.to_string()
                };
                ((index + 1) as f64, expected_value)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            serde_json::to_value(&series[0].values).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }

    #[test]
    fn record_batches_to_data_preserves_infinity_output() {
        // NaN and infinities use Rust's `f64::to_string()` output.
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_vec(vec![
                    1_000, 2_000, 3_000,
                ])) as _,
                Arc::new(Float64Vector::from(vec![
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(f64::NAN),
                ])) as _,
            ],
        )
        .unwrap();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();

        let response =
            PrometheusJsonResponse::record_batches_to_data(batches, None, ValueType::Matrix)
                .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Matrix(series),
            ..
        }) = response
        else {
            panic!("expected matrix response");
        };

        assert_eq!(series.len(), 1);
        assert_eq!(
            serde_json::to_value(&series[0].values).unwrap(),
            serde_json::json!([[1.0, "inf"], [2.0, "-inf"], [3.0, "NaN"]])
        );
    }

    #[test]
    fn record_batches_to_data_groups_clustered_series() {
        // Rows are clustered by series (a, b, a, a, b, c) and `a` is revisited
        // after `b`. The result must keep the first-occurrence order and
        // accumulate values per series.
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_vec(vec![
                    1_000, 2_000, 3_000, 4_000, 5_000, 6_000,
                ])) as _,
                Arc::new(StringVector::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("c"),
                ])) as _,
                Arc::new(Float64Vector::from(vec![
                    Some(1.0),
                    Some(2.0),
                    Some(3.0),
                    Some(4.0),
                    Some(5.0),
                    Some(6.0),
                ])) as _,
            ],
        )
        .unwrap();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();

        let response = PrometheusJsonResponse::record_batches_to_data(
            batches,
            Some("metric".to_string()),
            ValueType::Vector,
        )
        .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Vector(series),
            ..
        }) = response
        else {
            panic!("expected vector response");
        };

        assert_eq!(series.len(), 3);
        // Output order is first-occurrence order: a, b, c.
        assert_eq!(
            series
                .iter()
                .map(|series| series.metric["host"].as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        // Vector results keep the last sample of each series.
        assert_eq!(series[0].value, Some((4.0, "4.0".to_string())));
        assert_eq!(series[1].value, Some((5.0, "5.0".to_string())));
        assert_eq!(series[2].value, Some((6.0, "6.0".to_string())));
    }

    #[test]
    fn label_strategy_switches_preserve_all_rows_when_probes_miss_changes() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
        ]));
        let mut batches = Vec::new();
        let mut expected = BTreeMap::<String, Vec<(f64, PromSampleValue)>>::new();
        for layout in 0..3 {
            let labels = (0..1024)
                .map(|row| {
                    let alternating = match layout {
                        0 => true,
                        1 => row < 64,
                        _ => row >= 64,
                    };
                    if alternating && row % 2 != 0 {
                        "b"
                    } else {
                        "a"
                    }
                })
                .collect::<Vec<_>>();
            for (row, label) in labels.iter().enumerate() {
                let value = (layout * 1024 + row) as f64;
                expected
                    .entry((*label).to_string())
                    .or_default()
                    .push((value, PromSampleValue::Number(value)));
            }
            let batch = RecordBatch::new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondVector::from_values(
                        (0..1024).map(|row| (layout * 1024 + row) as i64 * 1000),
                    )) as _,
                    Arc::new(StringVector::from(
                        labels.into_iter().map(Some).collect::<Vec<_>>(),
                    )) as _,
                    Arc::new(Float64Vector::from(
                        (0..1024)
                            .map(|row| Some((layout * 1024 + row) as f64))
                            .collect::<Vec<_>>(),
                    )) as _,
                ],
            )
            .unwrap();
            // The middle batch alternates only over a prefix the probes miss,
            // so it takes the run path even though most rows are not runs.
            assert_eq!(
                prefer_label_runs(&[batch.column(1)], batch.num_rows()),
                layout == 1
            );
            batches.push(batch);
        }
        let response = PrometheusJsonResponse::record_batches_to_data(
            RecordBatches::try_new(schema, batches).unwrap(),
            None,
            ValueType::Matrix,
        )
        .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Matrix(series),
            ..
        }) = response
        else {
            panic!("expected matrix response");
        };
        assert_eq!(series.len(), expected.len());
        for series in series {
            assert_eq!(series.metric.len(), 1);
            assert!(series.histograms.is_empty());
            assert_eq!(
                series.values,
                expected.remove(&series.metric["host"]).unwrap()
            );
        }
        assert!(expected.is_empty());
    }

    #[test]
    fn label_runs_keep_null_and_empty_labels_apart_across_batches() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("rack", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
        ]));
        // Two batches of two runs each, with only `rack` changing: a null label
        // and an empty one must not share a run, and the run opening the second
        // batch continues the series that ended the first one.
        let mut batches = Vec::new();
        let mut expected = vec![Vec::new(), Vec::new()];
        for (batch_index, leading_null) in [true, false].into_iter().enumerate() {
            let mut racks = Vec::new();
            let mut values = Vec::new();
            for row in 0..1024 {
                let value = (batch_index * 1024 + row) as f64;
                let null_rack = (row < 512) == leading_null;
                racks.push((!null_rack).then_some(""));
                values.push(Some(value));
                expected[usize::from(!null_rack)].push((value, PromSampleValue::Number(value)));
            }
            batches.push(
                RecordBatch::new(
                    schema.clone(),
                    vec![
                        Arc::new(TimestampMillisecondVector::from_values(
                            values.iter().map(|value| value.unwrap() as i64 * 1000),
                        )) as _,
                        Arc::new(StringVector::from(vec![Some("a"); 1024])) as _,
                        Arc::new(StringVector::from(racks)) as _,
                        Arc::new(Float64Vector::from(values)) as _,
                    ],
                )
                .unwrap(),
            );
        }
        for batch in &batches {
            assert!(prefer_label_runs(
                &[batch.column(1), batch.column(2)],
                batch.num_rows()
            ));
        }
        for series in &mut expected {
            series.sort_by(|left, right| left.0.total_cmp(&right.0));
        }

        let response = PrometheusJsonResponse::record_batches_to_data(
            RecordBatches::try_new(schema, batches).unwrap(),
            None,
            ValueType::Matrix,
        )
        .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Matrix(series),
            ..
        }) = response
        else {
            panic!("expected matrix response");
        };
        assert_eq!(series.len(), 2);
        assert_eq!(
            series[0].metric,
            BTreeMap::from([("host".into(), "a".into())])
        );
        assert_eq!(series[0].values, expected[0]);
        assert_eq!(
            series[1].metric,
            BTreeMap::from([("host".into(), "a".into()), ("rack".into(), "".into())])
        );
        assert_eq!(series[1].values, expected[1]);
    }

    #[test]
    fn matrix_response_is_independent_of_input_row_order() {
        // Range queries run without the plan's output sort, so this function sees
        // series interleaved across batches with timestamps out of order. The
        // serialized matrix must be the same either way.
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("rack", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("histogram", native_histogram_value_type().clone(), true),
        ]));
        let histogram = |sum: f64| NativeHistogram {
            sum,
            ..sample_histogram()
        };
        // timestamp, host, rack, float value, histogram value
        type Row = (
            i64,
            Option<&'static str>,
            Option<&'static str>,
            Option<f64>,
            Option<NativeHistogram>,
        );
        let rows: Vec<Row> = vec![
            (1_000, Some("a"), Some("r"), Some(1.0), None),
            (3_000, Some("a"), Some("r"), Some(3.0), None),
            (2_000, Some("a"), Some("r"), Some(2.0), None),
            (5_000, Some("a"), None, Some(5.0), None),
            (4_000, Some("a"), None, Some(4.0), None),
            (7_000, Some(""), None, Some(7.0), None),
            (8_000, None, None, Some(8.0), None),
            (6_000, None, None, Some(6.0), None),
            (2_000, Some("h"), None, None, Some(histogram(20.0))),
            (1_000, Some("h"), None, None, Some(histogram(10.0))),
        ];
        let matrix = |order: &[usize], splits: &[usize]| {
            let batch = RecordBatch::new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMillisecondVector::from_vec(
                        order.iter().map(|&row| rows[row].0).collect(),
                    )) as _,
                    Arc::new(StringVector::from(
                        order.iter().map(|&row| rows[row].1).collect::<Vec<_>>(),
                    )) as _,
                    Arc::new(StringVector::from(
                        order.iter().map(|&row| rows[row].2).collect::<Vec<_>>(),
                    )) as _,
                    Arc::new(Float64Vector::from(
                        order.iter().map(|&row| rows[row].3).collect::<Vec<_>>(),
                    )) as _,
                    histogram_vector(
                        &order
                            .iter()
                            .map(|&row| rows[row].4.clone())
                            .collect::<Vec<_>>(),
                    ),
                ],
            )
            .unwrap();
            let mut batches = Vec::new();
            let mut start = 0;
            for &end in splits.iter().chain(std::iter::once(&order.len())) {
                batches.push(batch.slice(start, end - start).unwrap());
                start = end;
            }
            let response = PrometheusJsonResponse::record_batches_to_data(
                RecordBatches::try_new(schema.clone(), batches).unwrap(),
                Some("metric".to_string()),
                ValueType::Matrix,
            )
            .unwrap();
            let PrometheusResponse::PromData(PromData {
                result: PromQueryResult::Matrix(series),
                ..
            }) = response
            else {
                panic!("expected matrix response");
            };
            series
        };

        let clustered = matrix(&[0, 2, 1, 4, 3, 5, 7, 6, 9, 8], &[5]);
        let interleaved = matrix(&[8, 5, 1, 6, 0, 3, 9, 2, 7, 4], &[3, 6]);
        assert_eq!(
            serde_json::to_value(&interleaved).unwrap(),
            serde_json::to_value(&clustered).unwrap()
        );

        // Pin the canonical arrangement itself, not only its stability.
        assert_eq!(
            serde_json::to_value(&clustered[..4]).unwrap(),
            serde_json::json!([
                {"metric": {"__name__": "metric"}, "values": [[6.0, "6.0"], [8.0, "8.0"]]},
                {"metric": {"__name__": "metric", "host": ""}, "values": [[7.0, "7.0"]]},
                {"metric": {"__name__": "metric", "host": "a"},
                 "values": [[4.0, "4.0"], [5.0, "5.0"]]},
                {"metric": {"__name__": "metric", "host": "a", "rack": "r"},
                 "values": [[1.0, "1.0"], [2.0, "2.0"], [3.0, "3.0"]]},
            ])
        );
        assert_eq!(clustered[4].metric["host"], "h");
        assert_eq!(
            clustered[4]
                .histograms
                .iter()
                .map(|(timestamp, histogram)| (*timestamp, histogram.sum.as_str()))
                .collect::<Vec<_>>(),
            vec![(1.0, "10"), (2.0, "20")]
        );
    }

    #[test]
    fn record_batches_to_data_preserves_mixed_float_and_histogram_rows() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("kind", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("float", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("histogram", native_histogram_value_type().clone(), true),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_values([1_000, 1_000])) as _,
                Arc::new(StringVector::from(vec![Some("float"), Some("histogram")])) as _,
                Arc::new(Float64Vector::from(vec![Some(1.25), None])) as _,
                histogram_vector(&[None, Some(sample_histogram())]),
            ],
        )
        .unwrap();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();

        let response = PrometheusJsonResponse::record_batches_to_data(
            batches,
            Some("mixed_metric".to_string()),
            ValueType::Vector,
        )
        .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Vector(series),
            ..
        }) = response
        else {
            panic!("expected vector response");
        };

        assert_eq!(series.len(), 2);
        let float = series
            .iter()
            .find(|series| series.metric["kind"] == "float")
            .unwrap();
        assert_eq!(float.value, Some((1.0, "1.25".to_string())));
        assert!(float.histogram.is_none());

        let histogram = series
            .iter()
            .find(|series| series.metric["kind"] == "histogram")
            .unwrap();
        assert!(histogram.value.is_none());
        let (timestamp, histogram) = histogram.histogram.as_ref().unwrap();
        assert_eq!(*timestamp, 1.0);
        assert_eq!(histogram.count, "2");
        assert_eq!(histogram.sum, "3");
    }

    #[test]
    fn label_replace_with_utf8view_labels_does_not_panic() {
        // A PromQL `label_replace` query produces its new label through DataFusion's
        // `regexp_replace`, whose output materializes as a `Utf8View` array even when
        // the source label is a plain `Utf8`. Serializing such labels must not assume
        // the column is a `StringArray`.
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new("host_copy", ConcreteDataType::utf8_view_datatype(), false),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_values([1_000])) as _,
                Arc::new(StringVector::from(vec![Some("server-01")])) as _,
                Arc::new(StringVector::from(StringViewArray::from(vec![Some(
                    "server-01",
                )]))) as _,
                Arc::new(Float64Vector::from(vec![Some(1.0)])) as _,
            ],
        )
        .unwrap();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();

        let response = PrometheusJsonResponse::record_batches_to_data(
            batches,
            Some("label_replace_repro".to_string()),
            ValueType::Vector,
        )
        .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Vector(series),
            ..
        }) = response
        else {
            panic!("expected vector response");
        };

        assert_eq!(series.len(), 1);
        assert_eq!(series[0].metric["__name__"], "label_replace_repro");
        assert_eq!(series[0].metric["host"], "server-01");
        assert_eq!(series[0].metric["host_copy"], "server-01");
        assert_eq!(series[0].value, Some((1.0, "1.0".to_string())));
    }

    #[test]
    fn matrix_response_preserves_ordinary_histogram_nan_and_filters_stale_marker() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("histogram", native_histogram_value_type().clone(), true),
        ]));
        let mut ordinary_nan = sample_histogram();
        ordinary_nan.sum = f64::NAN;
        let mut stale = sample_histogram();
        stale.sum = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_values([1_000, 2_000])) as _,
                histogram_vector(&[Some(ordinary_nan), Some(stale)]),
            ],
        )
        .unwrap();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();

        let response =
            PrometheusJsonResponse::record_batches_to_data(batches, None, ValueType::Matrix)
                .unwrap();
        let PrometheusResponse::PromData(PromData {
            result: PromQueryResult::Matrix(series),
            ..
        }) = response
        else {
            panic!("expected matrix response");
        };

        assert_eq!(series.len(), 1);
        assert_eq!(series[0].histograms.len(), 1);
        assert_eq!(series[0].histograms[0].0, 1.0);
        assert_eq!(series[0].histograms[0].1.sum, "NaN");
    }

    #[test]
    fn native_histogram_json_closes_custom_bucket_zero() {
        let mut histogram = sample_histogram();
        histogram.schema = CUSTOM_BUCKETS_SCHEMA;
        histogram.zero_threshold = 0.0;
        histogram.custom_values = vec![1.0];
        histogram.positive_spans = vec![Span {
            offset: 0,
            length: 1,
        }];
        histogram.count = 1.0;
        histogram.sum = 0.0;
        histogram.zero_count = 0.0;

        let json = serde_json::to_value(prometheus_native_histogram(&histogram).unwrap()).unwrap();
        assert_eq!(json["buckets"], serde_json::json!([[3, "-Inf", "1", "1"]]));
    }

    #[test]
    fn native_histogram_json_preserves_terminal_finite_bucket() {
        let mut histogram = sample_histogram();
        histogram.positive_spans = vec![Span {
            offset: 1024,
            length: 2,
        }];
        histogram.positive_buckets = vec![1.0, 1.0];
        histogram.zero_count = 0.0;

        let json = serde_json::to_value(prometheus_native_histogram(&histogram).unwrap()).unwrap();
        assert_eq!(
            json["buckets"],
            serde_json::json!([
                [0, 2.0_f64.powi(1023).to_string(), f64::MAX.to_string(), "1"],
                [0, f64::MAX.to_string(), "+Inf", "1"]
            ])
        );
    }
}
