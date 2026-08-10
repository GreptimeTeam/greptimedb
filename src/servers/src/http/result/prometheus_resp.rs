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

use arrow::array::{Array, AsArray, StructArray};
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
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datatypes::arrow_array::string_array_value_at_index;
use datatypes::prelude::ConcreteDataType;
use indexmap::IndexMap;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::value::ValueType;
use serde::{Deserialize, Serialize};
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
    values: Vec<(f64, String)>,
    histograms: Vec<(f64, PromNativeHistogram)>,
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
            resp_metrics: Default::default(),
            status_code: None,
        }
    }

    /// Convert from `Result<Output>`
    pub async fn from_query_result(
        result: Result<Output>,
        metric_name: Option<String>,
        result_type: ValueType,
    ) -> Self {
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

        match response {
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
        }
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

            // assemble rows
            for row_index in 0..batch.num_rows() {
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

                // retrieve tags
                let mut tags = Vec::with_capacity(num_label_columns + 1);
                if let Some(metric_name) = &metric_name {
                    tags.push((METRIC_NAME, metric_name.as_str()));
                }
                for (tag_column, tag_name) in tag_columns.iter().zip(tag_names.iter()) {
                    if let Some(tag_value) = string_array_value_at_index(tag_column, row_index) {
                        tags.push((tag_name, tag_value));
                    }
                }

                let entry = buffer.entry(tags).or_default();
                if let Some((timestamp_millis, histogram)) = histogram {
                    entry
                        .histograms
                        .push((timestamp_millis as f64 / 1000.0, histogram));
                } else if let Some((timestamp_millis, value)) = value {
                    entry
                        .values
                        .push((timestamp_millis as f64 / 1000.0, value.to_string()));
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
                        samples.values.pop()
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
                    *v = samples.values.pop();
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::native_histogram::{
        CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, Span, build_histogram_array,
        native_histogram_value_type,
    };
    use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{
        Float64Vector, StringVector, StructVector, TimestampMillisecondVector, VectorRef,
    };

    use super::*;

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
            series[0].values,
            vec![(1.0, "1".to_string()), (2.0, "NaN".to_string())]
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
