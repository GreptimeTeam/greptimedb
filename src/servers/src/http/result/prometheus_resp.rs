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

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, TimestampMillisecondType};
use arrow_schema::DataType;
use axum::Json;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use indexmap::IndexMap;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::value::ValueType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    ArrowSnafu, CollectRecordbatchSnafu, Result, UnexpectedResultSnafu, status_code_to_http_status,
};
use crate::http::header::{GREPTIME_DB_HEADER_METRICS, collect_plan_metrics};
use crate::http::prometheus::{
    PromData, PromQueryResult, PromSeriesMatrix, PromSeriesVector, PrometheusResponse,
};

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

        let mut num_label_columns = 0;

        for (i, column) in batches.schema().column_schemas().iter().enumerate() {
            match column.data_type {
                ConcreteDataType::Timestamp(datatypes::types::TimestampType::Millisecond(_)) => {
                    if timestamp_column_index.is_none() {
                        timestamp_column_index = Some(i);
                    }
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
                | ConcreteDataType::UInt64(_) => {
                    if first_field_column_index.is_none() {
                        first_field_column_index = Some(i);
                    }
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
        let first_field_column_index = first_field_column_index.context(UnexpectedResultSnafu {
            reason: "no value column found".to_string(),
        })?;

        // Preserves the order of output tags.
        // Tag order matters, e.g., after sorc and sort_desc, the output order must be kept.
        let mut buffer = IndexMap::<Vec<(&str, &str)>, Vec<(f64, String)>>::new();

        let schema = batches.schema();
        for batch in batches.iter() {
            // prepare things...
            let tag_columns = tag_column_indices
                .iter()
                .map(|i| batch.column(*i).as_string::<i32>())
                .collect::<Vec<_>>();
            let tag_names = tag_column_indices
                .iter()
                .map(|c| schema.column_name_by_index(*c))
                .collect::<Vec<_>>();
            let timestamp_column = batch
                .column(timestamp_column_index)
                .as_primitive::<TimestampMillisecondType>();

            let array =
                arrow::compute::cast(batch.column(first_field_column_index), &DataType::Float64)
                    .context(ArrowSnafu)?;
            let field_column = array.as_primitive::<Float64Type>();

            // assemble rows
            for row_index in 0..batch.num_rows() {
                // retrieve value
                if field_column.is_valid(row_index) {
                    let v = field_column.value(row_index);
                    // ignore all NaN values to reduce the amount of data to be sent.
                    if v.is_nan() {
                        continue;
                    }

                    // retrieve tags
                    let mut tags = Vec::with_capacity(num_label_columns + 1);
                    if let Some(metric_name) = &metric_name {
                        tags.push((METRIC_NAME, metric_name.as_str()));
                    }
                    for (tag_column, tag_name) in tag_columns.iter().zip(tag_names.iter()) {
                        // TODO(ruihang): add test for NULL tag
                        if tag_column.is_valid(row_index) {
                            tags.push((tag_name, tag_column.value(row_index)));
                        }
                    }

                    // retrieve timestamp
                    let timestamp_millis = timestamp_column.value(row_index);
                    let timestamp = timestamp_millis as f64 / 1000.0;

                    buffer
                        .entry(tags)
                        .or_default()
                        .push((timestamp, Into::<f64>::into(v).to_string()));
                };
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
        buffer.into_iter().for_each(|(tags, mut values)| {
            let metric = tags
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>();
            match result {
                PromQueryResult::Vector(ref mut v) => {
                    v.push(PromSeriesVector {
                        metric,
                        value: values.pop(),
                    });
                }
                PromQueryResult::Matrix(ref mut v) => {
                    // sort values by timestamp
                    if !values.is_sorted_by(|a, b| a.0 <= b.0) {
                        values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                    }

                    v.push(PromSeriesMatrix { metric, values });
                }
                PromQueryResult::Scalar(ref mut v) => {
                    *v = values.pop();
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
