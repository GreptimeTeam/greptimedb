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
use std::collections::{BTreeMap, HashMap};

use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use axum::Json;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVector;
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::value::ValueType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{OptionExt, ResultExt};

use super::header::{collect_plan_metrics, GREPTIME_DB_HEADER_METRICS};
use super::prometheus::{
    PromData, PromQueryResult, PromSeriesMatrix, PromSeriesVector, PrometheusResponse,
};
use crate::error::{CollectRecordbatchSnafu, Result, UnexpectedResultSnafu};
use crate::http::error_result::status_code_to_http_status;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct PrometheusJsonResponse {
    pub status: String,
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
            data: PrometheusResponse::default(),
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
        metric_name: String,
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
        metric_name: String,
        result_type: ValueType,
    ) -> Result<PrometheusResponse> {
        // infer semantic type of each column from schema.
        // TODO(ruihang): wish there is a better way to do this.
        let mut timestamp_column_index = None;
        let mut tag_column_indices = Vec::new();
        let mut first_field_column_index = None;

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

        let metric_name = (METRIC_NAME.to_string(), metric_name);
        let mut buffer = BTreeMap::<Vec<(String, String)>, Vec<(f64, String)>>::new();

        for batch in batches.iter() {
            // prepare things...
            let tag_columns = tag_column_indices
                .iter()
                .map(|i| {
                    batch
                        .column(*i)
                        .as_any()
                        .downcast_ref::<StringVector>()
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let tag_names = tag_column_indices
                .iter()
                .map(|c| batches.schema().column_name_by_index(*c).to_string())
                .collect::<Vec<_>>();
            let timestamp_column = batch
                .column(timestamp_column_index)
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap();
            let casted_field_column = batch
                .column(first_field_column_index)
                .cast(&ConcreteDataType::float64_datatype())
                .unwrap();
            let field_column = casted_field_column
                .as_any()
                .downcast_ref::<Float64Vector>()
                .unwrap();

            // assemble rows
            for row_index in 0..batch.num_rows() {
                // retrieve tags
                // TODO(ruihang): push table name `__metric__`
                let mut tags = vec![metric_name.clone()];
                for (tag_column, tag_name) in tag_columns.iter().zip(tag_names.iter()) {
                    // TODO(ruihang): add test for NULL tag
                    if let Some(tag_value) = tag_column.get_data(row_index) {
                        tags.push((tag_name.to_string(), tag_value.to_string()));
                    }
                }

                // retrieve timestamp
                let timestamp_millis: i64 = timestamp_column.get_data(row_index).unwrap().into();
                let timestamp = timestamp_millis as f64 / 1000.0;

                // retrieve value
                if let Some(v) = field_column.get_data(row_index) {
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
            let metric = tags.into_iter().collect();
            match result {
                PromQueryResult::Vector(ref mut v) => {
                    v.push(PromSeriesVector {
                        metric,
                        value: values.pop(),
                    });
                }
                PromQueryResult::Matrix(ref mut v) => {
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

        let result_type_string = result_type.to_string();
        let data = PrometheusResponse::PromData(PromData {
            result_type: result_type_string,
            result,
        });

        Ok(data)
    }
}
