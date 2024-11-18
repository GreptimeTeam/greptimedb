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

use std::collections::{BTreeMap, HashMap};
use std::result::Result as StdResult;
use std::sync::Arc;
use std::time::Instant;

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
    Value as GreptimeValue,
};
use axum::body::HttpBody;
use axum::extract::{FromRequest, Multipart, Path, Query, State};
use axum::headers::ContentType;
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{async_trait, BoxError, Extension, Json, TypedHeader};
use bytes::Bytes;
use common_query::prelude::GREPTIME_TIMESTAMP;
use common_query::{Output, OutputData};
use common_telemetry::{error, warn};
use datatypes::value::column_data_to_json;
use lazy_static::lazy_static;
use loki_api::prost_types::Timestamp;
use pipeline::error::PipelineTransformSnafu;
use pipeline::util::to_pipeline_version;
use pipeline::PipelineVersion;
use prost::Message;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Map, Value};
use session::context::{Channel, QueryContext, QueryContextRef};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    DecodeOtlpRequestSnafu, Error, InvalidParameterSnafu, ParseJson5Snafu, ParseJsonSnafu,
    PipelineSnafu, Result, UnsupportedContentTypeSnafu,
};
use crate::http::extractor::LogTableName;
use crate::http::header::CONTENT_TYPE_PROTOBUF_STR;
use crate::http::result::greptime_manage_resp::GreptimedbManageResponse;
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::interceptor::{LogIngestInterceptor, LogIngestInterceptorRef};
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_HTTP_LOGS_INGESTION_COUNTER, METRIC_HTTP_LOGS_INGESTION_ELAPSED,
    METRIC_HTTP_LOGS_TRANSFORM_ELAPSED, METRIC_LOKI_LOGS_INGESTION_COUNTER,
    METRIC_LOKI_LOGS_INGESTION_ELAPSED, METRIC_SUCCESS_VALUE,
};
use crate::prom_store;
use crate::query_handler::LogHandlerRef;

const GREPTIME_INTERNAL_PIPELINE_NAME_PREFIX: &str = "greptime_";
const GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME: &str = "greptime_identity";

const LOKI_TABLE_NAME: &str = "loki_logs";
const LOKI_LINE_COLUMN: &str = "line";

lazy_static! {
    static ref LOKI_INIT_SCHEMAS: Vec<ColumnSchema> = vec![
        ColumnSchema {
            column_name: GREPTIME_TIMESTAMP.to_string(),
            datatype: ColumnDataType::TimestampNanosecond.into(),
            semantic_type: SemanticType::Timestamp.into(),
            datatype_extension: None,
            options: None,
        },
        ColumnSchema {
            column_name: LOKI_LINE_COLUMN.to_string(),
            datatype: ColumnDataType::String.into(),
            semantic_type: SemanticType::Field.into(),
            datatype_extension: None,
            options: None,
        },
    ];
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct LogIngesterQueryParams {
    pub table: Option<String>,
    pub db: Option<String>,
    pub pipeline_name: Option<String>,
    pub ignore_errors: Option<bool>,

    pub version: Option<String>,
    pub source: Option<String>,
}

pub struct PipelineContent(String);

#[async_trait]
impl<S, B> FromRequest<S, B> for PipelineContent
where
    B: HttpBody + Send + 'static,
    B::Data: Send,
    bytes::Bytes: std::convert::From<<B as HttpBody>::Data>,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request<B>, state: &S) -> StdResult<Self, Self::Rejection> {
        let content_type_header = req.headers().get(CONTENT_TYPE);
        let content_type = content_type_header.and_then(|value| value.to_str().ok());
        if let Some(content_type) = content_type {
            if content_type.ends_with("yaml") {
                let payload = String::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?;
                return Ok(Self(payload));
            }

            if content_type.starts_with("multipart/form-data") {
                let mut payload: Multipart = Multipart::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?;
                let file = payload
                    .next_field()
                    .await
                    .map_err(IntoResponse::into_response)?;
                let payload = file
                    .ok_or(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response())?
                    .text()
                    .await
                    .map_err(IntoResponse::into_response)?;
                return Ok(Self(payload));
            }
        }

        Err(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response())
    }
}

#[axum_macros::debug_handler]
pub async fn add_pipeline(
    State(state): State<LogState>,
    Path(pipeline_name): Path<String>,
    Extension(mut query_ctx): Extension<QueryContext>,
    PipelineContent(payload): PipelineContent,
) -> Result<GreptimedbManageResponse> {
    let start = Instant::now();
    let handler = state.log_handler;
    ensure!(
        !pipeline_name.is_empty(),
        InvalidParameterSnafu {
            reason: "pipeline_name is required in path",
        }
    );
    ensure!(
        !pipeline_name.starts_with(GREPTIME_INTERNAL_PIPELINE_NAME_PREFIX),
        InvalidParameterSnafu {
            reason: "pipeline_name cannot start with greptime_",
        }
    );
    ensure!(
        !payload.is_empty(),
        InvalidParameterSnafu {
            reason: "pipeline is required in body",
        }
    );

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    let content_type = "yaml";
    let result = handler
        .insert_pipeline(&pipeline_name, content_type, &payload, query_ctx)
        .await;

    result
        .map(|pipeline| {
            GreptimedbManageResponse::from_pipeline(
                pipeline_name,
                pipeline.0.to_timezone_aware_string(None),
                start.elapsed().as_millis() as u64,
            )
        })
        .map_err(|e| {
            error!(e; "failed to insert pipeline");
            e
        })
}

#[axum_macros::debug_handler]
pub async fn delete_pipeline(
    State(state): State<LogState>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Path(pipeline_name): Path<String>,
) -> Result<GreptimedbManageResponse> {
    let start = Instant::now();
    let handler = state.log_handler;
    ensure!(
        !pipeline_name.is_empty(),
        InvalidParameterSnafu {
            reason: "pipeline_name is required",
        }
    );

    let version_str = query_params.version.context(InvalidParameterSnafu {
        reason: "version is required",
    })?;

    let version = to_pipeline_version(Some(version_str.clone())).context(PipelineSnafu)?;

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    handler
        .delete_pipeline(&pipeline_name, version, query_ctx)
        .await
        .map(|v| {
            if v.is_some() {
                GreptimedbManageResponse::from_pipeline(
                    pipeline_name,
                    version_str,
                    start.elapsed().as_millis() as u64,
                )
            } else {
                GreptimedbManageResponse::from_pipelines(vec![], start.elapsed().as_millis() as u64)
            }
        })
        .map_err(|e| {
            error!(e; "failed to delete pipeline");
            e
        })
}

/// Transform NDJSON array into a single array
/// always return an array
fn transform_ndjson_array_factory(
    values: impl IntoIterator<Item = StdResult<Value, serde_json::Error>>,
    ignore_error: bool,
) -> Result<Vec<Value>> {
    values
        .into_iter()
        .try_fold(Vec::with_capacity(100), |mut acc_array, item| match item {
            Ok(item_value) => {
                match item_value {
                    Value::Array(item_array) => {
                        acc_array.extend(item_array);
                    }
                    Value::Object(_) => {
                        acc_array.push(item_value);
                    }
                    _ => {
                        if !ignore_error {
                            warn!("invalid item in array: {:?}", item_value);
                            return InvalidParameterSnafu {
                                reason: format!("invalid item:{} in array", item_value),
                            }
                            .fail();
                        }
                    }
                }
                Ok(acc_array)
            }
            Err(_) if !ignore_error => item.map(|x| vec![x]).context(ParseJsonSnafu),
            Err(_) => {
                warn!("invalid item in array: {:?}", item);
                Ok(acc_array)
            }
        })
}

#[axum_macros::debug_handler]
pub async fn pipeline_dryrun(
    State(log_state): State<LogState>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    payload: String,
) -> Result<Response> {
    let handler = log_state.log_handler;
    let pipeline_name = query_params.pipeline_name.context(InvalidParameterSnafu {
        reason: "pipeline_name is required",
    })?;

    let version = to_pipeline_version(query_params.version).context(PipelineSnafu)?;

    let ignore_errors = query_params.ignore_errors.unwrap_or(false);

    let value = extract_pipeline_value_by_content_type(content_type, payload, ignore_errors)?;

    ensure!(
        value.len() <= 10,
        InvalidParameterSnafu {
            reason: "too many rows for dryrun",
        }
    );

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    let pipeline = handler
        .get_pipeline(&pipeline_name, version, query_ctx.clone())
        .await?;

    let mut intermediate_state = pipeline.init_intermediate_state();

    let mut results = Vec::with_capacity(value.len());
    for v in value {
        pipeline
            .prepare(v, &mut intermediate_state)
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)?;
        let r = pipeline
            .exec_mut(&mut intermediate_state)
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)?;
        results.push(r);
        pipeline.reset_intermediate_state(&mut intermediate_state);
    }

    let colume_type_key = "colume_type";
    let data_type_key = "data_type";
    let name_key = "name";

    let schema = pipeline
        .schemas()
        .iter()
        .map(|cs| {
            let mut map = Map::new();
            map.insert(name_key.to_string(), Value::String(cs.column_name.clone()));
            map.insert(
                data_type_key.to_string(),
                Value::String(cs.datatype().as_str_name().to_string()),
            );
            map.insert(
                colume_type_key.to_string(),
                Value::String(cs.semantic_type().as_str_name().to_string()),
            );
            map.insert(
                "fulltext".to_string(),
                Value::Bool(
                    cs.options
                        .clone()
                        .is_some_and(|x| x.options.contains_key("fulltext")),
                ),
            );
            Value::Object(map)
        })
        .collect::<Vec<_>>();
    let rows = results
        .into_iter()
        .map(|row| {
            let row = row
                .values
                .into_iter()
                .enumerate()
                .map(|(idx, v)| {
                    v.value_data
                        .map(|d| {
                            let mut map = Map::new();
                            map.insert("value".to_string(), column_data_to_json(d));
                            map.insert("key".to_string(), schema[idx][name_key].clone());
                            map.insert(
                                "semantic_type".to_string(),
                                schema[idx][colume_type_key].clone(),
                            );
                            map.insert("data_type".to_string(), schema[idx][data_type_key].clone());
                            Value::Object(map)
                        })
                        .unwrap_or(Value::Null)
                })
                .collect();
            Value::Array(row)
        })
        .collect::<Vec<_>>();
    let mut result = Map::new();
    result.insert("schema".to_string(), Value::Array(schema));
    result.insert("rows".to_string(), Value::Array(rows));
    let result = Value::Object(result);
    Ok(Json(result).into_response())
}

#[axum_macros::debug_handler]
pub async fn loki_ingest(
    State(log_state): State<LogState>,
    Extension(mut ctx): Extension<QueryContext>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    LogTableName(table_name): LogTableName,
    bytes: Bytes,
) -> Result<HttpResponse> {
    ctx.set_channel(Channel::Loki);
    let ctx = Arc::new(ctx);
    let db = ctx.get_db_string();
    let db_str = db.as_str();
    let table_name = table_name.unwrap_or_else(|| LOKI_TABLE_NAME.to_string());
    let exec_timer = Instant::now();

    // decompress req
    ensure!(
        content_type.to_string() == CONTENT_TYPE_PROTOBUF_STR,
        UnsupportedContentTypeSnafu { content_type }
    );
    let decompressed = prom_store::snappy_decompress(&bytes).unwrap();
    let req = loki_api::logproto::PushRequest::decode(&decompressed[..])
        .context(DecodeOtlpRequestSnafu)?;

    // init schemas
    let mut schemas = LOKI_INIT_SCHEMAS.clone();

    let mut global_label_key_index: HashMap<String, i32> = HashMap::new();
    global_label_key_index.insert(GREPTIME_TIMESTAMP.to_string(), 0);
    global_label_key_index.insert(LOKI_LINE_COLUMN.to_string(), 1);

    let mut rows = vec![];

    for stream in req.streams {
        // parse labels for each row
        // encoding: https://github.com/grafana/alloy/blob/be34410b9e841cc0c37c153f9550d9086a304bca/internal/component/common/loki/client/batch.go#L114-L145
        // use very dirty hack to parse labels
        let labels = stream.labels.replace("=", ":");
        // use btreemap to keep order
        let labels: BTreeMap<String, String> = json5::from_str(&labels).context(ParseJson5Snafu)?;

        // process entries
        for entry in stream.entries {
            let ts = if let Some(ts) = entry.timestamp {
                ts
            } else {
                continue;
            };
            let line = entry.line;

            // create and init row
            let mut row = Vec::with_capacity(schemas.capacity());
            for _ in 0..row.capacity() {
                row.push(GreptimeValue { value_data: None });
            }
            // insert ts and line
            row[0] = GreptimeValue {
                value_data: Some(ValueData::TimestampNanosecondValue(prost_ts_to_nano(&ts))),
            };
            row[1] = GreptimeValue {
                value_data: Some(ValueData::StringValue(line)),
            };
            // insert labels
            for (k, v) in labels.iter() {
                if let Some(index) = global_label_key_index.get(k) {
                    // exist in schema
                    // insert value using index
                    row[*index as usize] = GreptimeValue {
                        value_data: Some(ValueData::StringValue(v.clone())),
                    };
                } else {
                    // not exist
                    // add schema and append to values
                    schemas.push(ColumnSchema {
                        column_name: k.clone(),
                        datatype: ColumnDataType::String.into(),
                        semantic_type: SemanticType::Tag.into(),
                        datatype_extension: None,
                        options: None,
                    });
                    global_label_key_index.insert(k.clone(), (schemas.len() - 1) as i32);

                    row.push(GreptimeValue {
                        value_data: Some(ValueData::StringValue(v.clone())),
                    });
                }
            }

            rows.push(row);
        }
    }

    // fill Null for missing values
    for row in rows.iter_mut() {
        if row.len() < schemas.len() {
            for _ in row.len()..schemas.len() {
                row.push(GreptimeValue { value_data: None });
            }
        }
    }

    let rows = Rows {
        rows: rows.into_iter().map(|values| Row { values }).collect(),
        schema: schemas,
    };

    let ins_req = RowInsertRequest {
        table_name,
        rows: Some(rows),
    };
    let ins_reqs = RowInsertRequests {
        inserts: vec![ins_req],
    };

    let handler = log_state.log_handler;
    let output = handler.insert_logs(ins_reqs, ctx).await;

    if let Ok(Output {
        data: OutputData::AffectedRows(rows),
        meta: _,
    }) = &output
    {
        METRIC_LOKI_LOGS_INGESTION_COUNTER
            .with_label_values(&[db_str])
            .inc_by(*rows as u64);
        METRIC_LOKI_LOGS_INGESTION_ELAPSED
            .with_label_values(&[db_str, METRIC_SUCCESS_VALUE])
            .observe(exec_timer.elapsed().as_secs_f64());
    } else {
        METRIC_LOKI_LOGS_INGESTION_ELAPSED
            .with_label_values(&[db_str, METRIC_FAILURE_VALUE])
            .observe(exec_timer.elapsed().as_secs_f64());
    }

    let response = GreptimedbV1Response::from_output(vec![output])
        .await
        .with_execution_time(exec_timer.elapsed().as_millis() as u64);
    Ok(response)
}

#[axum_macros::debug_handler]
pub async fn log_ingester(
    State(log_state): State<LogState>,
    Query(query_params): Query<LogIngesterQueryParams>,
    Extension(mut query_ctx): Extension<QueryContext>,
    TypedHeader(content_type): TypedHeader<ContentType>,
    payload: String,
) -> Result<HttpResponse> {
    // validate source and payload
    let source = query_params.source.as_deref();
    let response = match &log_state.log_validator {
        Some(validator) => validator.validate(source, &payload).await,
        None => None,
    };
    if let Some(response) = response {
        return response;
    }

    let handler = log_state.log_handler;

    let pipeline_name = query_params.pipeline_name.context(InvalidParameterSnafu {
        reason: "pipeline_name is required",
    })?;
    let table_name = query_params.table.context(InvalidParameterSnafu {
        reason: "table is required",
    })?;

    let version = to_pipeline_version(query_params.version).context(PipelineSnafu)?;

    let ignore_errors = query_params.ignore_errors.unwrap_or(false);

    let value = extract_pipeline_value_by_content_type(content_type, payload, ignore_errors)?;

    query_ctx.set_channel(Channel::Http);
    let query_ctx = Arc::new(query_ctx);

    let value = log_state
        .ingest_interceptor
        .as_ref()
        .pre_pipeline(value, query_ctx.clone())?;

    ingest_logs_inner(
        handler,
        pipeline_name,
        version,
        table_name,
        value,
        query_ctx,
    )
    .await
}

fn extract_pipeline_value_by_content_type(
    content_type: ContentType,
    payload: String,
    ignore_errors: bool,
) -> Result<Vec<Value>> {
    Ok(match content_type {
        ct if ct == ContentType::json() => transform_ndjson_array_factory(
            Deserializer::from_str(&payload).into_iter(),
            ignore_errors,
        )?,
        ct if ct == ContentType::text() || ct == ContentType::text_utf8() => payload
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| Value::String(line.to_string()))
            .collect(),
        _ => UnsupportedContentTypeSnafu { content_type }.fail()?,
    })
}

async fn ingest_logs_inner(
    state: LogHandlerRef,
    pipeline_name: String,
    version: PipelineVersion,
    table_name: String,
    pipeline_data: Vec<Value>,
    query_ctx: QueryContextRef,
) -> Result<HttpResponse> {
    let db = query_ctx.get_db_string();
    let exec_timer = std::time::Instant::now();

    let mut results = Vec::with_capacity(pipeline_data.len());
    let transformed_data: Rows;
    if pipeline_name == GREPTIME_INTERNAL_IDENTITY_PIPELINE_NAME {
        let rows = pipeline::identity_pipeline(pipeline_data)
            .context(PipelineTransformSnafu)
            .context(PipelineSnafu)?;
        transformed_data = rows;
    } else {
        let pipeline = state
            .get_pipeline(&pipeline_name, version, query_ctx.clone())
            .await?;

        let transform_timer = std::time::Instant::now();
        let mut intermediate_state = pipeline.init_intermediate_state();

        for v in pipeline_data {
            pipeline
                .prepare(v, &mut intermediate_state)
                .inspect_err(|_| {
                    METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                        .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
                        .observe(transform_timer.elapsed().as_secs_f64());
                })
                .context(PipelineTransformSnafu)
                .context(PipelineSnafu)?;
            let r = pipeline
                .exec_mut(&mut intermediate_state)
                .inspect_err(|_| {
                    METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
                        .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
                        .observe(transform_timer.elapsed().as_secs_f64());
                })
                .context(PipelineTransformSnafu)
                .context(PipelineSnafu)?;
            results.push(r);
            pipeline.reset_intermediate_state(&mut intermediate_state);
        }

        METRIC_HTTP_LOGS_TRANSFORM_ELAPSED
            .with_label_values(&[db.as_str(), METRIC_SUCCESS_VALUE])
            .observe(transform_timer.elapsed().as_secs_f64());

        transformed_data = Rows {
            rows: results,
            schema: pipeline.schemas().clone(),
        };
    }

    let insert_request = RowInsertRequest {
        rows: Some(transformed_data),
        table_name: table_name.clone(),
    };
    let insert_requests = RowInsertRequests {
        inserts: vec![insert_request],
    };
    let output = state.insert_logs(insert_requests, query_ctx).await;

    if let Ok(Output {
        data: OutputData::AffectedRows(rows),
        meta: _,
    }) = &output
    {
        METRIC_HTTP_LOGS_INGESTION_COUNTER
            .with_label_values(&[db.as_str()])
            .inc_by(*rows as u64);
        METRIC_HTTP_LOGS_INGESTION_ELAPSED
            .with_label_values(&[db.as_str(), METRIC_SUCCESS_VALUE])
            .observe(exec_timer.elapsed().as_secs_f64());
    } else {
        METRIC_HTTP_LOGS_INGESTION_ELAPSED
            .with_label_values(&[db.as_str(), METRIC_FAILURE_VALUE])
            .observe(exec_timer.elapsed().as_secs_f64());
    }

    let response = GreptimedbV1Response::from_output(vec![output])
        .await
        .with_execution_time(exec_timer.elapsed().as_millis() as u64);
    Ok(response)
}

#[async_trait]
pub trait LogValidator: Send + Sync {
    /// validate payload by source before processing
    /// Return a `Some` result to indicate validation failure.
    async fn validate(&self, source: Option<&str>, payload: &str) -> Option<Result<HttpResponse>>;
}

pub type LogValidatorRef = Arc<dyn LogValidator + 'static>;

/// axum state struct to hold log handler and validator
#[derive(Clone)]
pub struct LogState {
    pub log_handler: LogHandlerRef,
    pub log_validator: Option<LogValidatorRef>,
    pub ingest_interceptor: Option<LogIngestInterceptorRef<Error>>,
}

#[inline]
fn prost_ts_to_nano(ts: &Timestamp) -> i64 {
    ts.seconds * 1_000_000_000 + ts.nanos as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_ndjson() {
        let s = "{\"a\": 1}\n{\"b\": 2}";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1},{\"b\":2}]");

        let s = "{\"a\": 1}";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1}]");

        let s = "[{\"a\": 1}]";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1}]");

        let s = "[{\"a\": 1}, {\"b\": 2}]";
        let a = Value::Array(
            transform_ndjson_array_factory(Deserializer::from_str(s).into_iter(), false).unwrap(),
        )
        .to_string();
        assert_eq!(a, "[{\"a\":1},{\"b\":2}]");
    }

    #[test]
    fn test_ts_to_nano() {
        // ts = 1731748568804293888
        // seconds = 1731748568
        // nano = 804293888
        let ts = Timestamp {
            seconds: 1731748568,
            nanos: 804293888,
        };
        assert_eq!(prost_ts_to_nano(&ts), 1731748568804293888);
    }
}
