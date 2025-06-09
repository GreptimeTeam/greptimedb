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
use std::time::Instant;

use ahash::{HashMap, HashMapExt};
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, Row,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType, Value as GreptimeValue,
};
use axum::extract::State;
use axum::Extension;
use axum_extra::TypedHeader;
use bytes::Bytes;
use common_query::prelude::GREPTIME_TIMESTAMP;
use common_query::{Output, OutputData};
use common_telemetry::{error, warn};
use headers::ContentType;
use jsonb::Value;
use lazy_static::lazy_static;
use loki_proto::prost_types::Timestamp;
use prost::Message;
use quoted_string::test_utils::TestSpec;
use session::context::{Channel, QueryContext};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    DecodeOtlpRequestSnafu, InvalidLokiLabelsSnafu, InvalidLokiPayloadSnafu, ParseJsonSnafu,
    Result, UnsupportedContentTypeSnafu,
};
use crate::http::event::{LogState, JSON_CONTENT_TYPE, PB_CONTENT_TYPE};
use crate::http::extractor::LogTableName;
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_LOKI_LOGS_INGESTION_COUNTER, METRIC_LOKI_LOGS_INGESTION_ELAPSED,
    METRIC_SUCCESS_VALUE,
};
use crate::{prom_store, unwrap_or_warn_continue};

const LOKI_TABLE_NAME: &str = "loki_logs";
const LOKI_LINE_COLUMN: &str = "line";
const LOKI_STRUCTURED_METADATA_COLUMN: &str = "structured_metadata";

const STREAMS_KEY: &str = "streams";
const LABEL_KEY: &str = "stream";
const LINES_KEY: &str = "values";

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
        ColumnSchema {
            column_name: LOKI_STRUCTURED_METADATA_COLUMN.to_string(),
            datatype: ColumnDataType::Binary.into(),
            semantic_type: SemanticType::Field.into(),
            datatype_extension: Some(ColumnDataTypeExtension {
                type_ext: Some(api::v1::column_data_type_extension::TypeExt::JsonType(
                    JsonTypeExtension::JsonBinary.into()
                ))
            }),
            options: None,
        }
    ];
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
    let table_name = table_name.unwrap_or_else(|| LOKI_TABLE_NAME.to_string());
    let db = ctx.get_db_string();
    let db_str = db.as_str();
    let exec_timer = Instant::now();

    // init schemas
    let mut schemas = LOKI_INIT_SCHEMAS.clone();

    let mut rows = match content_type {
        x if x == *JSON_CONTENT_TYPE => handle_json_req(bytes, &mut schemas).await,
        x if x == *PB_CONTENT_TYPE => handle_pb_req(bytes, &mut schemas).await,
        _ => UnsupportedContentTypeSnafu { content_type }.fail(),
    }?;

    // fill Null for missing values
    for row in rows.iter_mut() {
        row.resize(schemas.len(), GreptimeValue::default());
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
    let output = handler.insert(ins_reqs, ctx).await;

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

async fn handle_json_req(
    bytes: Bytes,
    schemas: &mut Vec<ColumnSchema>,
) -> Result<Vec<Vec<GreptimeValue>>> {
    let mut column_indexer: HashMap<String, u16> = HashMap::new();
    column_indexer.insert(GREPTIME_TIMESTAMP.to_string(), 0);
    column_indexer.insert(LOKI_LINE_COLUMN.to_string(), 1);

    let payload: serde_json::Value =
        serde_json::from_slice(bytes.as_ref()).context(ParseJsonSnafu)?;

    let streams = payload
        .get(STREAMS_KEY)
        .context(InvalidLokiPayloadSnafu {
            msg: "missing streams",
        })?
        .as_array()
        .context(InvalidLokiPayloadSnafu {
            msg: "streams is not an array",
        })?;

    let mut rows = Vec::with_capacity(1000);

    for (stream_index, stream) in streams.iter().enumerate() {
        // parse lines first
        // do not use `?` in case there are multiple streams
        let lines = unwrap_or_warn_continue!(
            stream.get(LINES_KEY),
            "missing values on stream {}",
            stream_index
        );
        let lines = unwrap_or_warn_continue!(
            lines.as_array(),
            "values is not an array on stream {}",
            stream_index
        );

        // get labels
        let labels = stream
            .get(LABEL_KEY)
            .and_then(|label| label.as_object())
            .map(|l| {
                l.iter()
                    .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                    .collect::<BTreeMap<String, String>>()
            });

        // process each line
        for (line_index, line) in lines.iter().enumerate() {
            let line = unwrap_or_warn_continue!(
                line.as_array(),
                "missing line on stream {} index {}",
                stream_index,
                line_index
            );
            if line.len() < 2 {
                warn!(
                    "line on stream {} index {} is too short",
                    stream_index, line_index
                );
                continue;
            }
            // get ts
            let ts = unwrap_or_warn_continue!(
                line.first()
                    .and_then(|ts| ts.as_str())
                    .and_then(|ts| ts.parse::<i64>().ok()),
                "missing or invalid timestamp on stream {} index {}",
                stream_index,
                line_index
            );
            // get line
            let line_text = unwrap_or_warn_continue!(
                line.get(1)
                    .and_then(|line| line.as_str())
                    .map(|line| line.to_string()),
                "missing or invalid line on stream {} index {}",
                stream_index,
                line_index
            );

            let structured_metadata = match line.get(2) {
                Some(sdata) if sdata.is_object() => sdata
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), Value::String(s.into()))))
                    .collect(),
                _ => BTreeMap::new(),
            };
            let structured_metadata = Value::Object(structured_metadata);

            let mut row = init_row(schemas.len(), ts, line_text, structured_metadata);

            process_labels(&mut column_indexer, schemas, &mut row, labels.as_ref());

            rows.push(row);
        }
    }

    Ok(rows)
}

async fn handle_pb_req(
    bytes: Bytes,
    schemas: &mut Vec<ColumnSchema>,
) -> Result<Vec<Vec<GreptimeValue>>> {
    let decompressed = prom_store::snappy_decompress(&bytes).unwrap();
    let req = loki_proto::logproto::PushRequest::decode(&decompressed[..])
        .context(DecodeOtlpRequestSnafu)?;

    let mut column_indexer: HashMap<String, u16> = HashMap::new();
    column_indexer.insert(GREPTIME_TIMESTAMP.to_string(), 0);
    column_indexer.insert(LOKI_LINE_COLUMN.to_string(), 1);

    let cnt = req.streams.iter().map(|s| s.entries.len()).sum::<usize>();
    let mut rows = Vec::with_capacity(cnt);

    for stream in req.streams {
        let labels = parse_loki_labels(&stream.labels)
            .inspect_err(|e| {
                error!(e; "failed to parse loki labels");
            })
            .ok();

        // process entries
        for entry in stream.entries {
            let ts = if let Some(ts) = entry.timestamp {
                ts
            } else {
                continue;
            };
            let line = entry.line;

            let structured_metadata = entry
                .structured_metadata
                .into_iter()
                .map(|d| (d.name, Value::String(d.value.into())))
                .collect::<BTreeMap<String, Value>>();
            let structured_metadata = Value::Object(structured_metadata);

            let mut row = init_row(
                schemas.len(),
                prost_ts_to_nano(&ts),
                line,
                structured_metadata,
            );

            process_labels(&mut column_indexer, schemas, &mut row, labels.as_ref());

            rows.push(row);
        }
    }

    Ok(rows)
}

/// since we're hand-parsing the labels, if any error is encountered, we'll just skip the label
/// note: pub here for bench usage
/// ref:
/// 1. encoding: https://github.com/grafana/alloy/blob/be34410b9e841cc0c37c153f9550d9086a304bca/internal/component/common/loki/client/batch.go#L114-L145
/// 2. test data: https://github.com/grafana/loki/blob/a24ef7b206e0ca63ee74ca6ecb0a09b745cd2258/pkg/push/types_test.go
pub fn parse_loki_labels(labels: &str) -> Result<BTreeMap<String, String>> {
    let mut labels = labels.trim();
    ensure!(
        labels.len() >= 2,
        InvalidLokiLabelsSnafu {
            msg: "labels string too short"
        }
    );
    ensure!(
        labels.starts_with("{"),
        InvalidLokiLabelsSnafu {
            msg: "missing `{` at the beginning"
        }
    );
    ensure!(
        labels.ends_with("}"),
        InvalidLokiLabelsSnafu {
            msg: "missing `}` at the end"
        }
    );

    let mut result = BTreeMap::new();
    labels = &labels[1..labels.len() - 1];

    while !labels.is_empty() {
        // parse key
        let first_index = labels.find("=").with_context(|| InvalidLokiLabelsSnafu {
            msg: format!("missing `=` near: {}", labels),
        })?;
        let key = &labels[..first_index];
        labels = &labels[first_index + 1..];

        // parse value
        let qs = quoted_string::parse::<TestSpec>(labels)
            .map_err(|e| {
                InvalidLokiLabelsSnafu {
                    msg: format!(
                        "failed to parse quoted string near: {}, reason: {}",
                        labels, e.1
                    ),
                }
                .build()
            })?
            .quoted_string;

        labels = &labels[qs.len()..];

        let value = quoted_string::to_content::<TestSpec>(qs).map_err(|e| {
            InvalidLokiLabelsSnafu {
                msg: format!("failed to unquote the string: {}, reason: {}", qs, e),
            }
            .build()
        })?;

        // insert key and value
        result.insert(key.to_string(), value.to_string());

        if labels.is_empty() {
            break;
        }
        ensure!(
            labels.starts_with(","),
            InvalidLokiLabelsSnafu { msg: "missing `,`" }
        );
        labels = labels[1..].trim_start();
    }

    Ok(result)
}

#[inline]
fn prost_ts_to_nano(ts: &Timestamp) -> i64 {
    ts.seconds * 1_000_000_000 + ts.nanos as i64
}

fn init_row(
    schema_len: usize,
    ts: i64,
    line: String,
    structured_metadata: Value,
) -> Vec<GreptimeValue> {
    // create and init row
    let mut row = Vec::with_capacity(schema_len);
    // set ts and line
    row.push(GreptimeValue {
        value_data: Some(ValueData::TimestampNanosecondValue(ts)),
    });
    row.push(GreptimeValue {
        value_data: Some(ValueData::StringValue(line)),
    });
    row.push(GreptimeValue {
        value_data: Some(ValueData::BinaryValue(structured_metadata.to_vec())),
    });
    for _ in 0..(schema_len - 3) {
        row.push(GreptimeValue { value_data: None });
    }
    row
}

fn process_labels(
    column_indexer: &mut HashMap<String, u16>,
    schemas: &mut Vec<ColumnSchema>,
    row: &mut Vec<GreptimeValue>,
    labels: Option<&BTreeMap<String, String>>,
) {
    let Some(labels) = labels else {
        return;
    };

    // insert labels
    for (k, v) in labels {
        if let Some(index) = column_indexer.get(k) {
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
            column_indexer.insert(k.clone(), (schemas.len() - 1) as u16);

            row.push(GreptimeValue {
                value_data: Some(ValueData::StringValue(v.clone())),
            });
        }
    }
}

#[macro_export]
macro_rules! unwrap_or_warn_continue {
    ($expr:expr, $msg:expr) => {
        if let Some(value) = $expr {
            value
        } else {
            warn!($msg);
            continue;
        }
    };

    ($expr:expr, $fmt:expr, $($arg:tt)*) => {
        if let Some(value) = $expr {
            value
        } else {
            warn!($fmt, $($arg)*);
            continue;
        }
    };
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use loki_proto::prost_types::Timestamp;

    use crate::error::Error::InvalidLokiLabels;
    use crate::http::loki::{parse_loki_labels, prost_ts_to_nano};

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

    #[test]
    fn test_parse_loki_labels() {
        let mut expected = BTreeMap::new();
        expected.insert("job".to_string(), "foobar".to_string());
        expected.insert("cluster".to_string(), "foo-central1".to_string());
        expected.insert("namespace".to_string(), "bar".to_string());
        expected.insert("container_name".to_string(), "buzz".to_string());

        // perfect case
        let valid_labels =
            r#"{job="foobar", cluster="foo-central1", namespace="bar", container_name="buzz"}"#;
        let re = parse_loki_labels(valid_labels);
        assert!(re.is_ok());
        assert_eq!(re.unwrap(), expected);

        // too short
        let too_short = r#"}"#;
        let re = parse_loki_labels(too_short);
        assert!(matches!(re.err().unwrap(), InvalidLokiLabels { .. }));

        // missing start
        let missing_start = r#"job="foobar"}"#;
        let re = parse_loki_labels(missing_start);
        assert!(matches!(re.err().unwrap(), InvalidLokiLabels { .. }));

        // missing start
        let missing_end = r#"{job="foobar""#;
        let re = parse_loki_labels(missing_end);
        assert!(matches!(re.err().unwrap(), InvalidLokiLabels { .. }));

        // missing equal
        let missing_equal = r#"{job"foobar"}"#;
        let re = parse_loki_labels(missing_equal);
        assert!(matches!(re.err().unwrap(), InvalidLokiLabels { .. }));

        // missing quote
        let missing_quote = r#"{job=foobar}"#;
        let re = parse_loki_labels(missing_quote);
        assert!(matches!(re.err().unwrap(), InvalidLokiLabels { .. }));

        // missing comma
        let missing_comma = r#"{job="foobar" cluster="foo-central1"}"#;
        let re = parse_loki_labels(missing_comma);
        assert!(matches!(re.err().unwrap(), InvalidLokiLabels { .. }));
    }
}
