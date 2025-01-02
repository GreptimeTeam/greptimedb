use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows, SemanticType,
    Value as GreptimeValue,
};
use axum::extract::State;
use axum::headers::ContentType;
use axum::{Extension, TypedHeader};
use bytes::Bytes;
use common_query::prelude::GREPTIME_TIMESTAMP;
use common_query::{Output, OutputData};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use loki_api::prost_types::Timestamp;
use prost::Message;
use session::context::{Channel, QueryContext, QueryContextRef};
use snafu::ResultExt;

use crate::error::{DecodeOtlpRequestSnafu, ParseJson5Snafu, Result, UnsupportedContentTypeSnafu};
use crate::http::event::LogState;
use crate::http::extractor::LogTableName;
use crate::http::header::CONTENT_TYPE_PROTOBUF_STR;
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::http::HttpResponse;
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_LOKI_LOGS_INGESTION_COUNTER, METRIC_LOKI_LOGS_INGESTION_ELAPSED,
    METRIC_SUCCESS_VALUE,
};
use crate::prom_store;

const LOKI_TABLE_NAME: &str = "loki_logs";
const LOKI_LINE_COLUMN: &str = "line";

lazy_static! {
    static ref PB_CONTENT_TYPE: ContentType =
        ContentType::from_str(CONTENT_TYPE_PROTOBUF_STR).unwrap();
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

    match content_type {
        x if x == *PB_CONTENT_TYPE => handle_pb_req(bytes, table_name, log_state, ctx).await,
        _ => UnsupportedContentTypeSnafu { content_type }.fail(),
    }
}

async fn handle_pb_req(
    bytes: Bytes,
    table_name: String,
    log_state: LogState,
    ctx: QueryContextRef,
) -> Result<HttpResponse> {
    let db = ctx.get_db_string();
    let db_str = db.as_str();
    let exec_timer = Instant::now();

    let decompressed = prom_store::snappy_decompress(&bytes).unwrap();
    let req = loki_api::logproto::PushRequest::decode(&decompressed[..])
        .context(DecodeOtlpRequestSnafu)?;

    // init schemas
    let mut schemas = LOKI_INIT_SCHEMAS.clone();

    let mut global_label_key_index: HashMap<String, u16> = HashMap::new();
    global_label_key_index.insert(GREPTIME_TIMESTAMP.to_string(), 0);
    global_label_key_index.insert(LOKI_LINE_COLUMN.to_string(), 1);

    let cnt = req.streams.iter().map(|s| s.entries.len()).sum::<usize>();
    let mut rows = Vec::with_capacity(cnt);

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
            let mut row = Vec::with_capacity(schemas.len());
            // set ts and line
            row.push(GreptimeValue {
                value_data: Some(ValueData::TimestampNanosecondValue(prost_ts_to_nano(&ts))),
            });
            row.push(GreptimeValue {
                value_data: Some(ValueData::StringValue(line)),
            });
            for _ in 0..(schemas.len() - 2) {
                row.push(GreptimeValue { value_data: None });
            }

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
                    global_label_key_index.insert(k.clone(), (schemas.len() - 1) as u16);

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

#[inline]
fn prost_ts_to_nano(ts: &Timestamp) -> i64 {
    ts.seconds * 1_000_000_000 + ts.nanos as i64
}

#[cfg(test)]
mod tests {
    use loki_api::prost_types::Timestamp;

    use crate::http::loki::prost_ts_to_nano;

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
