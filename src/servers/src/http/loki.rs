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

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, Row,
    RowInsertRequest, Rows, SemanticType, Value as GreptimeValue,
};
use axum::Extension;
use axum::extract::State;
use axum_extra::TypedHeader;
use bytes::Bytes;
use chrono::DateTime;
use common_query::prelude::greptime_timestamp;
use common_query::{Output, OutputData};
use common_telemetry::{error, warn};
use headers::ContentType;
use jsonb::Value;
use lazy_static::lazy_static;
use loki_proto::logproto::LabelPairAdapter;
use loki_proto::prost_types::Timestamp as LokiTimestamp;
use pipeline::util::to_pipeline_version;
use pipeline::{ContextReq, PipelineContext, PipelineDefinition, SchemaInfo};
use prost::Message;
use quoted_string::test_utils::TestSpec;
use session::context::{Channel, QueryContext};
use snafu::{OptionExt, ResultExt, ensure};
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    DecodeLokiRequestSnafu, InvalidLokiLabelsSnafu, InvalidLokiPayloadSnafu, ParseJsonSnafu,
    PipelineSnafu, Result, UnsupportedContentTypeSnafu,
};
use crate::http::HttpResponse;
use crate::http::event::{JSON_CONTENT_TYPE, LogState, PB_CONTENT_TYPE, PipelineIngestRequest};
use crate::http::extractor::{LogTableName, PipelineInfo};
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::metrics::{
    METRIC_FAILURE_VALUE, METRIC_LOKI_LOGS_INGESTION_COUNTER, METRIC_LOKI_LOGS_INGESTION_ELAPSED,
    METRIC_SUCCESS_VALUE,
};
use crate::pipeline::run_pipeline;
use crate::prom_store;

const LOKI_TABLE_NAME: &str = "loki_logs";
const LOKI_LINE_COLUMN: &str = "line";
const LOKI_STRUCTURED_METADATA_COLUMN: &str = "structured_metadata";

const LOKI_LINE_COLUMN_NAME: &str = "loki_line";

const LOKI_PIPELINE_METADATA_PREFIX: &str = "loki_metadata_";
const LOKI_PIPELINE_LABEL_PREFIX: &str = "loki_label_";

const STREAMS_KEY: &str = "streams";
const LABEL_KEY: &str = "stream";
const LINES_KEY: &str = "values";

lazy_static! {
    static ref LOKI_INIT_SCHEMAS: Vec<ColumnSchema> = vec![
        ColumnSchema {
            column_name: greptime_timestamp().to_string(),
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
    pipeline_info: PipelineInfo,
    bytes: Bytes,
) -> Result<HttpResponse> {
    ctx.set_channel(Channel::Loki);
    let ctx = Arc::new(ctx);
    let table_name = table_name.unwrap_or_else(|| LOKI_TABLE_NAME.to_string());
    let db = ctx.get_db_string();
    let db_str = db.as_str();
    let exec_timer = Instant::now();

    let handler = log_state.log_handler;

    let ctx_req = if let Some(pipeline_name) = pipeline_info.pipeline_name {
        // go pipeline
        let version = to_pipeline_version(pipeline_info.pipeline_version.as_deref())
            .context(PipelineSnafu)?;
        let def =
            PipelineDefinition::from_name(&pipeline_name, version, None).context(PipelineSnafu)?;
        let pipeline_ctx =
            PipelineContext::new(&def, &pipeline_info.pipeline_params, Channel::Loki);

        let v = extract_item::<LokiPipeline>(content_type, bytes)?
            .map(|i| i.map)
            .collect::<Vec<_>>();

        let req = PipelineIngestRequest {
            table: table_name,
            values: v,
        };

        run_pipeline(&handler, &pipeline_ctx, req, &ctx, true).await?
    } else {
        // init schemas
        let mut schema_info = SchemaInfo::from_schema_list(LOKI_INIT_SCHEMAS.clone());
        let mut rows = Vec::with_capacity(256);
        for loki_row in extract_item::<LokiRawItem>(content_type, bytes)? {
            let mut row = init_row(
                schema_info.schema.len(),
                loki_row.ts,
                loki_row.line,
                loki_row.structured_metadata,
            );
            process_labels(&mut schema_info, &mut row, loki_row.labels);
            rows.push(row);
        }

        let schemas = schema_info.column_schemas()?;
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

        ContextReq::default_opt_with_reqs(vec![ins_req])
    };

    let mut outputs = Vec::with_capacity(ctx_req.map_len());
    for (temp_ctx, req) in ctx_req.as_req_iter(ctx) {
        let output = handler.insert(req, temp_ctx).await;

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
        outputs.push(output);
    }

    let response = GreptimedbV1Response::from_output(outputs)
        .await
        .with_execution_time(exec_timer.elapsed().as_millis() as u64);
    Ok(response)
}

/// This is the holder of the loki lines parsed from json or protobuf.
/// The generic here is either [VrlValue] or [Vec<LabelPairAdapter>].
/// Depending on the target destination, this can be converted to [LokiRawItem] or [LokiPipeline].
pub struct LokiMiddleItem<T> {
    pub ts: i64,
    pub line: String,
    pub structured_metadata: Option<T>,
    pub labels: Option<BTreeMap<String, String>>,
}

/// This is the line item for the Loki raw ingestion.
/// We'll persist the line in its whole, set labels into tags,
/// and structured metadata into a big JSON.
pub struct LokiRawItem {
    pub ts: i64,
    pub line: String,
    pub structured_metadata: Vec<u8>,
    pub labels: Option<BTreeMap<String, String>>,
}

/// This is the line item prepared for the pipeline engine.
pub struct LokiPipeline {
    pub map: VrlValue,
}

/// This is the flow of the Loki ingestion.
/// +--------+
/// | bytes  |
/// +--------+
///     |
/// +----------------------+----------------------+
/// |                      |                      |
/// |   JSON content type  |   PB content type    |
/// +----------------------+----------------------+
/// |                      |                      |
/// | JsonStreamItem       | PbStreamItem         |
/// | stream: serde_json   | stream: adapter      |
/// +----------------------+----------------------+
/// |                      |                      |
/// | MiddleItem<serde_json> | MiddleItem<entry>  |
/// +----------------------+----------------------+
///           \                  /
///            \                /
///             \              /
///         +----------------------+
///         |   MiddleItem<T>      |
///         +----------------------+
///                 |
///     +----------------+----------------+
///     |                                 |
/// +------------------+         +---------------------+
/// |   LokiRawItem    |         |  LokiPipelineItem   |
/// +------------------+         +---------------------+
///           |                             |
/// +------------------+         +---------------------+
/// |   Loki ingest    |         |   run_pipeline      |
/// +------------------+         +---------------------+
fn extract_item<T>(content_type: ContentType, bytes: Bytes) -> Result<Box<dyn Iterator<Item = T>>>
where
    LokiMiddleItem<VrlValue>: Into<T>,
    LokiMiddleItem<Vec<LabelPairAdapter>>: Into<T>,
{
    match content_type {
        x if x == *JSON_CONTENT_TYPE => Ok(Box::new(
            LokiJsonParser::from_bytes(bytes)?.flat_map(|item| item.into_iter().map(|i| i.into())),
        )),
        x if x == *PB_CONTENT_TYPE => Ok(Box::new(
            LokiPbParser::from_bytes(bytes)?.flat_map(|item| item.into_iter().map(|i| i.into())),
        )),
        _ => UnsupportedContentTypeSnafu { content_type }.fail(),
    }
}

struct LokiJsonParser {
    pub streams: VecDeque<VrlValue>,
}

impl LokiJsonParser {
    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let payload: VrlValue = serde_json::from_slice(bytes.as_ref()).context(ParseJsonSnafu)?;

        let VrlValue::Object(mut map) = payload else {
            return InvalidLokiPayloadSnafu {
                msg: "payload is not an object",
            }
            .fail();
        };

        let streams = map.remove(STREAMS_KEY).context(InvalidLokiPayloadSnafu {
            msg: "missing streams",
        })?;

        let VrlValue::Array(streams) = streams else {
            return InvalidLokiPayloadSnafu {
                msg: "streams is not an array",
            }
            .fail();
        };

        Ok(Self {
            streams: streams.into(),
        })
    }
}

impl Iterator for LokiJsonParser {
    type Item = JsonStreamItem;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(stream) = self.streams.pop_front() {
            // get lines from the map
            let VrlValue::Object(mut map) = stream else {
                warn!("stream is not an object, {:?}", stream);
                continue;
            };
            let Some(lines) = map.remove(LINES_KEY) else {
                warn!("missing lines on stream, {:?}", map);
                continue;
            };
            let VrlValue::Array(lines) = lines else {
                warn!("lines is not an array, {:?}", lines);
                continue;
            };

            // get labels
            let labels = map
                .remove(LABEL_KEY)
                .and_then(|m| match m {
                    VrlValue::Object(labels) => Some(labels),
                    _ => None,
                })
                .map(|m| {
                    m.into_iter()
                        .filter_map(|(k, v)| match v {
                            VrlValue::Bytes(v) => {
                                Some((k.into(), String::from_utf8_lossy(&v).to_string()))
                            }
                            _ => None,
                        })
                        .collect::<BTreeMap<String, String>>()
                });

            return Some(JsonStreamItem {
                lines: lines.into(),
                labels,
            });
        }
        None
    }
}

struct JsonStreamItem {
    pub lines: VecDeque<VrlValue>,
    pub labels: Option<BTreeMap<String, String>>,
}

impl Iterator for JsonStreamItem {
    type Item = LokiMiddleItem<VrlValue>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(line) = self.lines.pop_front() {
            let VrlValue::Array(line) = line else {
                warn!("line is not an array, {:?}", line);
                continue;
            };
            if line.len() < 2 {
                warn!("line is too short, {:?}", line);
                continue;
            }
            let mut line: VecDeque<VrlValue> = line.into();

            // get ts
            let ts = line.pop_front().and_then(|ts| match ts {
                VrlValue::Bytes(ts) => String::from_utf8_lossy(&ts).parse::<i64>().ok(),
                _ => {
                    warn!("missing or invalid timestamp, {:?}", ts);
                    None
                }
            });
            let Some(ts) = ts else {
                continue;
            };

            let line_text = line.pop_front().and_then(|l| match l {
                VrlValue::Bytes(l) => Some(String::from_utf8_lossy(&l).to_string()),
                _ => {
                    warn!("missing or invalid line, {:?}", l);
                    None
                }
            });
            let Some(line_text) = line_text else {
                continue;
            };

            let structured_metadata = line.pop_front();

            return Some(LokiMiddleItem {
                ts,
                line: line_text,
                structured_metadata,
                labels: self.labels.clone(),
            });
        }
        None
    }
}

impl From<LokiMiddleItem<VrlValue>> for LokiRawItem {
    fn from(val: LokiMiddleItem<VrlValue>) -> Self {
        let LokiMiddleItem {
            ts,
            line,
            structured_metadata,
            labels,
        } = val;

        let structured_metadata = structured_metadata
            .and_then(|m| match m {
                VrlValue::Object(m) => Some(m),
                _ => None,
            })
            .map(|m| {
                m.into_iter()
                    .filter_map(|(k, v)| match v {
                        VrlValue::Bytes(bytes) => Some((
                            k.into(),
                            Value::String(String::from_utf8_lossy(&bytes).to_string().into()),
                        )),
                        _ => None,
                    })
                    .collect::<BTreeMap<String, Value>>()
            })
            .unwrap_or_default();
        let structured_metadata = Value::Object(structured_metadata).to_vec();

        LokiRawItem {
            ts,
            line,
            structured_metadata,
            labels,
        }
    }
}

impl From<LokiMiddleItem<VrlValue>> for LokiPipeline {
    fn from(value: LokiMiddleItem<VrlValue>) -> Self {
        let LokiMiddleItem {
            ts,
            line,
            structured_metadata,
            labels,
        } = value;

        let mut map = BTreeMap::new();
        map.insert(
            KeyString::from(greptime_timestamp()),
            VrlValue::Timestamp(DateTime::from_timestamp_nanos(ts)),
        );
        map.insert(
            KeyString::from(LOKI_LINE_COLUMN_NAME),
            VrlValue::Bytes(line.into()),
        );

        if let Some(VrlValue::Object(m)) = structured_metadata {
            for (k, v) in m {
                map.insert(
                    KeyString::from(format!("{}{}", LOKI_PIPELINE_METADATA_PREFIX, k)),
                    v,
                );
            }
        }
        if let Some(v) = labels {
            v.into_iter().for_each(|(k, v)| {
                map.insert(
                    KeyString::from(format!("{}{}", LOKI_PIPELINE_LABEL_PREFIX, k)),
                    VrlValue::Bytes(v.into()),
                );
            });
        }

        LokiPipeline {
            map: VrlValue::Object(map),
        }
    }
}

pub struct LokiPbParser {
    pub streams: VecDeque<loki_proto::logproto::StreamAdapter>,
}

impl LokiPbParser {
    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let decompressed = prom_store::snappy_decompress(&bytes).unwrap();
        let req = loki_proto::logproto::PushRequest::decode(&decompressed[..])
            .context(DecodeLokiRequestSnafu)?;

        Ok(Self {
            streams: req.streams.into(),
        })
    }
}

impl Iterator for LokiPbParser {
    type Item = PbStreamItem;

    fn next(&mut self) -> Option<Self::Item> {
        let stream = self.streams.pop_front()?;

        let labels = parse_loki_labels(&stream.labels)
            .inspect_err(|e| {
                error!(e; "failed to parse loki labels, {:?}", stream.labels);
            })
            .ok();

        Some(PbStreamItem {
            entries: stream.entries.into(),
            labels,
        })
    }
}

pub struct PbStreamItem {
    pub entries: VecDeque<loki_proto::logproto::EntryAdapter>,
    pub labels: Option<BTreeMap<String, String>>,
}

impl Iterator for PbStreamItem {
    type Item = LokiMiddleItem<Vec<LabelPairAdapter>>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.entries.pop_front() {
            let ts = if let Some(ts) = entry.timestamp {
                ts
            } else {
                warn!("missing timestamp, {:?}", entry);
                continue;
            };
            let line = entry.line;

            let structured_metadata = entry.structured_metadata;

            return Some(LokiMiddleItem {
                ts: prost_ts_to_nano(&ts),
                line,
                structured_metadata: Some(structured_metadata),
                labels: self.labels.clone(),
            });
        }
        None
    }
}

impl From<LokiMiddleItem<Vec<LabelPairAdapter>>> for LokiRawItem {
    fn from(val: LokiMiddleItem<Vec<LabelPairAdapter>>) -> Self {
        let LokiMiddleItem {
            ts,
            line,
            structured_metadata,
            labels,
        } = val;

        let structured_metadata = structured_metadata
            .unwrap_or_default()
            .into_iter()
            .map(|d| (d.name, Value::String(d.value.into())))
            .collect::<BTreeMap<String, Value>>();
        let structured_metadata = Value::Object(structured_metadata).to_vec();

        LokiRawItem {
            ts,
            line,
            structured_metadata,
            labels,
        }
    }
}

impl From<LokiMiddleItem<Vec<LabelPairAdapter>>> for LokiPipeline {
    fn from(value: LokiMiddleItem<Vec<LabelPairAdapter>>) -> Self {
        let LokiMiddleItem {
            ts,
            line,
            structured_metadata,
            labels,
        } = value;

        let mut map = BTreeMap::new();
        map.insert(
            KeyString::from(greptime_timestamp()),
            VrlValue::Timestamp(DateTime::from_timestamp_nanos(ts)),
        );
        map.insert(
            KeyString::from(LOKI_LINE_COLUMN_NAME),
            VrlValue::Bytes(line.into()),
        );

        structured_metadata
            .unwrap_or_default()
            .into_iter()
            .for_each(|d| {
                map.insert(
                    KeyString::from(format!("{}{}", LOKI_PIPELINE_METADATA_PREFIX, d.name)),
                    VrlValue::Bytes(d.value.into()),
                );
            });

        if let Some(v) = labels {
            v.into_iter().for_each(|(k, v)| {
                map.insert(
                    KeyString::from(format!("{}{}", LOKI_PIPELINE_LABEL_PREFIX, k)),
                    VrlValue::Bytes(v.into()),
                );
            });
        }

        LokiPipeline {
            map: VrlValue::Object(map),
        }
    }
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
fn prost_ts_to_nano(ts: &LokiTimestamp) -> i64 {
    ts.seconds * 1_000_000_000 + ts.nanos as i64
}

fn init_row(
    schema_len: usize,
    ts: i64,
    line: String,
    structured_metadata: Vec<u8>,
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
        value_data: Some(ValueData::BinaryValue(structured_metadata)),
    });
    for _ in 0..(schema_len - 3) {
        row.push(GreptimeValue { value_data: None });
    }
    row
}

fn process_labels(
    schema_info: &mut SchemaInfo,
    row: &mut Vec<GreptimeValue>,
    labels: Option<BTreeMap<String, String>>,
) {
    let Some(labels) = labels else {
        return;
    };

    let column_indexer = &mut schema_info.index;
    let schemas = &mut schema_info.schema;

    // insert labels
    for (k, v) in labels {
        if let Some(index) = column_indexer.get(&k) {
            // exist in schema
            // insert value using index
            row[*index] = GreptimeValue {
                value_data: Some(ValueData::StringValue(v)),
            };
        } else {
            // not exist
            // add schema and append to values
            schemas.push(
                ColumnSchema {
                    column_name: k.clone(),
                    datatype: ColumnDataType::String.into(),
                    semantic_type: SemanticType::Tag.into(),
                    datatype_extension: None,
                    options: None,
                }
                .into(),
            );
            column_indexer.insert(k, schemas.len() - 1);

            row.push(GreptimeValue {
                value_data: Some(ValueData::StringValue(v)),
            });
        }
    }
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
