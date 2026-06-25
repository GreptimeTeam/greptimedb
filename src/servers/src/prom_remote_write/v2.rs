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

use std::collections::HashMap;

#[cfg(test)]
use api::greptime_proto::io::prometheus::write::v2::{
    Exemplar, Histogram, Metadata, Sample, metadata,
};
use api::greptime_proto::io::prometheus::write::v2::{Request, TimeSeries};
use api::v1::{RowInsertRequest, Rows, Value};
use bytes::Bytes;
use common_grpc::precision::Precision;
use common_query::prelude::{greptime_timestamp, greptime_value};
use pipeline::{ContextOpt, ContextReq};
use prost::Message;
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{self, Result};
use crate::prom_remote_write::row_builder::PromCtx;
use crate::prom_remote_write::try_decompress;
use crate::prom_remote_write::validation::validate_label_name;
#[allow(deprecated)]
use crate::prom_store::{
    DATABASE_LABEL, DATABASE_LABEL_ALT, METRIC_NAME_LABEL, PHYSICAL_TABLE_LABEL,
    PHYSICAL_TABLE_LABEL_ALT, SCHEMA_LABEL,
};
use crate::row_writer::{self, TableData};

type PromTags = Vec<(String, String)>;
type ResolvedSeriesLabels = (PromCtx, String, PromTags);

pub(crate) fn decode_remote_write_v2_request(is_zstd: bool, body: Bytes) -> Result<Request> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_DECODE_ELAPSED.start_timer();

    // Match the v1 decoder's VictoriaMetrics fallback: some clients may send a
    // mismatched content-encoding header, so try the other compression on failure.
    let buf = if let Ok(buf) = try_decompress(is_zstd, &body[..]) {
        buf
    } else {
        try_decompress(!is_zstd, &body[..])?
    };

    Request::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
}

pub(crate) trait RemoteWriteV2RequestExt {
    fn into_context_req(self) -> Result<ContextReq>;
}

impl RemoteWriteV2RequestExt for Request {
    fn into_context_req(self) -> Result<ContextReq> {
        let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_CONVERT_ELAPSED.start_timer();

        ensure!(
            self.symbols.first().map(|s| s.as_str()) == Some(""),
            error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 symbols must start with an empty string".to_string(),
            }
        );

        let mut tables = HashMap::<PromCtx, HashMap<String, TableData>>::new();

        for series in &self.timeseries {
            // Native histograms and exemplars are intentionally ignored for now.
            // They will be converted into Greptime rows once their ingestion path is implemented.
            if series.samples.is_empty() {
                continue;
            }

            let (prom_ctx, table_name, tags) = resolve_series_labels(&self.symbols, series)?;
            let table_data = tables
                .entry(prom_ctx)
                .or_default()
                .entry(table_name)
                .or_insert_with(|| TableData::new(tags.len() + 2, series.samples.len()));

            for sample in &series.samples {
                let mut row = table_data.alloc_one_row();
                row_writer::write_ts_to_millis(
                    table_data,
                    greptime_timestamp(),
                    Some(sample.timestamp),
                    Precision::Millisecond,
                    &mut row,
                )?;
                row_writer::write_f64(table_data, greptime_value(), sample.value, &mut row)?;
                row_writer::write_tags(table_data, tags.iter().cloned(), &mut row)?;
                table_data.add_row(row);
            }
        }

        Ok(into_context_req(tables))
    }
}

fn resolve_series_labels(symbols: &[String], series: &TimeSeries) -> Result<ResolvedSeriesLabels> {
    ensure!(
        series.labels_refs.len().is_multiple_of(2),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 labels_refs must contain name/value pairs".to_string(),
        }
    );

    let mut prom_ctx = PromCtx::default();
    let mut table_name = None;
    let mut tags = Vec::with_capacity(series.labels_refs.len() / 2);

    for pair in series.labels_refs.chunks_exact(2) {
        let name = symbol_ref(symbols, pair[0], "label name")?;
        let value = symbol_ref(symbols, pair[1], "label value")?;

        if name == METRIC_NAME_LABEL {
            table_name = Some(value.to_string());
            continue;
        }
        if apply_remote_write_special_label(name, value, &mut prom_ctx) {
            continue;
        }

        ensure!(
            validate_label_name(name.as_bytes()),
            error::InvalidPromRemoteRequestSnafu {
                msg: format!("Invalid label name: `{name}`"),
            }
        );
        tags.push((name.to_string(), value.to_string()));
    }

    let table_name = table_name.context(error::InvalidPromRemoteRequestSnafu {
        msg: "missing '__name__' label in time-series".to_string(),
    })?;

    Ok((prom_ctx, table_name, tags))
}

fn symbol_ref<'a>(symbols: &'a [String], idx: u32, field: &str) -> Result<&'a str> {
    symbols
        .get(idx as usize)
        .map(String::as_str)
        .with_context(|| error::InvalidPromRemoteRequestSnafu {
            msg: format!(
                "remote write v2 {field} symbol reference {idx} is out of range, symbols len: {}",
                symbols.len()
            ),
        })
}

#[allow(deprecated)]
fn apply_remote_write_special_label(name: &str, value: &str, prom_ctx: &mut PromCtx) -> bool {
    match name {
        SCHEMA_LABEL => {
            prom_ctx.schema = Some(value.to_string());
            true
        }
        DATABASE_LABEL | DATABASE_LABEL_ALT => {
            if prom_ctx.schema.is_none() {
                prom_ctx.schema = Some(value.to_string());
            }
            true
        }
        PHYSICAL_TABLE_LABEL | PHYSICAL_TABLE_LABEL_ALT => {
            prom_ctx.physical_table = Some(value.to_string());
            true
        }
        _ => false,
    }
}

fn into_context_req(tables: HashMap<PromCtx, HashMap<String, TableData>>) -> ContextReq {
    let mut ctx_req = ContextReq::default();
    for (prom_ctx, tables) in tables {
        let mut opt = ContextOpt::default();
        if let Some(schema) = prom_ctx.schema {
            opt.set_schema(schema);
        }
        if let Some(physical_table) = prom_ctx.physical_table {
            opt.set_physical_table(physical_table);
        }

        ctx_req.add_rows(
            opt,
            tables.into_iter().map(|(table_name, table_data)| {
                table_data_to_row_insert_request(table_name, table_data)
            }),
        );
    }
    ctx_req
}

fn table_data_to_row_insert_request(table_name: String, table_data: TableData) -> RowInsertRequest {
    let num_columns = table_data.num_columns();
    let (schema, mut rows) = table_data.into_schema_and_rows();
    for row in &mut rows {
        if num_columns > row.values.len() {
            row.values.resize(num_columns, Value { value_data: None });
        }
    }

    RowInsertRequest {
        table_name,
        rows: Some(Rows { schema, rows }),
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod test_util {
    use api::greptime_proto::io::prometheus::write::v2::{Histogram, Request, Sample, TimeSeries};

    pub fn request_with_labels_and_samples(
        labels: Vec<(&str, &str)>,
        samples: Vec<Sample>,
    ) -> Request {
        request_with_labels(labels, samples, Vec::new())
    }

    pub fn request_with_labels_and_histograms(
        labels: Vec<(&str, &str)>,
        histograms: Vec<Histogram>,
    ) -> Request {
        request_with_labels(labels, Vec::new(), histograms)
    }

    pub fn histogram(timestamp: i64) -> Histogram {
        Histogram {
            timestamp,
            ..Default::default()
        }
    }

    fn request_with_labels(
        labels: Vec<(&str, &str)>,
        samples: Vec<Sample>,
        histograms: Vec<Histogram>,
    ) -> Request {
        let mut symbols = vec!["".to_string()];
        let mut labels_refs = Vec::with_capacity(labels.len() * 2);
        for (name, value) in labels {
            labels_refs.push(push_symbol(&mut symbols, name));
            labels_refs.push(push_symbol(&mut symbols, value));
        }

        Request {
            symbols,
            timeseries: vec![TimeSeries {
                labels_refs,
                samples,
                histograms,
                exemplars: Vec::new(),
                metadata: None,
            }],
        }
    }

    fn push_symbol(symbols: &mut Vec<String>, symbol: &str) -> u32 {
        if let Some(idx) = symbols.iter().position(|s| s == symbol) {
            return idx as u32;
        }

        let idx = symbols.len();
        symbols.push(symbol.to_string());
        idx as u32
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::value::ValueData;
    use common_query::prelude::{greptime_timestamp, greptime_value};
    use session::context::QueryContext;

    use super::*;
    use crate::error;
    use crate::http::prom_store::PHYSICAL_TABLE_PARAM;
    use crate::prom_store::{DATABASE_LABEL, PHYSICAL_TABLE_LABEL};

    #[test]
    fn test_decode_remote_write_v2_request() {
        let request = Request {
            symbols: vec![
                "".to_string(),
                "__name__".to_string(),
                "http_requests_total".to_string(),
            ],
            timeseries: vec![TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![Sample {
                    value: 42.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: Some(Metadata {
                    r#type: metadata::MetricType::Counter as i32,
                    help_ref: 0,
                    unit_ref: 0,
                }),
            }],
        };
        let body =
            Bytes::from(crate::prom_store::snappy_compress(&request.encode_to_vec()).unwrap());

        let decoded = decode_remote_write_v2_request(false, body).unwrap();

        assert_eq!(decoded.symbols, request.symbols);
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].labels_refs, vec![1, 2]);
        assert_eq!(decoded.timeseries[0].samples.len(), 1);
        assert_eq!(decoded.timeseries[0].samples[0].value, 42.0);
        assert_eq!(decoded.timeseries[0].metadata.as_ref().unwrap().r#type, 1);
    }

    #[test]
    fn test_into_context_req_samples() {
        let ctx_req = test_util::request_with_labels_and_samples(
            vec![
                (METRIC_NAME_LABEL, "http_requests_total"),
                ("job", "api"),
                ("instance", "localhost:9090"),
            ],
            vec![
                Sample {
                    value: 42.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                },
                Sample {
                    value: 43.0,
                    timestamp: 2000,
                    start_timestamp: 0,
                },
            ],
        )
        .into_context_req()
        .unwrap();

        let mut inserts = ctx_req.all_req().collect::<Vec<_>>();
        assert_eq!(inserts.len(), 1);

        let request = inserts.pop().unwrap();
        assert_eq!(request.table_name, "http_requests_total");
        let rows = request.rows.unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.schema
                .iter()
                .map(|col| col.column_name.as_str())
                .collect::<Vec<_>>(),
            vec![greptime_timestamp(), greptime_value(), "job", "instance"]
        );
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMillisecondValue(1000))
        );
        assert_eq!(
            rows.rows[0].values[1].value_data,
            Some(ValueData::F64Value(42.0))
        );
        assert_eq!(
            rows.rows[0].values[2].value_data,
            Some(ValueData::StringValue("api".to_string()))
        );
        assert_eq!(
            rows.rows[0].values[3].value_data,
            Some(ValueData::StringValue("localhost:9090".to_string()))
        );
        assert_eq!(
            rows.rows[1].values[0].value_data,
            Some(ValueData::TimestampMillisecondValue(2000))
        );
        assert_eq!(
            rows.rows[1].values[1].value_data,
            Some(ValueData::F64Value(43.0))
        );
    }

    #[test]
    fn test_into_context_req_special_labels() {
        let ctx_req = test_util::request_with_labels_and_samples(
            vec![
                (METRIC_NAME_LABEL, "cpu_usage"),
                (DATABASE_LABEL, "tenant_a"),
                (PHYSICAL_TABLE_LABEL, "metrics_physical"),
                ("job", "api"),
            ],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        )
        .into_context_req()
        .unwrap();

        let mut iter = ctx_req.as_req_iter(Arc::new(QueryContext::with("greptime", "public")));
        let (ctx, reqs) = iter.next().unwrap();
        assert!(iter.next().is_none());

        assert_eq!(ctx.current_schema(), "tenant_a");
        assert_eq!(
            ctx.extension(PHYSICAL_TABLE_PARAM),
            Some("metrics_physical")
        );
        assert_eq!(reqs.inserts.len(), 1);

        let rows = reqs.inserts[0].rows.as_ref().unwrap();
        assert_eq!(
            rows.schema
                .iter()
                .map(|col| col.column_name.as_str())
                .collect::<Vec<_>>(),
            vec![greptime_timestamp(), greptime_value(), "job"]
        );
    }

    #[test]
    fn test_into_context_req_rejects_missing_metric_name() {
        assert_invalid(
            test_util::request_with_labels_and_samples(
                vec![("job", "api")],
                vec![Sample {
                    value: 1.0,
                    timestamp: 1000,
                    start_timestamp: 0,
                }],
            ),
            "missing '__name__'",
        );
    }

    #[test]
    fn test_into_context_req_rejects_odd_label_refs() {
        let mut request = test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        );
        request.timeseries[0].labels_refs.push(1);

        assert_invalid(request, "labels_refs must contain name/value pairs");
    }

    #[test]
    fn test_into_context_req_rejects_out_of_range_symbol_ref() {
        let mut request = test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        );
        request.timeseries[0].labels_refs[1] = 99;

        assert_invalid(request, "symbol reference 99 is out of range");
    }

    #[test]
    fn test_into_context_req_rejects_non_empty_first_symbol() {
        let mut request = test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        );
        request.symbols[0] = "not-empty".to_string();

        assert_invalid(request, "symbols must start with an empty string");
    }

    #[test]
    fn test_into_context_req_ignores_histograms_and_exemplars() {
        let mut request = test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        );
        request.timeseries[0].histograms.push(Histogram::default());
        request.timeseries[0].exemplars.push(Exemplar::default());

        let ctx_req = request.into_context_req().unwrap();

        assert_eq!(ctx_req.all_req().count(), 1);
    }

    #[test]
    fn test_into_context_req_skips_histogram_only_series() {
        let mut request =
            test_util::request_with_labels_and_samples(vec![(METRIC_NAME_LABEL, "metric")], vec![]);
        request.timeseries[0].histograms.push(Histogram::default());

        let ctx_req = request.into_context_req().unwrap();

        assert_eq!(ctx_req.all_req().count(), 0);
    }

    fn assert_invalid(request: Request, expected: &str) {
        let err = request.into_context_req().unwrap_err();
        assert!(matches!(err, error::Error::InvalidPromRemoteRequest { .. }));
        assert!(
            err.to_string().contains(expected),
            "expected error containing {expected:?}, got {err}"
        );
    }
}
