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

use std::collections::hash_map::Entry;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use api::greptime_proto::io::prometheus::write::v2::histogram::{Count, ZeroCount};
#[cfg(test)]
use api::greptime_proto::io::prometheus::write::v2::{Exemplar, Metadata, metadata};
use api::greptime_proto::io::prometheus::write::v2::{Histogram, Request, Sample, TimeSeries};
use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, ListValue, RowInsertRequest, Rows, SemanticType, Value};
use bytes::Bytes;
use common_grpc::precision::Precision;
use common_query::native_histogram::*;
use common_query::prelude::{greptime_timestamp, greptime_value};
use pipeline::{ContextOpt, ContextReq};
use prost::Message;
use snafu::{OptionExt, ResultExt, ensure};

use crate::error::{self, Result};
use crate::prom_remote_write::row_builder::PromCtx;
use crate::prom_remote_write::try_decompress;
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

pub(crate) struct RemoteWriteV2WriteRequests {
    pub samples: ContextReq,
    pub histograms: ContextReq,
    pub sample_count: u64,
    pub histogram_count: u64,
}

/// Converts a PRW v2 request into normal sample writes and native histogram writes.
///
/// A metric name may appear in only one payload kind per request because the
/// metric-engine logical table is either a float metric or a native histogram.
pub(crate) fn into_write_requests(request: Request) -> Result<RemoteWriteV2WriteRequests> {
    let _timer = crate::metrics::METRIC_HTTP_PROM_STORE_CONVERT_ELAPSED.start_timer();
    let Request {
        symbols,
        timeseries,
    } = request;

    ensure!(
        symbols.first().map(|s| s.as_str()) == Some(""),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 symbols must start with an empty string".to_string(),
        }
    );

    let mut sample_tables = HashMap::<PromCtx, HashMap<String, TableData>>::new();
    let mut histogram_tables = HashMap::<PromCtx, HashMap<String, TableData>>::new();
    let mut sample_metrics = HashSet::<(PromCtx, String)>::new();
    let mut histogram_metrics = HashSet::<(PromCtx, String)>::new();
    let mut sample_count_total = 0;
    let mut histogram_count_total = 0;

    for series in timeseries {
        // Exemplars are intentionally ignored for now.
        let sample_count = series.samples.len();
        let histogram_count = series.histograms.len();
        if sample_count == 0 && histogram_count == 0 {
            continue;
        }

        let (prom_ctx, table_name, tags) = resolve_series_labels(&symbols, &series)?;
        if histogram_count > 0 {
            ensure_no_internal_histogram_labels(&tags)?;
        }
        let metric_key = (prom_ctx.clone(), table_name.clone());
        if sample_count > 0 {
            ensure!(
                !histogram_metrics.contains(&metric_key),
                error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "remote write v2 metric `{table_name}` contains both samples and native histograms"
                    ),
                }
            );
            sample_metrics.insert(metric_key.clone());
        }
        if histogram_count > 0 {
            ensure!(
                !sample_metrics.contains(&metric_key),
                error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "remote write v2 metric `{table_name}` contains both samples and native histograms"
                    ),
                }
            );
            histogram_metrics.insert(metric_key);
        }

        if sample_count > 0 && histogram_count == 0 {
            // Fast path for regular sample-only series. Move the resolved labels into
            // the sample writer instead of cloning them for a histogram path we won't use.
            let table_data = get_or_create_table_data(
                &mut sample_tables,
                prom_ctx,
                table_name,
                tags.len() + 2,
                sample_count,
            );

            write_samples(table_data, series.samples, tags)?;
            sample_count_total += sample_count as u64;
            // The owned labels were moved above, so skip the mixed-series path below.
            continue;
        }

        if histogram_count > 0 {
            let table_data = get_or_create_table_data(
                &mut histogram_tables,
                prom_ctx,
                table_name,
                tags.len() + 2,
                histogram_count,
            );

            let mut histograms = series.histograms;
            let Some(last_histogram) = histograms.pop() else {
                continue;
            };
            for histogram in &histograms {
                write_native_histogram(table_data, histogram, tags.iter().cloned())?;
            }
            write_native_histogram(table_data, &last_histogram, tags.into_iter())?;
            histogram_count_total += histogram_count as u64;
        }
    }

    Ok(RemoteWriteV2WriteRequests {
        samples: into_context_req(sample_tables),
        histograms: into_context_req(histogram_tables),
        sample_count: sample_count_total,
        histogram_count: histogram_count_total,
    })
}

fn get_or_create_table_data(
    tables: &mut HashMap<PromCtx, HashMap<String, TableData>>,
    prom_ctx: PromCtx,
    table_name: String,
    column_count: usize,
    row_count: usize,
) -> &mut TableData {
    match tables.entry(prom_ctx).or_default().entry(table_name) {
        Entry::Occupied(entry) => {
            let table_data = entry.into_mut();
            table_data.reserve_rows(row_count);
            table_data
        }
        Entry::Vacant(entry) => entry.insert(TableData::new(column_count, row_count)),
    }
}

fn write_samples(
    table_data: &mut TableData,
    mut samples: Vec<Sample>,
    tags: PromTags,
) -> Result<()> {
    let Some(last_sample) = samples.pop() else {
        return Ok(());
    };

    for sample in &samples {
        write_sample(table_data, sample, tags.iter().cloned())?;
    }

    write_sample(table_data, &last_sample, tags.into_iter())
}

fn write_sample(
    table_data: &mut TableData,
    sample: &Sample,
    tags: impl Iterator<Item = (String, String)>,
) -> Result<()> {
    let mut row = table_data.alloc_one_row();
    row_writer::write_ts_to_millis(
        table_data,
        greptime_timestamp(),
        Some(sample.timestamp),
        Precision::Millisecond,
        &mut row,
    )?;
    row_writer::write_f64(table_data, greptime_value(), sample.value, &mut row)?;
    row_writer::write_tags(table_data, tags, &mut row)?;
    table_data.add_row(row);

    Ok(())
}

fn write_native_histogram(
    table_data: &mut TableData,
    histogram: &Histogram,
    tags: impl Iterator<Item = (String, String)>,
) -> Result<()> {
    // Persist both int and float families into the logical table schema. Only one
    // family is populated per row; the other is written as NULL so PromQL can
    // infer the original histogram flavor without a separate type column.
    let mut row = table_data.alloc_one_row();
    row_writer::write_ts_to_millis(
        table_data,
        greptime_timestamp(),
        Some(histogram.timestamp),
        Precision::Millisecond,
        &mut row,
    )?;

    write_native_histogram_value(table_data, histogram, &mut row)?;

    row_writer::write_tags(table_data, tags, &mut row)?;
    table_data.add_row(row);

    Ok(())
}

fn write_native_histogram_value(
    table_data: &mut TableData,
    histogram: &Histogram,
    row: &mut Vec<Value>,
) -> Result<()> {
    let column_schema = native_histogram_column_schema();
    let value = native_histogram_struct_value(histogram)?;

    row_writer::write_by_schema(
        table_data,
        std::iter::once((column_schema, Some(value))),
        row,
    )
}

fn native_histogram_column_schema() -> ColumnSchema {
    let (datatype, datatype_extension) = native_histogram_column_type().into_parts();

    ColumnSchema {
        column_name: NATIVE_HISTOGRAM_FIELD.to_string(),
        datatype: datatype as i32,
        semantic_type: SemanticType::Field as i32,
        datatype_extension,
        options: None,
    }
}

fn native_histogram_column_type() -> ColumnDataTypeWrapper {
    ColumnDataTypeWrapper::struct_datatype(vec![
        (
            SCHEMA_FIELD.to_string(),
            ColumnDataTypeWrapper::int32_datatype(),
        ),
        (
            ZERO_THRESHOLD_FIELD.to_string(),
            ColumnDataTypeWrapper::float64_datatype(),
        ),
        (
            SUM_FIELD.to_string(),
            ColumnDataTypeWrapper::float64_datatype(),
        ),
        (
            RESET_HINT_FIELD.to_string(),
            ColumnDataTypeWrapper::int32_datatype(),
        ),
        (
            START_TIMESTAMP_FIELD.to_string(),
            ColumnDataTypeWrapper::timestamp_millisecond_datatype(),
        ),
        (
            CUSTOM_VALUES_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::float64_datatype()),
        ),
        (
            POSITIVE_SPAN_OFFSETS_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::int32_datatype()),
        ),
        (
            POSITIVE_SPAN_LENGTHS_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::uint32_datatype()),
        ),
        (
            NEGATIVE_SPAN_OFFSETS_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::int32_datatype()),
        ),
        (
            NEGATIVE_SPAN_LENGTHS_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::uint32_datatype()),
        ),
        (
            COUNT_U64_FIELD.to_string(),
            ColumnDataTypeWrapper::uint64_datatype(),
        ),
        (
            ZERO_COUNT_U64_FIELD.to_string(),
            ColumnDataTypeWrapper::uint64_datatype(),
        ),
        (
            POSITIVE_BUCKETS_I64_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::int64_datatype()),
        ),
        (
            NEGATIVE_BUCKETS_I64_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::int64_datatype()),
        ),
        (
            COUNT_F64_FIELD.to_string(),
            ColumnDataTypeWrapper::float64_datatype(),
        ),
        (
            ZERO_COUNT_F64_FIELD.to_string(),
            ColumnDataTypeWrapper::float64_datatype(),
        ),
        (
            POSITIVE_BUCKETS_F64_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::float64_datatype()),
        ),
        (
            NEGATIVE_BUCKETS_F64_FIELD.to_string(),
            ColumnDataTypeWrapper::list_datatype(ColumnDataTypeWrapper::float64_datatype()),
        ),
    ])
}

fn native_histogram_struct_value(histogram: &Histogram) -> Result<ValueData> {
    let mut items = Vec::with_capacity(NATIVE_HISTOGRAM_FIELD_NAMES.len());
    items.extend([
        pb_value(ValueData::I32Value(histogram.schema)),
        pb_value(ValueData::F64Value(histogram.zero_threshold)),
        pb_value(ValueData::F64Value(histogram.sum)),
        pb_value(ValueData::I32Value(histogram.reset_hint)),
        optional_pb_value((histogram.start_timestamp != 0).then_some(
            ValueData::TimestampMillisecondValue(histogram.start_timestamp),
        )),
        f64_list_value(histogram.custom_values.iter().copied()),
        i32_list_value(histogram.positive_spans.iter().map(|span| span.offset)),
        u32_list_value(histogram.positive_spans.iter().map(|span| span.length)),
        i32_list_value(histogram.negative_spans.iter().map(|span| span.offset)),
        u32_list_value(histogram.negative_spans.iter().map(|span| span.length)),
    ]);

    let int_counts = match (&histogram.count, &histogram.zero_count) {
        (Some(Count::CountInt(count)), Some(ZeroCount::ZeroCountInt(zero_count))) => {
            (*count, *zero_count)
        }
        (Some(Count::CountInt(count)), _) => (*count, 0),
        (_, Some(ZeroCount::ZeroCountInt(zero_count))) => (0, *zero_count),
        _ => (0, 0),
    };
    let float_counts = match (&histogram.count, &histogram.zero_count) {
        (Some(Count::CountFloat(count)), Some(ZeroCount::ZeroCountFloat(zero_count))) => {
            Some((*count, *zero_count))
        }
        (Some(Count::CountFloat(count)), _) => Some((*count, 0.0)),
        _ => None,
    };

    if let Some(counts) = float_counts {
        items.extend([
            null_pb_value(),
            null_pb_value(),
            i64_list_value(std::iter::empty()),
            i64_list_value(std::iter::empty()),
            pb_value(ValueData::F64Value(counts.0)),
            pb_value(ValueData::F64Value(counts.1)),
            f64_list_value(histogram.positive_counts.iter().copied()),
            f64_list_value(histogram.negative_counts.iter().copied()),
        ]);
    } else {
        let positive_buckets = bucket_counts_from_deltas(&histogram.positive_deltas)?;
        let negative_buckets = bucket_counts_from_deltas(&histogram.negative_deltas)?;
        items.extend([
            pb_value(ValueData::U64Value(int_counts.0)),
            pb_value(ValueData::U64Value(int_counts.1)),
            i64_list_value(positive_buckets.iter().copied()),
            i64_list_value(negative_buckets.iter().copied()),
            null_pb_value(),
            null_pb_value(),
            f64_list_value(std::iter::empty()),
            f64_list_value(std::iter::empty()),
        ]);
    }

    Ok(ValueData::StructValue(api::v1::StructValue { items }))
}

fn pb_value(value_data: ValueData) -> Value {
    optional_pb_value(Some(value_data))
}

fn null_pb_value() -> Value {
    optional_pb_value(None)
}

fn optional_pb_value(value_data: Option<ValueData>) -> Value {
    Value { value_data }
}

fn list_value(values: impl IntoIterator<Item = ValueData>) -> Value {
    pb_value(ValueData::ListValue(ListValue {
        items: values.into_iter().map(pb_value).collect(),
    }))
}

fn i32_list_value(values: impl IntoIterator<Item = i32>) -> Value {
    list_value(values.into_iter().map(ValueData::I32Value))
}

fn u32_list_value(values: impl IntoIterator<Item = u32>) -> Value {
    list_value(values.into_iter().map(ValueData::U32Value))
}

fn i64_list_value(values: impl IntoIterator<Item = i64>) -> Value {
    list_value(values.into_iter().map(ValueData::I64Value))
}

fn f64_list_value(values: impl IntoIterator<Item = f64>) -> Value {
    list_value(values.into_iter().map(ValueData::F64Value))
}

fn bucket_counts_from_deltas(deltas: &[i64]) -> Result<Vec<i64>> {
    let mut count = 0_i64;
    let mut buckets = Vec::with_capacity(deltas.len());

    for delta in deltas {
        count = count
            .checked_add(*delta)
            .context(error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 native histogram bucket count overflows i64".to_string(),
            })?;
        ensure!(
            count >= 0,
            error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 native histogram bucket count is negative".to_string(),
            }
        );
        buckets.push(count);
    }

    Ok(buckets)
}

fn ensure_no_internal_histogram_labels(tags: &PromTags) -> Result<()> {
    // The histogram field column is generated from the protobuf payload.
    for (name, _) in tags {
        ensure!(
            name != NATIVE_HISTOGRAM_FIELD,
            error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "remote write v2 label `{name}` conflicts with an internal native histogram label"
                ),
            }
        );
    }

    Ok(())
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
    let mut label_names = HashSet::with_capacity(series.labels_refs.len() / 2);

    for pair in series.labels_refs.chunks_exact(2) {
        let name = symbol_ref(symbols, pair[0], "label name")?;
        let value = symbol_ref(symbols, pair[1], "label value")?;
        validate_label(name, value)?;
        ensure!(
            label_names.insert(name),
            error::InvalidPromRemoteRequestSnafu {
                msg: format!("remote write v2 label name `{name}` is repeated"),
            }
        );

        if name == METRIC_NAME_LABEL {
            table_name = Some(value.to_string());
            continue;
        }
        if apply_remote_write_special_label(name, value, &mut prom_ctx) {
            continue;
        }

        tags.push((name.to_string(), value.to_string()));
    }

    let table_name = table_name.context(error::InvalidPromRemoteRequestSnafu {
        msg: "missing '__name__' label in time-series".to_string(),
    })?;

    Ok((prom_ctx, table_name, tags))
}

fn validate_label(name: &str, value: &str) -> Result<()> {
    ensure!(
        !name.is_empty(),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 label names must not be empty".to_string(),
        }
    );
    ensure!(
        !value.is_empty(),
        error::InvalidPromRemoteRequestSnafu {
            msg: format!("remote write v2 label `{name}` value must not be empty"),
        }
    );

    Ok(())
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
    use api::v1::RowInsertRequest;
    use bytes::Bytes;

    use crate::error::Result;

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

    pub fn decode_request(is_zstd: bool, body: Bytes) -> Result<Request> {
        super::decode_remote_write_v2_request(is_zstd, body)
    }

    pub fn write_requests(
        request: Request,
    ) -> Result<(Vec<RowInsertRequest>, Vec<RowInsertRequest>, u64, u64)> {
        let requests = super::into_write_requests(request)?;
        Ok((
            requests.samples.all_req().collect(),
            requests.histograms.all_req().collect(),
            requests.sample_count,
            requests.histogram_count,
        ))
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
        let ctx_req = into_write_requests(test_util::request_with_labels_and_samples(
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
        ))
        .unwrap();

        assert_eq!(ctx_req.sample_count, 2);
        assert_eq!(ctx_req.histogram_count, 0);
        assert_eq!(ctx_req.histograms.all_req().count(), 0);
        let mut inserts = ctx_req.samples.all_req().collect::<Vec<_>>();
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
    fn test_into_context_req_accepts_utf8_label_names() {
        let ctx_req = into_write_requests(test_util::request_with_labels_and_samples(
            vec![
                (METRIC_NAME_LABEL, "http_requests_total"),
                ("service.name", "api"),
                ("区域", "华东"),
            ],
            vec![Sample {
                value: 42.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        ))
        .unwrap();

        let mut inserts = ctx_req.samples.all_req().collect::<Vec<_>>();
        assert_eq!(inserts.len(), 1);
        let rows = inserts.pop().unwrap().rows.unwrap();
        assert_eq!(
            rows.schema
                .iter()
                .map(|col| col.column_name.as_str())
                .collect::<Vec<_>>(),
            vec![
                greptime_timestamp(),
                greptime_value(),
                "service.name",
                "区域"
            ]
        );
        assert_eq!(
            rows.rows[0].values[2].value_data,
            Some(ValueData::StringValue("api".to_string()))
        );
        assert_eq!(
            rows.rows[0].values[3].value_data,
            Some(ValueData::StringValue("华东".to_string()))
        );
    }

    #[test]
    fn test_into_context_req_special_labels() {
        let ctx_req = into_write_requests(test_util::request_with_labels_and_samples(
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
        ))
        .unwrap();

        let mut iter = ctx_req
            .samples
            .as_req_iter(Arc::new(QueryContext::with("greptime", "public")));
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
    fn test_into_context_req_rejects_invalid_requests() {
        let mut cases = Vec::new();

        cases.push((
            "missing metric name",
            request_with_sample(vec![("job", "api")]),
            "missing '__name__'",
        ));

        let mut request = request_with_sample(vec![(METRIC_NAME_LABEL, "metric")]);
        request.timeseries[0].labels_refs.push(1);
        cases.push((
            "odd label refs",
            request,
            "labels_refs must contain name/value pairs",
        ));

        let mut request = request_with_sample(vec![(METRIC_NAME_LABEL, "metric")]);
        request.timeseries[0].labels_refs[1] = 99;
        cases.push((
            "out of range symbol ref",
            request,
            "symbol reference 99 is out of range",
        ));

        let mut request = request_with_sample(vec![(METRIC_NAME_LABEL, "metric")]);
        request.symbols[0] = "not-empty".to_string();
        cases.push((
            "non-empty first symbol",
            request,
            "symbols must start with an empty string",
        ));

        cases.push((
            "repeated label name",
            request_with_sample(vec![
                (METRIC_NAME_LABEL, "metric"),
                ("job", "api"),
                ("job", "worker"),
            ]),
            "label name `job` is repeated",
        ));

        cases.push((
            "empty label name",
            request_with_sample(vec![(METRIC_NAME_LABEL, "metric"), ("", "api")]),
            "label names must not be empty",
        ));

        cases.push((
            "empty label value",
            request_with_sample(vec![(METRIC_NAME_LABEL, "metric"), ("job", "")]),
            "label `job` value must not be empty",
        ));

        for (name, request, expected) in cases {
            assert_invalid(name, request, expected);
        }
    }

    #[test]
    fn test_into_context_req_rejects_same_metric_samples_and_histograms() {
        let mut request = test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        );
        request.timeseries[0].histograms.push(Histogram::default());

        assert_invalid(
            "same metric samples and histograms",
            request,
            "contains both samples and native histograms",
        );
    }

    #[test]
    fn test_into_context_req_converts_histograms_and_ignores_exemplars() {
        let request = Request {
            symbols: vec![
                "".to_string(),
                METRIC_NAME_LABEL.to_string(),
                "sample_metric".to_string(),
                "histogram_metric".to_string(),
            ],
            timeseries: vec![
                TimeSeries {
                    labels_refs: vec![1, 2],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 1000,
                        start_timestamp: 0,
                    }],
                    ..Default::default()
                },
                TimeSeries {
                    labels_refs: vec![1, 3],
                    histograms: vec![Histogram::default()],
                    exemplars: vec![Exemplar::default()],
                    ..Default::default()
                },
            ],
        };

        let ctx_req = into_write_requests(request).unwrap();

        assert_eq!(ctx_req.sample_count, 1);
        assert_eq!(ctx_req.histogram_count, 1);
        assert_eq!(ctx_req.samples.all_req().count(), 1);
        assert_eq!(ctx_req.histograms.all_req().count(), 1);
    }

    #[test]
    fn test_into_context_req_converts_histogram_only_series() {
        let mut request =
            test_util::request_with_labels_and_samples(vec![(METRIC_NAME_LABEL, "metric")], vec![]);
        request.timeseries[0].histograms.push(Histogram::default());

        let ctx_req = into_write_requests(request).unwrap();

        assert_eq!(ctx_req.sample_count, 0);
        assert_eq!(ctx_req.histogram_count, 1);
        assert_eq!(ctx_req.samples.all_req().count(), 0);
        let mut inserts = ctx_req.histograms.all_req().collect::<Vec<_>>();
        assert_eq!(inserts.len(), 1);

        let request = inserts.pop().unwrap();
        assert_eq!(request.table_name, "metric");
        let rows = request.rows.unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(
            rows.schema
                .iter()
                .map(|col| col.column_name.as_str())
                .collect::<Vec<_>>(),
            vec![greptime_timestamp(), NATIVE_HISTOGRAM_FIELD]
        );
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMillisecondValue(0))
        );
        assert_eq!(
            histogram_field_value(&rows, 0, SCHEMA_FIELD),
            Some(ValueData::I32Value(0))
        );
        assert_eq!(
            histogram_field_value(&rows, 0, COUNT_U64_FIELD),
            Some(ValueData::U64Value(0))
        );
        assert_eq!(histogram_field_value(&rows, 0, COUNT_F64_FIELD), None);
    }

    #[test]
    fn test_into_context_req_preserves_histogram_start_timestamp() {
        let ctx_req = into_write_requests(test_util::request_with_labels_and_histograms(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![Histogram {
                timestamp: 2000,
                start_timestamp: 1000,
                ..Default::default()
            }],
        ))
        .unwrap();

        let mut inserts = ctx_req.histograms.all_req().collect::<Vec<_>>();
        let rows = inserts.pop().unwrap().rows.unwrap();

        assert_eq!(
            histogram_field_value(&rows, 0, START_TIMESTAMP_FIELD),
            Some(ValueData::TimestampMillisecondValue(1000))
        );
    }

    #[test]
    fn test_into_context_req_rejects_internal_histogram_labels() {
        let mut request = test_util::request_with_labels_and_samples(
            vec![
                (METRIC_NAME_LABEL, "metric"),
                (NATIVE_HISTOGRAM_FIELD, "user_value"),
            ],
            vec![],
        );
        request.timeseries[0].histograms.push(Histogram::default());

        let err = match into_write_requests(request) {
            Ok(_) => panic!("expected invalid request error"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "Invalid prometheus remote request, msg: remote write v2 label `greptime_native_histogram` conflicts with an internal native histogram label"
        );
    }

    #[test]
    fn test_into_context_req_converts_int_and_float_histograms_to_one_schema() {
        let float_histogram = Histogram {
            count: Some(api::greptime_proto::io::prometheus::write::v2::histogram::Count::CountFloat(3.5)),
            zero_count: Some(
                api::greptime_proto::io::prometheus::write::v2::histogram::ZeroCount::ZeroCountFloat(
                    0.5,
                ),
            ),
            positive_counts: vec![2.0, 3.5],
            positive_spans: vec![api::greptime_proto::io::prometheus::write::v2::BucketSpan {
                offset: 3,
                length: 2,
            }],
            timestamp: 2000,
            ..Default::default()
        };
        let request = Request {
            symbols: vec![
                "".to_string(),
                METRIC_NAME_LABEL.to_string(),
                "metric".to_string(),
            ],
            timeseries: vec![
                TimeSeries {
                    labels_refs: vec![1, 2],
                    histograms: vec![test_util::histogram(1000)],
                    ..Default::default()
                },
                TimeSeries {
                    labels_refs: vec![1, 2],
                    histograms: vec![float_histogram],
                    ..Default::default()
                },
            ],
        };

        let ctx_req = into_write_requests(request).unwrap();

        assert_eq!(ctx_req.histogram_count, 2);
        let mut inserts = ctx_req.histograms.all_req().collect::<Vec<_>>();
        assert_eq!(inserts.len(), 1);
        let rows = inserts.pop().unwrap().rows.unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.schema
                .iter()
                .map(|col| col.column_name.as_str())
                .collect::<Vec<_>>(),
            vec![greptime_timestamp(), NATIVE_HISTOGRAM_FIELD]
        );

        assert_eq!(
            histogram_field_value(&rows, 0, COUNT_U64_FIELD),
            Some(ValueData::U64Value(0))
        );
        assert_eq!(histogram_field_value(&rows, 0, COUNT_F64_FIELD), None);
        assert!(matches!(
            histogram_field_value(&rows, 0, POSITIVE_BUCKETS_I64_FIELD),
            Some(ValueData::ListValue(_))
        ));
        assert!(is_empty_list(histogram_field_value(
            &rows,
            0,
            POSITIVE_BUCKETS_F64_FIELD
        )));

        assert_eq!(histogram_field_value(&rows, 1, COUNT_U64_FIELD), None);
        assert_eq!(
            histogram_field_value(&rows, 1, COUNT_F64_FIELD),
            Some(ValueData::F64Value(3.5))
        );
        assert!(is_empty_list(histogram_field_value(
            &rows,
            1,
            POSITIVE_BUCKETS_I64_FIELD
        )));
        assert!(matches!(
            histogram_field_value(&rows, 1, POSITIVE_BUCKETS_F64_FIELD),
            Some(ValueData::ListValue(_))
        ));
    }

    fn request_with_sample(labels: Vec<(&str, &str)>) -> Request {
        test_util::request_with_labels_and_samples(
            labels,
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        )
    }

    fn assert_invalid(name: &str, request: Request, expected: &str) {
        let err = match into_write_requests(request) {
            Ok(_) => panic!("{name}: expected invalid request error"),
            Err(err) => err,
        };
        assert!(
            matches!(err, error::Error::InvalidPromRemoteRequest { .. }),
            "{name}: expected invalid request error, got {err}"
        );
        assert!(
            err.to_string().contains(expected),
            "{name}: expected error containing {expected:?}, got {err}"
        );
    }

    fn column_index(schema: &[ColumnSchema], column_name: &str) -> usize {
        schema
            .iter()
            .position(|column| column.column_name == column_name)
            .unwrap()
    }

    fn histogram_field_value(rows: &Rows, row_idx: usize, field_name: &str) -> Option<ValueData> {
        let histogram_idx = column_index(&rows.schema, NATIVE_HISTOGRAM_FIELD);
        let Some(ValueData::StructValue(histogram)) =
            &rows.rows[row_idx].values[histogram_idx].value_data
        else {
            panic!("expected native histogram struct value");
        };
        let field_idx = NATIVE_HISTOGRAM_FIELD_NAMES
            .iter()
            .position(|name| *name == field_name)
            .unwrap();
        histogram.items[field_idx].value_data.clone()
    }

    fn is_empty_list(value: Option<ValueData>) -> bool {
        matches!(value, Some(ValueData::ListValue(list)) if list.items.is_empty())
    }
}
