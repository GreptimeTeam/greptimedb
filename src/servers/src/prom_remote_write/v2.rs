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
use api::greptime_proto::io::prometheus::write::v2::{
    BucketSpan, Exemplar, Histogram, Metadata, Sample, metadata,
};
#[cfg(test)]
use api::greptime_proto::io::prometheus::write::v2::{Request, TimeSeries};
use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnSchema, ListValue, RowInsertRequest, Rows, SemanticType, Value,
};
use bytes::{Buf, Bytes};
use common_grpc::precision::Precision;
use common_query::native_histogram::*;
use common_query::prelude::{greptime_native_histogram, greptime_timestamp, greptime_value};
use pipeline::{ContextOpt, ContextReq};
use prost::encoding::{
    DecodeContext, WireType, decode_key, decode_varint, message, skip_field, uint32,
};
use prost::{DecodeError, Message};
use snafu::{OptionExt, ResultExt, ensure};
use table::requests::{
    METADATA_QUALITY_DECLARED, SEMANTIC_METRIC_METADATA_QUALITY, SEMANTIC_METRIC_TYPE,
    SEMANTIC_METRIC_UNIT,
};

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
use crate::semantic::{
    METRIC_TYPE_COUNTER, METRIC_TYPE_GAUGE, METRIC_TYPE_GAUGE_HISTOGRAM, METRIC_TYPE_HISTOGRAM,
    METRIC_TYPE_INFO, METRIC_TYPE_STATESET, METRIC_TYPE_SUMMARY, SemanticIndexes,
    openmetrics_unit_to_ucum,
};

type PromTags<'a> = Vec<(&'a str, String)>;
type ResolvedSeriesLabels<'a> = (PromCtx, String, PromTags<'a>);
const MAX_REMOTE_WRITE_V2_SCHEMA: i32 = 8;
const MAX_REDUCIBLE_REMOTE_WRITE_V2_SCHEMA: i32 = 52;

struct BorrowedRequest<'a> {
    symbols: Vec<&'a str>,
    timeseries: Vec<&'a [u8]>,
}

impl<'a> BorrowedRequest<'a> {
    fn decode(mut buf: &'a [u8]) -> std::result::Result<Self, DecodeError> {
        let mut symbols = Vec::new();
        let mut timeseries = Vec::new();

        while buf.has_remaining() {
            let (tag, wire_type) = decode_key(&mut buf)?;
            match tag {
                4 => {
                    let value =
                        take_length_delimited(wire_type, &mut buf).map_err(|mut error| {
                            error.push("Request", "symbols");
                            error
                        })?;
                    let symbol = std::str::from_utf8(value).map_err(|_| {
                        let mut error =
                            DecodeError::new("invalid string value: data is not UTF-8 encoded");
                        error.push("Request", "symbols");
                        error
                    })?;
                    symbols.push(symbol);
                }
                5 => {
                    let series =
                        take_length_delimited(wire_type, &mut buf).map_err(|mut error| {
                            error.push("Request", "timeseries");
                            error
                        })?;
                    timeseries.push(series);
                }
                _ => skip_field(wire_type, tag, &mut buf, DecodeContext::default())?,
            }
        }

        Ok(Self {
            symbols,
            timeseries,
        })
    }
}

pub(crate) struct RemoteWriteV2WriteRequests {
    pub samples: ContextReq,
    pub histograms: ContextReq,
    pub sample_count: u64,
    pub histogram_count: u64,
    /// Per-table semantic metadata from the series' inline `Metadata`, folded
    /// into table options at auto-create time.
    pub semantic_index: SemanticIndexes,
}

pub(crate) fn decode_remote_write_v2(
    is_zstd: bool,
    body: Bytes,
    native_histograms_enabled: bool,
) -> Result<RemoteWriteV2WriteRequests> {
    let decode_timer = crate::metrics::METRIC_HTTP_PROM_STORE_DECODE_ELAPSED.start_timer();

    // Match the v1 decoder's VictoriaMetrics fallback: some clients may send a
    // mismatched content-encoding header, so try the other compression on failure.
    let buf = if let Ok(buf) = try_decompress(is_zstd, &body[..]) {
        buf
    } else {
        try_decompress(!is_zstd, &body[..])?
    };
    let request = BorrowedRequest::decode(&buf).context(error::DecodePromRemoteRequestSnafu)?;
    drop(decode_timer);

    let _convert_timer = crate::metrics::METRIC_HTTP_PROM_STORE_CONVERT_ELAPSED.start_timer();
    convert_remote_write_v2(request, native_histograms_enabled)
}

fn convert_remote_write_v2(
    request: BorrowedRequest<'_>,
    native_histograms_enabled: bool,
) -> Result<RemoteWriteV2WriteRequests> {
    ensure!(
        request.symbols.first().copied() == Some(""),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 symbols must start with an empty string".to_string(),
        }
    );

    let mut sample_tables = HashMap::<PromCtx, HashMap<String, TableData>>::new();
    let mut histogram_tables = HashMap::<PromCtx, HashMap<String, TableData>>::new();
    let mut label_names = HashSet::new();
    let mut sample_count_total = 0;
    let mut histogram_count_total = 0;
    let mut labels_refs = Vec::new();
    let mut metadata = Metadata::default();
    let mut scratch = LeafScratch::default();
    let mut semantic_index = SemanticIndexes::default();

    for series in request.timeseries {
        let counts = scan_series(series, &mut labels_refs, &mut metadata)
            .context(error::DecodePromRemoteRequestSnafu)?;

        ensure!(
            native_histograms_enabled || counts.histograms == 0,
            error::InvalidPromRemoteRequestSnafu {
                msg: "prometheus remote write v2 native histogram ingestion is experimental; set prom_store.experimental_enable_prometheus_native_histogram = true to enable it"
                    .to_string(),
            }
        );

        if counts.samples == 0 && counts.histograms == 0 {
            decode_series_leaves(series, None, Vec::new(), 0, &mut scratch)?;
            continue;
        }

        let (prom_ctx, table_name, tags) =
            resolve_series_labels(&request.symbols, &labels_refs, &mut label_names)?;
        ensure_no_internal_histogram_labels(&tags)?;
        record_series_metadata(
            &mut semantic_index,
            &request.symbols,
            &metadata,
            &prom_ctx,
            &table_name,
        )?;
        let has_other_value_type = if counts.samples > 0 {
            counts.histograms > 0
                || histogram_tables
                    .get(&prom_ctx)
                    .is_some_and(|tables| tables.contains_key(&table_name))
        } else {
            sample_tables
                .get(&prom_ctx)
                .is_some_and(|tables| tables.contains_key(&table_name))
        };
        ensure!(
            !has_other_value_type,
            error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "remote write v2 metric `{table_name}` contains both samples and native histograms"
                ),
            }
        );

        let column_count =
            tags.len()
                .checked_add(2)
                .with_context(|| error::InvalidPromRemoteRequestSnafu {
                    msg: "remote write v2 series has too many labels".to_string(),
                })?;
        let (writer, row_count) = if counts.samples > 0 {
            (
                SeriesWriter::Samples(get_or_create_table_data(
                    &mut sample_tables,
                    prom_ctx,
                    table_name,
                    column_count,
                    counts.samples,
                )),
                counts.samples,
            )
        } else {
            (
                SeriesWriter::Histograms(get_or_create_table_data(
                    &mut histogram_tables,
                    prom_ctx,
                    table_name,
                    column_count,
                    counts.histograms,
                )),
                counts.histograms,
            )
        };

        decode_series_leaves(series, Some(writer), tags, row_count, &mut scratch)?;
        sample_count_total = checked_total(sample_count_total, counts.samples, "sample")?;
        histogram_count_total =
            checked_total(histogram_count_total, counts.histograms, "histogram")?;
    }

    Ok(RemoteWriteV2WriteRequests {
        samples: into_context_req(sample_tables),
        histograms: into_context_req(histogram_tables),
        sample_count: sample_count_total,
        histogram_count: histogram_count_total,
        semantic_index,
    })
}

/// Stamps the series' inline metadata for the written table's auto-create: an
/// explicit metric type upgrades the table's metadata quality to `declared`,
/// `UNSPECIFIED` series keep the request-level `inferred` stamp, and units are
/// canonicalised from OpenMetrics words to UCUM. Type and unit stamp
/// independently, as OpenMetrics defines them. Help text is not persisted.
///
/// Every non-zero symbol reference is validated up front, independent of what
/// ends up persisted: the spec requires all references to point into the
/// symbol table.
fn record_series_metadata(
    index: &mut SemanticIndexes,
    symbols: &[&str],
    series_metadata: &Metadata,
    prom_ctx: &PromCtx,
    table_name: &str,
) -> Result<()> {
    // Symbol 0 is the mandatory empty string: no help / no unit.
    if series_metadata.help_ref != 0 {
        symbol_ref(symbols, series_metadata.help_ref, "metadata help")?;
    }
    let unit = if series_metadata.unit_ref != 0 {
        Some(symbol_ref(
            symbols,
            series_metadata.unit_ref,
            "metadata unit",
        )?)
    } else {
        None
    };
    let metric_type = metric_type_value(series_metadata.r#type);
    let ucum = unit.and_then(|unit| openmetrics_unit_to_ucum(unit.trim()));
    if metric_type.is_none() && ucum.is_none() {
        return Ok(());
    }

    let index = index.index_for(prom_ctx.schema.as_deref());
    if let Some(metric_type) = metric_type {
        index.record_scalar(table_name, SEMANTIC_METRIC_TYPE, metric_type);
        index.record_scalar(
            table_name,
            SEMANTIC_METRIC_METADATA_QUALITY,
            METADATA_QUALITY_DECLARED,
        );
    }
    if let Some(ucum) = ucum {
        index.record_scalar(table_name, SEMANTIC_METRIC_UNIT, ucum);
    }
    Ok(())
}

/// The `greptime.semantic.metric.type` value for a wire metric type; `None`
/// for `UNSPECIFIED` (nothing was declared) and out-of-range values.
fn metric_type_value(wire_type: i32) -> Option<&'static str> {
    match metadata::MetricType::try_from(wire_type).ok()? {
        metadata::MetricType::Unspecified => None,
        metadata::MetricType::Counter => Some(METRIC_TYPE_COUNTER),
        metadata::MetricType::Gauge => Some(METRIC_TYPE_GAUGE),
        metadata::MetricType::Histogram => Some(METRIC_TYPE_HISTOGRAM),
        metadata::MetricType::Gaugehistogram => Some(METRIC_TYPE_GAUGE_HISTOGRAM),
        metadata::MetricType::Summary => Some(METRIC_TYPE_SUMMARY),
        metadata::MetricType::Info => Some(METRIC_TYPE_INFO),
        metadata::MetricType::Stateset => Some(METRIC_TYPE_STATESET),
    }
}

#[derive(Default)]
struct SeriesCounts {
    samples: usize,
    histograms: usize,
}

fn scan_series(
    mut buf: &[u8],
    labels_refs: &mut Vec<u32>,
    metadata: &mut Metadata,
) -> std::result::Result<SeriesCounts, DecodeError> {
    labels_refs.clear();
    metadata.clear();
    let mut counts = SeriesCounts::default();

    while buf.has_remaining() {
        let (tag, wire_type) = decode_key(&mut buf)?;
        match tag {
            1 => uint32::merge_repeated(wire_type, labels_refs, &mut buf, DecodeContext::default())
                .map_err(|mut error| {
                    error.push("TimeSeries", "labels_refs");
                    error
                })?,
            2 => {
                take_length_delimited(wire_type, &mut buf).map_err(|mut error| {
                    error.push("TimeSeries", "samples");
                    error
                })?;
                counts.samples = counts.samples.checked_add(1).ok_or_else(|| {
                    DecodeError::new("remote write v2 sample count overflows usize")
                })?;
            }
            3 => {
                take_length_delimited(wire_type, &mut buf).map_err(|mut error| {
                    error.push("TimeSeries", "histograms");
                    error
                })?;
                counts.histograms = counts.histograms.checked_add(1).ok_or_else(|| {
                    DecodeError::new("remote write v2 histogram count overflows usize")
                })?;
            }
            4 => {
                take_length_delimited(wire_type, &mut buf)?;
            }
            5 => message::merge(wire_type, metadata, &mut buf, DecodeContext::default()).map_err(
                |mut error| {
                    error.push("TimeSeries", "metadata");
                    error
                },
            )?,
            _ => skip_field(wire_type, tag, &mut buf, DecodeContext::default())?,
        }
    }

    Ok(counts)
}

#[derive(Default)]
struct LeafScratch {
    sample: Sample,
    histogram: Histogram,
    exemplar: Exemplar,
}

enum SeriesWriter<'a> {
    Samples(&'a mut TableData),
    Histograms(&'a mut TableData),
}

fn decode_series_leaves(
    mut buf: &[u8],
    mut writer: Option<SeriesWriter<'_>>,
    mut tags: PromTags<'_>,
    mut rows_remaining: usize,
    scratch: &mut LeafScratch,
) -> Result<()> {
    let mut sample_row_template = None;

    while buf.has_remaining() {
        let (tag, wire_type) = decode_key(&mut buf).context(error::DecodePromRemoteRequestSnafu)?;
        match tag {
            1 => skip_field(wire_type, tag, &mut buf, DecodeContext::default())
                .context(error::DecodePromRemoteRequestSnafu)?,
            2 => {
                scratch.sample.clear();
                message::merge(
                    wire_type,
                    &mut scratch.sample,
                    &mut buf,
                    DecodeContext::default(),
                )
                .map_err(|mut error| {
                    error.push("TimeSeries", "samples");
                    error
                })
                .context(error::DecodePromRemoteRequestSnafu)?;
                rows_remaining = rows_remaining.checked_sub(1).with_context(|| {
                    error::InvalidPromRemoteRequestSnafu {
                        msg: "remote write v2 sample count changed between scans".to_string(),
                    }
                })?;
                if let Some(SeriesWriter::Samples(table_data)) = &mut writer {
                    if sample_row_template.is_none() {
                        let timestamp_index = table_data.ensure_column_by_name(
                            greptime_timestamp(),
                            ColumnDataType::TimestampMillisecond,
                            SemanticType::Timestamp,
                        )?;
                        let value_index = table_data.ensure_column_by_name(
                            greptime_value(),
                            ColumnDataType::Float64,
                            SemanticType::Field,
                        )?;
                        let mut row = table_data.alloc_one_row();
                        row_writer::write_tags(
                            table_data,
                            std::mem::take(&mut tags).into_iter(),
                            &mut row,
                        )?;
                        sample_row_template = Some((row, timestamp_index, value_index));
                    }

                    if let Some((row, timestamp_index, value_index)) = sample_row_template.as_mut()
                    {
                        row[*timestamp_index].value_data = Some(
                            ValueData::TimestampMillisecondValue(scratch.sample.timestamp),
                        );
                        row[*value_index].value_data =
                            Some(ValueData::F64Value(scratch.sample.value));
                        let row = if rows_remaining == 0 {
                            std::mem::take(row)
                        } else {
                            row.clone()
                        };
                        table_data.add_row(row);
                    }
                }
            }
            3 => {
                scratch.histogram.clear();
                message::merge(
                    wire_type,
                    &mut scratch.histogram,
                    &mut buf,
                    DecodeContext::default(),
                )
                .map_err(|mut error| {
                    error.push("TimeSeries", "histograms");
                    error
                })
                .context(error::DecodePromRemoteRequestSnafu)?;
                rows_remaining = rows_remaining.checked_sub(1).with_context(|| {
                    error::InvalidPromRemoteRequestSnafu {
                        msg: "remote write v2 histogram count changed between scans".to_string(),
                    }
                })?;
                if let Some(SeriesWriter::Histograms(table_data)) = &mut writer {
                    if rows_remaining == 0 {
                        write_native_histogram(
                            table_data,
                            &scratch.histogram,
                            std::mem::take(&mut tags).into_iter(),
                        )?;
                    } else {
                        write_native_histogram(
                            table_data,
                            &scratch.histogram,
                            tags.iter().cloned(),
                        )?;
                    }
                }
            }
            4 => {
                scratch.exemplar.clear();
                message::merge(
                    wire_type,
                    &mut scratch.exemplar,
                    &mut buf,
                    DecodeContext::default(),
                )
                .map_err(|mut error| {
                    error.push("TimeSeries", "exemplars");
                    error
                })
                .context(error::DecodePromRemoteRequestSnafu)?;
            }
            5 => {
                take_length_delimited(wire_type, &mut buf)
                    .map_err(|mut error| {
                        error.push("TimeSeries", "metadata");
                        error
                    })
                    .context(error::DecodePromRemoteRequestSnafu)?;
            }
            _ => skip_field(wire_type, tag, &mut buf, DecodeContext::default())
                .context(error::DecodePromRemoteRequestSnafu)?,
        }
    }

    ensure!(
        rows_remaining == 0,
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 row count changed between scans".to_string(),
        }
    );

    Ok(())
}

fn take_length_delimited<'a>(
    wire_type: WireType,
    buf: &mut &'a [u8],
) -> std::result::Result<&'a [u8], DecodeError> {
    if wire_type != WireType::LengthDelimited {
        return Err(DecodeError::new(format!(
            "invalid wire type: {wire_type:?} (expected LengthDelimited)"
        )));
    }

    let len = decode_varint(buf)?;
    let len =
        usize::try_from(len).map_err(|_| DecodeError::new("length delimiter exceeds usize"))?;
    if len > buf.len() {
        return Err(DecodeError::new("buffer underflow"));
    }
    let (value, remaining) = buf.split_at(len);
    *buf = remaining;
    Ok(value)
}

fn checked_total(total: u64, count: usize, name: &str) -> Result<u64> {
    let count =
        u64::try_from(count)
            .ok()
            .with_context(|| error::InvalidPromRemoteRequestSnafu {
                msg: format!("remote write v2 {name} count exceeds u64"),
            })?;
    total
        .checked_add(count)
        .with_context(|| error::InvalidPromRemoteRequestSnafu {
            msg: format!("remote write v2 {name} count overflows u64"),
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

fn write_native_histogram<'a>(
    table_data: &mut TableData,
    histogram: &Histogram,
    tags: impl Iterator<Item = (&'a str, String)>,
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
    let (datatype, datatype_extension) =
        ColumnDataTypeWrapper::try_from(native_histogram_value_type().clone())
            .expect("native histogram type is convertible to protobuf")
            .into_parts();

    ColumnSchema {
        column_name: greptime_native_histogram().to_string(),
        datatype: datatype as i32,
        semantic_type: SemanticType::Field as i32,
        datatype_extension,
        options: None,
    }
}

fn native_histogram_struct_value(histogram: &Histogram) -> Result<ValueData> {
    let uses_float_counts = native_histogram_uses_float_counts(histogram)?;
    validate_native_histogram(histogram, uses_float_counts)?;

    let mut items = Vec::with_capacity(NATIVE_HISTOGRAM_FIELD_NAMES.len());
    let positive_span_lengths = i32_span_lengths("positive", &histogram.positive_spans)?;
    let negative_span_lengths = i32_span_lengths("negative", &histogram.negative_spans)?;
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
        i32_list_value(positive_span_lengths.iter().copied()),
        i32_list_value(histogram.negative_spans.iter().map(|span| span.offset)),
        i32_list_value(negative_span_lengths.iter().copied()),
    ]);

    if uses_float_counts {
        validate_float_native_histogram_counts(histogram)?;
        let count = match histogram.count.as_ref() {
            Some(Count::CountFloat(count)) => *count,
            _ => 0.0,
        };
        let zero_count = match histogram.zero_count.as_ref() {
            Some(ZeroCount::ZeroCountFloat(zero_count)) => *zero_count,
            _ => 0.0,
        };
        items.extend([
            null_pb_value(),
            null_pb_value(),
            i64_list_value(std::iter::empty()),
            i64_list_value(std::iter::empty()),
            pb_value(ValueData::F64Value(count)),
            pb_value(ValueData::F64Value(zero_count)),
            f64_list_value(histogram.positive_counts.iter().copied()),
            f64_list_value(histogram.negative_counts.iter().copied()),
        ]);
    } else {
        let count = match histogram.count.as_ref() {
            Some(Count::CountInt(count)) => *count,
            _ => 0,
        };
        let zero_count = match histogram.zero_count.as_ref() {
            Some(ZeroCount::ZeroCountInt(zero_count)) => *zero_count,
            _ => 0,
        };
        let positive_buckets = bucket_counts_from_deltas(&histogram.positive_deltas)?;
        let negative_buckets = bucket_counts_from_deltas(&histogram.negative_deltas)?;
        validate_integer_native_histogram_counts(histogram, &positive_buckets, &negative_buckets)?;
        let count = i64::try_from(count)
            .ok()
            .context(error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "remote write v2 native histogram integer count {count} overflows i64"
                ),
            })?;
        let zero_count = i64::try_from(zero_count).ok().context(
            error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "remote write v2 native histogram integer zero_count {zero_count} overflows i64"
                ),
            },
        )?;
        items.extend([
            pb_value(ValueData::I64Value(count)),
            pb_value(ValueData::I64Value(zero_count)),
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

fn validate_native_histogram(histogram: &Histogram, uses_float_counts: bool) -> Result<()> {
    let exponential_overflow_index = validate_native_histogram_schema(histogram.schema)?;
    validate_native_histogram_custom_values(histogram)?;

    if histogram.schema == CUSTOM_BUCKETS_SCHEMA {
        ensure!(
            histogram.zero_threshold == 0.0 && native_histogram_zero_count_is_zero(histogram),
            error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 custom native histogram must not use a zero bucket"
                    .to_string(),
            }
        );
        ensure!(
            histogram.negative_spans.is_empty()
                && histogram.negative_deltas.is_empty()
                && histogram.negative_counts.is_empty(),
            error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 custom native histogram must not use negative buckets"
                    .to_string(),
            }
        );
    }

    let (positive_buckets, negative_buckets) = if uses_float_counts {
        (
            histogram.positive_counts.len(),
            histogram.negative_counts.len(),
        )
    } else {
        (
            histogram.positive_deltas.len(),
            histogram.negative_deltas.len(),
        )
    };
    let bucket_index_range = if let Some(overflow_index) = exponential_overflow_index {
        (i32::MIN, overflow_index)
    } else {
        (
            0,
            i32::try_from(histogram.custom_values.len()).ok().context(
                error::InvalidPromRemoteRequestSnafu {
                    msg: "remote write v2 custom native histogram has too many custom_values"
                        .to_string(),
                },
            )?,
        )
    };
    validate_native_histogram_spans(
        "positive",
        &histogram.positive_spans,
        positive_buckets,
        bucket_index_range,
    )?;
    validate_native_histogram_spans(
        "negative",
        &histogram.negative_spans,
        negative_buckets,
        bucket_index_range,
    )?;

    Ok(())
}

fn validate_native_histogram_schema(schema: i32) -> Result<Option<i32>> {
    if schema == CUSTOM_BUCKETS_SCHEMA {
        return Ok(None);
    }

    if let Some(overflow_index) = exponential_overflow_bucket_index(schema) {
        return Ok(Some(overflow_index));
    }

    if (MAX_REMOTE_WRITE_V2_SCHEMA + 1..=MAX_REDUCIBLE_REMOTE_WRITE_V2_SCHEMA).contains(&schema) {
        error::InvalidPromRemoteRequestSnafu {
            msg: format!(
                "remote write v2 native histogram schema {schema} must be reduced before ingestion"
            ),
        }
        .fail()
    } else {
        error::InvalidPromRemoteRequestSnafu {
            msg: format!("remote write v2 native histogram schema {schema} is unsupported"),
        }
        .fail()
    }
}

fn validate_native_histogram_custom_values(histogram: &Histogram) -> Result<()> {
    if histogram.schema != CUSTOM_BUCKETS_SCHEMA {
        ensure!(
            histogram.custom_values.is_empty(),
            error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 standard native histogram must not use custom_values"
                    .to_string(),
            }
        );
        return Ok(());
    }

    for value in &histogram.custom_values {
        ensure!(
            !value.is_nan() && *value != f64::INFINITY,
            error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 custom native histogram custom_values must not contain +Inf or NaN"
                    .to_string(),
            }
        );
    }
    for values in histogram.custom_values.windows(2) {
        ensure!(
            values[0] < values[1],
            error::InvalidPromRemoteRequestSnafu {
                msg: "remote write v2 custom native histogram custom_values must be sorted"
                    .to_string(),
            }
        );
    }

    Ok(())
}

fn validate_native_histogram_spans(
    name: &str,
    spans: &[BucketSpan],
    bucket_count: usize,
    bucket_index_range: (i32, i32),
) -> Result<()> {
    let span_len = spans
        .iter()
        .try_fold(0usize, |sum, span| sum.checked_add(span.length as usize))
        .with_context(|| error::InvalidPromRemoteRequestSnafu {
            msg: format!("remote write v2 native histogram {name} spans overflow"),
        })?;
    ensure!(
        span_len == bucket_count,
        error::InvalidPromRemoteRequestSnafu {
            msg: format!(
                "remote write v2 native histogram {name} spans describe {span_len} buckets, found {bucket_count}"
            ),
        }
    );

    let mut current_index = 0i32;
    for (span_index, span) in spans.iter().enumerate() {
        ensure!(
            span.offset >= 0 || (span_index == 0 && bucket_index_range.0 == i32::MIN),
            error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "remote write v2 native histogram {name} span {} has negative offset {}",
                    span_index + 1,
                    span.offset
                ),
            }
        );
        current_index = if span_index == 0 {
            span.offset
        } else {
            current_index.checked_add(span.offset).with_context(|| {
                error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "remote write v2 native histogram {name} span index overflows i32"
                    ),
                }
            })?
        };

        for _ in 0..span.length {
            ensure!(
                (bucket_index_range.0..=bucket_index_range.1).contains(&current_index),
                error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "remote write v2 native histogram {name} bucket index {current_index} is out of range"
                    ),
                }
            );
            current_index =
                current_index
                    .checked_add(1)
                    .context(error::InvalidPromRemoteRequestSnafu {
                        msg: format!(
                            "remote write v2 native histogram {name} span index overflows i32"
                        ),
                    })?;
        }
    }

    Ok(())
}

fn validate_float_native_histogram_counts(histogram: &Histogram) -> Result<()> {
    let count = match histogram.count.as_ref() {
        Some(Count::CountFloat(count)) => *count,
        _ => 0.0,
    };
    ensure!(
        count >= 0.0 || count.is_nan(),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 native histogram float count must not be negative".to_string(),
        }
    );

    let zero_count = match histogram.zero_count.as_ref() {
        Some(ZeroCount::ZeroCountFloat(zero_count)) => *zero_count,
        _ => 0.0,
    };
    ensure!(
        zero_count >= 0.0 || zero_count.is_nan(),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 native histogram float zero_count must not be negative"
                .to_string(),
        }
    );

    for (name, counts) in [
        ("positive", &histogram.positive_counts),
        ("negative", &histogram.negative_counts),
    ] {
        for (index, count) in counts.iter().enumerate() {
            ensure!(
                *count >= 0.0 || count.is_nan(),
                error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "remote write v2 native histogram {name} bucket {} count must not be negative",
                        index + 1
                    ),
                }
            );
        }
    }

    Ok(())
}

fn validate_integer_native_histogram_counts(
    histogram: &Histogram,
    positive_buckets: &[i64],
    negative_buckets: &[i64],
) -> Result<()> {
    let count = match histogram.count.as_ref() {
        Some(Count::CountInt(count)) => *count,
        _ => 0,
    };
    let zero_count = match histogram.zero_count.as_ref() {
        Some(ZeroCount::ZeroCountInt(zero_count)) => *zero_count,
        _ => 0,
    };
    let bucket_count = positive_buckets
        .iter()
        .chain(negative_buckets)
        .try_fold(zero_count, |total, bucket| {
            total.checked_add(*bucket as u64)
        })
        .with_context(|| error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 native histogram bucket total overflows u64".to_string(),
        })?;
    ensure!(
        if histogram.sum.is_nan() {
            bucket_count <= count
        } else {
            bucket_count == count
        },
        error::InvalidPromRemoteRequestSnafu {
            msg: format!(
                "remote write v2 native histogram has {bucket_count} observations in buckets, count is {count}"
            ),
        }
    );

    Ok(())
}

fn native_histogram_zero_count_is_zero(histogram: &Histogram) -> bool {
    match histogram.zero_count.as_ref() {
        Some(ZeroCount::ZeroCountInt(zero_count)) => *zero_count == 0,
        Some(ZeroCount::ZeroCountFloat(zero_count)) => *zero_count == 0.0,
        None => true,
    }
}

fn native_histogram_uses_float_counts(histogram: &Histogram) -> Result<bool> {
    let uses_float_count = matches!(histogram.count, Some(Count::CountFloat(_)))
        || matches!(histogram.zero_count, Some(ZeroCount::ZeroCountFloat(_)));
    let uses_int_count = matches!(histogram.count, Some(Count::CountInt(_)))
        || matches!(histogram.zero_count, Some(ZeroCount::ZeroCountInt(_)));
    let uses_float_buckets =
        !histogram.positive_counts.is_empty() || !histogram.negative_counts.is_empty();
    let uses_int_buckets =
        !histogram.positive_deltas.is_empty() || !histogram.negative_deltas.is_empty();

    if matches!(
        (&histogram.count, &histogram.zero_count),
        (Some(Count::CountInt(_)), Some(ZeroCount::ZeroCountFloat(_)))
            | (Some(Count::CountFloat(_)), Some(ZeroCount::ZeroCountInt(_)))
    ) {
        return error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 native histogram count and zero_count must use the same integer or float family".to_string(),
        }
        .fail();
    }

    ensure!(
        !(uses_float_buckets && uses_int_buckets),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 native histogram bucket counts must use either integer deltas or float counts".to_string(),
        }
    );
    ensure!(
        !(uses_float_count && uses_int_buckets),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 float native histogram must not use integer bucket deltas"
                .to_string(),
        }
    );
    ensure!(
        !(uses_int_count && uses_float_buckets),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 integer native histogram must not use float bucket counts"
                .to_string(),
        }
    );

    Ok(uses_float_count || uses_float_buckets)
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

fn i32_span_lengths(name: &str, spans: &[BucketSpan]) -> Result<Vec<i32>> {
    spans
        .iter()
        .map(|span| {
            i32::try_from(span.length)
                .ok()
                .context(error::InvalidPromRemoteRequestSnafu {
                    msg: format!(
                        "remote write v2 native histogram {name} span length {} overflows i32",
                        span.length
                    ),
                })
        })
        .collect()
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
        count =
            count
                .checked_add(*delta)
                .with_context(|| error::InvalidPromRemoteRequestSnafu {
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

fn ensure_no_internal_histogram_labels(tags: &PromTags<'_>) -> Result<()> {
    // The histogram field column is generated from the protobuf payload.
    for (name, _) in tags {
        ensure!(
            *name != greptime_native_histogram() && *name != NATIVE_HISTOGRAM_FIELD,
            error::InvalidPromRemoteRequestSnafu {
                msg: format!(
                    "remote write v2 label `{name}` conflicts with an internal native histogram label"
                ),
            }
        );
    }

    Ok(())
}

fn resolve_series_labels<'a>(
    symbols: &'a [&str],
    labels_refs: &[u32],
    label_names: &mut HashSet<&'a str>,
) -> Result<ResolvedSeriesLabels<'a>> {
    ensure!(
        labels_refs.len().is_multiple_of(2),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 labels_refs must contain name/value pairs".to_string(),
        }
    );

    let mut prom_ctx = PromCtx::default();
    let mut table_name = None;
    let mut tags = Vec::with_capacity(labels_refs.len() / 2);
    label_names.clear();

    for pair in labels_refs.chunks_exact(2) {
        let name = symbol_ref(symbols, pair[0], "label name")?;
        let value = symbol_ref(symbols, pair[1], "label value")?;
        validate_label(name)?;
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

        tags.push((name, value.to_string()));
    }

    let table_name = table_name.with_context(|| error::InvalidPromRemoteRequestSnafu {
        msg: "missing '__name__' label in time-series".to_string(),
    })?;
    ensure!(
        !table_name.is_empty(),
        error::InvalidPromRemoteRequestSnafu {
            msg: "remote write v2 label `__name__` value must not be empty".to_string(),
        }
    );

    Ok((prom_ctx, table_name, tags))
}

fn validate_label(name: &str) -> Result<()> {
    ensure!(
        validate_label_name(name.as_bytes()),
        error::InvalidPromRemoteRequestSnafu {
            msg: format!("remote write v2 invalid label name `{name}`"),
        }
    );

    Ok(())
}

fn symbol_ref<'a>(symbols: &'a [&str], idx: u32, field: &str) -> Result<&'a str> {
    let idx = usize::try_from(idx)
        .ok()
        .with_context(|| error::InvalidPromRemoteRequestSnafu {
            msg: format!("remote write v2 {field} symbol reference exceeds usize"),
        })?;
    symbols
        .get(idx)
        .copied()
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
    use prost::Message;
    use snafu::ResultExt;

    use crate::error::{self, Result};
    use crate::prom_remote_write::try_decompress;
    use crate::prom_store::snappy_compress;

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
        let buf = if let Ok(buf) = try_decompress(is_zstd, &body[..]) {
            buf
        } else {
            try_decompress(!is_zstd, &body[..])?
        };
        Request::decode(&buf[..]).context(error::DecodePromRemoteRequestSnafu)
    }

    pub fn write_requests(
        request: Request,
    ) -> Result<(Vec<RowInsertRequest>, Vec<RowInsertRequest>, u64, u64)> {
        let body = Bytes::from(snappy_compress(&request.encode_to_vec())?);
        decode_write_requests(false, body, true)
    }

    pub fn decode_write_requests(
        is_zstd: bool,
        body: Bytes,
        native_histograms_enabled: bool,
    ) -> Result<(Vec<RowInsertRequest>, Vec<RowInsertRequest>, u64, u64)> {
        let requests = super::decode_remote_write_v2(is_zstd, body, native_histograms_enabled)?;
        Ok((
            requests.samples.all_req().collect(),
            requests.histograms.all_req().collect(),
            requests.sample_count,
            requests.histogram_count,
        ))
    }

    pub fn decode_uncompressed_write_requests(
        body: &[u8],
        native_histograms_enabled: bool,
    ) -> Result<(Vec<RowInsertRequest>, Vec<RowInsertRequest>, u64, u64)> {
        let request =
            super::BorrowedRequest::decode(body).context(error::DecodePromRemoteRequestSnafu)?;
        let requests = super::convert_remote_write_v2(request, native_histograms_enabled)?;
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
    use common_query::prelude::{greptime_timestamp, greptime_value, set_default_prefix};
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

        let decoded = test_util::decode_request(false, body.clone()).unwrap();

        assert_eq!(decoded.symbols, request.symbols);
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].labels_refs, vec![1, 2]);
        assert_eq!(decoded.timeseries[0].samples.len(), 1);
        assert_eq!(decoded.timeseries[0].samples[0].value, 42.0);
        assert_eq!(decoded.timeseries[0].metadata.as_ref().unwrap().r#type, 1);
        assert_eq!(
            decode_remote_write_v2(true, body, true)
                .unwrap()
                .sample_count,
            1
        );
    }

    #[test]
    fn test_fused_decoder_accepts_arbitrary_field_order_and_split_labels() {
        let mut first_sample = Sample {
            value: 42.0,
            timestamp: 1000,
            start_timestamp: 500,
        }
        .encode_to_vec();
        first_sample.extend(varint_field(90, 1));
        let second_sample = Sample {
            value: 43.0,
            timestamp: 2000,
            start_timestamp: 1000,
        }
        .encode_to_vec();

        let mut series = encoded_message_field(2, &first_sample);
        series.extend(packed_u32_field(1, &[1]));
        series.extend(varint_field(90, 1));
        series.extend(encoded_message_field(2, &second_sample));
        series.extend(varint_field(1, 2));

        let mut wire = encoded_message_field(5, &series);
        wire.extend(string_field(4, b""));
        wire.extend(varint_field(90, 1));
        wire.extend(string_field(4, METRIC_NAME_LABEL.as_bytes()));
        wire.extend(encoded_message_field(5, &packed_u32_field(1, &[99])));
        wire.extend(string_field(4, b"http_requests_total"));

        let requests = decode_wire(&wire, true).unwrap();
        assert_eq!(requests.sample_count, 2);
        assert_eq!(requests.histogram_count, 0);
        let rows = requests.samples.all_req().next().unwrap().rows.unwrap();
        assert_eq!(rows.rows.len(), 2);
        assert_eq!(
            rows.schema
                .iter()
                .map(|column| column.column_name.as_str())
                .collect::<Vec<_>>(),
            vec![greptime_timestamp(), greptime_value()]
        );
        assert_eq!(
            rows.rows[0].values[1].value_data,
            Some(ValueData::F64Value(42.0))
        );
        assert_eq!(
            rows.rows[1].values[1].value_data,
            Some(ValueData::F64Value(43.0))
        );
    }

    #[test]
    fn test_fused_decoder_accepts_histogram_before_labels_and_unknown_fields() {
        let mut histogram = Histogram {
            count: Some(Count::CountInt(0)),
            zero_count: Some(ZeroCount::ZeroCountInt(0)),
            timestamp: 2000,
            start_timestamp: 1000,
            ..Default::default()
        }
        .encode_to_vec();
        histogram.extend(varint_field(90, 1));

        let mut series = encoded_message_field(3, &histogram);
        series.extend(packed_u32_field(1, &[1, 2]));
        let wire = request_wire(&["", METRIC_NAME_LABEL, "metric"], &[series]);

        let requests = decode_wire(&wire, true).unwrap();
        assert_eq!(requests.histogram_count, 1);
        let rows = requests.histograms.all_req().next().unwrap().rows.unwrap();
        assert_eq!(
            histogram_field_value(&rows, 0, START_TIMESTAMP_FIELD),
            Some(ValueData::TimestampMillisecondValue(1000))
        );
    }

    #[test]
    fn test_fused_decoder_rejects_malformed_wire() {
        let mut wrong_request_wire = varint_field(4, 0);
        let invalid_utf8 = string_field(4, &[0xff]);

        let mut truncated_request = Vec::new();
        prost::encoding::encode_key(5, WireType::LengthDelimited, &mut truncated_request);
        prost::encoding::encode_varint(2, &mut truncated_request);
        truncated_request.push(0);

        let mut oversized_request = Vec::new();
        prost::encoding::encode_key(4, WireType::LengthDelimited, &mut oversized_request);
        prost::encoding::encode_varint(u64::MAX, &mut oversized_request);

        let malformed_varint = vec![0x80; 10];

        let mut wrong_labels_wire = Vec::new();
        prost::encoding::encode_key(1, WireType::SixtyFourBit, &mut wrong_labels_wire);
        wrong_labels_wire.extend([0; 8]);
        wrong_labels_wire = request_wire(&[""], &[std::mem::take(&mut wrong_labels_wire)]);

        let wrong_series_wire = request_wire(&[""], &[varint_field(2, 0)]);

        let mut truncated_series = Vec::new();
        prost::encoding::encode_key(2, WireType::LengthDelimited, &mut truncated_series);
        prost::encoding::encode_varint(2, &mut truncated_series);
        truncated_series.push(0x08);
        let truncated_series = request_wire(&[""], &[truncated_series]);

        let mut oversized_series = Vec::new();
        prost::encoding::encode_key(3, WireType::LengthDelimited, &mut oversized_series);
        prost::encoding::encode_varint(u64::MAX, &mut oversized_series);
        let oversized_series = request_wire(&[""], &[oversized_series]);

        let invalid_sample = series_wire(&[1, 2], 2, &varint_field(1, 1));
        let invalid_sample = request_wire(&["", METRIC_NAME_LABEL, "metric"], &[invalid_sample]);
        let invalid_histogram = series_wire(&[1, 2], 3, &varint_field(3, 1));
        let invalid_histogram =
            request_wire(&["", METRIC_NAME_LABEL, "metric"], &[invalid_histogram]);

        for (name, wire) in [
            (
                "wrong request wire type",
                std::mem::take(&mut wrong_request_wire),
            ),
            ("invalid utf8", invalid_utf8),
            ("truncated request", truncated_request),
            ("oversized request length", oversized_request),
            ("malformed varint", malformed_varint),
            ("wrong labels wire type", wrong_labels_wire),
            ("wrong series wire type", wrong_series_wire),
            ("truncated series", truncated_series),
            ("oversized series length", oversized_series),
            ("invalid sample", invalid_sample),
            ("invalid histogram", invalid_histogram),
        ] {
            let error = decode_wire_error(&wire, true, name);
            assert!(
                matches!(error, error::Error::DecodePromRemoteRequest { .. }),
                "{name}: {error}"
            );
        }
    }

    #[test]
    fn test_fused_decoder_rejects_malformed_ignored_messages() {
        for tag in [4, 5] {
            let mut series = series_wire(&[1, 2], 2, &Sample::default().encode_to_vec());
            series.extend(encoded_message_field(tag, &[0x08]));
            let wire = request_wire(&["", METRIC_NAME_LABEL, "metric"], &[series]);

            let error = decode_wire_error(&wire, true, "malformed ignored message");
            assert!(matches!(
                error,
                error::Error::DecodePromRemoteRequest { .. }
            ));
        }
    }

    #[test]
    fn test_fused_decoder_ignores_exemplar_symbol_refs() {
        let request = Request {
            symbols: vec![
                String::new(),
                METRIC_NAME_LABEL.to_string(),
                "metric".to_string(),
            ],
            timeseries: vec![TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![Sample::default()],
                exemplars: vec![Exemplar {
                    labels_refs: vec![99],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        assert_eq!(decode_test_request(request).unwrap().sample_count, 1);
    }

    #[test]
    fn test_fused_decoder_preserves_empty_request_and_series_behavior() {
        let request = Request {
            symbols: vec![
                String::new(),
                METRIC_NAME_LABEL.to_string(),
                "metric".to_string(),
                "job".to_string(),
            ],
            timeseries: vec![
                TimeSeries {
                    labels_refs: vec![99, 99],
                    ..Default::default()
                },
                TimeSeries {
                    labels_refs: vec![1],
                    ..Default::default()
                },
                TimeSeries {
                    labels_refs: vec![3, 2, 3, 2],
                    ..Default::default()
                },
                TimeSeries::default(),
            ],
        };

        let requests = decode_test_request(request).unwrap();
        assert_eq!(requests.sample_count, 0);
        assert_eq!(requests.histogram_count, 0);

        let requests = decode_test_request(Request {
            symbols: vec![String::new()],
            timeseries: Vec::new(),
        })
        .unwrap();
        assert_eq!(requests.sample_count, 0);
        assert!(decode_wire(&[], true).is_err());
        assert!(decode_wire(&[0x0a, 0x00], true).is_err());
    }

    #[test]
    fn test_fused_decoder_rejects_duplicate_resolved_label_names() {
        let request = Request {
            symbols: vec![
                String::new(),
                METRIC_NAME_LABEL.to_string(),
                "metric".to_string(),
                "job".to_string(),
                "job".to_string(),
                "api".to_string(),
                "worker".to_string(),
            ],
            timeseries: vec![TimeSeries {
                labels_refs: vec![1, 2, 3, 5, 4, 6],
                samples: vec![Sample::default()],
                ..Default::default()
            }],
        };

        assert_invalid(
            "duplicate resolved labels",
            request,
            "label name `job` is repeated",
        );
    }

    #[test]
    fn test_fused_decoder_pins_experimental_error_precedence() {
        let histogram = series_wire(&[1, 2], 3, &Histogram::default().encode_to_vec());
        let malformed_sample = series_wire(&[1, 2], 2, &[0x08]);

        let wire = request_wire(
            &["", METRIC_NAME_LABEL, "metric"],
            &[histogram.clone(), malformed_sample.clone()],
        );
        let error = decode_wire_error(&wire, false, "histogram before malformed series");
        assert!(error.to_string().contains("ingestion is experimental"));

        let wire = request_wire(
            &["", METRIC_NAME_LABEL, "metric"],
            &[malformed_sample, histogram.clone()],
        );
        let error = decode_wire_error(&wire, false, "malformed series before histogram");
        assert!(matches!(
            error,
            error::Error::DecodePromRemoteRequest { .. }
        ));

        let missing_name = series_wire(&[3, 4], 2, &Sample::default().encode_to_vec());
        let wire = request_wire(
            &["", METRIC_NAME_LABEL, "metric", "job", "api"],
            &[missing_name, histogram],
        );
        let error = decode_wire_error(&wire, false, "conversion error before histogram");
        assert!(error.to_string().contains("missing '__name__'"));
    }

    #[test]
    fn test_into_context_req_samples() {
        let ctx_req = decode_test_request(test_util::request_with_labels_and_samples(
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
        assert_eq!(
            rows.rows[1].values[2].value_data,
            Some(ValueData::StringValue("api".to_string()))
        );
        assert_eq!(
            rows.rows[1].values[3].value_data,
            Some(ValueData::StringValue("localhost:9090".to_string()))
        );
    }

    #[test]
    fn test_into_context_req_special_labels() {
        let ctx_req = decode_test_request(test_util::request_with_labels_and_samples(
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
            "invalid label name",
        ));

        cases.push((
            "invalid label name",
            request_with_sample(vec![(METRIC_NAME_LABEL, "metric"), ("has-dash", "api")]),
            "invalid label name",
        ));

        cases.push((
            "dotted label name",
            request_with_sample(vec![(METRIC_NAME_LABEL, "metric"), ("service.name", "api")]),
            "invalid label name",
        ));

        cases.push((
            "non-ascii label name",
            request_with_sample(vec![(METRIC_NAME_LABEL, "metric"), ("区域", "api")]),
            "invalid label name",
        ));

        cases.push((
            "empty metric name",
            request_with_sample(vec![(METRIC_NAME_LABEL, "")]),
            "label `__name__` value must not be empty",
        ));

        cases.push((
            "internal histogram label on samples",
            request_with_sample(vec![
                (METRIC_NAME_LABEL, "metric"),
                (greptime_native_histogram(), "user_value"),
            ]),
            "conflicts with an internal native histogram label",
        ));

        cases.push((
            "int count with float zero count",
            request_with_histogram(Histogram {
                count: Some(Count::CountInt(1)),
                zero_count: Some(ZeroCount::ZeroCountFloat(0.5)),
                ..Default::default()
            }),
            "count and zero_count must use the same integer or float family",
        ));

        cases.push((
            "float count with int zero count",
            request_with_histogram(Histogram {
                count: Some(Count::CountFloat(1.0)),
                zero_count: Some(ZeroCount::ZeroCountInt(1)),
                ..Default::default()
            }),
            "count and zero_count must use the same integer or float family",
        ));

        cases.push((
            "reducible schema",
            request_with_histogram(Histogram {
                schema: 9,
                ..Default::default()
            }),
            "schema 9 must be reduced before ingestion",
        ));

        cases.push((
            "unsupported schema",
            request_with_histogram(Histogram {
                schema: 53,
                ..Default::default()
            }),
            "schema 53 is unsupported",
        ));

        cases.push((
            "standard schema with custom values",
            request_with_histogram(Histogram {
                schema: 1,
                custom_values: vec![1.0],
                ..Default::default()
            }),
            "standard native histogram must not use custom_values",
        ));

        cases.push((
            "custom values with inf",
            request_with_histogram(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![f64::INFINITY],
                ..Default::default()
            }),
            "custom_values must not contain +Inf or NaN",
        ));

        cases.push((
            "custom values not sorted",
            request_with_histogram(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![2.0, 1.0],
                ..Default::default()
            }),
            "custom_values must be sorted",
        ));

        cases.push((
            "custom schema with zero bucket",
            request_with_histogram(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                zero_count: Some(ZeroCount::ZeroCountInt(1)),
                ..Default::default()
            }),
            "custom native histogram must not use a zero bucket",
        ));

        cases.push((
            "custom schema with negative buckets",
            request_with_histogram(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                negative_spans: vec![BucketSpan {
                    offset: -1,
                    length: 1,
                }],
                negative_deltas: vec![1],
                ..Default::default()
            }),
            "custom native histogram must not use negative buckets",
        ));

        cases.push((
            "span count mismatch",
            request_with_histogram(Histogram {
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_deltas: vec![1],
                ..Default::default()
            }),
            "positive spans describe 2 buckets, found 1",
        ));

        cases.push((
            "negative offset after first span",
            request_with_histogram(Histogram {
                count: Some(Count::CountInt(2)),
                positive_spans: vec![
                    BucketSpan {
                        offset: 0,
                        length: 1,
                    },
                    BucketSpan {
                        offset: -1,
                        length: 1,
                    },
                ],
                positive_deltas: vec![1, 0],
                ..Default::default()
            }),
            "positive span 2 has negative offset -1",
        ));

        cases.push((
            "negative custom span offset",
            request_with_histogram(Histogram {
                count: Some(Count::CountInt(1)),
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![1.0],
                positive_spans: vec![BucketSpan {
                    offset: -1,
                    length: 1,
                }],
                positive_deltas: vec![1],
                ..Default::default()
            }),
            "positive span 1 has negative offset -1",
        ));

        cases.push((
            "integer bucket total mismatch",
            request_with_histogram(Histogram {
                count: Some(Count::CountInt(0)),
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                positive_deltas: vec![1],
                ..Default::default()
            }),
            "has 1 observations in buckets, count is 0",
        ));

        cases.push((
            "negative float count",
            request_with_histogram(Histogram {
                count: Some(Count::CountFloat(-1.0)),
                ..Default::default()
            }),
            "float count must not be negative",
        ));

        cases.push((
            "negative float zero count",
            request_with_histogram(Histogram {
                count: Some(Count::CountFloat(0.0)),
                zero_count: Some(ZeroCount::ZeroCountFloat(-1.0)),
                ..Default::default()
            }),
            "float zero_count must not be negative",
        ));

        cases.push((
            "negative float bucket count",
            request_with_histogram(Histogram {
                count: Some(Count::CountFloat(0.0)),
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                positive_counts: vec![-1.0],
                ..Default::default()
            }),
            "positive bucket 1 count must not be negative",
        ));

        cases.push((
            "custom span index out of range",
            request_with_histogram(Histogram {
                schema: CUSTOM_BUCKETS_SCHEMA,
                custom_values: vec![1.0],
                positive_spans: vec![BucketSpan {
                    offset: 2,
                    length: 1,
                }],
                positive_deltas: vec![1],
                ..Default::default()
            }),
            "positive bucket index 2 is out of range",
        ));

        for (name, request, expected) in cases {
            assert_invalid(name, request, expected);
        }
    }

    #[test]
    fn test_into_context_req_allows_nan_observations_outside_buckets() {
        decode_test_request(request_with_histogram(Histogram {
            count: Some(Count::CountInt(2)),
            sum: f64::NAN,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_deltas: vec![1],
            ..Default::default()
        }))
        .unwrap();
    }

    #[test]
    fn test_into_context_req_allows_empty_label_values() {
        let ctx_req = decode_test_request(test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "metric"), ("job", "")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        ))
        .unwrap();

        let rows = ctx_req.samples.all_req().next().unwrap().rows.unwrap();
        let job_idx = column_index(&rows.schema, "job");
        assert_eq!(
            rows.rows[0].values[job_idx].value_data,
            Some(ValueData::StringValue(String::new()))
        );
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

        let mut request = test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        );
        request.timeseries.push(TimeSeries {
            labels_refs: request.timeseries[0].labels_refs.clone(),
            histograms: vec![Histogram::default()],
            ..Default::default()
        });

        assert_invalid(
            "same metric samples and histograms across series",
            request,
            "contains both samples and native histograms",
        );
    }

    #[test]
    fn test_into_context_req_rejects_metric_kind_conflict_across_label_sets() {
        let request = Request {
            symbols: vec![
                "".to_string(),
                METRIC_NAME_LABEL.to_string(),
                "metric".to_string(),
                "job".to_string(),
                "api".to_string(),
                "worker".to_string(),
            ],
            timeseries: vec![
                TimeSeries {
                    labels_refs: vec![1, 2, 3, 4],
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 1000,
                        start_timestamp: 0,
                    }],
                    ..Default::default()
                },
                TimeSeries {
                    labels_refs: vec![1, 2, 3, 5],
                    histograms: vec![Histogram::default()],
                    ..Default::default()
                },
            ],
        };

        assert_invalid(
            "same metric kind conflict across label sets",
            request,
            "contains both samples and native histograms",
        );
    }

    #[test]
    fn test_into_context_req_validates_exponential_overflow_bucket_index() {
        for schema in [-4, 0, 8] {
            let max_index = exponential_overflow_bucket_index(schema).unwrap();
            for positive in [true, false] {
                let mut histogram = Histogram {
                    schema,
                    count: Some(Count::CountInt(1)),
                    ..Default::default()
                };
                if positive {
                    histogram.positive_spans = vec![BucketSpan {
                        offset: max_index,
                        length: 1,
                    }];
                    histogram.positive_deltas = vec![1];
                } else {
                    histogram.negative_spans = vec![BucketSpan {
                        offset: max_index,
                        length: 1,
                    }];
                    histogram.negative_deltas = vec![1];
                }
                decode_test_request(request_with_histogram(histogram.clone())).unwrap();

                let beyond = max_index + 1;
                if positive {
                    histogram.positive_spans[0].offset = beyond;
                } else {
                    histogram.negative_spans[0].offset = beyond;
                }
                assert_invalid(
                    "exponential overflow bucket index",
                    request_with_histogram(histogram),
                    &format!("bucket index {beyond} is out of range"),
                );
            }
        }
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

        let ctx_req = decode_test_request(request).unwrap();

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

        let ctx_req = decode_test_request(request).unwrap();

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
            vec![greptime_timestamp(), greptime_native_histogram()]
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
            histogram_field_value(&rows, 0, COUNT_I64_FIELD),
            Some(ValueData::I64Value(0))
        );
        assert_eq!(histogram_field_value(&rows, 0, COUNT_F64_FIELD), None);
    }

    #[test]
    fn test_into_context_req_preserves_histogram_start_timestamp() {
        let ctx_req = decode_test_request(test_util::request_with_labels_and_histograms(
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
                (greptime_native_histogram(), "user_value"),
            ],
            vec![],
        );
        request.timeseries[0].histograms.push(Histogram::default());

        let err = match decode_test_request(request) {
            Ok(_) => panic!("expected invalid request error"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "Invalid prometheus remote request, msg: remote write v2 label `greptime_native_histogram` conflicts with an internal native histogram label"
        );
    }

    #[test]
    fn test_rejects_legacy_histogram_label_after_prefix_change() {
        set_default_prefix(Some("custom")).unwrap();
        assert_eq!(greptime_native_histogram(), "custom_native_histogram");

        let err = ensure_no_internal_histogram_labels(&vec![(
            NATIVE_HISTOGRAM_FIELD,
            "user_value".to_string(),
        )])
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("conflicts with an internal native histogram label")
        );
    }

    #[test]
    fn test_into_context_req_converts_int_and_float_histograms_to_one_schema() {
        let float_histogram = Histogram {
            count: Some(api::greptime_proto::io::prometheus::write::v2::histogram::Count::CountFloat(6.0)),
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

        let ctx_req = decode_test_request(request).unwrap();

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
            vec![greptime_timestamp(), greptime_native_histogram()]
        );

        assert_eq!(
            histogram_field_value(&rows, 0, COUNT_I64_FIELD),
            Some(ValueData::I64Value(0))
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

        assert_eq!(histogram_field_value(&rows, 1, COUNT_I64_FIELD), None);
        assert_eq!(
            histogram_field_value(&rows, 1, COUNT_F64_FIELD),
            Some(ValueData::F64Value(6.0))
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

    fn decode_wire(
        wire: &[u8],
        native_histograms_enabled: bool,
    ) -> Result<RemoteWriteV2WriteRequests> {
        let body = Bytes::from(crate::prom_store::snappy_compress(wire).unwrap());
        decode_remote_write_v2(false, body, native_histograms_enabled)
    }

    fn decode_wire_error(wire: &[u8], native_histograms_enabled: bool, name: &str) -> error::Error {
        match decode_wire(wire, native_histograms_enabled) {
            Ok(_) => panic!("{name}: expected decoder error"),
            Err(error) => error,
        }
    }

    fn request_wire(symbols: &[&str], series: &[Vec<u8>]) -> Vec<u8> {
        let mut wire = Vec::new();
        for symbol in symbols {
            wire.extend(string_field(4, symbol.as_bytes()));
        }
        for series in series {
            wire.extend(encoded_message_field(5, series));
        }
        wire
    }

    fn series_wire(labels_refs: &[u32], leaf_tag: u32, leaf: &[u8]) -> Vec<u8> {
        let mut series = packed_u32_field(1, labels_refs);
        series.extend(encoded_message_field(leaf_tag, leaf));
        series
    }

    fn string_field(tag: u32, value: &[u8]) -> Vec<u8> {
        encoded_message_field(tag, value)
    }

    fn encoded_message_field(tag: u32, value: &[u8]) -> Vec<u8> {
        let mut field = Vec::new();
        prost::encoding::encode_key(tag, WireType::LengthDelimited, &mut field);
        prost::encoding::encode_varint(u64::try_from(value.len()).unwrap(), &mut field);
        field.extend_from_slice(value);
        field
    }

    fn varint_field(tag: u32, value: u64) -> Vec<u8> {
        let mut field = Vec::new();
        prost::encoding::encode_key(tag, WireType::Varint, &mut field);
        prost::encoding::encode_varint(value, &mut field);
        field
    }

    fn packed_u32_field(tag: u32, values: &[u32]) -> Vec<u8> {
        let mut packed = Vec::new();
        for value in values {
            prost::encoding::encode_varint(u64::from(*value), &mut packed);
        }
        encoded_message_field(tag, &packed)
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

    fn request_with_histogram(histogram: Histogram) -> Request {
        test_util::request_with_labels_and_histograms(
            vec![(METRIC_NAME_LABEL, "metric")],
            vec![histogram],
        )
    }

    fn decode_test_request(request: Request) -> Result<RemoteWriteV2WriteRequests> {
        decode_test_request_with_histograms(request, true)
    }

    fn decode_test_request_with_histograms(
        request: Request,
        native_histograms_enabled: bool,
    ) -> Result<RemoteWriteV2WriteRequests> {
        let body =
            Bytes::from(crate::prom_store::snappy_compress(&request.encode_to_vec()).unwrap());
        decode_remote_write_v2(false, body, native_histograms_enabled)
    }

    fn assert_invalid(name: &str, request: Request, expected: &str) {
        let err = match decode_test_request(request) {
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
        let histogram_idx = column_index(&rows.schema, greptime_native_histogram());
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

    fn push_test_symbol(symbols: &mut Vec<String>, symbol: &str) -> u32 {
        if let Some(idx) = symbols.iter().position(|s| s == symbol) {
            return idx as u32;
        }
        let idx = symbols.len();
        symbols.push(symbol.to_string());
        idx as u32
    }

    /// One sample-carrying test series: `(labels, metric_type, unit)`.
    type MetadataSeries<'a> = (Vec<(&'a str, &'a str)>, i32, Option<&'a str>);

    fn metadata_request(series: Vec<MetadataSeries<'_>>) -> Request {
        let mut symbols = vec!["".to_string()];
        let timeseries = series
            .into_iter()
            .map(|(labels, metric_type, unit)| {
                let mut labels_refs = Vec::with_capacity(labels.len() * 2);
                for (name, value) in labels {
                    labels_refs.push(push_test_symbol(&mut symbols, name));
                    labels_refs.push(push_test_symbol(&mut symbols, value));
                }
                let unit_ref = unit.map_or(0, |unit| push_test_symbol(&mut symbols, unit));
                TimeSeries {
                    labels_refs,
                    samples: vec![Sample {
                        value: 1.0,
                        timestamp: 1000,
                        start_timestamp: 0,
                    }],
                    histograms: Vec::new(),
                    exemplars: Vec::new(),
                    metadata: Some(Metadata {
                        r#type: metric_type,
                        help_ref: 0,
                        unit_ref,
                    }),
                }
            })
            .collect();
        Request {
            symbols,
            timeseries,
        }
    }

    type DecodedIndex = std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    >;

    fn decoded_index(request: Request) -> DecodedIndex {
        let encoded = decode_test_request(request)
            .unwrap()
            .semantic_index
            .encode("public")
            .expect("non-empty semantic index");
        serde_json::from_str(&encoded).unwrap()
    }

    #[test]
    fn test_metadata_stamps_semantic_index() {
        use table::requests::METADATA_QUALITY_DECLARED;

        let index = decoded_index(metadata_request(vec![
            (
                vec![(METRIC_NAME_LABEL, "http_requests_total")],
                metadata::MetricType::Counter as i32,
                Some("seconds"),
            ),
            (
                vec![(METRIC_NAME_LABEL, "queue_depth")],
                metadata::MetricType::Gauge as i32,
                // Outside the OpenMetrics base set: dropped, not passed through.
                Some("requests"),
            ),
        ]));

        let tables = &index["public"];
        let typed = &tables["http_requests_total"];
        assert_eq!(typed[SEMANTIC_METRIC_TYPE], "counter");
        assert_eq!(
            typed[SEMANTIC_METRIC_METADATA_QUALITY],
            METADATA_QUALITY_DECLARED
        );
        assert_eq!(typed[SEMANTIC_METRIC_UNIT], "s");

        let unitless = &tables["queue_depth"];
        assert_eq!(unitless[SEMANTIC_METRIC_TYPE], "gauge");
        assert!(!unitless.contains_key(SEMANTIC_METRIC_UNIT));
    }

    #[test]
    fn test_metadata_unspecified_stamps_unit_but_not_type() {
        let index = decoded_index(metadata_request(vec![(
            vec![(METRIC_NAME_LABEL, "untyped_total")],
            metadata::MetricType::Unspecified as i32,
            Some("seconds"),
        )]));
        let untyped = &index["public"]["untyped_total"];
        assert_eq!(untyped[SEMANTIC_METRIC_UNIT], "s");
        assert!(!untyped.contains_key(SEMANTIC_METRIC_TYPE));
        assert!(!untyped.contains_key(SEMANTIC_METRIC_METADATA_QUALITY));

        let requests = decode_test_request(metadata_request(vec![(
            vec![(METRIC_NAME_LABEL, "untyped_unitless_total")],
            metadata::MetricType::Unspecified as i32,
            None,
        )]))
        .unwrap();
        assert!(requests.semantic_index.is_empty());

        // No metadata at all behaves the same.
        let requests = decode_test_request(test_util::request_with_labels_and_samples(
            vec![(METRIC_NAME_LABEL, "bare_total")],
            vec![Sample {
                value: 1.0,
                timestamp: 1000,
                start_timestamp: 0,
            }],
        ))
        .unwrap();
        assert!(requests.semantic_index.is_empty());
    }

    #[test]
    fn test_metadata_type_conflict_collapses_to_mixed() {
        let index = decoded_index(metadata_request(vec![
            (
                vec![(METRIC_NAME_LABEL, "flappy_metric"), ("job", "a")],
                metadata::MetricType::Counter as i32,
                None,
            ),
            (
                vec![(METRIC_NAME_LABEL, "flappy_metric"), ("job", "b")],
                metadata::MetricType::Gauge as i32,
                None,
            ),
        ]));
        assert_eq!(
            index["public"]["flappy_metric"][SEMANTIC_METRIC_TYPE],
            "mixed"
        );
    }

    #[test]
    fn test_metadata_schema_overrides_stay_apart() {
        // The same metric name written into two schemas by one request must not
        // collapse each other's metadata.
        let index = decoded_index(metadata_request(vec![
            (
                vec![(METRIC_NAME_LABEL, "cpu_usage")],
                metadata::MetricType::Counter as i32,
                None,
            ),
            (
                vec![
                    (METRIC_NAME_LABEL, "cpu_usage"),
                    (DATABASE_LABEL, "tenant_b"),
                ],
                metadata::MetricType::Gauge as i32,
                None,
            ),
        ]));
        assert_eq!(
            index["public"]["cpu_usage"][SEMANTIC_METRIC_TYPE],
            "counter"
        );
        assert_eq!(
            index["tenant_b"]["cpu_usage"][SEMANTIC_METRIC_TYPE],
            "gauge"
        );
    }

    #[test]
    fn test_metadata_out_of_range_refs_are_rejected() {
        let mut request = metadata_request(vec![(
            vec![(METRIC_NAME_LABEL, "broken_total")],
            metadata::MetricType::Counter as i32,
            None,
        )]);
        request.timeseries[0].metadata.as_mut().unwrap().unit_ref = 999;
        let err = decode_test_request(request).err().unwrap();
        assert!(err.to_string().contains("out of range"), "{err}");

        // unit_ref must be validated even when the type is UNSPECIFIED
        // (which persists nothing).
        let mut request = metadata_request(vec![(
            vec![(METRIC_NAME_LABEL, "broken_total")],
            metadata::MetricType::Unspecified as i32,
            None,
        )]);
        request.timeseries[0].metadata.as_mut().unwrap().unit_ref = 999;
        let err = decode_test_request(request).err().unwrap();
        assert!(err.to_string().contains("out of range"), "{err}");

        // help_ref is validated although help is never persisted.
        let mut request = metadata_request(vec![(
            vec![(METRIC_NAME_LABEL, "broken_total")],
            metadata::MetricType::Counter as i32,
            None,
        )]);
        request.timeseries[0].metadata.as_mut().unwrap().help_ref = 999;
        let err = decode_test_request(request).err().unwrap();
        assert!(err.to_string().contains("out of range"), "{err}");
    }
}
