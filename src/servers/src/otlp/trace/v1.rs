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

use std::collections::{HashMap, HashSet};

use api::v1::column_data_type_extension::TypeExt;
use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, RowInsertRequests,
    Value,
};
use common_catalog::consts::{trace_operations_table_name, trace_services_table_name};
use common_grpc::precision::Precision;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use pipeline::{GreptimePipelineParams, PipelineWay};
use session::context::QueryContextRef;

use crate::error::Result;
use crate::otlp::trace::attributes::Attributes;
use crate::otlp::trace::span::TraceSpan;
use crate::otlp::trace::{
    DURATION_NANO_COLUMN, KEY_SERVICE_NAME, PARENT_SPAN_ID_COLUMN, SCOPE_NAME_COLUMN,
    SCOPE_VERSION_COLUMN, SERVICE_NAME_COLUMN, SPAN_EVENTS_COLUMN, SPAN_ID_COLUMN,
    SPAN_KIND_COLUMN, SPAN_NAME_COLUMN, SPAN_STATUS_CODE, SPAN_STATUS_MESSAGE_COLUMN,
    TIMESTAMP_COLUMN, TRACE_ID_COLUMN, TRACE_STATE_COLUMN, TraceAuxData,
};
use crate::otlp::utils::{any_value_to_jsonb, make_column_data, make_string_column_data};
use crate::query_handler::PipelineHandlerRef;
use crate::row_writer::{self, MultiTableData, TableData};

const APPROXIMATE_COLUMN_COUNT: usize = 30;

// Use a timestamp(2100-01-01 00:00:00) as large as possible.
const MAX_TIMESTAMP: i64 = 4102444800000000000;

/// Distinguishes raw bytes from JSONB values that both use a binary protobuf value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceBinaryType {
    /// Raw OTLP bytes with no datatype extension.
    Binary,
    /// An OTLP array or key-value list encoded with the JSONB extension.
    Json,
}

impl TraceBinaryType {
    /// Applies this logical binary type to a column schema.
    pub fn apply_to_schema(self, schema: &mut ColumnSchema) {
        schema.datatype = ColumnDataType::Binary as i32;
        schema.datatype_extension = match self {
            Self::Binary => None,
            Self::Json => Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
        };
    }
}

/// Per-column observations collected while building one trace chunk.
#[derive(Default)]
struct TraceBatchColumnSchema {
    value_types: Vec<ColumnDataType>,
    present_rows: Vec<usize>,
    binary_types: Vec<(usize, TraceBinaryType)>,
}

impl TraceBatchColumnSchema {
    /// Records a row once while preserving encounter order.
    fn observe_row(&mut self, row_index: usize) {
        if self.present_rows.last() != Some(&row_index) {
            self.present_rows.push(row_index);
        }
    }

    /// Returns whether raw binary and JSONB values share this column.
    fn has_incompatible_logical_types(&self) -> bool {
        let Some((_, first_type)) = self.binary_types.first() else {
            return false;
        };
        self.binary_types
            .iter()
            .any(|(_, binary_type)| binary_type != first_type)
    }
}

/// Sparse column metadata retained for span-level fallback.
pub struct TraceRetryColumn {
    /// Row indexes that contain this dynamic column.
    pub present_rows: Vec<usize>,
    /// Row-specific binary kinds needed to restore the logical schema.
    pub binary_types: Vec<(usize, TraceBinaryType)>,
}

/// Retry metadata keyed by dynamic trace column name.
pub type TraceRetryColumns = HashMap<String, TraceRetryColumn>;

/// Schema observations for dynamic columns in one converted trace chunk.
#[derive(Default)]
pub struct TraceBatchSchema {
    columns: HashMap<String, TraceBatchColumnSchema>,
}

impl TraceBatchSchema {
    /// Records a scalar value type and the row where the column is present.
    fn observe_value_type(&mut self, name: &str, row_index: usize, value_type: ColumnDataType) {
        let column = self.columns.entry(name.to_string()).or_default();
        column.observe_row(row_index);
        if !column.value_types.contains(&value_type) {
            column.value_types.push(value_type);
        }
    }

    /// Records the logical kind of a binary protobuf value for one row.
    fn observe_binary_type(&mut self, name: &str, row_index: usize, binary_type: TraceBinaryType) {
        let column = self.columns.entry(name.to_string()).or_default();
        column.observe_row(row_index);
        if !column.value_types.contains(&ColumnDataType::Binary) {
            column.value_types.push(ColumnDataType::Binary);
        }
        if let Some((last_row_index, last_type)) = column.binary_types.last_mut()
            && *last_row_index == row_index
        {
            *last_type = binary_type;
        } else {
            column.binary_types.push((row_index, binary_type));
        }
    }

    /// Records a sparse column occurrence that has no dynamic value type.
    fn observe_present_column(&mut self, name: &str, row_index: usize) {
        self.columns
            .entry(name.to_string())
            .or_default()
            .observe_row(row_index);
    }

    /// Returns the distinct value types in first-seen order for a column.
    pub fn value_types(&self, name: &str) -> Option<&[ColumnDataType]> {
        self.columns
            .get(name)
            .map(|column| column.value_types.as_slice())
    }

    /// Returns whether any column mixes raw binary and JSONB values.
    pub fn has_incompatible_logical_types(&self) -> bool {
        self.columns
            .values()
            .any(TraceBatchColumnSchema::has_incompatible_logical_types)
    }

    /// Returns whether the named column mixes raw binary and JSONB values.
    pub fn has_incompatible_logical_types_for(&self, name: &str) -> bool {
        self.columns
            .get(name)
            .is_some_and(TraceBatchColumnSchema::has_incompatible_logical_types)
    }

    /// Converts batch observations into sparse metadata for per-span retries.
    pub fn into_retry_columns(self) -> TraceRetryColumns {
        self.columns
            .into_iter()
            .filter(|(_, column)| !column.present_rows.is_empty())
            .map(|(name, column)| {
                let binary_types = if column.has_incompatible_logical_types()
                    || column
                        .value_types
                        .iter()
                        .any(|datatype| *datatype != ColumnDataType::Binary)
                {
                    column.binary_types
                } else {
                    Vec::new()
                };
                (
                    name,
                    TraceRetryColumn {
                        present_rows: column.present_rows,
                        binary_types,
                    },
                )
            })
            .collect()
    }
}

/// Converts trace spans into row insert requests for the main v1 trace table.
///
/// Auxiliary service and operation table writes are built separately so the
/// caller can update them only after the main span write succeeds.
pub fn v1_to_grpc_main_insert_requests(
    spans: &[TraceSpan],
    _pipeline: &PipelineWay,
    _pipeline_params: &GreptimePipelineParams,
    table_name: &str,
    _query_ctx: &QueryContextRef,
    _pipeline_handler: PipelineHandlerRef,
) -> Result<(RowInsertRequests, usize)> {
    let requests = v1_to_grpc_main_insert_requests_from_iter(spans.iter().cloned(), table_name)?;
    Ok((requests, spans.len()))
}

/// Converts owned spans into unpadded main-table rows and schema observations.
pub fn v1_to_main_table_data_with_schema(
    spans: Vec<TraceSpan>,
) -> Result<(TableData, TraceBatchSchema)> {
    build_trace_table_data_with_schema(spans.into_iter())
}

/// Builds the main-table request without collecting batch schema observations.
fn v1_to_grpc_main_insert_requests_from_iter(
    spans: impl ExactSizeIterator<Item = TraceSpan>,
    table_name: &str,
) -> Result<RowInsertRequests> {
    let mut multi_table_writer = MultiTableData::default();
    let trace_writer = build_trace_table_data_from_iter(spans, None)?;
    multi_table_writer.add_table_data(table_name, trace_writer);

    Ok(multi_table_writer.into_row_insert_requests().0)
}

/// Builds the row-oriented payload for the main v1 trace table.
pub fn build_trace_table_data(spans: &[TraceSpan]) -> Result<TableData> {
    build_trace_table_data_from_iter(spans.iter().cloned(), None)
}

/// Builds trace rows while collecting dynamic column observations.
fn build_trace_table_data_with_schema(
    spans: impl ExactSizeIterator<Item = TraceSpan>,
) -> Result<(TableData, TraceBatchSchema)> {
    let mut batch_schema = TraceBatchSchema::default();
    let trace_writer = build_trace_table_data_from_iter(spans, Some(&mut batch_schema))?;
    Ok((trace_writer, batch_schema))
}

/// Shared row builder with optional batch schema observation.
fn build_trace_table_data_from_iter(
    spans: impl ExactSizeIterator<Item = TraceSpan>,
    mut batch_schema: Option<&mut TraceBatchSchema>,
) -> Result<TableData> {
    let mut trace_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, spans.len());
    for span in spans {
        let row_index = trace_writer.num_rows();
        write_span_to_row_inner(
            &mut trace_writer,
            span,
            row_index,
            batch_schema.as_deref_mut(),
        )?;
    }
    Ok(trace_writer)
}

/// Builds row insert requests for the v1 trace auxiliary tables.
pub fn build_aux_table_requests(
    aux_data: TraceAuxData,
    table_name: &str,
) -> Result<(RowInsertRequests, usize)> {
    let mut multi_table_writer = MultiTableData::default();
    let mut trace_services_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, 1);
    let mut trace_operations_writer = TableData::new(APPROXIMATE_COLUMN_COUNT, 1);

    write_trace_services_to_row(&mut trace_services_writer, aux_data.services)?;
    write_trace_operations_to_row(&mut trace_operations_writer, aux_data.operations)?;

    multi_table_writer.add_table_data(trace_services_table_name(table_name), trace_services_writer);
    multi_table_writer.add_table_data(
        trace_operations_table_name(table_name),
        trace_operations_writer,
    );

    Ok(multi_table_writer.into_row_insert_requests())
}

pub fn write_span_to_row(writer: &mut TableData, span: TraceSpan) -> Result<()> {
    let row_index = writer.num_rows();
    write_span_to_row_inner(writer, span, row_index, None)
}

/// Writes one span and optionally records its dynamic columns for reconciliation.
fn write_span_to_row_inner(
    writer: &mut TableData,
    span: TraceSpan,
    row_index: usize,
    mut batch_schema: Option<&mut TraceBatchSchema>,
) -> Result<()> {
    let mut row = writer.alloc_one_row();

    // write ts
    row_writer::write_ts_to_nanos(
        writer,
        TIMESTAMP_COLUMN,
        Some(span.start_in_nanosecond as i64),
        Precision::Nanosecond,
        &mut row,
    )?;

    // write fields
    let fields = vec![
        make_column_data(
            "timestamp_end",
            ColumnDataType::TimestampNanosecond,
            Some(ValueData::TimestampNanosecondValue(
                span.end_in_nanosecond as i64,
            )),
        ),
        make_column_data(
            DURATION_NANO_COLUMN,
            ColumnDataType::Uint64,
            Some(ValueData::U64Value(
                span.end_in_nanosecond - span.start_in_nanosecond,
            )),
        ),
        make_string_column_data(PARENT_SPAN_ID_COLUMN, span.parent_span_id),
        make_string_column_data(TRACE_ID_COLUMN, Some(span.trace_id)),
        make_string_column_data(SPAN_ID_COLUMN, Some(span.span_id)),
        make_string_column_data(SPAN_KIND_COLUMN, Some(span.span_kind)),
        make_string_column_data(SPAN_NAME_COLUMN, Some(span.span_name)),
        make_string_column_data(SPAN_STATUS_CODE, Some(span.span_status_code)),
        make_string_column_data(SPAN_STATUS_MESSAGE_COLUMN, Some(span.span_status_message)),
        make_string_column_data(TRACE_STATE_COLUMN, Some(span.trace_state)),
        make_string_column_data(SCOPE_NAME_COLUMN, Some(span.scope_name)),
        make_string_column_data(SCOPE_VERSION_COLUMN, Some(span.scope_version)),
    ];
    row_writer::write_fields(writer, fields.into_iter(), &mut row)?;

    if let Some(service_name) = span.service_name {
        if let Some(batch_schema) = batch_schema.as_deref_mut() {
            batch_schema.observe_present_column(SERVICE_NAME_COLUMN, row_index);
        }
        row_writer::write_tags(
            writer,
            std::iter::once((SERVICE_NAME_COLUMN.to_string(), service_name)),
            &mut row,
        )?;
    }

    write_attributes_with_schema(
        writer,
        "span_attributes",
        span.span_attributes,
        &mut row,
        row_index,
        batch_schema.as_deref_mut(),
    )?;
    write_attributes_with_schema(
        writer,
        "scope_attributes",
        span.scope_attributes,
        &mut row,
        row_index,
        batch_schema.as_deref_mut(),
    )?;
    write_attributes_with_schema(
        writer,
        "resource_attributes",
        span.resource_attributes,
        &mut row,
        row_index,
        batch_schema,
    )?;

    row_writer::write_json(
        writer,
        SPAN_EVENTS_COLUMN,
        span.span_events.into(),
        &mut row,
    )?;
    row_writer::write_json(writer, "span_links", span.span_links.into(), &mut row)?;

    writer.add_row(row);

    Ok(())
}

fn write_trace_services_to_row(writer: &mut TableData, services: HashSet<String>) -> Result<()> {
    for service_name in services {
        let mut row = writer.alloc_one_row();
        // Write the timestamp as 0.
        row_writer::write_ts_to_nanos(
            writer,
            TIMESTAMP_COLUMN,
            Some(MAX_TIMESTAMP),
            Precision::Nanosecond,
            &mut row,
        )?;

        // Write the `service_name` column.
        row_writer::write_tags(
            writer,
            std::iter::once((SERVICE_NAME_COLUMN.to_string(), service_name)),
            &mut row,
        )?;
        writer.add_row(row);
    }

    Ok(())
}

fn write_trace_operations_to_row(
    writer: &mut TableData,
    operations: HashSet<(String, String, String)>,
) -> Result<()> {
    for (service_name, span_name, span_kind) in operations {
        let mut row = writer.alloc_one_row();
        // Write the timestamp as 0.
        row_writer::write_ts_to_nanos(
            writer,
            TIMESTAMP_COLUMN,
            Some(MAX_TIMESTAMP),
            Precision::Nanosecond,
            &mut row,
        )?;

        // Write the `service_name`, `span_name`, and `span_kind` columns as tags.
        row_writer::write_tags(
            writer,
            vec![
                (SERVICE_NAME_COLUMN.to_string(), service_name),
                (SPAN_NAME_COLUMN.to_string(), span_name),
                (SPAN_KIND_COLUMN.to_string(), span_kind),
            ]
            .into_iter(),
            &mut row,
        )?;
        writer.add_row(row);
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn write_attributes(
    writer: &mut TableData,
    prefix: &str,
    attributes: Attributes,
    row: &mut Vec<Value>,
) -> Result<()> {
    let row_index = writer.num_rows();
    write_attributes_with_schema(writer, prefix, attributes, row, row_index, None)
}

/// Writes flattened attributes without coercion and optionally records their actual types.
fn write_attributes_with_schema(
    writer: &mut TableData,
    prefix: &str,
    attributes: Attributes,
    row: &mut Vec<Value>,
    row_index: usize,
    mut batch_schema: Option<&mut TraceBatchSchema>,
) -> Result<()> {
    for attr in attributes.take().into_iter() {
        let key_suffix = attr.key;
        // skip resource_attributes.service.name because its already copied to
        // top level as `SERVICE_NAME_COLUMN`
        if prefix == "resource_attributes" && key_suffix == KEY_SERVICE_NAME {
            continue;
        }

        let key = format!("{}.{}", prefix, key_suffix);
        match attr.value.and_then(|v| v.value) {
            Some(OtlpValue::StringValue(v)) => {
                if let Some(batch_schema) = batch_schema.as_deref_mut() {
                    batch_schema.observe_value_type(&key, row_index, ColumnDataType::String);
                }
                // Keep the raw request value here. Mixed trace types are reconciled later
                // in the frontend once we can also see the existing table schema.
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::String,
                    Some(ValueData::StringValue(v)),
                    row,
                );
            }
            Some(OtlpValue::BoolValue(v)) => {
                if let Some(batch_schema) = batch_schema.as_deref_mut() {
                    batch_schema.observe_value_type(&key, row_index, ColumnDataType::Boolean);
                }
                // Do not coerce or promote types while building the request-local rows.
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::Boolean,
                    Some(ValueData::BoolValue(v)),
                    row,
                );
            }
            Some(OtlpValue::IntValue(v)) => {
                if let Some(batch_schema) = batch_schema.as_deref_mut() {
                    batch_schema.observe_value_type(&key, row_index, ColumnDataType::Int64);
                }
                // Preserving the original value avoids order-dependent behavior inside one batch.
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::Int64,
                    Some(ValueData::I64Value(v)),
                    row,
                );
            }
            Some(OtlpValue::DoubleValue(v)) => {
                if let Some(batch_schema) = batch_schema.as_deref_mut() {
                    batch_schema.observe_value_type(&key, row_index, ColumnDataType::Float64);
                }
                writer.write_field_unchecked(
                    &key,
                    ColumnDataType::Float64,
                    Some(ValueData::F64Value(v)),
                    row,
                );
            }
            Some(OtlpValue::ArrayValue(v)) => {
                if let Some(batch_schema) = batch_schema.as_deref_mut() {
                    batch_schema.observe_binary_type(&key, row_index, TraceBinaryType::Json);
                }
                writer.write_column_unchecked(
                    row_writer::build_json_column_schema(key),
                    Some(ValueData::BinaryValue(
                        any_value_to_jsonb(OtlpValue::ArrayValue(v)).to_vec(),
                    )),
                    row,
                );
            }
            Some(OtlpValue::KvlistValue(v)) => {
                if let Some(batch_schema) = batch_schema.as_deref_mut() {
                    batch_schema.observe_binary_type(&key, row_index, TraceBinaryType::Json);
                }
                writer.write_column_unchecked(
                    row_writer::build_json_column_schema(key),
                    Some(ValueData::BinaryValue(
                        any_value_to_jsonb(OtlpValue::KvlistValue(v)).to_vec(),
                    )),
                    row,
                );
            }
            Some(OtlpValue::BytesValue(v)) => {
                if let Some(batch_schema) = batch_schema.as_deref_mut() {
                    batch_schema.observe_binary_type(&key, row_index, TraceBinaryType::Binary);
                }
                writer.write_field_unchecked(
                    key,
                    ColumnDataType::Binary,
                    Some(ValueData::BinaryValue(v)),
                    row,
                );
            }
            None => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue};

    use super::*;
    use crate::otlp::trace::TraceAuxData;
    use crate::otlp::trace::attributes::Attributes;
    use crate::otlp::trace::span::{SpanEvents, SpanLinks};
    use crate::row_writer::TableData;

    fn make_kv(key: &str, value: OtlpValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    fn make_span(service_name: &str, trace_id: &str, span_id: &str) -> TraceSpan {
        TraceSpan {
            service_name: Some(service_name.to_string()),
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            resource_attributes: Attributes::from(vec![]),
            scope_name: "scope".to_string(),
            scope_version: "v1".to_string(),
            scope_attributes: Attributes::from(vec![]),
            trace_state: String::new(),
            span_name: "op".to_string(),
            span_kind: "SPAN_KIND_SERVER".to_string(),
            span_status_code: "STATUS_CODE_UNSET".to_string(),
            span_status_message: String::new(),
            span_attributes: Attributes::from(vec![]),
            span_events: SpanEvents::from(vec![]),
            span_links: SpanLinks::from(vec![]),
            start_in_nanosecond: 1,
            end_in_nanosecond: 2,
        }
    }

    #[test]
    fn test_batch_schema_preserves_column_order_and_attribute_types() {
        let mut span1 = make_span("svc-a", "trace-a", "span-a");
        span1.span_attributes = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(10))]);
        let mut span2 = make_span("svc-a", "trace-a", "span-b");
        span2.span_attributes = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(1.5))]);

        let (table_data, batch_schema) =
            build_trace_table_data_with_schema(vec![span1, span2].into_iter()).unwrap();

        assert_eq!(
            batch_schema.value_types("span_attributes.val").unwrap(),
            [ColumnDataType::Int64, ColumnDataType::Float64]
        );
        let timestamp_index = table_data
            .columns()
            .iter()
            .position(|column| column.column_name == TIMESTAMP_COLUMN)
            .unwrap();
        let attribute_index = table_data
            .columns()
            .iter()
            .position(|column| column.column_name == "span_attributes.val")
            .unwrap();
        assert!(timestamp_index < attribute_index);
    }

    #[test]
    fn test_optional_batch_schema_observation_preserves_rows() {
        let mut span = make_span("svc-a", "trace-a", "span-a");
        span.span_attributes = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(10))]);

        let without_schema = build_trace_table_data(std::slice::from_ref(&span)).unwrap();
        let (with_schema, batch_schema) =
            build_trace_table_data_with_schema(vec![span].into_iter()).unwrap();

        assert_eq!(
            without_schema.into_schema_and_rows(),
            with_schema.into_schema_and_rows()
        );
        assert_eq!(
            batch_schema.value_types("span_attributes.val").unwrap(),
            [ColumnDataType::Int64]
        );
        let retry_columns = batch_schema.into_retry_columns();
        let retry_column = &retry_columns["span_attributes.val"];
        assert_eq!(retry_column.present_rows, [0]);
        assert!(retry_column.binary_types.is_empty());
    }

    #[test]
    fn test_batch_schema_tracks_binary_and_json_rows() {
        let mut binary_span = make_span("svc-a", "trace-a", "span-a");
        binary_span.span_attributes = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::BytesValue(vec![1_u8, 2, 3]),
        )]);
        let mut json_span = make_span("svc-a", "trace-a", "span-b");
        json_span.span_attributes = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(OtlpValue::IntValue(1)),
                }],
            }),
        )]);

        let (_, batch_schema) =
            build_trace_table_data_with_schema(vec![binary_span, json_span].into_iter()).unwrap();

        assert!(batch_schema.has_incompatible_logical_types());
        assert!(batch_schema.has_incompatible_logical_types_for("span_attributes.val"));
        let retry_columns = batch_schema.into_retry_columns();
        let retry_column = &retry_columns["span_attributes.val"];
        assert_eq!(retry_column.present_rows, [0, 1]);
        assert_eq!(
            retry_column.binary_types,
            [(0, TraceBinaryType::Binary), (1, TraceBinaryType::Json)]
        );
    }

    #[test]
    fn test_batch_schema_preserves_scalar_then_binary_values() {
        let mut scalar_span = make_span("svc-a", "trace-a", "span-a");
        scalar_span.span_attributes = Attributes::from(vec![
            make_kv("bytes", OtlpValue::StringValue("text".to_string())),
            make_kv("json", OtlpValue::StringValue("text".to_string())),
        ]);
        let mut binary_span = make_span("svc-a", "trace-a", "span-b");
        binary_span.span_attributes = Attributes::from(vec![
            make_kv("bytes", OtlpValue::BytesValue(vec![1_u8, 2, 3])),
            make_kv(
                "json",
                OtlpValue::ArrayValue(ArrayValue {
                    values: vec![AnyValue {
                        value: Some(OtlpValue::IntValue(1)),
                    }],
                }),
            ),
        ]);

        let (_, batch_schema) =
            build_trace_table_data_with_schema(vec![scalar_span, binary_span].into_iter()).unwrap();

        assert_eq!(
            batch_schema.value_types("span_attributes.bytes").unwrap(),
            [ColumnDataType::String, ColumnDataType::Binary]
        );
        assert_eq!(
            batch_schema.value_types("span_attributes.json").unwrap(),
            [ColumnDataType::String, ColumnDataType::Binary]
        );
        let retry_columns = batch_schema.into_retry_columns();
        assert_eq!(
            retry_columns["span_attributes.bytes"].binary_types,
            [(1, TraceBinaryType::Binary)]
        );
        assert_eq!(
            retry_columns["span_attributes.json"].binary_types,
            [(1, TraceBinaryType::Json)]
        );
    }

    #[test]
    fn test_keep_mixed_numeric_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(1.5))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(42))]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();

        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Float64 as i32);

        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::F64Value(1.5))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::I64Value(42))
        );
    }

    #[test]
    fn test_keep_mixed_string_and_int_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(10))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("20".to_string()),
        )]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Int64 as i32);
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::StringValue("20".to_string()))
        );
    }

    #[test]
    fn test_keep_first_seen_schema_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("10".to_string()),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::IntValue(20))]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::String as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::StringValue("10".to_string()))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::I64Value(20))
        );
    }

    #[test]
    fn test_keep_mixed_string_and_float_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv("val", OtlpValue::DoubleValue(1.5))]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("1.5".to_string()),
        )]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Float64 as i32);
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::StringValue("1.5".to_string()))
        );
    }

    #[test]
    fn test_keep_mixed_string_and_bool_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("true".to_string()),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv("val", OtlpValue::BoolValue(false))]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::String as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::StringValue("true".to_string()))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::BoolValue(false))
        );
    }

    #[test]
    fn test_keep_mixed_binary_and_string_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::BytesValue(vec![1_u8, 2, 3]),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::StringValue("false".to_string()),
        )]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Binary as i32);
        assert_eq!(
            rows[0].values[col_idx].value_data,
            Some(ValueData::BinaryValue(vec![1_u8, 2, 3]))
        );
        assert_eq!(
            rows[1].values[col_idx].value_data,
            Some(ValueData::StringValue("false".to_string()))
        );
    }

    #[test]
    fn test_keep_mixed_binary_and_json_values_until_frontend_reconciliation() {
        let mut writer = TableData::new(4, 2);

        let attrs1 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::BytesValue(vec![1_u8, 2, 3]),
        )]);
        let mut row1 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs1, &mut row1).unwrap();
        writer.add_row(row1);

        let attrs2 = Attributes::from(vec![make_kv(
            "val",
            OtlpValue::ArrayValue(ArrayValue {
                values: vec![AnyValue {
                    value: Some(OtlpValue::IntValue(1)),
                }],
            }),
        )]);
        let mut row2 = writer.alloc_one_row();
        write_attributes(&mut writer, "attr", attrs2, &mut row2).unwrap();
        writer.add_row(row2);

        let (schema, rows) = writer.into_schema_and_rows();
        let col_idx = schema
            .iter()
            .position(|c| c.column_name == "attr.val")
            .unwrap();
        assert_eq!(schema[col_idx].datatype, ColumnDataType::Binary as i32);
        assert!(matches!(
            rows[1].values[col_idx].value_data.as_ref(),
            Some(ValueData::BinaryValue(_))
        ));
    }

    #[test]
    fn test_build_aux_table_requests_deduplicates_services_and_operations() {
        let spans = vec![
            make_span("svc-a", "trace-a", "span-a"),
            make_span("svc-a", "trace-b", "span-b"),
        ];
        let mut aux_data = TraceAuxData::default();
        for span in &spans {
            aux_data.observe_span(span);
        }

        let (requests, total_rows) =
            build_aux_table_requests(aux_data, "opentelemetry_traces").unwrap();
        assert_eq!(requests.inserts.len(), 2);
        assert_eq!(total_rows, 2);
    }
    // Conversion matrix coverage lives in the shared coercion helper tests.
}
