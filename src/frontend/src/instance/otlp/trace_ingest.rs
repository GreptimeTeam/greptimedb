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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::alter_table_expr::Kind;
use api::v1::{
    AlterTableExpr, ColumnDataType, ColumnSchema, ModifyColumnType, ModifyColumnTypes,
    RowInsertRequest, RowInsertRequests, Rows, Value,
};
use client::Output;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_telemetry::warn;
use datatypes::prelude::ConcreteDataType;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use servers::error::{self, Result as ServerResult};
use servers::otlp;
use servers::otlp::coerce::trace_value_datatype;
use servers::otlp::trace::TraceAuxData;
use servers::otlp::trace::span::{TraceSpan, TraceSpanGroup};
use servers::otlp::trace::v1::{TraceBatchSchema, TraceBinaryType, TraceRetryColumns};
use servers::query_handler::{PipelineHandlerRef, TraceIngestOutcome};
use session::context::QueryContextRef;
use snafu::{IntoError, ResultExt};
use table::requests::{
    SEMANTIC_PIPELINE, SEMANTIC_SIGNAL_TYPE, SEMANTIC_SOURCE, SEMANTIC_TRACE_CONVENTIONS,
    SEMANTIC_VALUE_MIXED, SEMANTIC_VALUE_UNKNOWN, SIGNAL_TYPE_TRACE, SOURCE_OPENTELEMETRY,
    TABLE_DATA_MODEL_TRACE_V1,
};

use crate::instance::Instance;
use crate::instance::otlp::trace_semconv::trace_semconv_fixed_type;
use crate::instance::otlp::trace_types::{
    PendingTraceColumnRewrite, PreparedTraceColumnRewrites, TraceColumnRewriteError,
    choose_trace_reconcile_decision, enrich_trace_reconcile_error,
    is_trace_reconcile_candidate_type, prepare_trace_column_rewrites, push_observed_trace_type,
};
use crate::metrics::{OTLP_TRACES_FAILURE_COUNT, OTLP_TRACES_ROWS};

const TRACE_FAILURE_MESSAGE_LIMIT: usize = 4;

/// Determines how trace ingestion responds to a failed chunk write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkFailureReaction {
    RetryPerSpan,
    DiscardChunk,
    Propagate,
}

impl ChunkFailureReaction {
    fn as_metric_label(self) -> &'static str {
        match self {
            Self::RetryPerSpan => "retry_per_span",
            Self::DiscardChunk => "discard_chunk",
            Self::Propagate => "propagate_failure",
        }
    }
}

/// Shared dependencies and request metadata used while ingesting trace chunks.
struct TraceChunkIngestContext<'a> {
    pipeline_handler: PipelineHandlerRef,
    pipeline: &'a PipelineWay,
    pipeline_params: &'a GreptimePipelineParams,
    table_name: &'a str,
    is_trace_v1_model: bool,
}

/// Accumulates trace outcomes, auxiliary rows, and bounded failure details.
struct TraceIngestState {
    aux_data: TraceAuxData,
    outcome: TraceIngestOutcome,
    failure_messages: Vec<String>,
}

/// How a v1 chunk should be reconciled when it is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceChunkSchemaState {
    Prepared,
    ReconcilePerChunk,
    SpanFallbackOnly,
}

impl TraceChunkSchemaState {
    fn mark_for_reconcile(&mut self) {
        if matches!(self, Self::Prepared) {
            *self = Self::ReconcilePerChunk;
        }
    }
}

/// Sparse representation retained for a v1 trace chunk.
struct TraceChunkRetry {
    table_name: String,
    schema: Vec<ColumnSchema>,
    rows: Vec<TraceRetryRow>,
}

/// The projected retry values and identifying metadata for one span.
struct TraceRetryRow {
    span_metadata: TraceSpanMetadata,
    values: Vec<TraceRetryValue>,
}

/// One retry cell and the logical type to restore before insertion.
struct TraceRetryValue {
    column_index: usize,
    value: Value,
    schema_type: Option<TraceRetrySchemaType>,
}

/// The scalar or binary logical type recorded for a retry value.
enum TraceRetrySchemaType {
    Scalar(ColumnDataType),
    Binary(TraceBinaryType),
}

/// Span identity and operation fields retained after request conversion.
struct TraceSpanMetadata {
    trace_id: String,
    span_id: String,
    operation: Option<(String, String, String)>,
}

impl From<&TraceSpan> for TraceSpanMetadata {
    fn from(span: &TraceSpan) -> Self {
        Self {
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            operation: span.service_name.as_ref().map(|service_name| {
                (
                    service_name.clone(),
                    span.span_name.clone(),
                    span.span_kind.clone(),
                )
            }),
        }
    }
}

impl TraceSpanMetadata {
    fn add_to_aux_data(self, aux_data: &mut TraceAuxData) {
        if let Some((service_name, span_name, span_kind)) = self.operation {
            aux_data.services.insert(service_name.clone());
            aux_data
                .operations
                .insert((service_name, span_name, span_kind));
        }
    }
}

impl TraceChunkRetry {
    fn try_new(
        table_name: String,
        rows: Rows,
        span_metadata: Vec<TraceSpanMetadata>,
        retry_columns: TraceRetryColumns,
    ) -> ServerResult<Self> {
        let Rows { schema, rows } = rows;
        let row_count = rows.len();
        if row_count != span_metadata.len() {
            return error::InternalSnafu {
                err_msg: format!(
                    "trace row count {} does not match span metadata count {}",
                    row_count,
                    span_metadata.len()
                ),
            }
            .fail();
        }

        let retry_columns_by_index = schema
            .iter()
            .map(|column| retry_columns.get(&column.column_name))
            .collect::<Vec<_>>();
        let mut fixed_columns = Vec::with_capacity(schema.len());
        let mut sparse_columns = vec![Vec::new(); rows.len()];
        for (column_index, retry_column) in retry_columns_by_index.iter().enumerate() {
            let Some(retry_column) = retry_column else {
                fixed_columns.push(column_index);
                continue;
            };
            for present_row in &retry_column.present_rows {
                if *present_row < sparse_columns.len() {
                    sparse_columns[*present_row].push(column_index);
                }
            }
        }

        let mut retry_rows = Vec::with_capacity(rows.len());
        for (row_index, (mut row, span_metadata)) in rows.into_iter().zip(span_metadata).enumerate()
        {
            if row.values.len() > schema.len() {
                return error::InternalSnafu {
                    err_msg: format!(
                        "trace row column count {} exceeds schema column count {}",
                        row.values.len(),
                        schema.len()
                    ),
                }
                .fail();
            }
            let sparse_row = &mut sparse_columns[row_index];
            let mut selected_columns = Vec::with_capacity(fixed_columns.len() + sparse_row.len());
            selected_columns.extend_from_slice(&fixed_columns);
            selected_columns.append(sparse_row);
            selected_columns.sort_unstable();
            selected_columns.dedup();

            let mut values = Vec::with_capacity(selected_columns.len());
            for column_index in selected_columns {
                let column = &schema[column_index];
                let value = row
                    .values
                    .get_mut(column_index)
                    .map(std::mem::take)
                    .unwrap_or_default();
                let value_type = value.value_data.as_ref().and_then(trace_value_datatype);
                let binary_type = if value_type == Some(ColumnDataType::Binary) {
                    retry_columns_by_index[column_index].and_then(|retry_column| {
                        retry_column
                            .binary_types
                            .binary_search_by_key(&row_index, |(row_index, _)| *row_index)
                            .ok()
                            .map(|index| retry_column.binary_types[index].1)
                    })
                } else {
                    None
                };
                let schema_type = binary_type.map(TraceRetrySchemaType::Binary).or_else(|| {
                    value_type
                        .filter(|datatype| {
                            is_trace_reconcile_candidate_type(*datatype)
                                && ColumnDataType::try_from(column.datatype).ok() != Some(*datatype)
                        })
                        .map(TraceRetrySchemaType::Scalar)
                });
                values.push(TraceRetryValue {
                    column_index,
                    value,
                    schema_type,
                });
            }
            retry_rows.push(TraceRetryRow {
                span_metadata,
                values,
            });
        }

        Ok(Self {
            table_name,
            schema,
            rows: retry_rows,
        })
    }

    fn to_request(&self) -> ServerResult<RowInsertRequest> {
        let mut rows = Vec::with_capacity(self.rows.len());
        for retry_row in &self.rows {
            let mut values = vec![Value::default(); self.schema.len()];
            for retry_value in &retry_row.values {
                let Some(value) = values.get_mut(retry_value.column_index) else {
                    return error::InternalSnafu {
                        err_msg: format!(
                            "trace retry column index {} is out of bounds",
                            retry_value.column_index
                        ),
                    }
                    .fail();
                };
                *value = retry_value.value.clone();
            }
            rows.push(api::v1::Row { values });
        }

        Ok(RowInsertRequest {
            table_name: self.table_name.clone(),
            rows: Some(Rows {
                schema: self.schema.clone(),
                rows,
            }),
        })
    }

    fn add_to_aux_data(self, aux_data: &mut TraceAuxData) {
        for row in self.rows {
            row.span_metadata.add_to_aux_data(aux_data);
        }
    }
}

impl TraceRetryRow {
    fn into_rows(self, schema: &[ColumnSchema]) -> ServerResult<(TraceSpanMetadata, Rows)> {
        let mut projected_schema = Vec::with_capacity(self.values.len());
        let mut projected_values = Vec::with_capacity(self.values.len());
        for retry_value in self.values {
            let Some(mut column) = schema.get(retry_value.column_index).cloned() else {
                return error::InternalSnafu {
                    err_msg: format!(
                        "trace fallback column index {} is out of bounds",
                        retry_value.column_index
                    ),
                }
                .fail();
            };
            match retry_value.schema_type {
                Some(TraceRetrySchemaType::Scalar(datatype)) => {
                    column.datatype = datatype as i32;
                    column.datatype_extension = None;
                }
                Some(TraceRetrySchemaType::Binary(binary_type)) => {
                    binary_type.apply_to_schema(&mut column);
                }
                None => {}
            }
            projected_schema.push(column);
            projected_values.push(retry_value.value);
        }

        Ok((
            self.span_metadata,
            Rows {
                schema: projected_schema,
                rows: vec![api::v1::Row {
                    values: projected_values,
                }],
            },
        ))
    }
}

/// Request-wide schema observations collected across all v1 trace chunks.
#[derive(Default)]
struct TraceRequestSchema {
    columns: Vec<TraceColumnRequestSchema>,
    column_indexes: HashMap<String, usize>,
}

/// Per-column observations and the resolved target type for a trace request.
struct TraceColumnRequestSchema {
    schema: ColumnSchema,
    batches: BTreeMap<usize, TraceBatchColumnObservation>,
    target_type: Option<ColumnDataType>,
}

/// The schema and value types observed for one column in one chunk.
struct TraceBatchColumnObservation {
    value_types: Vec<ColumnDataType>,
    schema_type: ColumnDataType,
    concrete_type: ConcreteDataType,
}

/// Maps incompatible columns to the chunk indexes that must use fallback.
type TraceSchemaExclusions = HashMap<String, HashSet<usize>>;

/// Table columns to add or widen before inserting the prepared chunks.
struct TraceTablePreAlter {
    ensure_columns: Vec<ColumnSchema>,
    modify_float64_columns: Vec<String>,
}

/// Either a ready table-alter plan or the observations that cannot be unified.
enum TraceRequestSchemaPlan {
    Ready(TraceTablePreAlter),
    IncompatibleObservations(TraceSchemaExclusions),
}

impl TraceTablePreAlter {
    fn requires_ddl(&self) -> bool {
        !self.ensure_columns.is_empty() || !self.modify_float64_columns.is_empty()
    }
}

impl TraceRequestSchema {
    fn observe_batch_schema(
        &mut self,
        batch_index: usize,
        schema: &[ColumnSchema],
        batch_schema: &TraceBatchSchema,
    ) {
        for column in schema {
            if batch_schema.has_incompatible_logical_types_for(&column.column_name) {
                continue;
            }
            let observed_types = batch_schema.value_types(&column.column_name);
            self.observe_trace_column(batch_index, column, None);
            if let Some(observed_types) = observed_types {
                for value_type in observed_types {
                    self.observe_trace_column(batch_index, column, Some(*value_type));
                }
            }
        }
    }

    fn observe_trace_column(
        &mut self,
        batch_index: usize,
        schema: &ColumnSchema,
        value_type: Option<ColumnDataType>,
    ) {
        let column_schema = self.observe_column(schema);
        if let Ok(current_type) = ColumnDataType::try_from(schema.datatype) {
            let observation = column_schema.batches.entry(batch_index).or_insert_with(|| {
                let wrapper =
                    ColumnDataTypeWrapper::new(current_type, schema.datatype_extension.clone());
                TraceBatchColumnObservation {
                    value_types: Vec::new(),
                    schema_type: current_type,
                    concrete_type: ConcreteDataType::from(wrapper),
                }
            });
            push_observed_trace_type(&mut observation.value_types, current_type);
        }
        if let Some(value_type) = value_type {
            column_schema.observe_type(batch_index, value_type);
        }
    }

    fn incompatible_schema_observations(
        &self,
        table_schema: Option<&datatypes::schema::Schema>,
    ) -> TraceSchemaExclusions {
        let mut exclusions = HashMap::new();
        for column in &self.columns {
            let existing_type = table_schema
                .and_then(|schema| schema.column_schema_by_name(&column.schema.column_name))
                .and_then(|table_col| {
                    ColumnDataTypeWrapper::try_from(table_col.data_type.clone())
                        .ok()
                        .map(|wrapper| {
                            let datatype = wrapper.datatype();
                            (datatype, ConcreteDataType::from(wrapper))
                        })
                });
            let incompatible_batches = column.incompatible_schema_batches(existing_type);
            if !incompatible_batches.is_empty() {
                exclusions.insert(column.schema.column_name.clone(), incompatible_batches);
            }
        }
        exclusions
    }

    fn resolve_table_schema(
        &mut self,
        table_schema: Option<&datatypes::schema::Schema>,
    ) -> TraceRequestSchemaPlan {
        let mut pre_alter = TraceTablePreAlter {
            ensure_columns: Vec::new(),
            modify_float64_columns: Vec::new(),
        };

        for column in &mut self.columns {
            let Some(current_type) = ColumnDataType::try_from(column.schema.datatype).ok() else {
                continue;
            };
            let observed_types = column.observed_types();

            let existing_type = table_schema
                .and_then(|schema| schema.column_schema_by_name(&column.schema.column_name))
                .and_then(|table_col| {
                    ColumnDataTypeWrapper::try_from(table_col.data_type.clone())
                        .ok()
                        .map(|wrapper| wrapper.datatype())
                });
            let fixed_type = trace_semconv_fixed_type(&column.schema.column_name);

            let needs_reconcile = observed_types
                .iter()
                .copied()
                .any(is_trace_reconcile_candidate_type)
                || existing_type
                    .map(is_trace_reconcile_candidate_type)
                    .unwrap_or(false)
                || fixed_type.is_some();

            let target_type = if needs_reconcile {
                let decision = match choose_trace_reconcile_decision(
                    &column.schema.column_name,
                    &observed_types,
                    existing_type,
                ) {
                    Ok(decision) => decision,
                    Err(_) => {
                        return TraceRequestSchemaPlan::IncompatibleObservations(HashMap::from([
                            (
                                column.schema.column_name.clone(),
                                column.batches.keys().copied().collect(),
                            ),
                        ]));
                    }
                };
                decision
                    .map(|decision| {
                        if decision.requires_alter() {
                            pre_alter
                                .modify_float64_columns
                                .push(column.schema.column_name.clone());
                        }
                        decision.target_type()
                    })
                    .unwrap_or(current_type)
            } else {
                current_type
            };

            column.target_type = Some(target_type);

            if table_schema
                .map(|schema| {
                    schema
                        .column_schema_by_name(&column.schema.column_name)
                        .is_none()
                })
                .unwrap_or(false)
            {
                pre_alter.ensure_columns.push(column.target_schema());
            }
        }

        if table_schema.is_none() {
            pre_alter.ensure_columns = self
                .columns
                .iter()
                .map(TraceColumnRequestSchema::target_schema)
                .collect();
        }

        TraceRequestSchemaPlan::Ready(pre_alter)
    }

    fn prepare_request_rewrite(
        &self,
        batch_index: usize,
        request: &RowInsertRequest,
    ) -> Result<Option<PreparedTraceColumnRewrites>, TraceColumnRewriteError> {
        let Some(rows) = request.rows.as_ref() else {
            return Ok(None);
        };
        let pending_rewrites = self.pending_batch_rewrites(batch_index, rows);
        if pending_rewrites.is_empty() {
            return Ok(None);
        }
        prepare_trace_column_rewrites(&rows.rows, pending_rewrites, &request.table_name).map(Some)
    }

    fn apply_request_rewrite(
        request: &mut RowInsertRequest,
        prepared: PreparedTraceColumnRewrites,
    ) {
        if let Some(rows) = request.rows.as_mut() {
            prepared.apply(rows);
        }
    }

    fn pending_batch_rewrites(
        &self,
        batch_index: usize,
        rows: &api::v1::Rows,
    ) -> Vec<PendingTraceColumnRewrite> {
        let mut pending_rewrites = Vec::new();
        for (col_idx, col_schema) in rows.schema.iter().enumerate() {
            let Some(global_idx) = self.column_indexes.get(&col_schema.column_name).copied() else {
                continue;
            };
            let global_column = &self.columns[global_idx];
            let Some(target_type) = global_column.target_type() else {
                continue;
            };
            if !global_column
                .batches
                .get(&batch_index)
                .is_some_and(|observation| {
                    observation
                        .value_types
                        .iter()
                        .any(|datatype| *datatype != target_type)
                })
            {
                continue;
            }
            pending_rewrites.push(PendingTraceColumnRewrite {
                col_idx,
                target_type,
                column_name: global_column.schema.column_name.clone(),
            });
        }

        pending_rewrites
    }

    fn remove_batches(&mut self, batch_indexes: &HashSet<usize>) {
        for column in &mut self.columns {
            column
                .batches
                .retain(|batch_index, _| !batch_indexes.contains(batch_index));
            column.target_type = None;
        }
        self.columns.retain(|column| !column.batches.is_empty());

        self.column_indexes.clear();
        for (index, column) in self.columns.iter().enumerate() {
            self.column_indexes
                .insert(column.schema.column_name.clone(), index);
        }
    }

    #[cfg(test)]
    fn resolved_target_types(&self) -> Vec<Option<ColumnDataType>> {
        self.columns
            .iter()
            .map(|column| column.target_type)
            .collect()
    }

    fn observe_column(&mut self, schema: &ColumnSchema) -> &mut TraceColumnRequestSchema {
        if let Some(index) = self.column_indexes.get(&schema.column_name).copied() {
            return &mut self.columns[index];
        }

        let index = self.columns.len();
        self.columns.push(TraceColumnRequestSchema {
            schema: schema.clone(),
            batches: BTreeMap::new(),
            target_type: None,
        });
        self.column_indexes
            .insert(schema.column_name.clone(), index);
        &mut self.columns[index]
    }
}

impl TraceColumnRequestSchema {
    fn observe_type(&mut self, batch_index: usize, datatype: ColumnDataType) {
        if let Some(observation) = self.batches.get_mut(&batch_index) {
            push_observed_trace_type(&mut observation.value_types, datatype);
        }
    }

    fn observed_types(&self) -> Vec<ColumnDataType> {
        let mut observed_types = Vec::new();
        for datatype in self
            .batches
            .values()
            .flat_map(|observation| &observation.value_types)
        {
            push_observed_trace_type(&mut observed_types, *datatype);
        }
        observed_types
    }

    fn incompatible_schema_batches(
        &self,
        existing_type: Option<(ColumnDataType, ConcreteDataType)>,
    ) -> HashSet<usize> {
        let mut incompatible_batches = HashSet::new();
        if let Some((existing_datatype, existing_concrete_type)) = existing_type {
            for (batch_index, observation) in &self.batches {
                if trace_logical_types_incompatible(
                    observation.schema_type,
                    &observation.concrete_type,
                    existing_datatype,
                    &existing_concrete_type,
                ) {
                    incompatible_batches.insert(*batch_index);
                }
            }
            return incompatible_batches;
        }

        let mut schemas = Vec::<(ColumnDataType, ConcreteDataType, bool)>::new();
        for observation in self.batches.values() {
            if schemas.iter().any(|(observed_datatype, observed_type, _)| {
                *observed_datatype == observation.schema_type
                    && observed_type == &observation.concrete_type
            }) {
                continue;
            }

            let mut conflicts = false;
            for (observed_datatype, observed_type, observed_conflicts) in &mut schemas {
                if trace_logical_types_incompatible(
                    *observed_datatype,
                    observed_type,
                    observation.schema_type,
                    &observation.concrete_type,
                ) {
                    *observed_conflicts = true;
                    conflicts = true;
                }
            }
            schemas.push((
                observation.schema_type,
                observation.concrete_type.clone(),
                conflicts,
            ));
        }

        for (batch_index, observation) in &self.batches {
            if schemas
                .iter()
                .any(|(observed_datatype, observed_type, conflict)| {
                    *observed_datatype == observation.schema_type
                        && observed_type == &observation.concrete_type
                        && *conflict
                })
            {
                incompatible_batches.insert(*batch_index);
            }
        }

        incompatible_batches
    }

    fn target_type(&self) -> Option<ColumnDataType> {
        self.target_type
            .or_else(|| ColumnDataType::try_from(self.schema.datatype).ok())
    }

    fn target_schema(&self) -> ColumnSchema {
        let mut schema = self.schema.clone();
        if let Some(target_type) = self.target_type() {
            schema.datatype = target_type as i32;
        }
        schema
    }
}

impl Instance {
    /// Ingest OTLP trace spans with chunk-level writes and span-level fallback on
    /// deterministic chunk failures.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn ingest_trace_spans(
        &self,
        pipeline_handler: PipelineHandlerRef,
        pipeline: &PipelineWay,
        pipeline_params: &GreptimePipelineParams,
        table_name: String,
        groups: Vec<TraceSpanGroup>,
        conventions: &str,
        ctx: QueryContextRef,
    ) -> ServerResult<TraceIngestOutcome> {
        let is_trace_v1_model = matches!(pipeline, PipelineWay::OtlpTraceDirectV1);

        // Only the main span table gets the identity; the derived `_services` /
        // `_operations` lookup tables keep the unstamped `ctx`.
        let main_ctx = {
            let mut c = (*ctx).clone();
            c.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_TRACE);
            c.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY);
            if is_trace_v1_model {
                c.set_extension(SEMANTIC_PIPELINE, TABLE_DATA_MODEL_TRACE_V1);
                c.set_extension(SEMANTIC_TRACE_CONVENTIONS, conventions);
            }
            Arc::new(c)
        };

        let ingest_ctx = TraceChunkIngestContext {
            pipeline_handler,
            pipeline,
            pipeline_params,
            table_name: &table_name,
            is_trace_v1_model,
        };
        let mut ingest_state = TraceIngestState {
            aux_data: TraceAuxData::default(),
            outcome: TraceIngestOutcome::default(),
            failure_messages: Vec::new(),
        };

        if is_trace_v1_model {
            let mut request_schema = TraceRequestSchema::default();
            let mut chunks = Vec::new();
            let mut chunk_states = Vec::new();
            // Convert each chunk once, then retain only its sparse values while
            // request-wide schema planning runs.
            for chunk in groups
                .into_iter()
                .flat_map(|group| chunk_owned(group.spans, self.trace_ingest_chunk_size))
            {
                let batch_index = chunks.len();
                let span_metadata = chunk.iter().map(TraceSpanMetadata::from).collect();
                let (table_data, batch_schema) =
                    otlp::trace::v1::v1_to_main_table_data_with_schema(chunk)?;
                let schema_state = if batch_schema.has_incompatible_logical_types() {
                    TraceChunkSchemaState::SpanFallbackOnly
                } else {
                    TraceChunkSchemaState::Prepared
                };
                let (schema, rows) = table_data.into_schema_and_rows();
                if schema_state == TraceChunkSchemaState::Prepared {
                    request_schema.observe_batch_schema(batch_index, &schema, &batch_schema);
                }
                chunks.push(TraceChunkRetry::try_new(
                    ingest_ctx.table_name.to_string(),
                    Rows { schema, rows },
                    span_metadata,
                    batch_schema.into_retry_columns(),
                )?);
                chunk_states.push(schema_state);
            }

            if !chunks.is_empty() {
                self.ingest_trace_v1_prepared_chunks(
                    &ingest_ctx,
                    request_schema,
                    chunks,
                    chunk_states,
                    main_ctx.clone(),
                    &mut ingest_state,
                )
                .await?;
            }
        } else {
            for group in groups {
                let chunks = chunk_owned(group.spans, self.trace_ingest_chunk_size);
                for chunk in chunks {
                    self.ingest_trace_chunk(
                        &ingest_ctx,
                        chunk,
                        main_ctx.clone(),
                        &mut ingest_state,
                    )
                    .await?;
                }
            }
        }

        OTLP_TRACES_ROWS.inc_by(ingest_state.outcome.accepted_spans as u64);

        if !ingest_state.aux_data.is_empty() {
            // Auxiliary trace tables are derived from spans whose main-table
            // writes are already confirmed, so they never create new accepted
            // spans and they do not affect rejected span counts.
            let (aux_requests, _) = otlp::trace::to_grpc_insert_requests_for_aux_tables(
                std::mem::take(&mut ingest_state.aux_data),
                ingest_ctx.pipeline,
                ingest_ctx.table_name,
            )?;

            if !aux_requests.inserts.is_empty() {
                match self
                    .insert_trace_requests(aux_requests, ingest_ctx.is_trace_v1_model, ctx)
                    .await
                {
                    Ok(output) => {
                        Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                    }
                    Err(err) => {
                        Self::push_trace_failure_message(
                            &mut ingest_state.failure_messages,
                            "aux_table_update_failed",
                            format!(
                                "Auxiliary trace tables were not fully updated ({})",
                                err.status_code().as_ref()
                            ),
                        );
                    }
                }
            }
        }

        ingest_state.outcome.error_message = Self::finish_trace_failure_message(
            ingest_state.outcome.accepted_spans,
            ingest_state.outcome.rejected_spans,
            ingest_state.failure_messages,
        );

        Ok(ingest_state.outcome)
    }

    /// Ingest one owned trace chunk so successful spans can be moved into the
    /// accepted set without extra cloning.
    async fn ingest_trace_chunk(
        &self,
        ingest_ctx: &TraceChunkIngestContext<'_>,
        chunk: Vec<TraceSpan>,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        // Try the fast path first so healthy batches keep their original
        // throughput and write amplification stays low.
        let (requests, chunk_rows) = otlp::trace::to_grpc_insert_requests_from_spans(
            &chunk,
            ingest_ctx.pipeline,
            ingest_ctx.pipeline_params,
            ingest_ctx.table_name,
            &ctx,
            ingest_ctx.pipeline_handler.clone(),
        )?;

        match self
            .insert_trace_requests(requests, ingest_ctx.is_trace_v1_model, ctx.clone())
            .await
        {
            Ok(output) => {
                Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                ingest_state.outcome.accepted_spans += chunk_rows;
                for span in &chunk {
                    ingest_state.aux_data.observe_span(span);
                }
            }
            Err(err) => match Self::classify_trace_chunk_failure(err.status_code()) {
                ChunkFailureReaction::RetryPerSpan => {
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::RetryPerSpan.as_metric_label(),
                        format!("Chunk fallback triggered by {}", err.status_code().as_ref()),
                    );
                    // Only deterministic failures are retried span by span.
                    // This includes schemaless table or column creation paths for
                    // trace ingestion. Ambiguous failures are handled below
                    // without retrying because the chunk may already have been
                    // ingested.
                    self.ingest_trace_chunk_span_by_span(
                        ingest_ctx,
                        chunk,
                        ctx.clone(),
                        ingest_state,
                    )
                    .await?;
                }
                ChunkFailureReaction::DiscardChunk => {
                    ingest_state.outcome.rejected_spans += chunk.len();
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::DiscardChunk.as_metric_label(),
                        format!(
                            "Discarded {} spans after ambiguous chunk failure ({})",
                            chunk.len(),
                            err.status_code().as_ref()
                        ),
                    );
                    // TODO(shuiyisong): Add an idempotent retry-safe recovery path for
                    // ambiguous chunk failures such as timeout-like errors.
                }
                // Retryable or ambiguous failures must fail the request instead of
                // becoming partial success. This path is not retry-safe because the
                // chunk may already have been committed before the error surfaced.
                ChunkFailureReaction::Propagate => {
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::Propagate.as_metric_label(),
                        format!(
                            "Propagating retryable chunk failure ({})",
                            err.status_code().as_ref()
                        ),
                    );
                    return Err(err);
                }
            },
        }

        Ok(())
    }

    /// Retry spans one by one only after a deterministic chunk failure.
    async fn ingest_trace_chunk_span_by_span(
        &self,
        ingest_ctx: &TraceChunkIngestContext<'_>,
        chunk: Vec<TraceSpan>,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        for span in chunk {
            let (requests, rows) = otlp::trace::to_grpc_insert_requests_from_spans(
                std::slice::from_ref(&span),
                ingest_ctx.pipeline,
                ingest_ctx.pipeline_params,
                ingest_ctx.table_name,
                &ctx,
                ingest_ctx.pipeline_handler.clone(),
            )?;

            let result = self
                .insert_trace_requests(requests, ingest_ctx.is_trace_v1_model, ctx.clone())
                .await;

            match result {
                Ok(output) => {
                    Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                    ingest_state.outcome.accepted_spans += rows;
                    ingest_state.aux_data.observe_span(&span);
                }
                Err(err) => {
                    if Self::should_propagate_trace_span_failure(err.status_code()) {
                        Self::push_trace_failure_message(
                            &mut ingest_state.failure_messages,
                            ChunkFailureReaction::Propagate.as_metric_label(),
                            format!(
                                "Propagating retryable span failure for {}:{} ({})",
                                span.trace_id,
                                span.span_id,
                                err.status_code().as_ref()
                            ),
                        );
                        return Err(err);
                    }

                    ingest_state.outcome.rejected_spans += 1;
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        "span_rejected",
                        format!(
                            "Rejected span {}:{} ({})",
                            span.trace_id,
                            span.span_id,
                            err.status_code().as_ref()
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    async fn ingest_trace_v1_prepared_chunks(
        &self,
        ingest_ctx: &TraceChunkIngestContext<'_>,
        mut request_schema: TraceRequestSchema,
        chunks: Vec<TraceChunkRetry>,
        mut chunk_states: Vec<TraceChunkSchemaState>,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        if let Err(err) = self
            .prepare_trace_v1_request_schema(
                ingest_ctx.table_name,
                &mut request_schema,
                &chunks,
                &mut chunk_states,
                &ctx,
            )
            .await
        {
            match Self::classify_trace_chunk_failure(err.status_code()) {
                ChunkFailureReaction::RetryPerSpan => {
                    for state in &mut chunk_states {
                        state.mark_for_reconcile();
                    }
                }
                ChunkFailureReaction::DiscardChunk => {
                    let span_count = chunks.iter().map(|chunk| chunk.rows.len()).sum::<usize>();
                    ingest_state.outcome.rejected_spans += span_count;
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::DiscardChunk.as_metric_label(),
                        format!(
                            "Discarded {} spans after ambiguous chunk failure ({})",
                            span_count,
                            err.status_code().as_ref()
                        ),
                    );
                    return Ok(());
                }
                ChunkFailureReaction::Propagate => return Err(err),
            }
        }

        for (batch_index, (retry, schema_state)) in chunks.into_iter().zip(chunk_states).enumerate()
        {
            self.ingest_trace_v1_chunk(
                &request_schema,
                batch_index,
                retry,
                schema_state,
                ctx.clone(),
                ingest_state,
            )
            .await?;
        }

        Ok(())
    }

    async fn ingest_trace_v1_chunk(
        &self,
        request_schema: &TraceRequestSchema,
        batch_index: usize,
        retry: TraceChunkRetry,
        mut schema_state: TraceChunkSchemaState,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        if schema_state == TraceChunkSchemaState::SpanFallbackOnly {
            Self::push_trace_failure_message(
                &mut ingest_state.failure_messages,
                ChunkFailureReaction::RetryPerSpan.as_metric_label(),
                "Chunk fallback triggered by incompatible binary and JSON values".to_string(),
            );
            return self
                .ingest_trace_v1_rows_span_by_span(retry, ctx, ingest_state)
                .await;
        }

        let mut request = retry.to_request()?;
        let _ = Self::prepare_trace_v1_chunk_rewrite(
            request_schema,
            batch_index,
            &mut request,
            &mut schema_state,
        )?;
        let span_count = retry.rows.len();
        let requests = RowInsertRequests {
            inserts: vec![request],
        };
        let result = match schema_state {
            TraceChunkSchemaState::Prepared => self
                .handle_trace_inserts(requests, ctx.clone())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu),
            TraceChunkSchemaState::ReconcilePerChunk | TraceChunkSchemaState::SpanFallbackOnly => {
                self.insert_trace_requests(requests, true, ctx.clone())
                    .await
            }
        };

        match result {
            Ok(output) => {
                Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                ingest_state.outcome.accepted_spans += span_count;
                retry.add_to_aux_data(&mut ingest_state.aux_data);
            }
            Err(err) => match Self::classify_trace_chunk_failure(err.status_code()) {
                ChunkFailureReaction::RetryPerSpan => {
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::RetryPerSpan.as_metric_label(),
                        format!("Chunk fallback triggered by {}", err.status_code().as_ref()),
                    );
                    self.ingest_trace_v1_rows_span_by_span(retry, ctx.clone(), ingest_state)
                        .await?;
                }
                ChunkFailureReaction::DiscardChunk => {
                    ingest_state.outcome.rejected_spans += span_count;
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::DiscardChunk.as_metric_label(),
                        format!(
                            "Discarded {} spans after ambiguous chunk failure ({})",
                            span_count,
                            err.status_code().as_ref()
                        ),
                    );
                }
                ChunkFailureReaction::Propagate => {
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        ChunkFailureReaction::Propagate.as_metric_label(),
                        format!(
                            "Propagating retryable chunk failure ({})",
                            err.status_code().as_ref()
                        ),
                    );
                    return Err(err);
                }
            },
        }

        Ok(())
    }

    async fn ingest_trace_v1_rows_span_by_span(
        &self,
        retry: TraceChunkRetry,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        for row in retry.rows {
            let (span, rows) = row.into_rows(&retry.schema)?;
            let requests = RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: retry.table_name.clone(),
                    rows: Some(rows),
                }],
            };

            match self
                .insert_trace_requests(requests, true, ctx.clone())
                .await
            {
                Ok(output) => {
                    Self::add_trace_write_cost(&mut ingest_state.outcome, output.meta.cost);
                    ingest_state.outcome.accepted_spans += 1;
                    span.add_to_aux_data(&mut ingest_state.aux_data);
                }
                Err(err) => {
                    if Self::should_propagate_trace_span_failure(err.status_code()) {
                        Self::push_trace_failure_message(
                            &mut ingest_state.failure_messages,
                            ChunkFailureReaction::Propagate.as_metric_label(),
                            format!(
                                "Propagating retryable span failure for {}:{} ({})",
                                span.trace_id,
                                span.span_id,
                                err.status_code().as_ref()
                            ),
                        );
                        return Err(err);
                    }

                    ingest_state.outcome.rejected_spans += 1;
                    Self::push_trace_failure_message(
                        &mut ingest_state.failure_messages,
                        "span_rejected",
                        format!(
                            "Rejected span {}:{} ({})",
                            span.trace_id,
                            span.span_id,
                            err.status_code().as_ref()
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    /// Reconcile and insert one trace request batch.
    async fn insert_trace_requests(
        &self,
        mut requests: RowInsertRequests,
        is_trace_v1_model: bool,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        if is_trace_v1_model {
            self.reconcile_trace_column_types(&mut requests, &ctx)
                .await?;
            self.handle_trace_inserts(requests, ctx)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        } else {
            self.handle_log_inserts(requests, ctx)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        }
    }

    async fn plan_trace_v1_request_schema(
        &self,
        table_name: &str,
        request_schema: &mut TraceRequestSchema,
        ctx: &QueryContextRef,
    ) -> ServerResult<TraceRequestSchemaPlan> {
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();
        let table = self
            .catalog_manager
            .table(catalog, &schema, table_name, None)
            .await?;
        let table_schema = table.as_ref().map(|table| table.schema());
        let exclusions = request_schema.incompatible_schema_observations(table_schema.as_deref());
        if !exclusions.is_empty() {
            return Ok(TraceRequestSchemaPlan::IncompatibleObservations(exclusions));
        }

        Ok(request_schema.resolve_table_schema(table_schema.as_deref()))
    }

    async fn prepare_trace_v1_request_schema(
        &self,
        table_name: &str,
        request_schema: &mut TraceRequestSchema,
        chunks: &[TraceChunkRetry],
        chunk_states: &mut [TraceChunkSchemaState],
        ctx: &QueryContextRef,
    ) -> ServerResult<()> {
        loop {
            if request_schema.columns.is_empty() {
                return Ok(());
            }

            let pre_alter = match self
                .plan_trace_v1_request_schema(table_name, request_schema, ctx)
                .await?
            {
                TraceRequestSchemaPlan::Ready(pre_alter) => pre_alter,
                TraceRequestSchemaPlan::IncompatibleObservations(exclusions) => {
                    Self::exclude_trace_v1_schema_observations(
                        request_schema,
                        chunk_states,
                        &exclusions,
                    );
                    continue;
                }
            };
            if pre_alter.requires_ddl() {
                let exclusions = Self::prevalidate_trace_v1_chunk_rewrites(
                    request_schema,
                    chunks,
                    chunk_states,
                )?;
                if !exclusions.is_empty() {
                    Self::exclude_trace_v1_schema_observations(
                        request_schema,
                        chunk_states,
                        &exclusions,
                    );
                    continue;
                }

                self.apply_trace_v1_pre_alter(ctx, table_name, pre_alter)
                    .await?;
                let final_plan = self
                    .plan_trace_v1_request_schema(table_name, request_schema, ctx)
                    .await?;
                let schema_converged = matches!(
                    final_plan,
                    TraceRequestSchemaPlan::Ready(final_plan) if !final_plan.requires_ddl()
                );
                if !schema_converged {
                    for state in chunk_states.iter_mut() {
                        state.mark_for_reconcile();
                    }
                    return Ok(());
                }

                return Ok(());
            }

            return Ok(());
        }
    }

    fn prevalidate_trace_v1_chunk_rewrites(
        request_schema: &TraceRequestSchema,
        chunks: &[TraceChunkRetry],
        chunk_states: &mut [TraceChunkSchemaState],
    ) -> ServerResult<TraceSchemaExclusions> {
        let mut exclusions = HashMap::<String, HashSet<usize>>::new();
        for (batch_index, (chunk, state)) in chunks.iter().zip(chunk_states).enumerate() {
            if *state != TraceChunkSchemaState::Prepared {
                continue;
            }
            let mut request = chunk.to_request()?;
            if let Some(column_name) = Self::prepare_trace_v1_chunk_rewrite(
                request_schema,
                batch_index,
                &mut request,
                state,
            )? {
                exclusions
                    .entry(column_name)
                    .or_default()
                    .insert(batch_index);
            }
        }
        Ok(exclusions)
    }

    fn prepare_trace_v1_chunk_rewrite(
        request_schema: &TraceRequestSchema,
        batch_index: usize,
        request: &mut RowInsertRequest,
        state: &mut TraceChunkSchemaState,
    ) -> ServerResult<Option<String>> {
        if *state != TraceChunkSchemaState::Prepared {
            return Ok(None);
        }

        match request_schema.prepare_request_rewrite(batch_index, request) {
            Ok(Some(rewrite)) => {
                TraceRequestSchema::apply_request_rewrite(request, rewrite);
                Ok(None)
            }
            Ok(None) => Ok(None),
            Err(failure) => {
                if !matches!(
                    Self::classify_trace_chunk_failure(failure.error.status_code()),
                    ChunkFailureReaction::RetryPerSpan
                ) {
                    return Err(failure.error);
                }
                state.mark_for_reconcile();
                Ok(Some(failure.column_name))
            }
        }
    }

    fn exclude_trace_v1_schema_observations(
        request_schema: &mut TraceRequestSchema,
        chunk_states: &mut [TraceChunkSchemaState],
        exclusions: &TraceSchemaExclusions,
    ) {
        let batch_indexes = exclusions
            .values()
            .flatten()
            .copied()
            .collect::<HashSet<_>>();
        for batch_index in &batch_indexes {
            if let Some(state) = chunk_states.get_mut(*batch_index) {
                state.mark_for_reconcile();
            }
        }
        request_schema.remove_batches(&batch_indexes);
    }

    async fn apply_trace_v1_pre_alter(
        &self,
        ctx: &QueryContextRef,
        table_name: &str,
        pre_alter: TraceTablePreAlter,
    ) -> ServerResult<()> {
        let TraceTablePreAlter {
            ensure_columns,
            modify_float64_columns,
        } = pre_alter;

        if !ensure_columns.is_empty() {
            self.inserter
                .ensure_trace_table_on_demand(
                    table_name,
                    ensure_columns,
                    ctx,
                    &self.statement_executor,
                )
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?;
        }

        if !modify_float64_columns.is_empty() {
            self.alter_trace_table_columns_to_float64(ctx, table_name, &modify_float64_columns)
                .await?;
        }

        Ok(())
    }

    fn classify_trace_chunk_failure(status: StatusCode) -> ChunkFailureReaction {
        match status {
            StatusCode::InvalidArguments
            | StatusCode::InvalidSyntax
            | StatusCode::Unsupported
            | StatusCode::TableNotFound
            | StatusCode::TableColumnNotFound => ChunkFailureReaction::RetryPerSpan,
            StatusCode::DatabaseNotFound => ChunkFailureReaction::DiscardChunk,
            StatusCode::Cancelled | StatusCode::DeadlineExceeded => ChunkFailureReaction::Propagate,
            StatusCode::StorageUnavailable
            | StatusCode::RuntimeResourcesExhausted
            | StatusCode::Internal
            | StatusCode::RegionNotReady
            | StatusCode::TableUnavailable
            | StatusCode::RegionBusy => ChunkFailureReaction::Propagate,
            _ => ChunkFailureReaction::DiscardChunk,
        }
    }

    fn should_propagate_trace_span_failure(status: StatusCode) -> bool {
        matches!(
            Self::classify_trace_chunk_failure(status),
            ChunkFailureReaction::Propagate
        )
    }

    fn add_trace_write_cost(outcome: &mut TraceIngestOutcome, cost: usize) {
        outcome.write_cost += cost;
    }

    fn push_trace_failure_message(messages: &mut Vec<String>, label: &str, message: String) {
        OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).inc();

        if messages.len() < TRACE_FAILURE_MESSAGE_LIMIT {
            messages.push(message);
        } else if messages.len() == TRACE_FAILURE_MESSAGE_LIMIT {
            tracing::debug!(
                label,
                limit = TRACE_FAILURE_MESSAGE_LIMIT,
                "Trace ingest failure message limit reached; suppressing additional failure details"
            );
        }
    }

    fn finish_trace_failure_message(
        accepted_spans: usize,
        rejected_spans: usize,
        messages: Vec<String>,
    ) -> Option<String> {
        if rejected_spans == 0 && messages.is_empty() {
            return None;
        }

        let mut summary = format!(
            "Accepted {} spans, rejected {} spans",
            accepted_spans, rejected_spans
        );

        if !messages.is_empty() {
            summary.push_str(": ");
            summary.push_str(&messages.join("; "));
        }

        Some(summary)
    }

    /// Widen existing trace table columns to Float64 before request rewrite.
    async fn alter_trace_table_columns_to_float64(
        &self,
        ctx: &QueryContextRef,
        table_name: &str,
        column_names: &[String],
    ) -> ServerResult<()> {
        let catalog_name = ctx.current_catalog().to_string();
        let schema_name = ctx.current_schema();
        let alter_expr = AlterTableExpr {
            catalog_name: catalog_name.clone(),
            schema_name: schema_name.clone(),
            table_name: table_name.to_string(),
            kind: Some(Kind::ModifyColumnTypes(ModifyColumnTypes {
                modify_column_types: column_names
                    .iter()
                    .map(|column_name| ModifyColumnType {
                        column_name: column_name.clone(),
                        target_type: ColumnDataType::Float64 as i32,
                        target_type_extension: None,
                    })
                    .collect(),
            })),
        };

        if let Err(err) = self
            .statement_executor
            .alter_table_inner(alter_expr, ctx.clone())
            .await
        {
            let table = self
                .catalog_manager
                .table(&catalog_name, &schema_name, table_name, None)
                .await
                .map_err(servers::error::Error::from)?;
            let alter_already_applied = table
                .map(|table| {
                    let table_schema = table.schema();
                    column_names.iter().all(|column_name| {
                        table_schema
                            .column_schema_by_name(column_name)
                            .and_then(|table_col| {
                                ColumnDataTypeWrapper::try_from(table_col.data_type.clone())
                                    .ok()
                                    .map(|wrapper| wrapper.datatype())
                            })
                            == Some(ColumnDataType::Float64)
                    })
                })
                .unwrap_or(false);

            if alter_already_applied {
                return Ok(());
            }

            warn!(
                table_name,
                columns = ?column_names,
                error = %err,
                "failed to widen trace columns before insert"
            );

            return Err(wrap_trace_alter_failure(err));
        }

        Ok(())
    }

    /// Coerce request column types and values to match the existing table schema
    /// for compatible type pairs. Existing table schema wins when present;
    /// otherwise the full request batch decides a stable target type.
    async fn reconcile_trace_column_types(
        &self,
        requests: &mut RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> ServerResult<()> {
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();

        for req in &mut requests.inserts {
            let table = self
                .catalog_manager
                .table(catalog, &schema, &req.table_name, None)
                .await?;

            let Some(rows) = req.rows.as_mut() else {
                continue;
            };

            let table_schema = table.map(|table| table.schema());
            let mut pending_rewrites = Vec::new();
            let mut pending_alter_columns = Vec::new();

            for (col_idx, col_schema) in rows.schema.iter().enumerate() {
                let Some(current_type) = ColumnDataType::try_from(col_schema.datatype).ok() else {
                    continue;
                };

                let mut observed_types = Vec::new();
                push_observed_trace_type(&mut observed_types, current_type);

                // Scan the full request first so the final type decision is not affected
                // by row order inside the batch.
                for row in &rows.rows {
                    let Some(value) = row
                        .values
                        .get(col_idx)
                        .and_then(|value| value.value_data.as_ref())
                    else {
                        continue;
                    };

                    let Some(value_type) = trace_value_datatype(value) else {
                        continue;
                    };
                    push_observed_trace_type(&mut observed_types, value_type);
                }

                let existing_schema_type = table_schema
                    .as_ref()
                    .and_then(|schema| schema.column_schema_by_name(&col_schema.column_name))
                    .and_then(|table_col| {
                        ColumnDataTypeWrapper::try_from(table_col.data_type.clone())
                            .ok()
                            .map(|wrapper| {
                                let datatype = wrapper.datatype();
                                (datatype, ConcreteDataType::from(wrapper))
                            })
                    });
                let request_concrete_type = ConcreteDataType::from(ColumnDataTypeWrapper::new(
                    current_type,
                    col_schema.datatype_extension.clone(),
                ));
                if let Some((existing_datatype, existing_concrete_type)) =
                    existing_schema_type.as_ref()
                    && trace_logical_types_incompatible(
                        current_type,
                        &request_concrete_type,
                        *existing_datatype,
                        existing_concrete_type,
                    )
                {
                    return error::InvalidParameterSnafu {
                        reason: format!(
                            "incompatible logical types for trace column '{}' in table '{}'",
                            col_schema.column_name, req.table_name
                        ),
                    }
                    .fail();
                }
                let existing_type = existing_schema_type.map(|(datatype, _)| datatype);
                let fixed_type = trace_semconv_fixed_type(&col_schema.column_name);

                if !observed_types
                    .iter()
                    .copied()
                    .any(is_trace_reconcile_candidate_type)
                    && existing_type
                        .map(|datatype| !is_trace_reconcile_candidate_type(datatype))
                        .unwrap_or(true)
                    && fixed_type.is_none()
                {
                    continue;
                }

                // Decide the final type once per column, then rewrite all affected cells
                // together in one row pass below.
                let Some(decision) = choose_trace_reconcile_decision(
                    &col_schema.column_name,
                    &observed_types,
                    existing_type,
                )
                .map_err(|_| {
                    enrich_trace_reconcile_error(
                        &req.table_name,
                        &col_schema.column_name,
                        &observed_types,
                        existing_type,
                        fixed_type,
                    )
                })?
                else {
                    continue;
                };
                let target_type = decision.target_type();

                if !decision.requires_alter()
                    && observed_types
                        .iter()
                        .all(|observed| *observed == target_type)
                    && col_schema.datatype == target_type as i32
                {
                    continue;
                }

                if decision.requires_alter()
                    && !pending_alter_columns.contains(&col_schema.column_name)
                {
                    pending_alter_columns.push(col_schema.column_name.clone());
                }

                pending_rewrites.push(PendingTraceColumnRewrite {
                    col_idx,
                    target_type,
                    column_name: col_schema.column_name.clone(),
                });
            }

            if pending_rewrites.is_empty() {
                continue;
            }

            let prepared_rewrites =
                prepare_trace_column_rewrites(&rows.rows, pending_rewrites, &req.table_name)
                    .map_err(|failure| failure.error)?;

            if !pending_alter_columns.is_empty() {
                self.alter_trace_table_columns_to_float64(
                    ctx,
                    &req.table_name,
                    &pending_alter_columns,
                )
                .await?;
            }

            prepared_rewrites.apply(rows);
        }

        Ok(())
    }
}

fn trace_logical_types_incompatible(
    left_datatype: ColumnDataType,
    left_concrete_type: &ConcreteDataType,
    right_datatype: ColumnDataType,
    right_concrete_type: &ConcreteDataType,
) -> bool {
    left_concrete_type != right_concrete_type
        && (left_datatype == right_datatype
            || !is_trace_reconcile_candidate_type(left_datatype)
            || !is_trace_reconcile_candidate_type(right_datatype))
}

fn chunk_owned<T>(items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
    if items.is_empty() {
        return Vec::new();
    }

    if chunk_size == 0 {
        return vec![items];
    }

    let mut chunks = Vec::with_capacity(items.len().div_ceil(chunk_size));
    let mut iter = items.into_iter();
    while iter.len() > 0 {
        chunks.push(iter.by_ref().take(chunk_size).collect());
    }
    chunks
}

/// Preserve the original alter failure status so chunk retry behavior stays correct.
fn wrap_trace_alter_failure<E>(err: E) -> servers::error::Error
where
    E: ErrorExt + Send + Sync + 'static,
{
    error::ExecuteGrpcQuerySnafu.into_error(BoxedError::new(err))
}

/// Derives `trace.conventions` from the request's resource/scope `schema_url`s.
/// A single distinct non-empty value is concrete; multiple distinct values are
/// `mixed`; none is `unknown`. `schema_url` is row-level in OTLP, so the
/// table-level value is best-effort per the RFC conflict rule.
pub(super) fn trace_conventions(request: &ExportTraceServiceRequest) -> String {
    let mut seen: Option<&str> = None;
    let mut mixed = false;

    for resource_spans in &request.resource_spans {
        let urls = std::iter::once(resource_spans.schema_url.as_str()).chain(
            resource_spans
                .scope_spans
                .iter()
                .map(|s| s.schema_url.as_str()),
        );
        for url in urls {
            if url.is_empty() {
                continue;
            }
            match seen {
                None => seen = Some(url),
                Some(prev) if prev == url => {}
                Some(_) => {
                    mixed = true;
                    break;
                }
            }
        }
        if mixed {
            break;
        }
    }

    if mixed {
        SEMANTIC_VALUE_MIXED.to_string()
    } else {
        seen.map(str::to_string)
            .unwrap_or_else(|| SEMANTIC_VALUE_UNKNOWN.to_string())
    }
}

#[cfg(test)]
mod tests;
