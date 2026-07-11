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
use async_trait::async_trait;
use auth::{PermissionChecker, PermissionCheckerRef, PermissionReq};
use client::Output;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
use common_telemetry::{tracing, warn};
use datatypes::prelude::ConcreteDataType;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use pipeline::{GreptimePipelineParams, PipelineWay};
use servers::error::{self, AuthSnafu, Result as ServerResult};
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{OpenTelemetryProtocolInterceptor, OpenTelemetryProtocolInterceptorRef};
use servers::otlp;
use servers::otlp::coerce::trace_value_datatype;
use servers::otlp::trace::span::{TraceSpan, TraceSpanGroup};
use servers::otlp::trace::v1::{TraceBatchSchema, TraceBinaryType, TraceRetryColumns};
use servers::otlp::trace::{SERVICE_NAME_COLUMN, TraceAuxData};
use servers::query_handler::{
    OpenTelemetryProtocolHandler, PipelineHandlerRef, TraceIngestOutcome,
};
use session::context::QueryContextRef;
use snafu::{IntoError, ResultExt};
use table::requests::{
    OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM, SEMANTIC_PER_TABLE_INDEX_KEY,
    SEMANTIC_PIPELINE, SEMANTIC_SIGNAL_TYPE, SEMANTIC_SOURCE, SEMANTIC_TRACE_CONVENTIONS,
    SEMANTIC_VALUE_MIXED, SEMANTIC_VALUE_UNKNOWN, SIGNAL_TYPE_LOG, SIGNAL_TYPE_METRIC,
    SIGNAL_TYPE_TRACE, SOURCE_OPENTELEMETRY, TABLE_DATA_MODEL_TRACE_V1,
};

use crate::instance::Instance;
use crate::instance::otlp::trace_semconv::trace_semconv_fixed_type;
use crate::instance::otlp::trace_types::{
    PendingTraceColumnRewrite, PreparedTraceColumnRewrites, TraceColumnRewriteError,
    choose_trace_reconcile_decision, enrich_trace_reconcile_error,
    is_trace_reconcile_candidate_type, prepare_trace_column_rewrites, push_observed_trace_type,
};
use crate::metrics::{
    OTLP_LOGS_ROWS, OTLP_METRICS_ROWS, OTLP_TRACES_FAILURE_COUNT, OTLP_TRACES_ROWS,
};

pub mod trace_semconv;
pub mod trace_types;

const TRACE_FAILURE_MESSAGE_LIMIT: usize = 4;

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

struct TraceChunkIngestContext<'a> {
    pipeline_handler: PipelineHandlerRef,
    pipeline: &'a PipelineWay,
    pipeline_params: &'a GreptimePipelineParams,
    table_name: &'a str,
    is_trace_v1_model: bool,
}

struct TraceIngestState {
    aux_data: TraceAuxData,
    outcome: TraceIngestOutcome,
    failure_messages: Vec<String>,
}

struct TraceChunkInsert {
    span_metadata: Vec<TraceSpanMetadata>,
    request: RowInsertRequest,
    retry_columns: TraceRetryColumns,
    schema_prepared: bool,
    span_fallback_only: bool,
}

struct TraceChunkRetry {
    table_name: String,
    schema: Vec<ColumnSchema>,
    rows: Vec<TraceRetryRow>,
}

struct TraceRetryRow {
    span_metadata: TraceSpanMetadata,
    values: Vec<TraceRetryValue>,
}

struct TraceRetryValue {
    column_index: usize,
    value: Value,
    schema_type: Option<TraceRetrySchemaType>,
}

enum TraceRetrySchemaType {
    Scalar(ColumnDataType),
    Binary(TraceBinaryType),
}

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
        request: &RowInsertRequest,
        span_metadata: Vec<TraceSpanMetadata>,
        retry_columns: TraceRetryColumns,
    ) -> ServerResult<Self> {
        let Some(rows) = request.rows.as_ref() else {
            return error::InternalSnafu {
                err_msg: "v1 trace main-table request has no rows".to_string(),
            }
            .fail();
        };
        let row_count = rows.rows.len();
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

        let mut fixed_columns = Vec::with_capacity(rows.schema.len());
        let mut sparse_columns = vec![Vec::new(); rows.rows.len()];
        for (column_index, column) in rows.schema.iter().enumerate() {
            if !is_sparse_trace_column(&column.column_name) {
                fixed_columns.push(column_index);
                continue;
            }
            let Some(retry_column) = retry_columns.get(&column.column_name) else {
                continue;
            };
            for present_row in &retry_column.present_rows {
                if *present_row < sparse_columns.len() {
                    sparse_columns[*present_row].push(column_index);
                }
            }
        }

        let mut retry_rows = Vec::with_capacity(rows.rows.len());
        for (row_index, (row, span_metadata)) in rows.rows.iter().zip(span_metadata).enumerate() {
            if rows.schema.len() != row.values.len() {
                return error::InternalSnafu {
                    err_msg: format!(
                        "trace schema column count {} does not match row value count {}",
                        rows.schema.len(),
                        row.values.len()
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
                let column = &rows.schema[column_index];
                let value = &row.values[column_index];
                let value_type = value.value_data.as_ref().and_then(trace_value_datatype);
                let binary_type = (value_type == Some(ColumnDataType::Binary))
                    .then(|| retry_columns.get(&column.column_name))
                    .flatten()
                    .and_then(|retry_column| {
                        retry_column
                            .binary_types
                            .binary_search_by_key(&row_index, |(row_index, _)| *row_index)
                            .ok()
                            .map(|index| retry_column.binary_types[index].1)
                    });
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
                    value: value.clone(),
                    schema_type,
                });
            }
            retry_rows.push(TraceRetryRow {
                span_metadata,
                values,
            });
        }

        Ok(Self {
            table_name: request.table_name.clone(),
            schema: rows.schema.clone(),
            rows: retry_rows,
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

#[derive(Default)]
struct TraceRequestSchema {
    columns: Vec<TraceColumnRequestSchema>,
    column_indexes: HashMap<String, usize>,
}

struct TraceColumnRequestSchema {
    schema: ColumnSchema,
    batches: BTreeMap<usize, TraceBatchColumnObservation>,
    target_type: Option<ColumnDataType>,
}

struct TraceBatchColumnObservation {
    value_types: Vec<ColumnDataType>,
    schema_type: ColumnDataType,
    concrete_type: ConcreteDataType,
}

struct PreparedTraceChunkRewrite {
    chunk_index: usize,
    rewrite: PreparedTraceColumnRewrites,
}

type TraceSchemaExclusions = HashMap<String, HashSet<usize>>;

struct TraceTablePreAlter {
    ensure_columns: Vec<ColumnSchema>,
    modify_float64_columns: Vec<String>,
}

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
        request: &RowInsertRequest,
        batch_schema: &TraceBatchSchema,
    ) {
        let Some(rows) = request.rows.as_ref() else {
            return;
        };
        for schema in &rows.schema {
            if batch_schema.has_incompatible_logical_types_for(&schema.column_name) {
                continue;
            }
            let observed_types = batch_schema.value_types(&schema.column_name);
            self.observe_trace_column(batch_index, schema, None);
            if let Some(observed_types) = observed_types {
                for value_type in observed_types {
                    self.observe_trace_column(batch_index, schema, Some(*value_type));
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

    fn remove_observations(&mut self, exclusions: &TraceSchemaExclusions) {
        for column in &mut self.columns {
            if let Some(batch_indexes) = exclusions.get(&column.schema.column_name) {
                column
                    .batches
                    .retain(|batch_index, _| !batch_indexes.contains(batch_index));
            }
            column.target_type = None;
        }
        self.columns.retain(|column| !column.batches.is_empty());

        self.column_indexes.clear();
        for (index, column) in self.columns.iter().enumerate() {
            self.column_indexes
                .insert(column.schema.column_name.clone(), index);
        }
    }

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

#[async_trait]
impl OpenTelemetryProtocolHandler for Instance {
    #[tracing::instrument(skip_all)]
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;

        let input_names = request
            .resource_metrics
            .iter()
            .flat_map(|r| r.scope_metrics.iter())
            .flat_map(|s| s.metrics.iter().map(|m| &m.name))
            .collect::<Vec<_>>();

        // See [`OtlpMetricCtx`] for details
        let is_legacy = self.check_otlp_legacy(&input_names, ctx.clone()).await?;

        let mut metric_ctx = ctx
            .protocol_ctx()
            .get_otlp_metric_ctx()
            .cloned()
            .unwrap_or_default();
        metric_ctx.is_legacy = is_legacy;

        let (requests, rows, semantic_index) =
            otlp::metrics::to_grpc_insert_requests(request, &mut metric_ctx)?;
        OTLP_METRICS_ROWS.inc_by(rows as u64);

        let ctx = {
            let mut c = (*ctx).clone();
            c.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC);
            c.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY);
            // Per-table metric specifics + resource/scope lineage ride this
            // internal channel; the auto-create path folds them per table name.
            if let Some(index) = semantic_index.encode() {
                c.set_extension(SEMANTIC_PER_TABLE_INDEX_KEY, index);
            }
            if !is_legacy {
                c.set_extension(OTLP_METRIC_COMPAT_KEY, OTLP_METRIC_COMPAT_PROM.to_string());
            }
            Arc::new(c)
        };

        // If the user uses the legacy path, it is by default without metric engine.
        if metric_ctx.is_legacy || !metric_ctx.with_metric_engine {
            self.handle_row_inserts(requests, ctx, false, false)
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        } else {
            let physical_table = ctx
                .extension(PHYSICAL_TABLE_PARAM)
                .unwrap_or(GREPTIME_PHYSICAL_TABLE)
                .to_string();
            self.handle_metric_row_inserts(requests, ctx, physical_table.clone())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        }
    }

    #[tracing::instrument(skip_all)]
    async fn traces(
        &self,
        pipeline_handler: PipelineHandlerRef,
        request: ExportTraceServiceRequest,
        pipeline: PipelineWay,
        pipeline_params: GreptimePipelineParams,
        table_name: String,
        ctx: QueryContextRef,
    ) -> ServerResult<TraceIngestOutcome> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;

        // `schema_url` is consumed by `parse`, so derive conventions first.
        let conventions = trace_conventions(&request);
        let spans = otlp::trace::span::parse(request);
        self.ingest_trace_spans(
            pipeline_handler,
            &pipeline,
            &pipeline_params,
            table_name,
            spans,
            &conventions,
            ctx,
        )
        .await
    }

    #[tracing::instrument(skip_all)]
    async fn logs(
        &self,
        pipeline_handler: PipelineHandlerRef,
        request: ExportLogsServiceRequest,
        pipeline: PipelineWay,
        pipeline_params: GreptimePipelineParams,
        table_name: String,
        ctx: QueryContextRef,
    ) -> ServerResult<Vec<Output>> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission(ctx.current_user(), PermissionReq::Otlp)
            .context(AuthSnafu)?;

        let interceptor_ref = self
            .plugins
            .get::<OpenTelemetryProtocolInterceptorRef<servers::error::Error>>();
        interceptor_ref.pre_execute(ctx.clone())?;

        // `as_req_iter` clones this ctx into each `temp_ctx`, so identity set here
        // reaches the context that drives table auto-create.
        let ctx = {
            let mut c = (*ctx).clone();
            c.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_LOG);
            c.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY);
            Arc::new(c)
        };

        let opt_req = otlp::logs::to_grpc_insert_requests(
            request,
            pipeline,
            pipeline_params,
            table_name,
            &ctx,
            pipeline_handler,
        )
        .await?;

        let mut outputs = vec![];

        for (temp_ctx, requests) in opt_req.as_req_iter(ctx) {
            let cnt = requests
                .inserts
                .iter()
                .filter_map(|r| r.rows.as_ref().map(|r| r.rows.len()))
                .sum::<usize>();

            let o = self
                .handle_log_inserts(requests, temp_ctx)
                .await
                .inspect(|_| OTLP_LOGS_ROWS.inc_by(cnt as u64))
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)?;
            outputs.push(o);
        }

        Ok(outputs)
    }
}

impl Instance {
    /// Ingest OTLP trace spans with chunk-level writes and span-level fallback on
    /// deterministic chunk failures.
    #[allow(clippy::too_many_arguments)]
    async fn ingest_trace_spans(
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
            let mut chunk_inserts = Vec::new();
            for group in groups {
                for chunk in chunk_owned(group.spans, self.trace_ingest_chunk_size) {
                    let batch_index = chunk_inserts.len();
                    let span_metadata = chunk.iter().map(TraceSpanMetadata::from).collect();
                    let (request, batch_schema) =
                        otlp::trace::v1::v1_to_grpc_main_insert_requests_with_schema(
                            chunk,
                            ingest_ctx.table_name,
                        )?;
                    let span_fallback_only = batch_schema.has_incompatible_logical_types();
                    request_schema.observe_batch_schema(batch_index, &request, &batch_schema);
                    chunk_inserts.push(TraceChunkInsert {
                        span_metadata,
                        request,
                        retry_columns: batch_schema.into_retry_columns(),
                        schema_prepared: !span_fallback_only,
                        span_fallback_only,
                    });
                }
            }

            if !chunk_inserts.is_empty() {
                self.ingest_trace_v1_prepared_chunks(
                    &ingest_ctx,
                    request_schema,
                    chunk_inserts,
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
        mut chunk_inserts: Vec<TraceChunkInsert>,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        if let Err(err) = self
            .prepare_trace_v1_request_schema(
                ingest_ctx.table_name,
                &mut request_schema,
                &mut chunk_inserts,
                &ctx,
            )
            .await
        {
            if !matches!(
                Self::classify_trace_chunk_failure(err.status_code()),
                ChunkFailureReaction::RetryPerSpan
            ) {
                return Err(err);
            }
            for chunk_insert in &mut chunk_inserts {
                chunk_insert.schema_prepared = false;
            }
        }

        for chunk_insert in chunk_inserts {
            self.ingest_trace_v1_chunk(chunk_insert, ctx.clone(), ingest_state)
                .await?;
        }

        Ok(())
    }

    async fn ingest_trace_v1_chunk(
        &self,
        chunk_insert: TraceChunkInsert,
        ctx: QueryContextRef,
        ingest_state: &mut TraceIngestState,
    ) -> ServerResult<()> {
        let TraceChunkInsert {
            span_metadata,
            request,
            retry_columns,
            schema_prepared,
            span_fallback_only,
        } = chunk_insert;

        // Keep only present dynamic values in the retry copy. The main request is
        // dense and can be much larger when spans use different attribute keys.
        let retry = TraceChunkRetry::try_new(&request, span_metadata, retry_columns)?;
        if span_fallback_only {
            Self::push_trace_failure_message(
                &mut ingest_state.failure_messages,
                ChunkFailureReaction::RetryPerSpan.as_metric_label(),
                "Chunk fallback triggered by incompatible binary and JSON values".to_string(),
            );
            return self
                .ingest_trace_v1_rows_span_by_span(retry, ctx, ingest_state)
                .await;
        }

        let span_count = retry.rows.len();
        let requests = RowInsertRequests {
            inserts: vec![request],
        };
        let result = if schema_prepared {
            self.handle_trace_inserts(requests, ctx.clone())
                .await
                .map_err(BoxedError::new)
                .context(error::ExecuteGrpcQuerySnafu)
        } else {
            self.insert_trace_requests(requests, true, ctx.clone())
                .await
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
        chunk_inserts: &mut [TraceChunkInsert],
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
                        chunk_inserts,
                        &exclusions,
                    );
                    continue;
                }
            };
            if pre_alter.requires_ddl() {
                // Failed batches must not contribute columns to persistent DDL.
                let (mut prepared_rewrites, exclusions) =
                    Self::prepare_trace_v1_chunk_rewrites(request_schema, chunk_inserts)?;
                if !exclusions.is_empty() {
                    Self::exclude_trace_v1_schema_observations(
                        request_schema,
                        chunk_inserts,
                        &exclusions,
                    );
                    continue;
                }

                let prevalidated_targets = request_schema.resolved_target_types();
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
                    for chunk_insert in chunk_inserts.iter_mut() {
                        chunk_insert.schema_prepared = false;
                    }
                    return Ok(());
                }

                let targets_unchanged =
                    prevalidated_targets == request_schema.resolved_target_types();
                if !targets_unchanged {
                    prepared_rewrites =
                        Self::prepare_trace_v1_chunk_rewrites(request_schema, chunk_inserts)?.0;
                }
                Self::apply_prepared_trace_v1_chunk_rewrites(chunk_inserts, prepared_rewrites);
                return Ok(());
            }

            let (prepared_rewrites, _) =
                Self::prepare_trace_v1_chunk_rewrites(request_schema, chunk_inserts)?;
            Self::apply_prepared_trace_v1_chunk_rewrites(chunk_inserts, prepared_rewrites);
            return Ok(());
        }
    }

    fn prepare_trace_v1_chunk_rewrites(
        request_schema: &TraceRequestSchema,
        chunk_inserts: &mut [TraceChunkInsert],
    ) -> ServerResult<(Vec<PreparedTraceChunkRewrite>, TraceSchemaExclusions)> {
        let mut prepared_rewrites = Vec::new();
        let mut exclusions = HashMap::<String, HashSet<usize>>::new();
        for (chunk_index, chunk_insert) in chunk_inserts.iter_mut().enumerate() {
            let apply_rewrites = chunk_insert.schema_prepared;
            match request_schema.prepare_request_rewrite(chunk_index, &chunk_insert.request) {
                Ok(Some(rewrite)) if apply_rewrites => {
                    prepared_rewrites.push(PreparedTraceChunkRewrite {
                        chunk_index,
                        rewrite,
                    })
                }
                Ok(_) => {}
                Err(failure) => {
                    if !matches!(
                        Self::classify_trace_chunk_failure(failure.error.status_code()),
                        ChunkFailureReaction::RetryPerSpan
                    ) {
                        return Err(failure.error);
                    }
                    chunk_insert.schema_prepared = false;
                    exclusions
                        .entry(failure.column_name)
                        .or_default()
                        .insert(chunk_index);
                }
            }
        }
        Ok((prepared_rewrites, exclusions))
    }

    fn exclude_trace_v1_schema_observations(
        request_schema: &mut TraceRequestSchema,
        chunk_inserts: &mut [TraceChunkInsert],
        exclusions: &TraceSchemaExclusions,
    ) {
        for batch_index in exclusions.values().flatten() {
            if let Some(chunk_insert) = chunk_inserts.get_mut(*batch_index) {
                chunk_insert.schema_prepared = false;
            }
        }
        request_schema.remove_observations(exclusions);
    }

    fn apply_prepared_trace_v1_chunk_rewrites(
        chunk_inserts: &mut [TraceChunkInsert],
        prepared_rewrites: Vec<PreparedTraceChunkRewrite>,
    ) {
        for prepared_chunk in prepared_rewrites {
            TraceRequestSchema::apply_request_rewrite(
                &mut chunk_inserts[prepared_chunk.chunk_index].request,
                prepared_chunk.rewrite,
            );
        }
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
            || (!is_trace_reconcile_candidate_type(left_datatype)
                && !is_trace_reconcile_candidate_type(right_datatype)))
}

fn is_sparse_trace_column(column_name: &str) -> bool {
    column_name == SERVICE_NAME_COLUMN
        || column_name.starts_with("span_attributes.")
        || column_name.starts_with("scope_attributes.")
        || column_name.starts_with("resource_attributes.")
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
fn trace_conventions(request: &ExportTraceServiceRequest) -> String {
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
mod tests {
    use std::collections::{HashMap, HashSet};

    use api::v1::column_data_type_extension::TypeExt;
    use api::v1::value::ValueData;
    use api::v1::{
        ColumnDataType, ColumnDataTypeExtension, ColumnSchema, JsonTypeExtension, Row,
        RowInsertRequest, Rows, SemanticType, Value,
    };
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{
        ColumnSchema as DatatypesColumnSchema, SchemaBuilder as DatatypesSchemaBuilder,
    };
    use servers::otlp::trace::v1::{TraceBinaryType, TraceRetryColumn};
    use servers::query_handler::TraceIngestOutcome;

    use super::{
        ChunkFailureReaction, Instance, TraceChunkInsert, TraceChunkRetry, TraceRequestSchema,
        TraceRequestSchemaPlan, TraceSpanMetadata, TraceTablePreAlter, chunk_owned,
        wrap_trace_alter_failure,
    };
    use crate::metrics::OTLP_TRACES_FAILURE_COUNT;

    #[test]
    fn test_chunk_owned() {
        let chunks = chunk_owned(vec![1, 2, 3], 2);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 1]);

        let chunks = chunk_owned(vec![1, 2, 3], 0);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 3);

        assert!(chunk_owned::<i32>(Vec::new(), 0).is_empty());
    }

    #[test]
    fn test_classify_trace_chunk_failure() {
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::InvalidArguments),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::InvalidSyntax),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Unsupported),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::TableColumnNotFound),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::TableNotFound),
            ChunkFailureReaction::RetryPerSpan
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::DatabaseNotFound),
            ChunkFailureReaction::DiscardChunk
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::DeadlineExceeded),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Cancelled),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::StorageUnavailable),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Internal),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::RegionNotReady),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::TableUnavailable),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::RegionBusy),
            ChunkFailureReaction::Propagate
        );
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::RuntimeResourcesExhausted),
            ChunkFailureReaction::Propagate
        );
    }

    #[test]
    fn test_classify_trace_span_failure() {
        assert!(Instance::should_propagate_trace_span_failure(
            StatusCode::DeadlineExceeded
        ));
        assert!(Instance::should_propagate_trace_span_failure(
            StatusCode::StorageUnavailable
        ));
        assert!(!Instance::should_propagate_trace_span_failure(
            StatusCode::InvalidArguments
        ));
    }

    #[test]
    fn test_add_trace_write_cost() {
        let mut outcome = TraceIngestOutcome::default();
        Instance::add_trace_write_cost(&mut outcome, 3);
        Instance::add_trace_write_cost(&mut outcome, 5);
        assert_eq!(outcome.write_cost, 8);
    }

    #[test]
    fn test_finish_trace_failure_message() {
        let message = Instance::finish_trace_failure_message(
            3,
            2,
            vec!["Rejected span trace:span (InvalidArguments)".to_string()],
        )
        .unwrap();
        assert!(message.contains("Accepted 3 spans, rejected 2 spans"));
        assert!(message.contains("Rejected span trace:span"));

        assert_eq!(Instance::finish_trace_failure_message(2, 0, vec![]), None);
    }

    #[test]
    fn test_finish_trace_failure_message_without_detail_messages() {
        assert_eq!(
            Instance::finish_trace_failure_message(0, 2, vec![]),
            Some("Accepted 0 spans, rejected 2 spans".to_string())
        );
    }

    #[test]
    fn test_push_trace_failure_message_increments_labeled_counter() {
        let label = "retry_per_span_counter_test";
        let initial = OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).get();
        let mut messages = Vec::new();

        Instance::push_trace_failure_message(
            &mut messages,
            label,
            "Chunk fallback triggered by InvalidArguments".to_string(),
        );

        assert_eq!(messages.len(), 1);
        assert_eq!(
            OTLP_TRACES_FAILURE_COUNT.with_label_values(&[label]).get(),
            initial + 1
        );
    }

    #[test]
    fn test_push_trace_failure_message_caps_recorded_messages() {
        let label = "retry_per_span_limit_test";
        let mut messages = Vec::new();

        for idx in 0..=4 {
            Instance::push_trace_failure_message(&mut messages, label, format!("failure-{idx}"));
        }

        assert_eq!(messages.len(), 4);
        assert_eq!(
            messages,
            vec![
                "failure-0".to_string(),
                "failure-1".to_string(),
                "failure-2".to_string(),
                "failure-3".to_string()
            ]
        );
    }

    #[test]
    fn test_classify_trace_chunk_failure_defaults_to_discard() {
        assert_eq!(
            Instance::classify_trace_chunk_failure(StatusCode::Unknown),
            ChunkFailureReaction::DiscardChunk
        );
    }

    #[test]
    fn test_wrap_trace_alter_failure_preserves_status_code() {
        let err = wrap_trace_alter_failure(
            servers::error::TableNotFoundSnafu {
                catalog: "greptime".to_string(),
                schema: "public".to_string(),
                table: "trace_type_missing".to_string(),
            }
            .build(),
        );

        assert_eq!(err.status_code(), StatusCode::TableNotFound);
    }

    fn field_schema(name: &str, datatype: ColumnDataType) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            datatype: datatype as i32,
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        }
    }

    fn json_field_schema(name: &str) -> ColumnSchema {
        ColumnSchema {
            datatype_extension: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::JsonType(JsonTypeExtension::JsonBinary.into())),
            }),
            ..field_schema(name, ColumnDataType::Binary)
        }
    }

    fn row(values: Vec<Option<ValueData>>) -> Row {
        Row {
            values: values
                .into_iter()
                .map(|value_data| Value { value_data })
                .collect(),
        }
    }

    fn row_insert_request(
        table_name: &str,
        schema: Vec<ColumnSchema>,
        rows: Vec<Row>,
    ) -> RowInsertRequest {
        RowInsertRequest {
            table_name: table_name.to_string(),
            rows: Some(Rows { schema, rows }),
        }
    }

    fn ready_plan(plan: TraceRequestSchemaPlan) -> TraceTablePreAlter {
        let TraceRequestSchemaPlan::Ready(plan) = plan else {
            panic!("expected a ready trace schema plan");
        };
        plan
    }

    fn prepare_and_apply(
        request_schema: &TraceRequestSchema,
        batch_index: usize,
        request: &mut RowInsertRequest,
    ) {
        if let Some(prepared) = request_schema
            .prepare_request_rewrite(batch_index, request)
            .unwrap()
        {
            TraceRequestSchema::apply_request_rewrite(request, prepared);
        }
    }

    #[test]
    fn test_trace_chunk_retry_drops_absent_sparse_columns() {
        let schema = vec![
            field_schema("parent_span_id", ColumnDataType::String),
            field_schema("service_name", ColumnDataType::String),
            field_schema("span_attributes.absent", ColumnDataType::Int64),
            field_schema("scope_attributes.present", ColumnDataType::Boolean),
        ];
        let requests = row_insert_request(
            "traces",
            schema,
            vec![row(vec![
                None,
                None,
                None,
                Some(ValueData::BoolValue(true)),
            ])],
        );
        let retry = TraceChunkRetry::try_new(
            &requests,
            vec![TraceSpanMetadata {
                trace_id: "trace-id".to_string(),
                span_id: "span-id".to_string(),
                operation: None,
            }],
            HashMap::from([(
                "scope_attributes.present".to_string(),
                TraceRetryColumn {
                    present_rows: vec![0],
                    binary_types: Vec::new(),
                },
            )]),
        )
        .unwrap();
        assert_eq!(retry.rows[0].values.len(), 2);

        let mut rows = retry.rows.into_iter();
        let (_, projected) = rows.next().unwrap().into_rows(&retry.schema).unwrap();

        assert_eq!(
            projected
                .schema
                .iter()
                .map(|column| column.column_name.as_str())
                .collect::<Vec<_>>(),
            ["parent_span_id", "scope_attributes.present"]
        );
        assert_eq!(projected.rows[0].values.len(), 2);
        assert_eq!(projected.rows[0].values[0].value_data, None);
        assert_eq!(
            projected.rows[0].values[1].value_data,
            Some(ValueData::BoolValue(true))
        );
    }

    #[test]
    fn test_trace_chunk_retry_restores_row_logical_types() {
        let column_name = "span_attributes.payload";
        let requests = row_insert_request(
            "traces",
            vec![field_schema(column_name, ColumnDataType::Binary)],
            vec![
                row(vec![Some(ValueData::BinaryValue(vec![1, 2, 3]))]),
                row(vec![Some(ValueData::BinaryValue(vec![4, 5, 6]))]),
                row(vec![Some(ValueData::StringValue("value".to_string()))]),
            ],
        );
        let metadata = ["span-1", "span-2", "span-3"]
            .into_iter()
            .map(|span_id| TraceSpanMetadata {
                trace_id: "trace-id".to_string(),
                span_id: span_id.to_string(),
                operation: None,
            })
            .collect();
        let retry = TraceChunkRetry::try_new(
            &requests,
            metadata,
            HashMap::from([(
                column_name.to_string(),
                TraceRetryColumn {
                    present_rows: vec![0, 1, 2],
                    binary_types: vec![(0, TraceBinaryType::Binary), (1, TraceBinaryType::Json)],
                },
            )]),
        )
        .unwrap();

        let projected = retry
            .rows
            .into_iter()
            .map(|row| row.into_rows(&retry.schema).unwrap().1)
            .collect::<Vec<_>>();

        assert!(projected[0].schema[0].datatype_extension.is_none());
        assert!(matches!(
            projected[1].schema[0]
                .datatype_extension
                .as_ref()
                .and_then(|extension| extension.type_ext.as_ref()),
            Some(TypeExt::JsonType(_))
        ));
        assert_eq!(
            projected[2].schema[0].datatype,
            ColumnDataType::String as i32
        );
        assert!(projected[2].schema[0].datatype_extension.is_none());
    }

    #[test]
    fn test_trace_request_schema_detects_datatype_extension_conflicts() {
        let column_name = "span_attributes.payload";
        let binary_schema = field_schema(column_name, ColumnDataType::Binary);
        let json_schema = json_field_schema(column_name);
        let legacy_json_schema = ColumnSchema {
            datatype: ColumnDataType::Json as i32,
            ..json_schema.clone()
        };

        let mut request_schema = TraceRequestSchema::default();
        request_schema.observe_trace_column(0, &binary_schema, None);
        request_schema.observe_trace_column(1, &json_schema, None);
        assert_eq!(
            request_schema.incompatible_schema_observations(None),
            HashMap::from([(column_name.to_string(), HashSet::from([0, 1]))])
        );

        let mut equivalent_json_request_schema = TraceRequestSchema::default();
        equivalent_json_request_schema.observe_trace_column(0, &json_schema, None);
        equivalent_json_request_schema.observe_trace_column(1, &legacy_json_schema, None);
        assert!(
            equivalent_json_request_schema
                .incompatible_schema_observations(None)
                .is_empty()
        );

        let mut incompatible_json_request_schema = TraceRequestSchema::default();
        incompatible_json_request_schema.observe_trace_column(0, &binary_schema, None);
        incompatible_json_request_schema.observe_trace_column(1, &legacy_json_schema, None);
        assert_eq!(
            incompatible_json_request_schema.incompatible_schema_observations(None),
            HashMap::from([(column_name.to_string(), HashSet::from([0, 1]))])
        );

        let existing_json_schema =
            DatatypesSchemaBuilder::try_from_columns(vec![DatatypesColumnSchema::new(
                column_name,
                ConcreteDataType::json_datatype(),
                true,
            )])
            .unwrap()
            .build()
            .unwrap();
        let mut binary_request_schema = TraceRequestSchema::default();
        binary_request_schema.observe_trace_column(0, &binary_schema, None);
        assert_eq!(
            binary_request_schema.incompatible_schema_observations(Some(&existing_json_schema)),
            HashMap::from([(column_name.to_string(), HashSet::from([0]))])
        );

        let mut json_request_schema = TraceRequestSchema::default();
        json_request_schema.observe_trace_column(0, &json_schema, None);
        assert!(
            json_request_schema
                .incompatible_schema_observations(Some(&existing_json_schema))
                .is_empty()
        );
    }

    #[test]
    fn test_trace_request_schema_isolates_unsupported_type_batches() {
        let mixed_column = "span_attributes.mixed";
        let unrelated_column = "span_attributes.unrelated";
        let int_schema = field_schema(mixed_column, ColumnDataType::Int64);
        let bool_schema = field_schema(mixed_column, ColumnDataType::Boolean);
        let string_schema = field_schema(unrelated_column, ColumnDataType::String);
        let mut request_schema = TraceRequestSchema::default();
        request_schema.observe_trace_column(0, &int_schema, Some(ColumnDataType::Int64));
        request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
        request_schema.observe_trace_column(1, &bool_schema, Some(ColumnDataType::Boolean));
        request_schema.observe_trace_column(1, &string_schema, Some(ColumnDataType::String));

        let TraceRequestSchemaPlan::IncompatibleObservations(exclusions) =
            request_schema.resolve_table_schema(None)
        else {
            panic!("expected incompatible trace schema batches");
        };
        assert_eq!(
            exclusions,
            HashMap::from([(mixed_column.to_string(), HashSet::from([0, 1]))])
        );

        request_schema.remove_observations(&exclusions);
        let plan = ready_plan(request_schema.resolve_table_schema(None));
        assert_eq!(plan.ensure_columns.len(), 1);
        assert_eq!(plan.ensure_columns[0].column_name, unrelated_column);
    }

    #[test]
    fn test_trace_request_schema_isolates_failed_batch() {
        let table_name = "trace_failed_batch";
        let column_name = "span_attributes.value";
        let failed_only_column = "span_attributes.server.port";
        let safe_column = "span_attributes.safe";
        let string_schema = field_schema(column_name, ColumnDataType::String);
        let float_schema = field_schema(column_name, ColumnDataType::Float64);
        let failed_only_schema = field_schema(failed_only_column, ColumnDataType::String);
        let safe_schema = field_schema(safe_column, ColumnDataType::Boolean);

        let good_requests = row_insert_request(
            table_name,
            vec![string_schema.clone()],
            vec![row(vec![Some(ValueData::StringValue("1.5".to_string()))])],
        );
        let bad_requests = row_insert_request(
            table_name,
            vec![
                string_schema.clone(),
                failed_only_schema.clone(),
                safe_schema.clone(),
            ],
            vec![row(vec![
                Some(ValueData::StringValue("not-a-number".to_string())),
                Some(ValueData::StringValue("invalid-port".to_string())),
                Some(ValueData::BoolValue(true)),
            ])],
        );

        let mut request_schema = TraceRequestSchema::default();
        request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
        request_schema.observe_trace_column(0, &float_schema, Some(ColumnDataType::Float64));
        request_schema.observe_trace_column(1, &string_schema, Some(ColumnDataType::String));
        request_schema.observe_trace_column(1, &failed_only_schema, Some(ColumnDataType::String));
        request_schema.observe_trace_column(1, &safe_schema, Some(ColumnDataType::Boolean));
        ready_plan(request_schema.resolve_table_schema(None));

        let mut chunks = vec![
            TraceChunkInsert {
                span_metadata: Vec::new(),
                request: good_requests,
                retry_columns: HashMap::new(),
                schema_prepared: true,
                span_fallback_only: false,
            },
            TraceChunkInsert {
                span_metadata: Vec::new(),
                request: bad_requests.clone(),
                retry_columns: HashMap::new(),
                schema_prepared: true,
                span_fallback_only: false,
            },
        ];

        let (_, exclusions) =
            Instance::prepare_trace_v1_chunk_rewrites(&request_schema, &mut chunks).unwrap();
        assert_eq!(
            exclusions,
            HashMap::from([(column_name.to_string(), HashSet::from([1]))])
        );
        assert!(chunks[0].schema_prepared);
        assert!(!chunks[1].schema_prepared);

        request_schema.remove_observations(&exclusions);
        ready_plan(request_schema.resolve_table_schema(None));
        let (_, exclusions) =
            Instance::prepare_trace_v1_chunk_rewrites(&request_schema, &mut chunks).unwrap();
        assert_eq!(
            exclusions,
            HashMap::from([(failed_only_column.to_string(), HashSet::from([1]))])
        );
        request_schema.remove_observations(&exclusions);
        assert!(
            !request_schema
                .column_indexes
                .contains_key(failed_only_column)
        );
        assert!(request_schema.column_indexes.contains_key(safe_column));
        let plan = ready_plan(request_schema.resolve_table_schema(None));
        assert!(
            plan.ensure_columns
                .iter()
                .any(|column| column.column_name == safe_column)
        );
        let (prepared, exclusions) =
            Instance::prepare_trace_v1_chunk_rewrites(&request_schema, &mut chunks).unwrap();
        assert!(exclusions.is_empty());
        Instance::apply_prepared_trace_v1_chunk_rewrites(&mut chunks, prepared);

        let good_rows = chunks[0].request.rows.as_ref().unwrap();
        assert_eq!(good_rows.schema[0].datatype, ColumnDataType::Float64 as i32);
        assert_eq!(
            good_rows.rows[0].values[0].value_data,
            Some(ValueData::F64Value(1.5))
        );
        assert_eq!(chunks[1].request, bad_requests);
    }

    #[test]
    fn test_trace_request_schema_uses_global_columns_and_types() {
        let table_name = "trace_global_schema";
        let attr_num = "span_attributes.attr_num";
        let attr_later = "span_attributes.attr_later";
        let mut first_chunk = row_insert_request(
            table_name,
            vec![field_schema(attr_num, ColumnDataType::Int64)],
            vec![row(vec![Some(ValueData::I64Value(1))])],
        );
        let mut second_chunk = row_insert_request(
            table_name,
            vec![
                field_schema(attr_num, ColumnDataType::Float64),
                field_schema(attr_later, ColumnDataType::Boolean),
            ],
            vec![row(vec![
                Some(ValueData::F64Value(2.5)),
                Some(ValueData::BoolValue(true)),
            ])],
        );

        let mut request_schema = TraceRequestSchema::default();
        let attr_num_i64 = field_schema(attr_num, ColumnDataType::Int64);
        let attr_num_f64 = field_schema(attr_num, ColumnDataType::Float64);
        let attr_later_bool = field_schema(attr_later, ColumnDataType::Boolean);
        request_schema.observe_trace_column(0, &attr_num_i64, Some(ColumnDataType::Int64));
        request_schema.observe_trace_column(1, &attr_num_f64, Some(ColumnDataType::Float64));
        request_schema.observe_trace_column(1, &attr_later_bool, Some(ColumnDataType::Boolean));

        let existing_schema =
            DatatypesSchemaBuilder::try_from_columns(vec![DatatypesColumnSchema::new(
                attr_num,
                ConcreteDataType::int64_datatype(),
                true,
            )])
            .unwrap()
            .build()
            .unwrap();
        let pre_alter = ready_plan(request_schema.resolve_table_schema(Some(&existing_schema)));
        assert_eq!(pre_alter.modify_float64_columns, vec![attr_num.to_string()]);
        assert_eq!(pre_alter.ensure_columns.len(), 1);
        assert_eq!(pre_alter.ensure_columns[0].column_name, attr_later);

        prepare_and_apply(&request_schema, 0, &mut first_chunk);
        prepare_and_apply(&request_schema, 1, &mut second_chunk);

        let rows = first_chunk.rows.as_ref().unwrap();
        assert_eq!(rows.schema.len(), 1);
        assert_eq!(rows.schema[0].column_name, attr_num);
        assert_eq!(rows.schema[0].datatype, ColumnDataType::Float64 as i32);
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::F64Value(1.0))
        );

        let rows = second_chunk.rows.as_ref().unwrap();
        assert_eq!(rows.schema.len(), 2);
        assert_eq!(rows.schema[0].column_name, attr_num);
        assert_eq!(rows.schema[0].datatype, ColumnDataType::Float64 as i32);
        assert_eq!(rows.schema[1].column_name, attr_later);
        assert_eq!(rows.schema[1].datatype, ColumnDataType::Boolean as i32);
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::F64Value(2.5))
        );
        assert_eq!(
            rows.rows[0].values[1].value_data,
            Some(ValueData::BoolValue(true))
        );
    }

    #[test]
    fn test_trace_request_schema_creates_table_without_padding_batches() {
        let table_name = "trace_create_schema";
        let attr_num = "span_attributes.attr_num";
        let attr_later = "span_attributes.attr_later";
        let mut first_chunk = row_insert_request(
            table_name,
            vec![field_schema(attr_num, ColumnDataType::Int64)],
            vec![row(vec![Some(ValueData::I64Value(1))])],
        );
        let mut second_chunk = row_insert_request(
            table_name,
            vec![
                field_schema(attr_num, ColumnDataType::Float64),
                field_schema(attr_later, ColumnDataType::Boolean),
            ],
            vec![row(vec![
                Some(ValueData::F64Value(2.5)),
                Some(ValueData::BoolValue(true)),
            ])],
        );

        let mut request_schema = TraceRequestSchema::default();
        let attr_num_i64 = field_schema(attr_num, ColumnDataType::Int64);
        let attr_num_f64 = field_schema(attr_num, ColumnDataType::Float64);
        let attr_later_bool = field_schema(attr_later, ColumnDataType::Boolean);
        request_schema.observe_trace_column(0, &attr_num_i64, Some(ColumnDataType::Int64));
        request_schema.observe_trace_column(1, &attr_num_f64, Some(ColumnDataType::Float64));
        request_schema.observe_trace_column(1, &attr_later_bool, Some(ColumnDataType::Boolean));

        let pre_alter = ready_plan(request_schema.resolve_table_schema(None));
        assert!(pre_alter.modify_float64_columns.is_empty());
        assert_eq!(pre_alter.ensure_columns.len(), 2);
        assert_eq!(pre_alter.ensure_columns[0].column_name, attr_num);
        assert_eq!(
            pre_alter.ensure_columns[0].datatype,
            ColumnDataType::Float64 as i32
        );
        assert_eq!(pre_alter.ensure_columns[1].column_name, attr_later);

        prepare_and_apply(&request_schema, 0, &mut first_chunk);
        prepare_and_apply(&request_schema, 1, &mut second_chunk);

        let rows = first_chunk.rows.as_ref().unwrap();
        assert_eq!(rows.schema.len(), 1);
        assert_eq!(rows.schema[0].column_name, attr_num);
        assert_eq!(rows.schema[0].datatype, ColumnDataType::Float64 as i32);
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::F64Value(1.0))
        );
        assert_eq!(rows.rows[0].values.len(), 1);

        let rows = second_chunk.rows.as_ref().unwrap();
        assert_eq!(rows.schema.len(), 2);
        assert_eq!(rows.schema[0].column_name, attr_num);
        assert_eq!(rows.schema[1].column_name, attr_later);
    }

    #[test]
    fn test_trace_request_schema_re_resolves_after_concurrent_create() {
        let table_name = "trace_concurrent_create";
        let column_name = "span_attributes.value";
        let string_schema = field_schema(column_name, ColumnDataType::String);
        let mut requests = row_insert_request(
            table_name,
            vec![string_schema.clone()],
            vec![row(vec![Some(ValueData::StringValue("42".to_string()))])],
        );

        let mut request_schema = TraceRequestSchema::default();
        request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
        let initial_plan = ready_plan(request_schema.resolve_table_schema(None));
        assert!(!initial_plan.ensure_columns.is_empty());
        let initial_targets = request_schema.resolved_target_types();

        let concurrent_schema =
            DatatypesSchemaBuilder::try_from_columns(vec![DatatypesColumnSchema::new(
                column_name,
                ConcreteDataType::int64_datatype(),
                true,
            )])
            .unwrap()
            .build()
            .unwrap();
        let final_plan = ready_plan(request_schema.resolve_table_schema(Some(&concurrent_schema)));
        assert!(!final_plan.requires_ddl());
        assert_ne!(initial_targets, request_schema.resolved_target_types());

        prepare_and_apply(&request_schema, 0, &mut requests);
        let rows = requests.rows.as_ref().unwrap();
        assert_eq!(rows.schema[0].datatype, ColumnDataType::Int64 as i32);
        assert_eq!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::I64Value(42))
        );
    }

    #[test]
    fn test_trace_request_schema_keeps_request_intact_on_coercion_failure() {
        let table_name = "trace_atomic_rewrite";
        let column_name = "span_attributes.value";
        let string_schema = field_schema(column_name, ColumnDataType::String);
        let float_schema = field_schema(column_name, ColumnDataType::Float64);
        let requests = row_insert_request(
            table_name,
            vec![string_schema.clone()],
            vec![row(vec![Some(ValueData::StringValue(
                "not-a-number".to_string(),
            ))])],
        );
        let original = requests.clone();

        let mut request_schema = TraceRequestSchema::default();
        request_schema.observe_trace_column(0, &string_schema, Some(ColumnDataType::String));
        request_schema.observe_trace_column(0, &float_schema, Some(ColumnDataType::Float64));
        ready_plan(request_schema.resolve_table_schema(None));

        assert!(
            request_schema
                .prepare_request_rewrite(0, &requests)
                .is_err()
        );
        assert_eq!(requests, original);
    }

    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};

    use super::{ExportTraceServiceRequest, trace_conventions};

    fn resource_spans(resource_url: &str, scope_urls: &[&str]) -> ResourceSpans {
        ResourceSpans {
            schema_url: resource_url.to_string(),
            scope_spans: scope_urls
                .iter()
                .map(|u| ScopeSpans {
                    schema_url: u.to_string(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn test_trace_conventions() {
        let unknown = ExportTraceServiceRequest::default();
        assert_eq!(trace_conventions(&unknown), "unknown");

        let url = "https://opentelemetry.io/schemas/1.27.0";
        let single = ExportTraceServiceRequest {
            resource_spans: vec![resource_spans("", &[url, url])],
        };
        assert_eq!(trace_conventions(&single), url);

        let resource_level = ExportTraceServiceRequest {
            resource_spans: vec![resource_spans(url, &[""])],
        };
        assert_eq!(trace_conventions(&resource_level), url);

        let conflicting = ExportTraceServiceRequest {
            resource_spans: vec![resource_spans(
                "",
                &[url, "https://opentelemetry.io/schemas/1.30.0"],
            )],
        };
        assert_eq!(trace_conventions(&conflicting), "mixed");
    }
}
