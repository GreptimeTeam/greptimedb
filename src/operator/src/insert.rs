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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use api::v1::alter_table_expr::Kind;
use api::v1::column_def::options_from_skipping;
use api::v1::region::{
    InsertRequest as RegionInsertRequest, InsertRequests as RegionInsertRequests,
    RegionRequestHeader,
};
use api::v1::{
    AlterTableExpr, ColumnDataType, ColumnSchema, CreateTableExpr, InsertRequests,
    RowInsertRequest, RowInsertRequests, SemanticType,
};
use catalog::CatalogManagerRef;
use client::{OutputData, OutputMeta};
use common_catalog::consts::{
    DEFAULT_PRIVATE_SCHEMA_NAME, PARENT_SPAN_ID_COLUMN, SERVICE_NAME_COLUMN, TRACE_ID_COLUMN,
    TRACE_TABLE_NAME, TRACE_TABLE_NAME_SESSION_KEY, default_engine, is_ddl_reserved_table,
    trace_operations_table_name, trace_services_table_name,
};
use common_event_recorder::DEFAULT_EVENTS_TABLE_NAME;
use common_frontend::slow_query_event::SLOW_QUERY_TABLE_NAME;
use common_grpc_expr::util::ColumnExpr;
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::datanode::REGION_STATS_HISTORY_TABLE_NAME;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
use common_meta::rpc::ddl::TriggerReason;
use common_query::Output;
use common_query::native_histogram::{is_native_histogram_value_type, native_histogram_value_type};
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info, warn};
use datatypes::schema::SkippingIndexOptions;
use meter_macros::write_meter;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::ResultExt;
use snafu::prelude::*;
use sql::partition::partition_rule_for_hexstring;
use sql::statements::create::Partitions;
use sql::statements::insert::Insert;
use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};
use store_api::mito_engine_options::{
    APPEND_MODE_KEY, COMPACTION_TYPE, COMPACTION_TYPE_TWCS, MERGE_MODE_KEY, TTL_KEY,
    TWCS_TIME_WINDOW,
};
use store_api::storage::{RegionId, TableId};
use table::TableRef;
use table::metadata::TableInfo;
use table::requests::{
    AUTO_CREATE_TABLE_KEY, InsertRequest as TableInsertRequest, SEMANTIC_PER_TABLE_INDEX_KEY,
    TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1, TRACE_TABLE_PARTITIONS_HINT_KEY,
    VALID_TABLE_OPTION_KEYS, is_semantic_option_key, validate_semantic_option,
};
use table::table_reference::TableReference;
use tokio_util::sync::CancellationToken;

use crate::error::{
    CatalogSnafu, ColumnOptionsSnafu, CreatePartitionRulesSnafu, InvalidInsertRequestSnafu,
    JoinTaskSnafu, RequestInsertsSnafu, Result, TableNotFoundSnafu, WriteRejectedSnafu,
};
use crate::expr_helper;
use crate::region_req_factory::RegionRequestFactory;
use crate::req_convert::common::preprocess_row_insert_requests;
use crate::req_convert::insert::{
    ColumnToRow, RowToRegion, StatementToRegion, TableToRegion, fill_reqs_with_impure_default,
};
use crate::statement::StatementExecutor;
use crate::write_dispatcher::{WriteDispatcher, WriteDispatcherRef};

pub struct Inserter {
    catalog_manager: CatalogManagerRef,
    pub(crate) partition_manager: PartitionRuleManagerRef,
    pub(crate) node_manager: NodeManagerRef,
    pub(crate) write_dispatcher: WriteDispatcherRef,
    pub(crate) table_flownode_set_cache: TableFlownodeSetCacheRef,
    /// Server-side upper bound for auto table creation on write.
    /// When `false`, missing tables are never auto-created regardless of the
    /// per-request `auto_create_table` hint. When `true`, the hint still applies.
    auto_create_table: bool,
    /// Rows handed to detached flow mirror tasks that have not finished yet.
    /// Bounds how much row data in-flight mirror writes may keep alive, see
    /// [`MAX_MIRROR_PENDING_ROWS`].
    mirror_pending_rows: Arc<AtomicU64>,
}

pub type InserterRef = Arc<Inserter>;

/// Hint for the table type to create automatically.
#[derive(Clone)]
pub enum AutoCreateTableType {
    /// A logical table with the physical table name.
    Logical(String),
    /// A physical table.
    Physical,
    /// A log table which is append-only.
    Log,
    /// A table that merges rows by `last_non_null` strategy.
    LastNonNull,
    /// Create table that build index and default partition rules on trace_id
    Trace { alter_existing: bool },
}

impl AutoCreateTableType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AutoCreateTableType::Logical(_) => "logical",
            AutoCreateTableType::Physical => "physical",
            AutoCreateTableType::Log => "log",
            AutoCreateTableType::LastNonNull => "last_non_null",
            AutoCreateTableType::Trace { .. } => "trace",
        }
    }

    fn alter_existing(&self) -> bool {
        !matches!(
            self,
            Self::Trace {
                alter_existing: false
            }
        )
    }
}

/// Split insert requests into normal and instant requests.
///
/// Where instant requests are requests with ttl=instant,
/// and normal requests are requests with ttl set to other values.
///
/// This is used to split requests for different processing.
#[derive(Clone)]
pub struct InstantAndNormalInsertRequests {
    /// Requests with normal ttl.
    pub normal_requests: RegionInsertRequests,
    /// Requests with ttl=instant.
    /// Will be discarded immediately at frontend, wouldn't even insert into memtable, and only sent to flow node if needed.
    pub instant_requests: RegionInsertRequests,
}

impl Inserter {
    /// Creates a row inserter with the default write dispatcher.
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
        table_flownode_set_cache: TableFlownodeSetCacheRef,
        auto_create_table: bool,
    ) -> Self {
        Self {
            write_dispatcher: Arc::new(WriteDispatcher::new(
                node_manager.clone(),
                partition_manager.clone(),
            )),
            catalog_manager,
            partition_manager,
            node_manager,
            table_flownode_set_cache,
            auto_create_table,
            mirror_pending_rows: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Installs the shared dispatcher used by direct writes.
    pub fn with_write_dispatcher(mut self, dispatcher: WriteDispatcherRef) -> Self {
        self.write_dispatcher = dispatcher;
        self
    }

    pub async fn handle_column_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<Output> {
        let row_inserts = ColumnToRow::convert(requests)?;
        self.handle_row_inserts(row_inserts, ctx, statement_executor, false, false)
            .await
    }

    /// Handles row inserts request and creates a physical table on demand.
    pub async fn handle_row_inserts(
        &self,
        mut requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
        accommodate_existing_schema: bool,
        is_single_value: bool,
    ) -> Result<Output> {
        preprocess_row_insert_requests(&mut requests.inserts)?;
        self.handle_row_inserts_with_create_type(
            requests,
            ctx,
            statement_executor,
            AutoCreateTableType::Physical,
            accommodate_existing_schema,
            is_single_value,
        )
        .await
    }

    /// Handles row inserts request and creates a log table on demand.
    pub async fn handle_log_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<Output> {
        self.handle_row_inserts_with_create_type(
            requests,
            ctx,
            statement_executor,
            AutoCreateTableType::Log,
            false,
            false,
        )
        .await
    }

    pub async fn handle_trace_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<Output> {
        self.handle_row_inserts_with_create_type(
            requests,
            ctx,
            statement_executor,
            AutoCreateTableType::Trace {
                alter_existing: true,
            },
            false,
            false,
        )
        .await
    }

    /// Handles row inserts request and creates a table with `last_non_null` merge mode on demand.
    pub async fn handle_last_non_null_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
        accommodate_existing_schema: bool,
        is_single_value: bool,
    ) -> Result<Output> {
        self.handle_row_inserts_with_create_type(
            requests,
            ctx,
            statement_executor,
            AutoCreateTableType::LastNonNull,
            accommodate_existing_schema,
            is_single_value,
        )
        .await
    }

    /// Handles row inserts request with specified [AutoCreateTableType].
    async fn handle_row_inserts_with_create_type(
        &self,
        mut requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
        create_type: AutoCreateTableType,
        accommodate_existing_schema: bool,
        is_single_value: bool,
    ) -> Result<Output> {
        let skip_wal = ctx.skip_wal();

        // remove empty requests
        requests.inserts.retain(|req| {
            req.rows
                .as_ref()
                .map(|r| !r.rows.is_empty())
                .unwrap_or_default()
        });
        validate_column_count_match(&requests)?;

        let CreateAlterTableResult {
            instant_table_ids,
            table_infos,
        } = self
            .create_or_alter_tables_on_demand(
                &mut requests,
                &ctx,
                create_type,
                statement_executor,
                accommodate_existing_schema,
                is_single_value,
            )
            .await?;

        let name_to_info = table_infos
            .values()
            .map(|info| (info.name.clone(), info.clone()))
            .collect::<HashMap<_, _>>();
        let inserts = RowToRegion::new(
            name_to_info,
            instant_table_ids,
            self.partition_manager.as_ref(),
        )
        .convert(requests, skip_wal)
        .await?;

        self.do_request(inserts, &table_infos, &ctx).await
    }

    /// Handles row inserts request with metric engine.
    pub async fn handle_metric_row_inserts(
        &self,
        mut requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
        physical_table: String,
    ) -> Result<Output> {
        let skip_wal = ctx.skip_wal();

        // remove empty requests
        requests.inserts.retain(|req| {
            req.rows
                .as_ref()
                .map(|r| !r.rows.is_empty())
                .unwrap_or_default()
        });
        validate_column_count_match(&requests)?;

        // check and create physical table
        self.create_physical_table_on_demand(&ctx, physical_table.clone(), statement_executor)
            .await?;

        // check and create logical tables
        let CreateAlterTableResult {
            instant_table_ids,
            table_infos,
        } = self
            .create_or_alter_tables_on_demand(
                &mut requests,
                &ctx,
                AutoCreateTableType::Logical(physical_table.clone()),
                statement_executor,
                true,
                true,
            )
            .await?;
        let name_to_info = table_infos
            .values()
            .map(|info| (info.name.clone(), info.clone()))
            .collect::<HashMap<_, _>>();
        let inserts = RowToRegion::new(name_to_info, instant_table_ids, &self.partition_manager)
            .convert(requests, skip_wal)
            .await?;

        self.do_request(inserts, &table_infos, &ctx).await
    }

    pub async fn handle_table_insert(
        &self,
        request: TableInsertRequest,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog = request.catalog_name.as_str();
        let schema = request.schema_name.as_str();
        let table_name = request.table_name.as_str();
        let table = self.get_table(catalog, schema, table_name).await?;
        let table = table.with_context(|| TableNotFoundSnafu {
            table_name: common_catalog::format_full_table_name(catalog, schema, table_name),
        })?;
        let table_info = table.table_info();

        let inserts = TableToRegion::new(&table_info, &self.partition_manager)
            .convert(request)
            .await?;

        let table_infos = HashMap::from_iter([(table_info.table_id(), table_info.clone())]);

        self.do_request(inserts, &table_infos, &ctx).await
    }

    pub async fn handle_statement_insert(
        &self,
        insert: &Insert,
        ctx: &QueryContextRef,
    ) -> Result<Output> {
        let (inserts, table_info) =
            StatementToRegion::new(self.catalog_manager.as_ref(), &self.partition_manager, ctx)
                .convert(insert, ctx)
                .await?;

        let table_infos = HashMap::from_iter([(table_info.table_id(), table_info.clone())]);

        self.do_request(inserts, &table_infos, ctx).await
    }
}

impl Inserter {
    async fn do_request(
        &self,
        requests: InstantAndNormalInsertRequests,
        table_infos: &HashMap<TableId, Arc<TableInfo>>,
        ctx: &QueryContextRef,
    ) -> Result<Output> {
        // Fill impure default values in the request
        let requests = fill_reqs_with_impure_default(table_infos, requests)?;

        let write_cost = write_meter!(
            ctx.current_catalog(),
            ctx.current_schema(),
            requests,
            ctx.channel() as u8
        );
        let request_factory = RegionRequestFactory::new(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            dbname: ctx.get_db_string(),
            ..Default::default()
        });

        let InstantAndNormalInsertRequests {
            normal_requests,
            instant_requests,
        } = requests;

        // Mirror requests for source table to flownode asynchronously
        let flow_mirror_task = FlowMirrorTask::new(
            &self.table_flownode_set_cache,
            normal_requests
                .requests
                .iter()
                .chain(instant_requests.requests.iter()),
        )
        .await?;
        let has_instant_rows = instant_requests.requests.iter().any(|request| {
            request
                .rows
                .as_ref()
                .is_some_and(|rows| !rows.rows.is_empty())
        });
        flow_mirror_task.detach(
            self.node_manager.clone(),
            self.mirror_pending_rows.clone(),
            has_instant_rows,
        )?;

        let dispatcher = self.write_dispatcher.clone();
        let request = request_factory.build_insert(normal_requests);
        let writes = dispatcher.prepare(vec![request]).await?;
        // Dropping the caller stops retries without abandoning in-flight writes.
        let cancellation = CancellationToken::new();
        let _guard = cancellation.clone().drop_guard();
        let affected_rows = common_runtime::spawn_global(async move {
            dispatcher.execute(writes, None, Some(&cancellation)).await
        })
        .await
        .context(JoinTaskSnafu)?
        .context(RequestInsertsSnafu)?
        .affected_rows;
        crate::metrics::DIST_INGEST_ROW_COUNT
            .with_label_values(&[ctx.get_db_string().as_str()])
            .inc_by(affected_rows as u64);
        Ok(Output::new(
            OutputData::AffectedRows(affected_rows),
            OutputMeta::new_with_cost(write_cost as _),
        ))
    }

    /// Returns `Some(reason)` if the config or request hint disables automatic
    /// table creation. Exempt private system tables are handled by
    /// [`Self::is_auto_create_exempt_private_table`].
    fn auto_create_disabled_reason(&self, ctx: &QueryContextRef) -> Result<Option<&'static str>> {
        let auto_create_table_hint = ctx
            .extension(AUTO_CREATE_TABLE_KEY)
            .map(|v| v.parse::<bool>())
            .transpose()
            .map_err(|_| {
                InvalidInsertRequestSnafu {
                    reason: "`auto_create_table` hint must be a boolean",
                }
                .build()
            })?
            .unwrap_or(true);
        Ok(if !self.auto_create_table {
            Some("auto-create table is disabled by frontend config")
        } else if !auto_create_table_hint {
            Some("`auto_create_table` hint is disabled")
        } else {
            None
        })
    }

    /// Returns whether a private system table may infer and reconcile its schema
    /// even when automatic table creation is disabled.
    fn is_auto_create_exempt_private_table(schema: &str, table: &str) -> bool {
        schema == DEFAULT_PRIVATE_SCHEMA_NAME
            && matches!(
                table,
                DEFAULT_EVENTS_TABLE_NAME | SLOW_QUERY_TABLE_NAME | REGION_STATS_HISTORY_TABLE_NAME
            )
    }

    /// Ensures a trace table has the request-global schema without requiring a
    /// padded data row to drive on-demand creation or alteration. When
    /// `alter_existing` is false, a table created after planning is left for the
    /// caller to re-plan.
    pub async fn ensure_trace_table_on_demand(
        &self,
        table_name: &str,
        request_schema: Vec<ColumnSchema>,
        alter_existing: bool,
        ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<()> {
        let mut requests = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: table_name.to_string(),
                rows: Some(api::v1::Rows {
                    schema: request_schema,
                    rows: Vec::new(),
                }),
            }],
        };
        self.create_or_alter_tables_on_demand(
            &mut requests,
            ctx,
            AutoCreateTableType::Trace { alter_existing },
            statement_executor,
            false,
            false,
        )
        .await?;
        Ok(())
    }

    /// Creates or alter tables on demand:
    /// - if table does not exist, create table by inferred CreateExpr
    /// - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    ///
    /// Returns a mapping from table name to table id, where table name is the table name involved in the requests.
    /// This mapping is used in the conversion of RowToRegion.
    ///
    /// `accommodate_existing_schema` is used to determine if the existing schema should override the new schema.
    /// It only works for TIME_INDEX and single VALUE columns. This is for the case where the user creates a table with
    /// custom schema, and then inserts data with endpoints that have default schema setting, like prometheus
    /// remote write. This will modify the `RowInsertRequests` in place.
    /// `is_single_value` indicates whether the default schema only contains single value column so we can accommodate it.
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &mut RowInsertRequests,
        ctx: &QueryContextRef,
        auto_create_table_type: AutoCreateTableType,
        statement_executor: &StatementExecutor,
        accommodate_existing_schema: bool,
        is_single_value: bool,
    ) -> Result<CreateAlterTableResult> {
        let _timer = crate::metrics::CREATE_ALTER_ON_DEMAND
            .with_label_values(&[auto_create_table_type.as_str()])
            .start_timer();
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();

        let auto_create_disabled_reason = self.auto_create_disabled_reason(ctx)?;
        // Enabled batches permit every table, so only disabled batches need a whitelist scan.
        let has_auto_create_exempt_table = auto_create_disabled_reason.is_some()
            && requests
                .inserts
                .iter()
                .any(|req| Self::is_auto_create_exempt_private_table(&schema, &req.table_name));
        let mut table_infos = HashMap::new();
        // Without exempt tables, verify existing tables and reject missing ones without inferring schemas.
        if let Some(disabled_reason) = auto_create_disabled_reason
            && !has_auto_create_exempt_table
        {
            let mut instant_table_ids = HashSet::new();
            for req in &requests.inserts {
                let table = match self.get_table(catalog, &schema, &req.table_name).await? {
                    Some(table) => table,
                    // System-defined table: created canonically by the system,
                    // so the auto-create config/hint does not apply.
                    None if is_ddl_reserved_table(&schema, &req.table_name) => {
                        statement_executor
                            .create_declared_relationships_table(catalog, ctx.clone())
                            .await?
                    }
                    None => {
                        return InvalidInsertRequestSnafu {
                            reason: format!(
                                "Table `{}` does not exist, and {}",
                                req.table_name, disabled_reason
                            ),
                        }
                        .fail();
                    }
                };
                let table_info = table.table_info();
                if table_info.is_ttl_instant_table() {
                    instant_table_ids.insert(table_info.table_id());
                }
                table_infos.insert(table_info.table_id(), table.table_info());
            }
            let ret = CreateAlterTableResult {
                instant_table_ids,
                table_infos,
            };
            return Ok(ret);
        }

        let mut create_tables = vec![];
        let mut alter_tables = vec![];
        let mut need_refresh_table_infos = HashSet::new();
        let mut instant_table_ids = HashSet::new();
        let mut per_table_semantics: Option<Option<PerTableSemanticIndex>> = None;

        for req in &mut requests.inserts {
            // Mixed batches need a per-table decision so an exempt table cannot authorize others.
            let auto_create_allowed = auto_create_disabled_reason.is_none()
                || Self::is_auto_create_exempt_private_table(&schema, &req.table_name);
            match self.get_table(catalog, &schema, &req.table_name).await? {
                Some(table) => {
                    let table_info = table.table_info();
                    if table_info.is_ttl_instant_table() {
                        instant_table_ids.insert(table_info.table_id());
                    }
                    if auto_create_allowed
                        && let Some(alter_expr) = self.get_alter_table_expr_on_demand(
                            req,
                            &table,
                            ctx,
                            accommodate_existing_schema,
                            is_single_value,
                            auto_create_table_type.alter_existing(),
                        )?
                    {
                        alter_tables.push(alter_expr);
                        need_refresh_table_infos.insert((
                            catalog.to_string(),
                            schema.clone(),
                            req.table_name.clone(),
                        ));
                    } else {
                        table_infos.insert(table_info.table_id(), table.table_info());
                    }
                }
                // A DDL-reserved table's definition never derives from the
                // write request; the system creates it canonically, below the
                // user-DDL guard that rejects the generic create path.
                None if is_ddl_reserved_table(&schema, &req.table_name) => {
                    let table = statement_executor
                        .create_declared_relationships_table(catalog, ctx.clone())
                        .await?;
                    let table_info = table.table_info();
                    if table_info.is_ttl_instant_table() {
                        instant_table_ids.insert(table_info.table_id());
                    }
                    table_infos.insert(table_info.table_id(), table_info);
                }
                None if !auto_create_allowed
                    && let Some(disabled_reason) = auto_create_disabled_reason =>
                {
                    return InvalidInsertRequestSnafu {
                        reason: format!(
                            "Table `{}` does not exist, and {}",
                            req.table_name, disabled_reason,
                        ),
                    }
                    .fail();
                }
                None => {
                    let semantic_index = per_table_semantics
                        .get_or_insert_with(|| parse_per_table_semantic_index(ctx))
                        .as_ref();
                    let create_expr = self.get_create_table_expr_on_demand(
                        req,
                        &auto_create_table_type,
                        ctx,
                        semantic_index,
                    )?;
                    create_tables.push(create_expr);
                }
            }
        }

        match auto_create_table_type {
            AutoCreateTableType::Logical(_) => {
                if !create_tables.is_empty() {
                    // Creates logical tables in batch.
                    let tables = self
                        .create_logical_tables(create_tables, ctx, statement_executor)
                        .await?;

                    for table in tables {
                        let table_info = table.table_info();
                        if table_info.is_ttl_instant_table() {
                            instant_table_ids.insert(table_info.table_id());
                        }
                        table_infos.insert(table_info.table_id(), table.table_info());
                    }
                }
                if !alter_tables.is_empty() {
                    // Alter logical tables in batch.
                    statement_executor
                        .alter_logical_tables(alter_tables, ctx.clone(), TriggerReason::AutoAlter)
                        .await?;
                }
            }
            AutoCreateTableType::Physical
            | AutoCreateTableType::Log
            | AutoCreateTableType::LastNonNull => {
                // note that auto create table shouldn't be ttl instant table
                // for it's a very unexpected behavior and should be set by user explicitly
                for create_table in create_tables {
                    let table = self
                        .create_physical_table(create_table, None, ctx, statement_executor)
                        .await?;
                    let table_info = table.table_info();
                    if table_info.is_ttl_instant_table() {
                        instant_table_ids.insert(table_info.table_id());
                    }
                    table_infos.insert(table_info.table_id(), table.table_info());
                }
                for alter_expr in alter_tables.into_iter() {
                    statement_executor
                        .alter_table_inner(alter_expr, ctx.clone(), TriggerReason::AutoAlter)
                        .await?;
                }
            }

            AutoCreateTableType::Trace { .. } => {
                let trace_table_name = ctx
                    .extension(TRACE_TABLE_NAME_SESSION_KEY)
                    .unwrap_or(TRACE_TABLE_NAME);

                let trace_table_partitions = if let Some(trace_table_partitions) =
                    ctx.extension(TRACE_TABLE_PARTITIONS_HINT_KEY)
                {
                    let p = trace_table_partitions.parse::<u32>().map_err(|_| {
                        InvalidInsertRequestSnafu {
                            reason: format!(
                                "Failed to parse trace_table_partitions: {}",
                                trace_table_partitions
                            ),
                        }
                        .build()
                    })?;
                    Some(p)
                } else {
                    None
                };

                // note that auto create table shouldn't be ttl instant table
                // for it's a very unexpected behavior and should be set by user explicitly
                for mut create_table in create_tables {
                    if create_table.table_name == trace_services_table_name(trace_table_name)
                        || create_table.table_name == trace_operations_table_name(trace_table_name)
                    {
                        // Disable append mode for auxiliary tables (services/operations) since they require upsert behavior.
                        create_table
                            .table_options
                            .insert(APPEND_MODE_KEY.to_string(), "false".to_string());
                        // Remove `ttl` key from table options if it exists
                        create_table.table_options.remove(TTL_KEY);

                        let table = self
                            .create_physical_table(create_table, None, ctx, statement_executor)
                            .await?;
                        let table_info = table.table_info();
                        if table_info.is_ttl_instant_table() {
                            instant_table_ids.insert(table_info.table_id());
                        }
                        table_infos.insert(table_info.table_id(), table.table_info());
                    } else {
                        // prebuilt partition rules for uuid data: see the function
                        // for more information
                        let partitions = if matches!(trace_table_partitions, Some(0) | Some(1)) {
                            // disable partitions
                            None
                        } else {
                            let p = partition_rule_for_hexstring(
                                TRACE_ID_COLUMN,
                                trace_table_partitions,
                            )
                            .context(CreatePartitionRulesSnafu)?;
                            Some(p)
                        };

                        // add skip index to
                        // - trace_id: when searching by trace id
                        // - parent_span_id: when searching root span
                        // - span_name: when searching certain types of span
                        let index_columns =
                            [TRACE_ID_COLUMN, PARENT_SPAN_ID_COLUMN, SERVICE_NAME_COLUMN];
                        for index_column in index_columns {
                            if let Some(col) = create_table
                                .column_defs
                                .iter_mut()
                                .find(|c| c.name == index_column)
                            {
                                col.options =
                                    options_from_skipping(&SkippingIndexOptions::default())
                                        .context(ColumnOptionsSnafu)?;
                            } else {
                                warn!(
                                    "Column {} not found when creating index for trace table: {}.",
                                    index_column, create_table.table_name
                                );
                            }
                        }

                        // use table_options to mark table model version
                        create_table.table_options.insert(
                            TABLE_DATA_MODEL.to_string(),
                            TABLE_DATA_MODEL_TRACE_V1.to_string(),
                        );

                        let table = self
                            .create_physical_table(
                                create_table,
                                partitions,
                                ctx,
                                statement_executor,
                            )
                            .await?;
                        let table_info = table.table_info();
                        if table_info.is_ttl_instant_table() {
                            instant_table_ids.insert(table_info.table_id());
                        }
                        table_infos.insert(table_info.table_id(), table.table_info());
                    }
                }
                for alter_expr in alter_tables.into_iter() {
                    statement_executor
                        .alter_table_inner(alter_expr, ctx.clone(), TriggerReason::AutoAlter)
                        .await?;
                }
            }
        }

        // refresh table infos for altered tables
        for (catalog, schema, table_name) in need_refresh_table_infos {
            let table = self
                .get_table(&catalog, &schema, &table_name)
                .await?
                .context(TableNotFoundSnafu {
                    table_name: common_catalog::format_full_table_name(
                        &catalog,
                        &schema,
                        &table_name,
                    ),
                })?;
            let table_info = table.table_info();
            table_infos.insert(table_info.table_id(), table.table_info());
        }

        Ok(CreateAlterTableResult {
            instant_table_ids,
            table_infos,
        })
    }

    async fn create_physical_table_on_demand(
        &self,
        ctx: &QueryContextRef,
        physical_table: String,
        statement_executor: &StatementExecutor,
    ) -> Result<()> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        // check if exist
        if self
            .get_table(catalog_name, &schema_name, &physical_table)
            .await?
            .is_some()
        {
            return Ok(());
        }

        // Gate here too, otherwise a disabled switch would still leak the physical table.
        if let Some(disabled_reason) = self.auto_create_disabled_reason(ctx)? {
            return InvalidInsertRequestSnafu {
                reason: format!(
                    "Physical table `{physical_table}` does not exist, and {disabled_reason}"
                ),
            }
            .fail();
        }

        let table_reference = TableReference::full(catalog_name, &schema_name, &physical_table);
        info!("Physical metric table `{table_reference}` does not exist, try creating table");

        // schema with timestamp and field column
        let default_schema = vec![
            ColumnSchema {
                column_name: greptime_timestamp().to_string(),
                datatype: ColumnDataType::TimestampMillisecond as _,
                semantic_type: SemanticType::Timestamp as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: greptime_value().to_string(),
                datatype: ColumnDataType::Float64 as _,
                semantic_type: SemanticType::Field as _,
                datatype_extension: None,
                options: None,
            },
        ];
        let create_table_expr =
            &mut build_create_table_expr(&table_reference, &default_schema, default_engine())?;

        create_table_expr.engine = METRIC_ENGINE_NAME.to_string();
        create_table_expr
            .table_options
            .insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), "true".to_string());

        // create physical table
        let res = statement_executor
            .create_table_inner(
                create_table_expr,
                None,
                ctx.clone(),
                TriggerReason::AutoCreate,
            )
            .await;

        match res {
            Ok(_) => {
                info!("Successfully created table {table_reference}",);
                Ok(())
            }
            Err(err) => {
                error!(err; "Failed to create table {table_reference}");
                Err(err)
            }
        }
    }

    async fn get_table(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<TableRef>> {
        self.catalog_manager
            .table(catalog, schema, table, None)
            .await
            .context(CatalogSnafu)
    }

    fn get_create_table_expr_on_demand(
        &self,
        req: &RowInsertRequest,
        create_type: &AutoCreateTableType,
        ctx: &QueryContextRef,
        semantic_index: Option<&PerTableSemanticIndex>,
    ) -> Result<CreateTableExpr> {
        let schema = ctx.current_schema();
        let mut table_options = std::collections::HashMap::with_capacity(4);
        fill_table_options_for_create(&mut table_options, create_type, ctx);
        apply_per_table_semantic_options(
            &mut table_options,
            semantic_index,
            ctx.current_schema().as_str(),
            &req.table_name,
        );

        let engine_name = if let AutoCreateTableType::Logical(_) = create_type {
            // engine should be metric engine when creating logical tables.
            METRIC_ENGINE_NAME
        } else {
            default_engine()
        };

        let table_ref = TableReference::full(ctx.current_catalog(), &schema, &req.table_name);
        // SAFETY: `req.rows` is guaranteed to be `Some` by `handle_row_inserts_with_create_type()`.
        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let mut create_table_expr =
            build_create_table_expr(&table_ref, request_schema, engine_name)?;

        // extension set by the Splunk HEC handler for identity path
        if ctx.extension(SPLUNK_PK_METADATA_ORDER_KEY).is_some() {
            reorder_splunk_primary_keys(&mut create_table_expr.primary_keys);
        }

        info!("Table `{table_ref}` does not exist, try creating table");
        create_table_expr.table_options.extend(table_options);
        Ok(create_table_expr)
    }

    /// Returns an alter table expression if it finds new columns in the request.
    /// When `accommodate_existing_schema` is false, it always adds columns if not exist.
    /// When `accommodate_existing_schema` is true, it may modify the input `req` to
    /// accommodate it with existing schema. See [`create_or_alter_tables_on_demand`](Self::create_or_alter_tables_on_demand)
    /// for more details.
    /// When `is_single_value` is true, it also rejects native-histogram/float kind changes.
    /// When both options are true, it considers fields when modifying the input `req`.
    fn get_alter_table_expr_on_demand(
        &self,
        req: &mut RowInsertRequest,
        table: &TableRef,
        ctx: &QueryContextRef,
        accommodate_existing_schema: bool,
        is_single_value: bool,
        alter_existing: bool,
    ) -> Result<Option<AlterTableExpr>> {
        if !alter_existing {
            return Ok(None);
        }

        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();
        let table_name = table.table_info().name.clone();

        // Never auto-alter a system-defined table to fit a write; a request
        // with unknown columns fails instead.
        if is_ddl_reserved_table(&schema_name, &table_name) {
            return Ok(None);
        }

        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let request_field_count = request_schema
            .iter()
            .filter(|col| col.semantic_type == SemanticType::Field as i32)
            .count();
        let column_exprs = ColumnExpr::from_column_schemas(request_schema);
        let add_columns = expr_helper::extract_add_columns_expr(&table.schema(), column_exprs)?;
        let Some(mut add_columns) = add_columns else {
            return Ok(None);
        };

        if is_single_value {
            let request_is_native_histogram = request_is_native_histogram(request_schema);
            let table_is_native_histogram = table_is_native_histogram(table);
            ensure!(
                request_is_native_histogram == table_is_native_histogram,
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "Table `{table_name}` cannot mix native histogram and float sample fields"
                    ),
                }
            );
        }

        // If accommodate_existing_schema is true, update request schema for Timestamp/Field columns
        if accommodate_existing_schema {
            let table_schema = table.schema();
            // Find timestamp column name
            let ts_col_name = table_schema.timestamp_column().map(|c| c.name.clone());
            // Find field column name if there is only one and `is_single_value` is true.
            let mut field_col_name = None;
            if is_single_value && request_field_count <= 1 {
                let mut multiple_field_cols = false;
                table.field_columns().for_each(|col| {
                    if field_col_name.is_none() {
                        field_col_name = Some(col.name.clone());
                    } else {
                        multiple_field_cols = true;
                    }
                });
                if multiple_field_cols {
                    field_col_name = None;
                }
            }

            // Update column name in request schema for Timestamp/Field columns
            if let Some(rows) = req.rows.as_mut() {
                for col in &mut rows.schema {
                    match col.semantic_type {
                        x if x == SemanticType::Timestamp as i32 => {
                            if let Some(ref ts_name) = ts_col_name
                                && col.column_name != *ts_name
                            {
                                col.column_name = ts_name.clone();
                            }
                        }
                        x if x == SemanticType::Field as i32 => {
                            if let Some(ref field_name) = field_col_name
                                && col.column_name != *field_name
                            {
                                col.column_name = field_name.clone();
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Only keep columns that are tags or non-single field.
            add_columns.add_columns.retain(|col| {
                let def = col.column_def.as_ref().unwrap();
                def.semantic_type == SemanticType::Tag as i32
                    || (def.semantic_type == SemanticType::Field as i32 && field_col_name.is_none())
            });

            if add_columns.add_columns.is_empty() {
                return Ok(None);
            }
        }

        Ok(Some(AlterTableExpr {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.clone(),
            table_name: table_name.clone(),
            kind: Some(Kind::AddColumns(add_columns)),
        }))
    }

    /// Creates a table with options.
    async fn create_physical_table(
        &self,
        mut create_table_expr: CreateTableExpr,
        partitions: Option<Partitions>,
        ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<TableRef> {
        {
            let table_ref = TableReference::full(
                &create_table_expr.catalog_name,
                &create_table_expr.schema_name,
                &create_table_expr.table_name,
            );

            info!("Table `{table_ref}` does not exist, try creating table");
        }
        let res = statement_executor
            .create_table_inner(
                &mut create_table_expr,
                partitions,
                ctx.clone(),
                TriggerReason::AutoCreate,
            )
            .await;

        let table_ref = TableReference::full(
            &create_table_expr.catalog_name,
            &create_table_expr.schema_name,
            &create_table_expr.table_name,
        );

        match res {
            Ok(table) => {
                info!(
                    "Successfully created table {} with options: {:?}",
                    table_ref, create_table_expr.table_options,
                );
                Ok(table)
            }
            Err(err) => {
                error!(err; "Failed to create table {}", table_ref);
                Err(err)
            }
        }
    }

    async fn create_logical_tables(
        &self,
        create_table_exprs: Vec<CreateTableExpr>,
        ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<Vec<TableRef>> {
        let res = statement_executor
            .create_logical_tables(&create_table_exprs, ctx.clone(), TriggerReason::AutoCreate)
            .await;

        match res {
            Ok(res) => {
                info!("Successfully created logical tables");
                Ok(res)
            }
            Err(err) => {
                let failed_tables = create_table_exprs
                    .into_iter()
                    .map(|expr| {
                        format!(
                            "{}.{}.{}",
                            expr.catalog_name, expr.schema_name, expr.table_name
                        )
                    })
                    .collect::<Vec<_>>();
                error!(
                    err;
                    "Failed to create logical tables {:?}",
                    failed_tables
                );
                Err(err)
            }
        }
    }

    /// Returns the dispatcher shared with batching execution paths.
    pub fn write_dispatcher(&self) -> &WriteDispatcherRef {
        &self.write_dispatcher
    }

    pub fn node_manager(&self) -> &NodeManagerRef {
        &self.node_manager
    }

    pub fn partition_manager(&self) -> &PartitionRuleManagerRef {
        &self.partition_manager
    }

    pub fn table_flownode_set_cache(&self) -> &TableFlownodeSetCacheRef {
        &self.table_flownode_set_cache
    }
}

fn request_is_native_histogram(request_schema: &[ColumnSchema]) -> bool {
    let mut fields = request_schema
        .iter()
        .filter(|col| col.semantic_type == SemanticType::Field as i32);
    let Some(col) = fields.next() else {
        return false;
    };

    fields.next().is_none()
        && api::helper::is_column_type_value_eq(
            col.datatype,
            col.datatype_extension.clone(),
            native_histogram_value_type(),
        )
}

fn table_is_native_histogram(table: &TableRef) -> bool {
    let mut fields = table.field_columns();
    let Some(col) = fields.next() else {
        return false;
    };

    fields.next().is_none() && is_native_histogram_value_type(&col.data_type)
}

fn validate_column_count_match(requests: &RowInsertRequests) -> Result<()> {
    for request in &requests.inserts {
        let rows = request.rows.as_ref().unwrap();
        let column_count = rows.schema.len();
        rows.rows.iter().try_for_each(|r| {
            ensure!(
                r.values.len() == column_count,
                InvalidInsertRequestSnafu {
                    reason: format!(
                        "column count mismatch, columns: {}, values: {}",
                        column_count,
                        r.values.len()
                    )
                }
            );
            Ok(())
        })?;
    }
    Ok(())
}

/// Fill table options for a new table by create type.
pub fn fill_table_options_for_create(
    table_options: &mut std::collections::HashMap<String, String>,
    create_type: &AutoCreateTableType,
    ctx: &QueryContextRef,
) {
    for key in VALID_TABLE_OPTION_KEYS {
        if let Some(value) = ctx.extension(key) {
            table_options.insert(key.to_string(), value.to_string());
        }
    }

    // Semantic keys use their own vocabulary instead of the fixed option list.
    for (key, value) in ctx.extensions() {
        if is_semantic_option_key(&key) && validate_semantic_option(&key, &value) {
            table_options.insert(key, value);
        }
    }

    match create_type {
        AutoCreateTableType::Logical(physical_table) => {
            table_options.insert(
                LOGICAL_TABLE_METADATA_KEY.to_string(),
                physical_table.clone(),
            );
        }
        AutoCreateTableType::Physical => {
            if let Some(append_mode) = ctx.extension(APPEND_MODE_KEY) {
                table_options.insert(APPEND_MODE_KEY.to_string(), append_mode.to_string());
            }
            if let Some(merge_mode) = ctx.extension(MERGE_MODE_KEY) {
                table_options.insert(MERGE_MODE_KEY.to_string(), merge_mode.to_string());
            }
            if let Some(time_window) = ctx.extension(TWCS_TIME_WINDOW) {
                table_options.insert(TWCS_TIME_WINDOW.to_string(), time_window.to_string());
                // We need to set the compaction type explicitly.
                table_options.insert(
                    COMPACTION_TYPE.to_string(),
                    COMPACTION_TYPE_TWCS.to_string(),
                );
            }
        }
        // Set append_mode to true for log table.
        // because log tables should keep rows with the same ts and tags.
        AutoCreateTableType::Log => {
            table_options.insert(APPEND_MODE_KEY.to_string(), "true".to_string());
        }
        AutoCreateTableType::LastNonNull => {
            if ctx
                .extension(APPEND_MODE_KEY)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"))
            {
                table_options.insert(APPEND_MODE_KEY.to_string(), "true".to_string());
                table_options.insert(MERGE_MODE_KEY.to_string(), "last_row".to_string());
            } else if let Some(merge_mode) = ctx.extension(MERGE_MODE_KEY) {
                table_options.insert(MERGE_MODE_KEY.to_string(), merge_mode.to_string());
            } else {
                table_options.insert(MERGE_MODE_KEY.to_string(), "last_non_null".to_string());
            }
        }
        AutoCreateTableType::Trace { .. } => {
            table_options.insert(APPEND_MODE_KEY.to_string(), "true".to_string());
        }
    }
}

/// The parsed per-table semantic index: `{schema -> {table -> {key -> value}}}`,
/// produced by the OTLP metrics encode path (where one metric can fan out into
/// several tables with distinct keys) and the Prometheus remote write v2 path
/// (where per-series metadata declares type/unit, and a series may override its
/// target schema).
pub type PerTableSemanticIndex = BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>;

/// Parses the per-table semantic index off the context extension. Call once per
/// create-planning round: a first write creating N tables would otherwise
/// re-parse the whole index N times. `None` when the request carries no index
/// (logs, traces, Prom RW v1) or it fails to parse.
pub fn parse_per_table_semantic_index(ctx: &QueryContextRef) -> Option<PerTableSemanticIndex> {
    let raw = ctx.extension(SEMANTIC_PER_TABLE_INDEX_KEY)?;
    match serde_json::from_str(raw) {
        Ok(index) => Some(index),
        Err(_) => {
            warn!("failed to parse semantic per-table index, skipping per-table options");
            None
        }
    }
}

/// Folds the semantic keys of the table being created into `table_options`.
///
/// Common keys shared by every table in a request travel as plain semantic
/// extensions and are handled by [`fill_table_options_for_create`]; this
/// carries only the per-table tail and is applied after it, so a per-table
/// value (e.g. `declared` quality) wins. Keys are re-checked against the
/// vocabulary defensively.
pub fn apply_per_table_semantic_options(
    table_options: &mut std::collections::HashMap<String, String>,
    index: Option<&PerTableSemanticIndex>,
    schema: &str,
    table_name: &str,
) {
    let Some(entry) = index
        .and_then(|index| index.get(schema))
        .and_then(|tables| tables.get(table_name))
    else {
        return;
    };
    for (key, value) in entry {
        if is_semantic_option_key(key) && validate_semantic_option(key, value) {
            table_options.insert(key.clone(), value.clone());
        }
    }
}

pub fn build_create_table_expr(
    table: &TableReference,
    request_schema: &[ColumnSchema],
    engine: &str,
) -> Result<CreateTableExpr> {
    expr_helper::create_table_expr_by_column_schemas(table, request_schema, engine, None)
}

/// `QueryContext` extension key the Splunk HEC handler sets (to `"true"`) on its identity
/// path to request metadata-first primary-key ordering at table creation. It is absent for
/// user-supplied pipelines, so their primary-key order is left untouched.
pub const SPLUNK_PK_METADATA_ORDER_KEY: &str = "splunk_pk_metadata_order";

/// Moves Splunk's metadata tags (`host`, `source`, `sourcetype`) to the front of the
/// primary key, keeping the relative order of the remaining tags.
fn reorder_splunk_primary_keys(primary_keys: &mut [String]) {
    const LEAD: [&str; 3] = ["host", "source", "sourcetype"];
    // Stable sort: `LEAD` columns move to the front in `host`/`source`/`sourcetype` order;
    // every other column keeps its existing relative position.
    primary_keys.sort_by_key(|name| {
        LEAD.iter()
            .position(|&lead| lead == name.as_str())
            .unwrap_or(LEAD.len())
    });
}

/// Result of `create_or_alter_tables_on_demand`.
struct CreateAlterTableResult {
    /// table ids of ttl=instant tables.
    instant_table_ids: HashSet<TableId>,
    /// Table Info of the created tables.
    table_infos: HashMap<TableId, Arc<TableInfo>>,
}

/// Upper bound of rows buffered by detached flow mirror tasks on this frontend.
///
/// Rows retained by in-flight mirror RPCs count against this bound. If ordinary
/// persisted source-table mirroring exceeds it, that mirror batch is best-effort
/// dropped while its datanode write continues. Instant-TTL batches fail retryably
/// on temporary saturation; oversized batches fail non-retryably so the client
/// can reduce the batch size.
const MAX_MIRROR_PENDING_ROWS: usize = 1_000_000;

/// How often at most each mirror event is reported.
const MIRROR_LOG_INTERVAL: Duration = Duration::from_secs(10);
const MIRROR_LOG_NEVER_REPORTED: u64 = u64::MAX;

struct MirrorLog {
    last_log_millis: AtomicU64,
    events: AtomicU64,
}

impl MirrorLog {
    const fn new() -> Self {
        Self {
            last_log_millis: AtomicU64::new(MIRROR_LOG_NEVER_REPORTED),
            events: AtomicU64::new(0),
        }
    }

    /// Returns the count since the previous report, including this event, to
    /// the single caller that claims the interval.
    fn claim_report(&self, now_millis: u64) -> Option<u64> {
        self.events.fetch_add(1, Ordering::Relaxed);
        let last = self.last_log_millis.load(Ordering::Relaxed);
        if last != MIRROR_LOG_NEVER_REPORTED
            && now_millis.saturating_sub(last) < MIRROR_LOG_INTERVAL.as_millis() as u64
        {
            return None;
        }
        if self
            .last_log_millis
            .compare_exchange(last, now_millis, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        Some(self.events.swap(0, Ordering::Relaxed))
    }
}

static MIRROR_DROP_LOG: MirrorLog = MirrorLog::new();
static MIRROR_FAILURE_LOG: MirrorLog = MirrorLog::new();

/// Milliseconds since the first call, so the drop log is rate limited on a
/// monotonic clock instead of the wall clock, which can jump.
fn mirror_drop_log_millis() -> u64 {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    START.elapsed().as_millis() as u64
}

struct FlowMirrorTask {
    requests: HashMap<Peer, RegionInsertRequests>,
}

impl FlowMirrorTask {
    async fn new(
        cache: &TableFlownodeSetCacheRef,
        requests: impl Iterator<Item = &RegionInsertRequest>,
    ) -> Result<Self> {
        let mut src_table_reqs: HashMap<TableId, Option<(Vec<Peer>, RegionInsertRequests)>> =
            HashMap::new();

        for req in requests {
            let table_id = RegionId::from_u64(req.region_id).table_id();
            match src_table_reqs.get_mut(&table_id) {
                Some(Some((_peers, reqs))) => reqs.requests.push(req.clone()),
                // already know this is not source table
                Some(None) => continue,
                _ => {
                    // dedup peers
                    let peers = cache
                        .get(table_id)
                        .await
                        .context(RequestInsertsSnafu)?
                        .unwrap_or_default()
                        .values()
                        .cloned()
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>();

                    if !peers.is_empty() {
                        let mut reqs = RegionInsertRequests::default();
                        reqs.requests.push(req.clone());
                        src_table_reqs.insert(table_id, Some((peers, reqs)));
                    } else {
                        // insert a empty entry to avoid repeat query
                        src_table_reqs.insert(table_id, None);
                    }
                }
            }
        }

        let mut inserts: HashMap<Peer, RegionInsertRequests> = HashMap::new();

        for (_table_id, (peers, reqs)) in src_table_reqs
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
        {
            if peers.len() == 1 {
                // fast path, zero copy
                inserts
                    .entry(peers[0].clone())
                    .or_default()
                    .requests
                    .extend(reqs.requests);
                continue;
            } else {
                // TODO(discord9): need to split requests to multiple flownodes
                for flownode in peers {
                    inserts
                        .entry(flownode.clone())
                        .or_default()
                        .requests
                        .extend(reqs.requests.clone());
                }
            }
        }

        Ok(Self { requests: inserts })
    }

    /// Rows of row data this task keeps alive once detached: the payload it
    /// spawns per peer, not the source requests it was built from.
    ///
    /// A source table mapped to several flownodes clones its requests per peer
    /// and every clone owns its own copy of the rows, so reserving from
    /// `self.requests` is what the per-peer shares released by [`Self::detach`]
    /// sum to. Counting the source requests instead under-reserves whenever a
    /// batch holds several requests for the same source table.
    fn pending_rows(&self) -> u64 {
        self.requests.values().map(region_inserts_rows).sum()
    }

    fn detach(
        self,
        node_manager: NodeManagerRef,
        mirror_pending_rows: Arc<AtomicU64>,
        has_instant_rows: bool,
    ) -> Result<()> {
        // Reserve the cloned per-peer payload before spawning. A full budget
        // drops normal mirrors or rejects instant writes before datanode dispatch.
        let num_rows = self.pending_rows();
        if num_rows == 0 {
            return Ok(());
        }
        if has_instant_rows && num_rows > MAX_MIRROR_PENDING_ROWS as u64 {
            return InvalidInsertRequestSnafu {
                reason: format!(
                    "flow mirror batch has {num_rows} peer-fan-out rows, exceeding the limit of {}; reduce the batch size",
                    MAX_MIRROR_PENDING_ROWS
                ),
            }
            .fail();
        }

        let pending = reserve_mirror_pending_rows(&mirror_pending_rows, num_rows);
        if pending > MAX_MIRROR_PENDING_ROWS as u64 {
            release_mirror_pending_rows(&mirror_pending_rows, num_rows);
            if has_instant_rows {
                return WriteRejectedSnafu {
                    reason: "flow mirror pending rows limit exceeded for instant-TTL insert",
                }
                .fail();
            }
            crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.inc_by(num_rows);
            if let Some(events) = MIRROR_DROP_LOG.claim_report(mirror_drop_log_millis()) {
                warn!(
                    "Flow mirror write dropped: {} pending rows exceeds limit {}, {} drop events since last report",
                    pending, MAX_MIRROR_PENDING_ROWS, events
                );
            }
            return Ok(());
        }

        for (peer, inserts) in self.requests {
            // Each spawned task releases exactly its own share of the
            // reservation. The shares are computed over the same per-peer
            // payload as `pending_rows`, so they sum to the reservation.
            let peer_rows = region_inserts_rows(&inserts);
            let node_manager = node_manager.clone();
            let mirror_pending_rows = mirror_pending_rows.clone();
            common_runtime::spawn_global(async move {
                let result = node_manager
                    .flownode(&peer)
                    .await
                    .handle_inserts(inserts)
                    .await
                    .context(RequestInsertsSnafu);

                match result {
                    Ok(resp) => {
                        let affected_rows = resp.affected_rows;
                        crate::metrics::DIST_MIRROR_ROW_COUNT.inc_by(affected_rows);
                    }
                    Err(err) => {
                        if let Some(events) =
                            MIRROR_FAILURE_LOG.claim_report(mirror_drop_log_millis())
                        {
                            error!(err; flownode_id = peer.id, flownode_addr = %peer.addr, "Failed to insert data into flownode ({} total mirror failures across peers since last report)", events);
                        }
                    }
                }
                // Release the reservation on both the success and the failure
                // path, otherwise a broken flownode would fill the budget forever.
                release_mirror_pending_rows(&mirror_pending_rows, peer_rows);
            });
        }

        Ok(())
    }
}

/// Rows of row data carried by `inserts`. A request without a `rows` payload
/// carries nothing to keep buffered and contributes zero.
fn region_inserts_rows(inserts: &RegionInsertRequests) -> u64 {
    inserts
        .requests
        .iter()
        .filter_map(|req| req.rows.as_ref())
        .map(|rows| rows.rows.len() as u64)
        .sum()
}

/// Reserves `rows` of the pending mirror budget and returns the resulting
/// pending row count. The caller must [`release_mirror_pending_rows`] exactly
/// these rows once they are no longer buffered, whether it spawns the batch or
/// drops it.
fn reserve_mirror_pending_rows(mirror_pending_rows: &AtomicU64, rows: u64) -> u64 {
    let pending = mirror_pending_rows.fetch_add(rows, Ordering::Relaxed) + rows;
    // Keep the gauge in step by applying the same increment, never by setting an
    // absolute value: another task may move the budget in between.
    crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.add(rows as i64);
    pending
}

/// Releases `rows` of the pending mirror budget.
///
/// Every release mirrors the reservation it hands back, so a well-behaved
/// release never subtracts more than is pending; the saturating update only
/// keeps a duplicated release from wrapping the shared budget around.
fn release_mirror_pending_rows(mirror_pending_rows: &AtomicU64, rows: u64) {
    let prev = mirror_pending_rows
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
            Some(pending.saturating_sub(rows))
        })
        .unwrap_or_else(|prev| prev);
    // Subtract from the gauge with the opposite sign of
    // [`reserve_mirror_pending_rows`]; reading the budget here and `set`-ting it
    // would publish a stale value whenever another task releases concurrently.
    let released = rows.min(prev);
    if released > 0 {
        crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.sub(released as i64);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use api::helper::ColumnDataTypeWrapper;
    use api::v1::flow::FlowResponse;
    use api::v1::helper::{field_column_schema, time_index_column_schema};
    use api::v1::{RowInsertRequest, Rows, Value};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::cache::new_table_flownode_set_cache;
    use common_meta::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
    use common_meta::test_util::{MockDatanodeManager, MockFlownodeHandler, MockFlownodeManager};
    use common_query::native_histogram::NATIVE_HISTOGRAM_FIELD;
    use common_query::prelude::{greptime_native_histogram, set_default_prefix};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use moka::future::Cache;
    use session::context::QueryContext;
    use table::TableRef;
    use table::dist_table::DummyDataSource;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder, TableType};

    use super::*;
    use crate::tests::{
        create_partition_rule_manager, new_test_table_info, prepare_mocked_backend,
    };

    fn make_table_ref_with_schema(
        ts_name: &str,
        field_name: &str,
        field_type: ConcreteDataType,
    ) -> TableRef {
        let schema = datatypes::schema::SchemaBuilder::try_from_columns(vec![
            ColumnSchema::new(
                ts_name,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(field_name, field_type, true),
        ])
        .unwrap()
        .build()
        .unwrap();
        let meta = TableMetaBuilder::empty()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![])
            .value_indices(vec![1])
            .engine("mito")
            .next_column_id(0)
            .options(Default::default())
            .created_on(Default::default())
            .build()
            .unwrap();
        let info = Arc::new(
            TableInfoBuilder::default()
                .table_id(1)
                .table_version(0)
                .name("test_table")
                .schema_name(DEFAULT_SCHEMA_NAME)
                .catalog_name(DEFAULT_CATALOG_NAME)
                .desc(None)
                .table_type(TableType::Base)
                .meta(meta)
                .build()
                .unwrap(),
        );
        Arc::new(table::Table::new(
            info,
            table::metadata::FilterPushDownType::Unsupported,
            Arc::new(DummyDataSource),
        ))
    }

    #[tokio::test]
    async fn test_accommodate_existing_schema_and_reject_kind_changes() {
        let ts_name = "my_ts";
        let field_name = "my_field";
        let table =
            make_table_ref_with_schema(ts_name, field_name, ConcreteDataType::float64_datatype());

        // The request uses different names for timestamp and field columns
        let mut req = RowInsertRequest {
            table_name: "test_table".to_string(),
            rows: Some(Rows {
                schema: vec![
                    time_index_column_schema("ts_wrong", ColumnDataType::TimestampMillisecond),
                    field_column_schema("field_wrong", ColumnDataType::Float64),
                ],
                rows: vec![api::v1::Row {
                    values: vec![Value::default(), Value::default()],
                }],
            }),
        };
        let ctx = Arc::new(QueryContext::with(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
        ));

        let kv_backend = prepare_mocked_backend().await;
        let inserter = Inserter::new(
            catalog::memory::MemoryCatalogManager::new(),
            create_partition_rule_manager(kv_backend.clone()).await,
            Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)),
            Arc::new(new_table_flownode_set_cache(
                String::new(),
                Cache::new(100),
                kv_backend.clone(),
            )),
            true,
        );
        // Do not apply an absent-table plan to a table that appeared concurrently.
        assert!(
            inserter
                .get_alter_table_expr_on_demand(&mut req, &table, &ctx, false, false, false)
                .unwrap()
                .is_none()
        );
        let alter_expr = inserter
            .get_alter_table_expr_on_demand(&mut req, &table, &ctx, true, true, true)
            .unwrap();
        assert!(alter_expr.is_none());

        // The request's schema should have updated names for timestamp and field columns
        let req_schema = req.rows.as_ref().unwrap().schema.clone();
        assert_eq!(req_schema[0].column_name, ts_name);
        assert_eq!(req_schema[1].column_name, field_name);

        let (datatype, datatype_extension) =
            ColumnDataTypeWrapper::try_from(native_histogram_value_type().clone())
                .unwrap()
                .into_parts();
        let mut histogram_req = RowInsertRequest {
            table_name: "test_table".to_string(),
            rows: Some(Rows {
                schema: vec![
                    time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
                    api::v1::ColumnSchema {
                        column_name: greptime_native_histogram().to_string(),
                        datatype: datatype as i32,
                        semantic_type: SemanticType::Field as i32,
                        datatype_extension,
                        options: None,
                    },
                ],
                rows: vec![],
            }),
        };
        let error = inserter
            .get_alter_table_expr_on_demand(&mut histogram_req, &table, &ctx, false, true, true)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot mix native histogram and float sample fields")
        );

        let histogram_table = make_table_ref_with_schema(
            "ts",
            greptime_native_histogram(),
            native_histogram_value_type().clone(),
        );
        let mut sample_req = RowInsertRequest {
            table_name: "test_table".to_string(),
            rows: Some(Rows {
                schema: vec![
                    time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
                    field_column_schema(greptime_value(), ColumnDataType::Float64),
                ],
                rows: vec![],
            }),
        };
        let error = inserter
            .get_alter_table_expr_on_demand(
                &mut sample_req,
                &histogram_table,
                &ctx,
                false,
                true,
                true,
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot mix native histogram and float sample fields")
        );
    }

    #[test]
    fn test_native_histogram_detection_survives_prefix_change() {
        set_default_prefix(Some("custom")).unwrap();
        let table = make_table_ref_with_schema(
            "custom_timestamp",
            NATIVE_HISTOGRAM_FIELD,
            native_histogram_value_type().clone(),
        );
        let (datatype, datatype_extension) =
            ColumnDataTypeWrapper::try_from(native_histogram_value_type().clone())
                .unwrap()
                .into_parts();
        let request_schema = [api::v1::ColumnSchema {
            column_name: greptime_native_histogram().to_string(),
            datatype: datatype as i32,
            semantic_type: SemanticType::Field as i32,
            datatype_extension,
            options: None,
        }];

        assert!(request_is_native_histogram(&request_schema));
        assert!(table_is_native_histogram(&table));
    }

    #[test]
    fn test_skip_wal_does_not_change_table_options() {
        check_skip_wal_does_not_change_table_options(false);
        check_skip_wal_does_not_change_table_options(true);
    }

    fn check_skip_wal_does_not_change_table_options(skip_wal: bool) {
        let ctx = Arc::new(QueryContext::with(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
        ));
        ctx.set_skip_wal(skip_wal);
        let mut options = Default::default();
        fill_table_options_for_create(&mut options, &AutoCreateTableType::Physical, &ctx);
        assert!(!options.contains_key(session::hints::INSERT_SKIP_WAL_HINT));
        assert!(!options.contains_key("skip_wal"));
    }

    #[test]
    fn test_last_non_null_create_options_preserve_default_without_append_mode() {
        let ctx = Arc::new(QueryContext::with(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
        ));
        let mut table_options = Default::default();

        fill_table_options_for_create(&mut table_options, &AutoCreateTableType::LastNonNull, &ctx);

        assert_eq!(
            Some("last_non_null"),
            table_options.get(MERGE_MODE_KEY).map(String::as_str)
        );
        assert!(!table_options.contains_key(APPEND_MODE_KEY));
    }

    #[test]
    fn test_fill_table_options_copies_semantic_extensions() {
        use table::requests::{
            SEMANTIC_METRIC_TYPE, SEMANTIC_PER_TABLE_INDEX_KEY, SEMANTIC_SIGNAL_TYPE,
            SEMANTIC_SOURCE, SEMANTIC_SOURCE_VERSION, SIGNAL_TYPE_METRIC, SOURCE_OPENTELEMETRY,
        };

        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        ctx.set_extension(SEMANTIC_SIGNAL_TYPE, SIGNAL_TYPE_METRIC);
        ctx.set_extension(SEMANTIC_SOURCE, SOURCE_OPENTELEMETRY);
        ctx.set_extension(SEMANTIC_SOURCE_VERSION, "2.0");
        ctx.set_extension(SEMANTIC_METRIC_TYPE, "bogus");
        // The internal transport key must NOT be copied into table options.
        ctx.set_extension(SEMANTIC_PER_TABLE_INDEX_KEY, "{}");
        let ctx = Arc::new(ctx);
        let mut table_options = Default::default();

        fill_table_options_for_create(&mut table_options, &AutoCreateTableType::Physical, &ctx);

        assert_eq!(
            Some(SIGNAL_TYPE_METRIC),
            table_options.get(SEMANTIC_SIGNAL_TYPE).map(String::as_str)
        );
        assert_eq!(
            Some(SOURCE_OPENTELEMETRY),
            table_options.get(SEMANTIC_SOURCE).map(String::as_str)
        );
        assert_eq!(
            Some("2.0"),
            table_options
                .get(SEMANTIC_SOURCE_VERSION)
                .map(String::as_str)
        );
        assert!(!table_options.contains_key(SEMANTIC_METRIC_TYPE));
        assert!(!table_options.contains_key(SEMANTIC_PER_TABLE_INDEX_KEY));
    }

    #[test]
    fn test_apply_per_table_semantic_options() {
        use table::requests::{
            SEMANTIC_METRIC_TYPE, SEMANTIC_METRIC_UNIT, SEMANTIC_PER_TABLE_INDEX_KEY,
        };

        let index = format!(
            r#"{{
            "{DEFAULT_SCHEMA_NAME}": {{
                "http_requests_total": {{
                    "greptime.semantic.metric.type": "counter",
                    "greptime.semantic.metric.unit": "By",
                    "greptime.semantic.metric.type_BOGUS": "x"
                }},
                "other_table": {{
                    "greptime.semantic.metric.type": "gauge"
                }}
            }},
            "other_schema": {{
                "http_requests_total": {{
                    "greptime.semantic.metric.type": "gauge"
                }}
            }}
        }}"#
        );
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        ctx.set_extension(SEMANTIC_PER_TABLE_INDEX_KEY, index);
        let ctx = Arc::new(ctx);

        let index = parse_per_table_semantic_index(&ctx);
        assert!(index.is_some());
        let index = index.as_ref();

        let mut table_options = std::collections::HashMap::new();
        apply_per_table_semantic_options(
            &mut table_options,
            index,
            DEFAULT_SCHEMA_NAME,
            "http_requests_total",
        );
        // The write schema's entry applies — not other_schema's `gauge`.
        assert_eq!(
            table_options.get(SEMANTIC_METRIC_TYPE).map(String::as_str),
            Some("counter")
        );
        assert_eq!(
            table_options.get(SEMANTIC_METRIC_UNIT).map(String::as_str),
            Some("By")
        );
        // The unknown key is rejected by the vocabulary check; other tables' keys
        // never appear.
        assert!(!table_options.contains_key("greptime.semantic.metric.type_BOGUS"));
        assert_eq!(table_options.len(), 2);

        let mut empty = std::collections::HashMap::new();
        apply_per_table_semantic_options(&mut empty, index, DEFAULT_SCHEMA_NAME, "not_in_index");
        assert!(empty.is_empty());

        // A schema with no entry is a no-op even when the table name matches
        // elsewhere.
        let mut opts = std::collections::HashMap::new();
        apply_per_table_semantic_options(
            &mut opts,
            index,
            "schema_without_entry",
            "http_requests_total",
        );
        assert!(opts.is_empty());

        // No extension at all parses to no index (e.g. logs / Prom RW v1).
        let bare = Arc::new(QueryContext::with(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
        ));
        assert!(parse_per_table_semantic_index(&bare).is_none());
        let mut opts = std::collections::HashMap::new();
        apply_per_table_semantic_options(
            &mut opts,
            None,
            DEFAULT_SCHEMA_NAME,
            "http_requests_total",
        );
        assert!(opts.is_empty());
    }

    #[test]
    fn test_last_non_null_create_options_preserve_default_with_append_mode_false() {
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        ctx.set_extension(APPEND_MODE_KEY, "false");
        let ctx = Arc::new(ctx);
        let mut table_options = Default::default();

        fill_table_options_for_create(&mut table_options, &AutoCreateTableType::LastNonNull, &ctx);

        assert!(!table_options.contains_key(APPEND_MODE_KEY));
        assert_eq!(
            Some("last_non_null"),
            table_options.get(MERGE_MODE_KEY).map(String::as_str)
        );
    }

    #[test]
    fn test_last_non_null_create_options_use_configured_merge_mode() {
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        ctx.set_extension(MERGE_MODE_KEY, "last_row");
        let ctx = Arc::new(ctx);
        let mut table_options = Default::default();

        fill_table_options_for_create(&mut table_options, &AutoCreateTableType::LastNonNull, &ctx);

        assert_eq!(
            Some("last_row"),
            table_options.get(MERGE_MODE_KEY).map(String::as_str)
        );
        assert!(!table_options.contains_key(APPEND_MODE_KEY));
    }

    #[test]
    fn test_last_non_null_create_options_use_last_row_with_append_mode_true() {
        let mut ctx = QueryContext::with(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME);
        ctx.set_extension(APPEND_MODE_KEY, "true");
        let ctx = Arc::new(ctx);
        let mut table_options = Default::default();

        fill_table_options_for_create(&mut table_options, &AutoCreateTableType::LastNonNull, &ctx);

        assert_eq!(
            Some("true"),
            table_options.get(APPEND_MODE_KEY).map(String::as_str)
        );
        assert_eq!(
            Some("last_row"),
            table_options.get(MERGE_MODE_KEY).map(String::as_str)
        );
    }

    /// Flownode handler that keeps mirror inserts in flight until the test
    /// releases the gate, so the pending mirror budget can be observed
    /// deterministically.
    #[derive(Clone)]
    struct GatedFlownodeHandler {
        gate: Arc<tokio::sync::Semaphore>,
        affected_rows: u64,
    }

    #[async_trait::async_trait]
    impl MockFlownodeHandler for GatedFlownodeHandler {
        async fn handle_inserts(
            &self,
            _peer: &Peer,
            _requests: api::v1::region::InsertRequests,
        ) -> common_meta::error::Result<FlowResponse> {
            let _permit = self.gate.acquire().await.unwrap();
            Ok(FlowResponse {
                affected_rows: self.affected_rows,
                ..Default::default()
            })
        }
    }

    fn flownode_peer() -> Peer {
        Peer {
            id: 1,
            addr: "127.0.0.1:4001".to_string(),
        }
    }

    #[derive(Clone)]
    struct RecordingNodeManager {
        datanode_dispatch: tokio::sync::mpsc::UnboundedSender<usize>,
        flownode_dispatch: tokio::sync::mpsc::UnboundedSender<Peer>,
        gates: Arc<std::sync::Mutex<HashMap<u64, Arc<tokio::sync::Semaphore>>>>,
        fail_flownode: bool,
    }

    #[async_trait::async_trait]
    impl common_meta::node_manager::DatanodeManager for RecordingNodeManager {
        async fn datanode(&self, peer: &Peer) -> common_meta::node_manager::DatanodeRef {
            Arc::new(RecordingNode {
                peer: peer.clone(),
                manager: self.clone(),
            })
        }
    }

    #[async_trait::async_trait]
    impl common_meta::node_manager::FlownodeManager for RecordingNodeManager {
        async fn flownode(&self, peer: &Peer) -> common_meta::node_manager::FlownodeRef {
            Arc::new(RecordingNode {
                peer: peer.clone(),
                manager: self.clone(),
            })
        }
    }

    struct RecordingNode {
        peer: Peer,
        manager: RecordingNodeManager,
    }

    #[async_trait::async_trait]
    impl common_meta::node_manager::Datanode for RecordingNode {
        async fn handle_request(
            &self,
            _request: Arc<api::v1::region::RegionRequest>,
        ) -> common_meta::error::Result<api::region::RegionResponse> {
            let rows = match _request.body.as_ref() {
                Some(api::v1::region::region_request::Body::Inserts(requests)) => requests
                    .requests
                    .iter()
                    .filter_map(|request| request.rows.as_ref())
                    .map(|rows| rows.rows.len())
                    .sum(),
                _ => 0,
            };
            let _ = self.manager.datanode_dispatch.send(rows);
            Ok(api::region::RegionResponse::new(rows))
        }

        async fn handle_query(
            &self,
            _request: common_query::request::QueryRequest,
        ) -> common_meta::error::Result<common_recordbatch::SendableRecordBatchStream> {
            unreachable!()
        }
    }

    #[async_trait::async_trait]
    impl common_meta::node_manager::Flownode for RecordingNode {
        async fn handle(
            &self,
            _request: api::v1::flow::FlowRequest,
        ) -> common_meta::error::Result<FlowResponse> {
            unreachable!()
        }

        async fn handle_inserts(
            &self,
            _request: api::v1::region::InsertRequests,
        ) -> common_meta::error::Result<FlowResponse> {
            let _ = self.manager.flownode_dispatch.send(self.peer.clone());
            if self.manager.fail_flownode {
                return Err(common_meta::error::UnexpectedSnafu {
                    err_msg: "test flownode failure".to_string(),
                }
                .build());
            }
            let gate = self
                .manager
                .gates
                .lock()
                .unwrap()
                .get(&self.peer.id)
                .cloned();
            if let Some(gate) = gate {
                let _permit = gate.acquire().await.unwrap();
            }
            Ok(FlowResponse::default())
        }

        async fn handle_mark_window_dirty(
            &self,
            _request: api::v1::flow::DirtyWindowRequests,
        ) -> common_meta::error::Result<FlowResponse> {
            unreachable!()
        }
    }

    fn mirror_requests(peer: &Peer, num_rows: usize) -> HashMap<Peer, RegionInsertRequests> {
        HashMap::from_iter([(
            peer.clone(),
            RegionInsertRequests {
                requests: vec![RegionInsertRequest {
                    region_id: RegionId::new(1, 1).as_u64(),
                    rows: Some(Rows {
                        schema: vec![],
                        rows: vec![api::v1::Row { values: vec![] }; num_rows],
                    }),
                    ..Default::default()
                }],
            },
        )])
    }

    /// Serializes the mirror metric tests below: the metrics are process-wide
    /// singletons, so these tests would otherwise observe each other's updates
    /// when the test binary runs tests in parallel (they are isolated per
    /// process only under nextest).
    static MIRROR_METRIC_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn lock_mirror_metrics() -> tokio::sync::MutexGuard<'static, ()> {
        MIRROR_METRIC_TEST_LOCK.lock().await
    }

    // The tests below share the process-wide mirror metrics, so they take
    // `MIRROR_METRIC_TEST_LOCK` and assert on the difference their own budget
    // publishes instead of on absolute values.
    #[tokio::test]
    async fn flow_mirror_saturation_preserves_instant_and_normal_contracts() {
        use common_error::ext::{ErrorExt, RetryHint};
        use common_error::status_code::StatusCode;

        let _guard = lock_mirror_metrics().await;
        let kv_backend = prepare_mocked_backend().await;
        let partition_manager = create_partition_rule_manager(kv_backend.clone()).await;
        let (datanode_tx, mut datanode_rx) = tokio::sync::mpsc::unbounded_channel();
        let (flownode_tx, mut flownode_rx) = tokio::sync::mpsc::unbounded_channel();
        let node_manager = Arc::new(RecordingNodeManager {
            datanode_dispatch: datanode_tx,
            flownode_dispatch: flownode_tx,
            gates: Arc::new(std::sync::Mutex::new(HashMap::new())),
            fail_flownode: false,
        });
        let flow_cache = Cache::new(10);
        let flow_cache_backend = prepare_mocked_backend().await;
        let inserter = Inserter::new(
            catalog::memory::MemoryCatalogManager::new(),
            partition_manager,
            node_manager,
            Arc::new(new_table_flownode_set_cache(
                String::new(),
                flow_cache.clone(),
                flow_cache_backend,
            )),
            true,
        );
        let peer = flownode_peer();
        inserter
            .table_flownode_set_cache
            .invalidate(&[common_meta::instruction::CacheIdent::CreateFlow(
                common_meta::instruction::CreateFlow {
                    flow_id: 1,
                    source_table_ids: vec![1, 2],
                    partition_to_peer_mapping: vec![(0, peer)],
                },
            )])
            .await
            .unwrap();
        let mut normal_info = new_test_table_info(1, "normal_table", [1].into_iter());
        normal_info.catalog_name = DEFAULT_CATALOG_NAME.to_string();
        normal_info.schema_name = DEFAULT_SCHEMA_NAME.to_string();
        let mut instant_info = new_test_table_info(2, "instant_table", [1].into_iter());
        instant_info.meta.options.ttl = Some(common_time::ttl::TimeToLive::Instant);
        let table_infos =
            HashMap::from_iter([(1, Arc::new(normal_info)), (2, Arc::new(instant_info))]);
        let ctx = Arc::new(QueryContext::with(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
        ));
        let insert = |table_id, num_rows| RegionInsertRequests {
            requests: vec![RegionInsertRequest {
                region_id: RegionId::new(table_id, 1).as_u64(),
                rows: Some(Rows {
                    schema: vec![],
                    rows: vec![api::v1::Row::default(); num_rows],
                }),
                ..Default::default()
            }],
        };
        let make_request = |normal, instant| InstantAndNormalInsertRequests {
            normal_requests: if normal == 0 {
                RegionInsertRequests::default()
            } else {
                insert(1, normal)
            },
            instant_requests: if instant == 0 {
                RegionInsertRequests::default()
            } else {
                insert(2, instant)
            },
        };

        let pending = inserter.mirror_pending_rows.clone();
        let gauge_before = crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get();
        crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.add(MAX_MIRROR_PENDING_ROWS as i64);
        pending.store(MAX_MIRROR_PENDING_ROWS as u64, Ordering::Relaxed);
        let dropped_before = crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get();
        for request in [make_request(0, 1), make_request(1, 1)] {
            let error = inserter
                .do_request(request, &table_infos, &ctx)
                .await
                .unwrap_err();
            assert_eq!(error.status_code(), StatusCode::RateLimited);
            assert_eq!(error.retry_hint(), RetryHint::Retryable);
            assert!(datanode_rx.try_recv().is_err());
            assert!(flownode_rx.try_recv().is_err());
            assert_eq!(
                pending.load(Ordering::Relaxed),
                MAX_MIRROR_PENDING_ROWS as u64
            );
        }
        assert_eq!(
            crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get(),
            dropped_before
        );

        for pending_before in [0, MAX_MIRROR_PENDING_ROWS as u64] {
            if pending_before == 0 {
                pending.store(0, Ordering::Relaxed);
                crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.sub(MAX_MIRROR_PENDING_ROWS as i64);
            }
            let gauge_before_rejection = crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get();
            for (normal, instant) in [
                (0, MAX_MIRROR_PENDING_ROWS + 1),
                (MAX_MIRROR_PENDING_ROWS / 2, MAX_MIRROR_PENDING_ROWS / 2 + 1),
            ] {
                let error = inserter
                    .do_request(make_request(normal, instant), &table_infos, &ctx)
                    .await
                    .unwrap_err();
                assert_eq!(error.status_code(), StatusCode::InvalidArguments);
                assert_eq!(error.retry_hint(), RetryHint::NonRetryable);
                let message = error.to_string();
                assert!(message.contains("1000001"), "{message}");
                assert!(message.contains("1000000"), "{message}");
                assert!(message.contains("reduce the batch size"), "{message}");
                assert!(datanode_rx.try_recv().is_err());
                assert!(flownode_rx.try_recv().is_err());
                assert_eq!(pending.load(Ordering::Relaxed), pending_before);
                assert_eq!(
                    crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get(),
                    gauge_before_rejection
                );
                assert_eq!(
                    crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get(),
                    dropped_before
                );
            }
            if pending_before == 0 {
                crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.add(MAX_MIRROR_PENDING_ROWS as i64);
                pending.store(MAX_MIRROR_PENDING_ROWS as u64, Ordering::Relaxed);
            }
        }

        let result = inserter
            .do_request(make_request(1, 0), &table_infos, &ctx)
            .await
            .unwrap();
        assert!(matches!(result.data, OutputData::AffectedRows(1)));
        assert_eq!(1, datanode_rx.try_recv().unwrap());
        assert!(flownode_rx.try_recv().is_err());
        assert_eq!(
            crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get(),
            dropped_before + 1
        );
        pending.store(0, Ordering::Relaxed);
        crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.sub(MAX_MIRROR_PENDING_ROWS as i64);
        assert_eq!(
            crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get(),
            gauge_before
        );
    }

    #[tokio::test]
    async fn flow_mirror_new_counts_regions_and_peer_clones() {
        use common_meta::instruction::{CacheIdent, CreateFlow};

        let peer1 = flownode_peer();
        let peer2 = Peer {
            id: 2,
            addr: "127.0.0.1:4002".to_string(),
        };
        let cache_backend = prepare_mocked_backend().await;
        let cache = Arc::new(new_table_flownode_set_cache(
            String::new(),
            Cache::new(10),
            cache_backend,
        ));
        cache
            .invalidate(&[CacheIdent::CreateFlow(CreateFlow {
                flow_id: 1,
                source_table_ids: vec![1],
                partition_to_peer_mapping: vec![(0, peer1), (1, peer2)],
            })])
            .await
            .unwrap();
        let request = |region_id| RegionInsertRequest {
            region_id,
            rows: Some(Rows {
                schema: vec![],
                rows: vec![api::v1::Row { values: vec![] }; 3],
            }),
            ..Default::default()
        };
        let requests = [
            request(RegionId::new(1, 0).as_u64()),
            request(RegionId::new(1, 1).as_u64()),
        ];
        let task = FlowMirrorTask::new(&cache, requests.iter()).await.unwrap();
        assert_eq!(12, task.pending_rows());
        assert_eq!(2, task.requests.len());
    }

    #[tokio::test]
    async fn flow_mirror_dropped_when_pending_exceeds_limit() {
        let _guard = lock_mirror_metrics().await;
        let pending = Arc::new(AtomicU64::new(MAX_MIRROR_PENDING_ROWS as u64));
        let dropped_before = crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get();
        let gauge_before = crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get();
        let num_rows = 100;

        let task = FlowMirrorTask {
            requests: mirror_requests(&flownode_peer(), num_rows),
        };
        // The budget is already full, so nothing may be spawned for this batch.
        task.detach(
            Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)),
            pending.clone(),
            false,
        )
        .unwrap();

        assert_eq!(
            num_rows as u64,
            crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get() - dropped_before
        );
        // The dropped batch must not leak its reservation.
        assert_eq!(
            MAX_MIRROR_PENDING_ROWS as u64,
            pending.load(Ordering::Relaxed)
        );
        // The reservation taken before the limit check is released again, so the
        // gauge nets out and keeps matching the pending budget.
        assert_eq!(
            gauge_before,
            crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get(),
            "a dropped batch must not change the pending gauge"
        );

        let task = FlowMirrorTask {
            requests: mirror_requests(&flownode_peer(), num_rows),
        };
        let error = task
            .detach(
                Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)),
                pending.clone(),
                true,
            )
            .err()
            .unwrap();
        use common_error::ext::{ErrorExt, RetryHint};
        use common_error::status_code::StatusCode;
        assert_eq!(error.status_code(), StatusCode::RateLimited);
        assert_eq!(error.retry_hint(), RetryHint::Retryable);
        assert_eq!(
            crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get(),
            dropped_before + num_rows as u64,
            "rejected instant rows are not successful best-effort drops"
        );
        assert_eq!(
            MAX_MIRROR_PENDING_ROWS as u64,
            pending.load(Ordering::Relaxed)
        );
        assert_eq!(
            gauge_before,
            crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get()
        );
    }

    /// A source table mapped to several flownodes clones its requests per peer,
    /// so the reservation must cover every clone and each spawned task must hand
    /// back exactly its own share; otherwise the shared budget drifts.
    #[tokio::test]
    async fn flow_mirror_reserves_and_releases_per_peer_shares() {
        let _guard = lock_mirror_metrics().await;
        let per_peer_rows = 10;
        let total_rows = (2 * per_peer_rows) as u64;
        // Exactly enough room for the whole batch, cloned rows included.
        let start = MAX_MIRROR_PENDING_ROWS as u64 - total_rows;
        let pending = Arc::new(AtomicU64::new(start));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let node_manager = Arc::new(MockFlownodeManager::new(GatedFlownodeHandler {
            gate: gate.clone(),
            affected_rows: per_peer_rows as u64,
        }));

        let mut requests = mirror_requests(&flownode_peer(), per_peer_rows);
        requests.extend(mirror_requests(
            &Peer {
                id: 2,
                addr: "127.0.0.1:4002".to_string(),
            },
            per_peer_rows,
        ));

        let gauge_before = crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get();
        FlowMirrorTask { requests }
            .detach(node_manager, pending.clone(), false)
            .unwrap();

        // Both per-peer payloads are counted, so the budget is exactly full
        // instead of over-reserved by the duplicated rows.
        assert_eq!(
            MAX_MIRROR_PENDING_ROWS as u64,
            pending.load(Ordering::Relaxed)
        );

        // Let both tasks finish and check that the budget lands back on its
        // starting value: over-releasing a shared reservation would saturate it.
        // The gauge is checked too because `release_mirror_pending_rows` moves
        // the budget before the gauge; exiting on the budget alone could leak a
        // pending `gauge.sub` into the next test holding the metrics lock.
        gate.add_permits(2);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while pending.load(Ordering::Relaxed) != start
            || crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get() != gauge_before
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the mirror tasks must release exactly their own shares"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[tokio::test]
    async fn flow_mirror_fast_peer_releases_only_its_share_before_drop() {
        let _guard = lock_mirror_metrics().await;
        let peer1 = flownode_peer();
        let peer2 = Peer {
            id: 2,
            addr: "127.0.0.1:4002".to_string(),
        };
        let gate1 = Arc::new(tokio::sync::Semaphore::new(0));
        let gate2 = Arc::new(tokio::sync::Semaphore::new(0));
        let gates = Arc::new(std::sync::Mutex::new(HashMap::from_iter([
            (peer1.id, gate1.clone()),
            (peer2.id, gate2.clone()),
        ])));
        let (datanode_tx, _datanode_rx) = tokio::sync::mpsc::unbounded_channel();
        let (flownode_tx, mut flownode_rx) = tokio::sync::mpsc::unbounded_channel();
        let node_manager = Arc::new(RecordingNodeManager {
            datanode_dispatch: datanode_tx,
            flownode_dispatch: flownode_tx,
            gates,
            fail_flownode: false,
        });
        let pending = Arc::new(AtomicU64::new(MAX_MIRROR_PENDING_ROWS as u64 - 20));
        let gauge_before = crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get();
        let mut requests = mirror_requests(&peer1, 10);
        requests.extend(mirror_requests(&peer2, 10));
        let task = FlowMirrorTask { requests };
        task.detach(node_manager.clone(), pending.clone(), false)
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(10), flownode_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(10), flownode_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_ne!(first.id, second.id);
        let fast_gate = if first.id == peer1.id {
            gate1.clone()
        } else {
            gate2.clone()
        };
        let slow_gate = if first.id == peer1.id {
            gate2.clone()
        } else {
            gate1.clone()
        };
        fast_gate.add_permits(1);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while pending.load(Ordering::Relaxed) != MAX_MIRROR_PENDING_ROWS as u64 - 10 {
            assert!(
                std::time::Instant::now() < deadline,
                "fast peer did not release"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let dropped_before = crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get();
        FlowMirrorTask {
            requests: mirror_requests(&peer1, 11),
        }
        .detach(node_manager, pending.clone(), false)
        .unwrap();
        assert_eq!(
            crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get(),
            dropped_before + 11
        );
        assert!(flownode_rx.try_recv().is_err());
        slow_gate.add_permits(1);
        while pending.load(Ordering::Relaxed) != MAX_MIRROR_PENDING_ROWS as u64 - 20
            || crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get() != gauge_before
        {
            assert!(
                std::time::Instant::now() < deadline,
                "slow peer did not release"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[tokio::test]
    async fn flow_mirror_failure_releases_budget_and_gauge() {
        let _guard = lock_mirror_metrics().await;
        let (datanode_tx, _datanode_rx) = tokio::sync::mpsc::unbounded_channel();
        let (flownode_tx, mut flownode_rx) = tokio::sync::mpsc::unbounded_channel();
        let node_manager = Arc::new(RecordingNodeManager {
            datanode_dispatch: datanode_tx,
            flownode_dispatch: flownode_tx,
            gates: Arc::new(std::sync::Mutex::new(HashMap::new())),
            fail_flownode: true,
        });
        let pending = Arc::new(AtomicU64::new(0));
        let gauge_before = crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get();
        FlowMirrorTask {
            requests: mirror_requests(&flownode_peer(), 5),
        }
        .detach(node_manager, pending.clone(), false)
        .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), flownode_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .id,
            flownode_peer().id
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while pending.load(Ordering::Relaxed) != 0
            || crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get() != gauge_before
        {
            assert!(
                std::time::Instant::now() < deadline,
                "failed task leaked reservation"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[tokio::test]
    async fn flow_mirror_pending_counter_consistent() {
        let _guard = lock_mirror_metrics().await;
        let pending = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let node_manager = Arc::new(MockFlownodeManager::new(GatedFlownodeHandler {
            gate: gate.clone(),
            affected_rows: 7,
        }));

        // A batch without rows short-circuits and leaves the budget untouched.
        let gauge_before = crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get();
        FlowMirrorTask {
            requests: HashMap::new(),
        }
        .detach(node_manager.clone(), pending.clone(), false)
        .unwrap();
        assert_eq!(0, pending.load(Ordering::Relaxed));
        assert_eq!(
            gauge_before,
            crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get(),
            "an empty batch must not touch the pending gauge"
        );

        let dropped_before = crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get();
        let num_rows = 7;

        FlowMirrorTask {
            requests: mirror_requests(&flownode_peer(), num_rows),
        }
        .detach(node_manager, pending.clone(), false)
        .unwrap();

        // The spawned task is still waiting on the gated flownode, so the
        // reservation made by `detach` is the value observable here.
        assert_eq!(num_rows as u64, pending.load(Ordering::Relaxed));
        assert_eq!(
            num_rows as i64,
            crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get() - gauge_before,
            "the pending gauge must track the pending rows"
        );
        assert_eq!(
            dropped_before,
            crate::metrics::DIST_MIRROR_DROPPED_ROW_COUNT.get(),
            "a batch within the limit must not be counted as dropped"
        );

        // Release the in-flight task and wait for it to hand its reservation
        // back, so no task is left holding the shared metrics after this test.
        gate.add_permits(1);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while pending.load(Ordering::Relaxed) != 0
            || crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.get() != gauge_before
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the mirror task did not release its pending reservation"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    #[test]
    fn mirror_logs_are_rate_limited() {
        let log = Arc::new(MirrorLog::new());
        assert_eq!(Some(1), log.claim_report(1_000));
        assert_eq!(None, log.claim_report(1_500));
        assert_eq!(None, log.claim_report(10_999));
        assert_eq!(Some(3), log.claim_report(11_000));
        assert_eq!(None, log.claim_report(11_001));
        assert_eq!(Some(2), log.claim_report(21_000));

        let concurrent = Arc::new(MirrorLog::new());
        assert_eq!(Some(1), concurrent.claim_report(50_000));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads = (0..8)
            .map(|_| {
                let log = concurrent.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    log.claim_report(60_000)
                })
            })
            .collect::<Vec<_>>();
        let winners = threads
            .into_iter()
            .filter_map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(winners.len(), 1);
        let residual = concurrent.events.swap(0, Ordering::Relaxed);
        assert_eq!(winners[0] + residual, 8);
    }
}
