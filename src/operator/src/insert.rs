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

use std::sync::Arc;

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
    default_engine, trace_services_table_name, PARENT_SPAN_ID_COLUMN, SERVICE_NAME_COLUMN,
    TRACE_ID_COLUMN, TRACE_TABLE_NAME, TRACE_TABLE_NAME_SESSION_KEY,
};
use common_grpc_expr::util::ColumnExpr;
use common_meta::cache::TableFlownodeSetCacheRef;
use common_meta::node_manager::{AffectedRows, NodeManagerRef};
use common_meta::peer::Peer;
use common_query::prelude::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
use common_query::Output;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{error, info, warn};
use datatypes::schema::SkippingIndexOptions;
use futures_util::future;
use meter_macros::write_meter;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::prelude::*;
use snafu::ResultExt;
use sql::partition::partition_rule_for_hexstring;
use sql::statements::create::Partitions;
use sql::statements::insert::Insert;
use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};
use store_api::mito_engine_options::{APPEND_MODE_KEY, MERGE_MODE_KEY};
use store_api::storage::{RegionId, TableId};
use table::metadata::TableInfo;
use table::requests::{
    InsertRequest as TableInsertRequest, AUTO_CREATE_TABLE_KEY, TABLE_DATA_MODEL,
    TABLE_DATA_MODEL_TRACE_V1, VALID_TABLE_OPTION_KEYS,
};
use table::table_reference::TableReference;
use table::TableRef;

use crate::error::{
    CatalogSnafu, ColumnOptionsSnafu, CreatePartitionRulesSnafu, FindRegionLeaderSnafu,
    InvalidInsertRequestSnafu, JoinTaskSnafu, RequestInsertsSnafu, Result, TableNotFoundSnafu,
};
use crate::expr_helper;
use crate::region_req_factory::RegionRequestFactory;
use crate::req_convert::common::preprocess_row_insert_requests;
use crate::req_convert::insert::{
    fill_reqs_with_impure_default, ColumnToRow, RowToRegion, StatementToRegion, TableToRegion,
};
use crate::statement::StatementExecutor;

pub struct Inserter {
    catalog_manager: CatalogManagerRef,
    pub(crate) partition_manager: PartitionRuleManagerRef,
    pub(crate) node_manager: NodeManagerRef,
    table_flownode_set_cache: TableFlownodeSetCacheRef,
}

pub type InserterRef = Arc<Inserter>;

/// Hint for the table type to create automatically.
#[derive(Clone)]
enum AutoCreateTableType {
    /// A logical table with the physical table name.
    Logical(String),
    /// A physical table.
    Physical,
    /// A log table which is append-only.
    Log,
    /// A table that merges rows by `last_non_null` strategy.
    LastNonNull,
    /// Create table that build index and default partition rules on trace_id
    Trace,
}

impl AutoCreateTableType {
    fn as_str(&self) -> &'static str {
        match self {
            AutoCreateTableType::Logical(_) => "logical",
            AutoCreateTableType::Physical => "physical",
            AutoCreateTableType::Log => "log",
            AutoCreateTableType::LastNonNull => "last_non_null",
            AutoCreateTableType::Trace => "trace",
        }
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
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
        table_flownode_set_cache: TableFlownodeSetCacheRef,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            node_manager,
            table_flownode_set_cache,
        }
    }

    pub async fn handle_column_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<Output> {
        let row_inserts = ColumnToRow::convert(requests)?;
        self.handle_row_inserts(row_inserts, ctx, statement_executor, false)
            .await
    }

    /// Handles row inserts request and creates a physical table on demand.
    pub async fn handle_row_inserts(
        &self,
        mut requests: RowInsertRequests,
        ctx: QueryContextRef,
        statement_executor: &StatementExecutor,
        accommodate_existing_schema: bool,
    ) -> Result<Output> {
        preprocess_row_insert_requests(&mut requests.inserts)?;
        self.handle_row_inserts_with_create_type(
            requests,
            ctx,
            statement_executor,
            AutoCreateTableType::Physical,
            accommodate_existing_schema,
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
            AutoCreateTableType::Trace,
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
    ) -> Result<Output> {
        self.handle_row_inserts_with_create_type(
            requests,
            ctx,
            statement_executor,
            AutoCreateTableType::LastNonNull,
            accommodate_existing_schema,
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
    ) -> Result<Output> {
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
        .convert(requests)
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
                AutoCreateTableType::Logical(physical_table.to_string()),
                statement_executor,
                true,
            )
            .await?;
        let name_to_info = table_infos
            .values()
            .map(|info| (info.name.clone(), info.clone()))
            .collect::<HashMap<_, _>>();
        let inserts = RowToRegion::new(name_to_info, instant_table_ids, &self.partition_manager)
            .convert(requests)
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

        let table_infos =
            HashMap::from_iter([(table_info.table_id(), table_info.clone())].into_iter());

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

        let table_infos =
            HashMap::from_iter([(table_info.table_id(), table_info.clone())].into_iter());

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
        flow_mirror_task.detach(self.node_manager.clone())?;

        // Write requests to datanode and wait for response
        let write_tasks = self
            .group_requests_by_peer(normal_requests)
            .await?
            .into_iter()
            .map(|(peer, inserts)| {
                let node_manager = self.node_manager.clone();
                let request = request_factory.build_insert(inserts);
                common_runtime::spawn_global(async move {
                    node_manager
                        .datanode(&peer)
                        .await
                        .handle(request)
                        .await
                        .context(RequestInsertsSnafu)
                })
            });
        let results = future::try_join_all(write_tasks)
            .await
            .context(JoinTaskSnafu)?;
        let affected_rows = results
            .into_iter()
            .map(|resp| resp.map(|r| r.affected_rows))
            .sum::<Result<AffectedRows>>()?;
        crate::metrics::DIST_INGEST_ROW_COUNT.inc_by(affected_rows as u64);
        Ok(Output::new(
            OutputData::AffectedRows(affected_rows),
            OutputMeta::new_with_cost(write_cost as _),
        ))
    }

    async fn group_requests_by_peer(
        &self,
        requests: RegionInsertRequests,
    ) -> Result<HashMap<Peer, RegionInsertRequests>> {
        // group by region ids first to reduce repeatedly call `find_region_leader`
        // TODO(discord9): determine if a addition clone is worth it
        let mut requests_per_region: HashMap<RegionId, RegionInsertRequests> = HashMap::new();
        for req in requests.requests {
            let region_id = RegionId::from_u64(req.region_id);
            requests_per_region
                .entry(region_id)
                .or_default()
                .requests
                .push(req);
        }

        let mut inserts: HashMap<Peer, RegionInsertRequests> = HashMap::new();

        for (region_id, reqs) in requests_per_region {
            let peer = self
                .partition_manager
                .find_region_leader(region_id)
                .await
                .context(FindRegionLeaderSnafu)?;
            inserts
                .entry(peer)
                .or_default()
                .requests
                .extend(reqs.requests);
        }

        Ok(inserts)
    }

    /// Creates or alter tables on demand:
    /// - if table does not exist, create table by inferred CreateExpr
    /// - if table exist, check if schema matches. If any new column found, alter table by inferred `AlterExpr`
    ///
    /// Returns a mapping from table name to table id, where table name is the table name involved in the requests.
    /// This mapping is used in the conversion of RowToRegion.
    ///
    /// `accommodate_existing_schema` is used to determine if the existing schema should override the new schema.
    /// It only works for TIME_INDEX and VALUE columns. This is for the case where the user creates a table with
    /// custom schema, and then inserts data with endpoints that have default schema setting, like prometheus
    /// remote write. This will modify the `RowInsertRequests` in place.
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &mut RowInsertRequests,
        ctx: &QueryContextRef,
        auto_create_table_type: AutoCreateTableType,
        statement_executor: &StatementExecutor,
        accommodate_existing_schema: bool,
    ) -> Result<CreateAlterTableResult> {
        let _timer = crate::metrics::CREATE_ALTER_ON_DEMAND
            .with_label_values(&[auto_create_table_type.as_str()])
            .start_timer();

        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();

        let mut table_infos = HashMap::new();
        // If `auto_create_table` hint is disabled, skip creating/altering tables.
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
        if !auto_create_table_hint {
            let mut instant_table_ids = HashSet::new();
            for req in &requests.inserts {
                let table = self
                    .get_table(catalog, &schema, &req.table_name)
                    .await?
                    .context(InvalidInsertRequestSnafu {
                        reason: format!(
                            "Table `{}` does not exist, and `auto_create_table` hint is disabled",
                            req.table_name
                        ),
                    })?;
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
        let mut instant_table_ids = HashSet::new();

        for req in &mut requests.inserts {
            match self.get_table(catalog, &schema, &req.table_name).await? {
                Some(table) => {
                    let table_info = table.table_info();
                    if table_info.is_ttl_instant_table() {
                        instant_table_ids.insert(table_info.table_id());
                    }
                    table_infos.insert(table_info.table_id(), table.table_info());
                    if let Some(alter_expr) = self.get_alter_table_expr_on_demand(
                        req,
                        &table,
                        ctx,
                        accommodate_existing_schema,
                    )? {
                        alter_tables.push(alter_expr);
                    }
                }
                None => {
                    let create_expr =
                        self.get_create_table_expr_on_demand(req, &auto_create_table_type, ctx)?;
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
                        .alter_logical_tables(alter_tables, ctx.clone())
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
                        .alter_table_inner(alter_expr, ctx.clone())
                        .await?;
                }
            }

            AutoCreateTableType::Trace => {
                let trace_table_name = ctx
                    .extension(TRACE_TABLE_NAME_SESSION_KEY)
                    .unwrap_or(TRACE_TABLE_NAME);

                // note that auto create table shouldn't be ttl instant table
                // for it's a very unexpected behavior and should be set by user explicitly
                for mut create_table in create_tables {
                    if create_table.table_name == trace_services_table_name(trace_table_name) {
                        // Disable append mode for trace services table since it requires upsert behavior.
                        create_table
                            .table_options
                            .insert(APPEND_MODE_KEY.to_string(), "false".to_string());
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
                        let partitions = partition_rule_for_hexstring(TRACE_ID_COLUMN)
                            .context(CreatePartitionRulesSnafu)?;
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
                                Some(partitions),
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
                        .alter_table_inner(alter_expr, ctx.clone())
                        .await?;
                }
            }
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

        let table_reference = TableReference::full(catalog_name, &schema_name, &physical_table);
        info!("Physical metric table `{table_reference}` does not exist, try creating table");

        // schema with timestamp and field column
        let default_schema = vec![
            ColumnSchema {
                column_name: GREPTIME_TIMESTAMP.to_string(),
                datatype: ColumnDataType::TimestampMillisecond as _,
                semantic_type: SemanticType::Timestamp as _,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: GREPTIME_VALUE.to_string(),
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
            .create_table_inner(create_table_expr, None, ctx.clone())
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
    ) -> Result<CreateTableExpr> {
        let mut table_options = Vec::with_capacity(4);
        for key in VALID_TABLE_OPTION_KEYS {
            if let Some(value) = ctx.extension(key) {
                table_options.push((key, value));
            }
        }

        let mut engine_name = default_engine();
        match create_type {
            AutoCreateTableType::Logical(physical_table) => {
                engine_name = METRIC_ENGINE_NAME;
                table_options.push((LOGICAL_TABLE_METADATA_KEY, physical_table));
            }
            AutoCreateTableType::Physical => {
                if let Some(append_mode) = ctx.extension(APPEND_MODE_KEY) {
                    table_options.push((APPEND_MODE_KEY, append_mode));
                }
                if let Some(merge_mode) = ctx.extension(MERGE_MODE_KEY) {
                    table_options.push((MERGE_MODE_KEY, merge_mode));
                }
            }
            // Set append_mode to true for log table.
            // because log tables should keep rows with the same ts and tags.
            AutoCreateTableType::Log => {
                table_options.push((APPEND_MODE_KEY, "true"));
            }
            AutoCreateTableType::LastNonNull => {
                table_options.push((MERGE_MODE_KEY, "last_non_null"));
            }
            AutoCreateTableType::Trace => {
                table_options.push((APPEND_MODE_KEY, "true"));
            }
        }

        let schema = ctx.current_schema();
        let table_ref = TableReference::full(ctx.current_catalog(), &schema, &req.table_name);
        // SAFETY: `req.rows` is guaranteed to be `Some` by `handle_row_inserts_with_create_type()`.
        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let mut create_table_expr =
            build_create_table_expr(&table_ref, request_schema, engine_name)?;

        info!("Table `{table_ref}` does not exist, try creating table");
        for (k, v) in table_options {
            create_table_expr
                .table_options
                .insert(k.to_string(), v.to_string());
        }

        Ok(create_table_expr)
    }

    /// Returns an alter table expression if it finds new columns in the request.
    /// When `accommodate_existing_schema` is false, it always adds columns if not exist.
    /// When `accommodate_existing_schema` is true, it may modify the input `req` to
    /// accommodate it with existing schema. See [`create_or_alter_tables_on_demand`](Self::create_or_alter_tables_on_demand)
    /// for more details.
    fn get_alter_table_expr_on_demand(
        &self,
        req: &mut RowInsertRequest,
        table: &TableRef,
        ctx: &QueryContextRef,
        accommodate_existing_schema: bool,
    ) -> Result<Option<AlterTableExpr>> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();
        let table_name = table.table_info().name.clone();

        let request_schema = req.rows.as_ref().unwrap().schema.as_slice();
        let column_exprs = ColumnExpr::from_column_schemas(request_schema);
        let add_columns = expr_helper::extract_add_columns_expr(&table.schema(), column_exprs)?;
        let Some(mut add_columns) = add_columns else {
            return Ok(None);
        };

        // If accommodate_existing_schema is true, update request schema for Timestamp/Field columns
        if accommodate_existing_schema {
            let table_schema = table.schema();
            // Find timestamp column name
            let ts_col_name = table_schema.timestamp_column().map(|c| c.name.clone());
            // Find field column name if there is only one
            let mut field_col_name = None;
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

            // Update column name in request schema for Timestamp/Field columns
            if let Some(rows) = req.rows.as_mut() {
                for col in &mut rows.schema {
                    match col.semantic_type {
                        x if x == SemanticType::Timestamp as i32 => {
                            if let Some(ref ts_name) = ts_col_name {
                                if col.column_name != *ts_name {
                                    col.column_name = ts_name.clone();
                                }
                            }
                        }
                        x if x == SemanticType::Field as i32 => {
                            if let Some(ref field_name) = field_col_name {
                                if col.column_name != *field_name {
                                    col.column_name = field_name.clone();
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }

            // Remove from add_columns any column that is timestamp or field (if there is only one field column)
            add_columns.add_columns.retain(|col| {
                let def = col.column_def.as_ref().unwrap();
                def.semantic_type != SemanticType::Timestamp as i32
                    && (def.semantic_type != SemanticType::Field as i32 && field_col_name.is_some())
            });

            if add_columns.add_columns.is_empty() {
                return Ok(None);
            }
        }

        Ok(Some(AlterTableExpr {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
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
            .create_table_inner(&mut create_table_expr, partitions, ctx.clone())
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
            .create_logical_tables(&create_table_exprs, ctx.clone())
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

fn build_create_table_expr(
    table: &TableReference,
    request_schema: &[ColumnSchema],
    engine: &str,
) -> Result<CreateTableExpr> {
    expr_helper::create_table_expr_by_column_schemas(table, request_schema, engine, None)
}

/// Result of `create_or_alter_tables_on_demand`.
struct CreateAlterTableResult {
    /// table ids of ttl=instant tables.
    instant_table_ids: HashSet<TableId>,
    /// Table Info of the created tables.
    table_infos: HashMap<TableId, Arc<TableInfo>>,
}

struct FlowMirrorTask {
    requests: HashMap<Peer, RegionInsertRequests>,
    num_rows: usize,
}

impl FlowMirrorTask {
    async fn new(
        cache: &TableFlownodeSetCacheRef,
        requests: impl Iterator<Item = &RegionInsertRequest>,
    ) -> Result<Self> {
        let mut src_table_reqs: HashMap<TableId, Option<(Vec<Peer>, RegionInsertRequests)>> =
            HashMap::new();
        let mut num_rows = 0;

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
                        num_rows += reqs
                            .requests
                            .iter()
                            .map(|r| r.rows.as_ref().unwrap().rows.len())
                            .sum::<usize>();
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

        Ok(Self {
            requests: inserts,
            num_rows,
        })
    }

    fn detach(self, node_manager: NodeManagerRef) -> Result<()> {
        crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.add(self.num_rows as i64);
        for (peer, inserts) in self.requests {
            let node_manager = node_manager.clone();
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
                        crate::metrics::DIST_MIRROR_PENDING_ROW_COUNT.sub(affected_rows as _);
                    }
                    Err(err) => {
                        error!(err; "Failed to insert data into flownode {}", peer);
                    }
                }
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{ColumnSchema as GrpcColumnSchema, RowInsertRequest, Rows, SemanticType, Value};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::cache::new_table_flownode_set_cache;
    use common_meta::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
    use common_meta::test_util::MockDatanodeManager;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use moka::future::Cache;
    use session::context::QueryContext;
    use table::dist_table::DummyDataSource;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder, TableType};
    use table::TableRef;

    use super::*;
    use crate::tests::{create_partition_rule_manager, prepare_mocked_backend};

    fn make_table_ref_with_schema(ts_name: &str, field_name: &str) -> TableRef {
        let schema = datatypes::schema::SchemaBuilder::try_from_columns(vec![
            ColumnSchema::new(
                ts_name,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(field_name, ConcreteDataType::float64_datatype(), true),
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
            .region_numbers(vec![0])
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
    async fn test_accommodate_existing_schema_logic() {
        let ts_name = "my_ts";
        let field_name = "my_field";
        let table = make_table_ref_with_schema(ts_name, field_name);

        // The request uses different names for timestamp and field columns
        let mut req = RowInsertRequest {
            table_name: "test_table".to_string(),
            rows: Some(Rows {
                schema: vec![
                    GrpcColumnSchema {
                        column_name: "ts_wrong".to_string(),
                        datatype: api::v1::ColumnDataType::TimestampMillisecond as i32,
                        semantic_type: SemanticType::Timestamp as i32,
                        ..Default::default()
                    },
                    GrpcColumnSchema {
                        column_name: "field_wrong".to_string(),
                        datatype: api::v1::ColumnDataType::Float64 as i32,
                        semantic_type: SemanticType::Field as i32,
                        ..Default::default()
                    },
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
        );
        let alter_expr = inserter
            .get_alter_table_expr_on_demand(&mut req, &table, &ctx, true)
            .unwrap();
        assert!(alter_expr.is_none());

        // The request's schema should have updated names for timestamp and field columns
        let req_schema = req.rows.as_ref().unwrap().schema.clone();
        assert_eq!(req_schema[0].column_name, ts_name);
        assert_eq!(req_schema[1].column_name, field_name);
    }
}
