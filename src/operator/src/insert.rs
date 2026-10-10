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
use api::v1::column_def::{options_from_skipping, try_as_column_def};
use api::v1::region::{
    InsertRequest as RegionInsertRequest, InsertRequests as RegionInsertRequests,
    RegionRequestHeader,
};
use api::v1::value::ValueData;
use api::v1::{
    AlterTableExpr, ColumnDataType, ColumnSchema, CreateTableExpr, InsertRequests,
    RowInsertRequest, RowInsertRequests, Rows, SemanticType,
};
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
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
use common_meta::node_manager::{AffectedRows, NodeManagerRef};
use common_meta::peer::Peer;
use common_meta::rpc::ddl::TriggerReason;
use common_query::Output;
use common_query::native_histogram::{is_native_histogram_value_type, native_histogram_value_type};
use common_query::prelude::{greptime_timestamp, greptime_value};
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, error, warn};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datatypes::schema::SkippingIndexOptions;
use futures_util::future;
use meter_core::data::MeterRecord;
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
use table::metadata::{TableInfo, TableInfoRef};
use table::requests::{
    AUTO_CREATE_TABLE_KEY, InsertRequest as TableInsertRequest, SEMANTIC_PER_TABLE_INDEX_KEY,
    SEMANTIC_PIPELINE, TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1, TABLE_DATA_MODEL_TRACE_V2,
    TRACE_TABLE_PARTITIONS_HINT_KEY, VALID_TABLE_OPTION_KEYS, is_semantic_option_key,
    validate_semantic_option,
};
use table::table_reference::TableReference;

use crate::batcher::PendingRowsBatcher;
use crate::error::{
    CatalogSnafu, ColumnOptionsSnafu, CreatePartitionRulesSnafu, FindRegionLeaderSnafu,
    InvalidInsertRequestSnafu, JoinTaskSnafu, RequestInsertsSnafu, Result, TableNotFoundSnafu,
    WriteRejectedSnafu,
};
use crate::expr_helper;
use crate::region_req_factory::RegionRequestFactory;
use crate::req_convert::common::preprocess_row_insert_requests;
use crate::req_convert::insert::{
    ColumnToRow, ImpureDefaultFiller, RowToRegion, StatementToRegion, TableToRegion,
    fill_reqs_with_impure_default, rows_to_record_batch,
};
use crate::statement::StatementExecutor;

pub struct Inserter {
    catalog_manager: CatalogManagerRef,
    pub(crate) partition_manager: PartitionRuleManagerRef,
    pub(crate) node_manager: NodeManagerRef,
    pub(crate) table_flownode_set_cache: TableFlownodeSetCacheRef,
    /// Server-side upper bound for auto table creation on write.
    /// When `false`, missing tables are never auto-created regardless of the
    /// per-request `auto_create_table` hint. When `true`, the hint still applies.
    auto_create_table: bool,
    pending_rows_batcher: Option<Arc<dyn PendingRowsBatcher>>,
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
    /// Checks the assumptions of the logical bulk path without changing tables.
    /// Unsupported requests retain ordinary insertion, including schema policy,
    /// defaults, instant TTL and row-based Flow delivery.
    pub async fn can_batch_metric_rows(
        &self,
        requests: &RowInsertRequests,
        ctx: &QueryContextRef,
        physical_table: &str,
    ) -> Result<bool> {
        if self.auto_create_disabled_reason(ctx)?.is_some() || ctx.extension(TTL_KEY).is_some() {
            return Ok(false);
        }
        for request in &requests.inserts {
            // The logical bulk encoder only supports scalar metric schemas;
            // any time index unit is accepted — requests are converted to the
            // destination table's unit during batch alignment.
            // Check new tables too, before catalog lookup or schema changes.
            if request.rows.as_ref().is_some_and(|rows| {
                rows.schema.iter().any(|column| {
                    column.datatype_extension.is_some()
                        || !matches!(
                            ColumnDataType::try_from(column.datatype),
                            Ok(ColumnDataType::TimestampSecond
                                | ColumnDataType::TimestampMillisecond
                                | ColumnDataType::TimestampMicrosecond
                                | ColumnDataType::TimestampNanosecond
                                | ColumnDataType::Float64
                                | ColumnDataType::String)
                        )
                })
            }) {
                return Ok(false);
            }

            let Some(table) = self
                .get_table(
                    ctx.current_catalog(),
                    &ctx.current_schema(),
                    &request.table_name,
                )
                .await?
            else {
                continue;
            };
            let info = table.table_info();
            if info.meta.engine != METRIC_ENGINE_NAME
                || info.is_ttl_instant_table()
                || info
                    .meta
                    .options
                    .extra_options
                    .get(LOGICAL_TABLE_METADATA_KEY)
                    .map(String::as_str)
                    != Some(physical_table)
                || info
                    .meta
                    .schema
                    .column_schemas()
                    .iter()
                    .any(|column| column.default_constraint().is_some())
            {
                return Ok(false);
            }
            // Physical metric tags are nullable even when their logical schema
            // is not. Arrow alignment is stricter than ordinary metric insertion.
            if info
                .meta
                .primary_key_indices
                .iter()
                .any(|&index| !info.meta.schema.column_schemas()[index].is_nullable())
            {
                return Ok(false);
            }
            // The current Flow cache does not distinguish streaming and batch
            // flows. Keep all Flow sources on the row-based delivery path.
            match self.table_flownode_set_cache.get(info.table_id()).await {
                Ok(None) => {}
                Ok(Some(flows)) if flows.is_empty() => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// Meters an original logical-table request before bulk routing, without
    /// cloning its rows or changing the request boundary used for accounting.
    pub async fn meter_row_inserts(
        requests: &mut RowInsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<u64> {
        let metered = InstantAndNormalInsertRequests {
            normal_requests: RegionInsertRequests {
                requests: requests
                    .inserts
                    .iter_mut()
                    .map(|request| RegionInsertRequest {
                        rows: request.rows.take(),
                        ..Default::default()
                    })
                    .collect(),
            },
            instant_requests: RegionInsertRequests::default(),
        };
        let cost = write_meter!(
            ctx.current_catalog(),
            ctx.current_schema(),
            metered,
            ctx.write_rows_to_admit(
                ctx.current_catalog(),
                &ctx.current_schema(),
                count_insert_rows(&metered)?
            ),
            ctx.channel() as u8
        )
        .await
        .context(WriteRejectedSnafu);
        for (request, region) in requests
            .inserts
            .iter_mut()
            .zip(metered.normal_requests.requests)
        {
            request.rows = region.rows;
        }
        cost
    }

    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
        table_flownode_set_cache: TableFlownodeSetCacheRef,
        auto_create_table: bool,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            node_manager,
            table_flownode_set_cache,
            auto_create_table,
            pending_rows_batcher: None,
            mirror_pending_rows: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Installs the shared batcher; callers explicitly select its ingestion entry point.
    pub fn with_pending_rows_batcher(
        mut self,
        batcher: Option<Arc<dyn PendingRowsBatcher>>,
    ) -> Self {
        self.pending_rows_batcher = batcher;
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

        let batcher = self
            .pending_rows_batcher
            .as_ref()
            .filter(|_| ctx.batching_enabled());

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
                None,
            )
            .await?;

        // Instant tables have no persisted data for dirty-window Flow to read.
        // Metric tables keep their existing dedicated ingestion path.
        if let Some(batcher) = batcher
            && instant_table_ids.is_empty()
            && table_infos
                .values()
                .all(|info| info.meta.engine == default_engine())
        {
            return self
                .submit_pending_rows(requests, table_infos, ctx, batcher)
                .await;
        }

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

    async fn submit_pending_rows(
        &self,
        mut requests: RowInsertRequests,
        table_infos: HashMap<TableId, Arc<TableInfo>>,
        ctx: QueryContextRef,
        batcher: &Arc<dyn PendingRowsBatcher>,
    ) -> Result<Output> {
        // All entry points, including single-table and SQL writes, skip empty input
        // before evaluating defaults or converting prepared rows.
        requests.inserts.retain(|request| {
            request
                .rows
                .as_ref()
                .is_some_and(|rows| !rows.rows.is_empty())
        });
        let by_name = table_infos
            .values()
            .map(|info| (info.name.as_str(), info))
            .collect::<HashMap<_, _>>();
        let mut prepared = Vec::with_capacity(requests.inserts.len());
        for request in &mut requests.inserts {
            let table_info =
                by_name
                    .get(request.table_name.as_str())
                    .context(TableNotFoundSnafu {
                        table_name: &request.table_name,
                    })?;
            let Some(rows) = &mut request.rows else {
                continue;
            };
            ImpureDefaultFiller::new((*table_info).clone())?.fill_rows(rows);
            let batch = rows_to_record_batch(rows, table_info)?;
            prepared.push(((*table_info).clone(), batch));
        }
        // Preserve the existing meter input and original request boundary. These
        // envelopes are only for accounting; routing happens after batching.
        let metered = InstantAndNormalInsertRequests {
            normal_requests: RegionInsertRequests {
                requests: requests
                    .inserts
                    .into_iter()
                    .map(|request| RegionInsertRequest {
                        rows: request.rows,
                        ..Default::default()
                    })
                    .collect(),
            },
            instant_requests: RegionInsertRequests::default(),
        };
        let table_info = table_infos.values().next();
        let catalog = table_info.map_or(ctx.current_catalog(), |info| info.catalog_name.as_str());
        let schema =
            table_info.map_or_else(|| ctx.current_schema(), |info| info.schema_name.clone());
        let write_cost = write_meter!(
            catalog,
            &schema,
            metered,
            ctx.write_rows_to_admit(catalog, &schema, count_insert_rows(&metered)?),
            ctx.channel() as u8
        )
        .await
        .context(WriteRejectedSnafu)?;
        prepared.retain(|(_, batch)| batch.num_rows() != 0);
        let results = if prepared.is_empty() {
            Vec::new()
        } else {
            // One original request shares admission across all table submissions.
            let permit = batcher.acquire().await?;
            let submissions = prepared.into_iter().map(|(info, batch)| {
                // Route to the same target database used for admission above.
                let mut target_ctx = ctx.fork();
                target_ctx.set_current_catalog(&info.catalog_name);
                target_ctx.set_current_schema(&info.schema_name);
                batcher.submit(info, batch, Arc::new(target_ctx), permit.clone())
            });
            // Observe every table completion even when another table fails.
            future::join_all(submissions).await
        };
        let mut affected_rows = 0;
        let mut meta = OutputMeta::new_with_cost(write_cost as _);
        for result in results {
            let output = result?;
            affected_rows += output.extract_rows_and_cost().0;
            meta.write_completions.extend(output.meta.write_completions);
        }
        Ok(Output::new(OutputData::AffectedRows(affected_rows), meta))
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
        let physical_table_ref = self
            .create_physical_table_on_demand(&ctx, physical_table.clone(), statement_executor)
            .await?;

        // check and create logical tables; `create_or_alter_tables_on_demand`
        // aligns each request's time index unit with the unit of the table it
        // targets, inside its existing table lookups: existing tables keep
        // their own unit (which matches the physical table they are bound
        // to), and new tables use the selected physical table's unit (from
        // `physical_table_ref`). Ingestion endpoints encode timestamps in a
        // fixed unit (prometheus remote write always uses millisecond; OTLP
        // keeps nanosecond precision on the metric engine path), and the
        // metric engine requires each logical table's requests to match its
        // time index unit. Narrowing conversions truncate the sub-unit part
        // (floor), following `Timestamp::convert_to`.
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
                table_time_index_unit(&physical_table_ref),
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

    fn table_batcher(
        &self,
        table_info: &TableInfoRef,
        ctx: &QueryContextRef,
    ) -> Option<&Arc<dyn PendingRowsBatcher>> {
        self.pending_rows_batcher.as_ref().filter(|_| {
            ctx.batching_enabled()
                && !table_info.is_ttl_instant_table()
                && table_info.meta.engine == default_engine()
        })
    }

    async fn submit_table_rows(
        &self,
        rows: Rows,
        table_info: TableInfoRef,
        ctx: QueryContextRef,
        batcher: &Arc<dyn PendingRowsBatcher>,
    ) -> Result<Output> {
        let requests = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: table_info.name.clone(),
                rows: Some(rows),
            }],
        };
        let table_infos = HashMap::from_iter([(table_info.table_id(), table_info)]);
        self.submit_pending_rows(requests, table_infos, ctx, batcher)
            .await
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

        let converter = TableToRegion::new(&table_info, &self.partition_manager);
        let skip_wal = request.skip_wal;
        let rows = converter.prepare(request)?;
        if let Some(batcher) = self.table_batcher(&table_info, &ctx) {
            return self.submit_table_rows(rows, table_info, ctx, batcher).await;
        }
        let inserts = converter.partition(rows, skip_wal).await?;

        let table_infos = HashMap::from_iter([(table_info.table_id(), table_info.clone())]);

        self.do_request(inserts, &table_infos, &ctx).await
    }

    pub async fn handle_statement_insert(
        &self,
        insert: &Insert,
        ctx: &QueryContextRef,
    ) -> Result<Output> {
        let converter =
            StatementToRegion::new(self.catalog_manager.as_ref(), &self.partition_manager, ctx);
        let (rows, table_info) = converter.prepare(insert, ctx).await?;
        if let Some(batcher) = self.table_batcher(&table_info, ctx) {
            return self
                .submit_table_rows(rows, table_info, ctx.clone(), batcher)
                .await;
        }
        let inserts = converter.partition(rows, table_info.clone(), ctx).await?;

        let table_infos = HashMap::from_iter([(table_info.table_id(), table_info.clone())]);

        self.do_request(inserts, &table_infos, ctx).await
    }
}

/// Admits a finite request before it is split into internal writes.
/// The returned context preserves accounting while preventing a second row debit.
pub async fn admit_write(rows: u64, ctx: &QueryContextRef) -> Result<QueryContextRef> {
    // The zero value is WCU: this record only admits rows. Actual inserts retain
    // their existing WCU accounting, so charging here would count it twice.
    write_meter!(MeterRecord::new(
        ctx.current_catalog().to_string(),
        ctx.current_schema(),
        0,
        ctx.write_rows_to_admit(ctx.current_catalog(), &ctx.current_schema(), rows),
        ctx.channel() as u8,
    ))
    .await
    .context(WriteRejectedSnafu)?;
    Ok(Arc::new(ctx.with_write_admission()))
}

/// Admits all database totals before dispatching any batch of a finite request.
/// Each batch keeps its own protocol options and target database.
pub async fn admit_row_insert_batches(
    batches: &mut [(QueryContextRef, RowInsertRequests)],
) -> Result<()> {
    let mut totals = BTreeMap::<_, (QueryContextRef, u64)>::new();
    for (ctx, requests) in batches.iter() {
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();
        if ctx.write_rows_to_admit(catalog, &schema, 1) == 0 {
            continue;
        }
        let (_, total) = totals
            .entry((catalog.to_string(), schema.clone()))
            .or_insert_with(|| (ctx.clone(), 0));
        for rows in requests.inserts.iter().filter_map(|r| r.rows.as_ref()) {
            *total =
                total
                    .checked_add(rows.rows.len() as u64)
                    .context(InvalidInsertRequestSnafu {
                        reason: "Insert row count exceeds u64::MAX",
                    })?;
        }
    }
    for (ctx, rows) in totals.values() {
        admit_write(*rows, ctx).await?;
    }
    for (ctx, _) in batches {
        *ctx = Arc::new(ctx.with_write_admission());
    }
    Ok(())
}

fn count_insert_rows(requests: &InstantAndNormalInsertRequests) -> Result<u64> {
    requests
        .normal_requests
        .requests
        .iter()
        .chain(&requests.instant_requests.requests)
        .filter_map(|request| request.rows.as_ref())
        .try_fold(0u64, |total, rows| {
            total
                .checked_add(rows.rows.len() as u64)
                .context(InvalidInsertRequestSnafu {
                    reason: "Insert row count exceeds u64::MAX",
                })
        })
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

        // All tables in a batch resolve to the same database. Qualified SQL
        // inserts may target a different database than the session's current one.
        let table_info = table_infos.values().next();
        let catalog = table_info.map_or(ctx.current_catalog(), |info| info.catalog_name.as_str());
        let schema =
            table_info.map_or_else(|| ctx.current_schema(), |info| info.schema_name.clone());
        let write_cost = write_meter!(
            catalog,
            schema.clone(),
            requests,
            ctx.write_rows_to_admit(catalog, &schema, count_insert_rows(&requests)?),
            ctx.channel() as u8
        )
        .await
        .context(WriteRejectedSnafu)?;
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
        crate::metrics::DIST_INGEST_ROW_COUNT
            .with_label_values(&[ctx.get_db_string().as_str()])
            .inc_by(affected_rows as u64);
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

    /// Adds missing columns from a bulk stream's schema and returns the refreshed table.
    /// Call once when initializing the stream, before writing its first batch.
    /// Does not infer new nested or dictionary columns.
    pub async fn ensure_bulk_insert_schema(
        &self,
        table: TableRef,
        request_schema: &ArrowSchema,
        ctx: &QueryContextRef,
        statement_executor: &StatementExecutor,
    ) -> Result<TableRef> {
        let table_info = table.table_info();
        if self.auto_create_disabled_reason(ctx)?.is_some()
            && !Self::is_auto_create_exempt_private_table(&table_info.schema_name, &table_info.name)
        {
            return Ok(table);
        }

        let table_schema = table.schema();
        let schema = request_schema
            .fields()
            .iter()
            .filter(|field| table_schema.column_schema_by_name(field.name()).is_none())
            .map(|field| {
                let data_type = field.data_type();
                // Dictionary values can reach the same infallible child-type conversion
                // as nested types, even when Arrow's is_nested() returns false.
                ensure!(
                    !data_type.is_nested() && !matches!(data_type, ArrowDataType::Dictionary(..)),
                    crate::error::NotSupportedSnafu {
                        feat: format!(
                            "automatically adding bulk insert column '{}' with type {:?}",
                            field.name(),
                            data_type
                        ),
                    }
                );
                let column = datatypes::schema::ColumnSchema::try_from(field.as_ref())
                    .context(crate::error::ConvertSchemaSnafu)?;
                // Arrow fields do not carry primary-key semantics. New columns are
                // fields, unless explicitly marked as a time index.
                let column_def =
                    try_as_column_def(&column, false).context(crate::error::ColumnDataTypeSnafu)?;
                Ok(ColumnSchema {
                    column_name: column_def.name,
                    datatype: column_def.data_type,
                    semantic_type: column_def.semantic_type,
                    datatype_extension: column_def.datatype_extension,
                    options: column_def.options,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut request = RowInsertRequest {
            table_name: table_info.name.clone(),
            rows: Some(Rows {
                schema,
                rows: Vec::new(),
            }),
        };
        let Some(alter_expr) =
            self.get_alter_table_expr_on_demand(&mut request, &table, ctx, false, false, true)?
        else {
            return Ok(table);
        };

        statement_executor
            .alter_table_inner(alter_expr, ctx.clone(), TriggerReason::AutoAlter)
            .await?;
        self.get_table(
            &table_info.catalog_name,
            &table_info.schema_name,
            &table_info.name,
        )
        .await?
        .with_context(|| TableNotFoundSnafu {
            table_name: table_info.full_table_name(),
        })
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
            None,
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
    ///
    /// `align_time_index_unit` is the selected physical metric table's time
    /// index unit; passing `Some` (metric engine path only) rewrites each
    /// request's time index column, inside this function's existing table
    /// lookups (no extra catalog access): existing destination tables are
    /// converted to their own unit, and new tables to the given unit.
    #[allow(clippy::too_many_arguments)]
    async fn create_or_alter_tables_on_demand(
        &self,
        requests: &mut RowInsertRequests,
        ctx: &QueryContextRef,
        auto_create_table_type: AutoCreateTableType,
        statement_executor: &StatementExecutor,
        accommodate_existing_schema: bool,
        is_single_value: bool,
        align_time_index_unit: Option<TimeUnit>,
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
            for req in &mut requests.inserts {
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
                // Metric path: an existing destination table keeps its own
                // time index unit (it may be bound to another physical
                // table than the one selected by this request).
                if align_time_index_unit.is_some()
                    && let Some(rows) = req.rows.as_mut()
                    && let Some(target_unit) = table_time_index_unit(&table)
                {
                    convert_rows_time_unit(rows, target_unit)?;
                }
                let table_info = table.table_info();
                if matches!(auto_create_table_type, AutoCreateTableType::Trace { .. }) {
                    validate_trace_table_model(&table_info, ctx)?;
                }
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
                    if matches!(auto_create_table_type, AutoCreateTableType::Trace { .. }) {
                        validate_trace_table_model(&table_info, ctx)?;
                    }
                    if table_info.is_ttl_instant_table() {
                        instant_table_ids.insert(table_info.table_id());
                    }
                    // Metric path: an existing destination table keeps its
                    // own time index unit (it may be bound to another
                    // physical table than the one selected by this request).
                    if align_time_index_unit.is_some()
                        && let Some(rows) = req.rows.as_mut()
                        && let Some(target_unit) = table_time_index_unit(&table)
                    {
                        convert_rows_time_unit(rows, target_unit)?;
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
                    // Metric path: a new table uses the selected physical
                    // table's unit; convert before the create expression is
                    // derived from the request schema.
                    if let Some(physical_unit) = align_time_index_unit
                        && let Some(rows) = req.rows.as_mut()
                    {
                        convert_rows_time_unit(rows, physical_unit)?;
                    }
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
                            ctx.extension(SEMANTIC_PIPELINE)
                                .unwrap_or(TABLE_DATA_MODEL_TRACE_V1)
                                .to_string(),
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
    ) -> Result<TableRef> {
        let catalog_name = ctx.current_catalog();
        let schema_name = ctx.current_schema();

        // check if exist
        if let Some(table) = self
            .get_table(catalog_name, &schema_name, &physical_table)
            .await?
        {
            return Ok(table);
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
        debug!("Ensuring physical metric table `{table_reference}` exists for insert");

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
            Ok(table) => Ok(table),
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

        debug!("Ensuring table `{table_ref}` exists for insert");
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
                validate_trace_table_model(&table.table_info(), ctx)?;
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
            Ok(res) => Ok(res),
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

/// Returns the table's time index unit, if any. A metric table without a
/// timestamp column is left alone by the unit alignment: the metric engine
/// rejects it anyway.
fn table_time_index_unit(table: &TableRef) -> Option<TimeUnit> {
    table
        .table_info()
        .meta
        .schema
        .timestamp_column()
        .and_then(|col| col.data_type.as_timestamp().map(|ts| ts.unit()))
}

fn convert_rows_time_unit(rows: &mut Rows, target_unit: TimeUnit) -> Result<()> {
    let Some(ts_index) = rows
        .schema
        .iter()
        .position(|col| col.semantic_type == SemanticType::Timestamp as i32)
    else {
        return Ok(());
    };
    let Some(source_unit) = ColumnDataType::try_from(rows.schema[ts_index].datatype)
        .ok()
        .and_then(api::helper::timestamp_unit)
    else {
        return Ok(());
    };
    if source_unit == target_unit {
        return Ok(());
    }

    rows.schema[ts_index].datatype = api::helper::timestamp_datatype(target_unit) as i32;
    // Timestamp columns never carry a datatype extension.
    rows.schema[ts_index].datatype_extension = None;

    // Note: the schema is rewritten before the rows are converted, so an
    // overflow error mid-batch leaves this request half-converted. That is
    // harmless: the error aborts the whole insert request.
    //
    // `validate_column_count_match` guarantees every row carries exactly one
    // value per schema column, so the time index position is directly in
    // bounds; no per-value search is needed.
    for row in &mut rows.rows {
        debug_assert_eq!(row.values.len(), rows.schema.len());
        let value = &mut row.values[ts_index];
        let Some(value_data) = value.value_data.take() else {
            continue;
        };
        value.value_data =
            convert_timestamp_value_data(value_data, source_unit, target_unit, ts_index)?;
    }
    Ok(())
}

fn convert_timestamp_value_data(
    value_data: ValueData,
    source_unit: TimeUnit,
    target_unit: TimeUnit,
    column_index: usize,
) -> Result<Option<ValueData>> {
    let timestamp = match value_data {
        ValueData::TimestampSecondValue(v) => Timestamp::new_second(v),
        ValueData::TimestampMillisecondValue(v) => Timestamp::new_millisecond(v),
        ValueData::TimestampMicrosecondValue(v) => Timestamp::new_microsecond(v),
        ValueData::TimestampNanosecondValue(v) => Timestamp::new_nanosecond(v),
        // Null or non-timestamp value; nothing to convert.
        other => return Ok(Some(other)),
    };
    let converted = timestamp
        .convert_to(target_unit)
        .with_context(|| InvalidInsertRequestSnafu {
            reason: format!(
                "timestamp column {column_index} value {} in unit {source_unit:?} overflows when converting to unit {target_unit:?}",
                timestamp.value()
            ),
        })?;
    Ok(api::helper::to_grpc_value(datatypes::value::Value::Timestamp(converted)).value_data)
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

/// Rejects writes from a different built-in trace model before schema mutation.
/// Unstamped, explicitly created tables remain subject to normal schema validation.
pub fn validate_trace_table_model(table_info: &TableInfo, ctx: &QueryContextRef) -> Result<()> {
    let Some(expected @ (TABLE_DATA_MODEL_TRACE_V1 | TABLE_DATA_MODEL_TRACE_V2)) =
        ctx.extension(SEMANTIC_PIPELINE)
    else {
        return Ok(());
    };
    if let Some(actual) = table_info.meta.options.data_model() {
        ensure!(
            actual == expected,
            InvalidInsertRequestSnafu {
                reason: format!(
                    "Trace table `{}` uses {actual}, but the request uses {expected}",
                    table_info.name,
                ),
            }
        );
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
                return Err(meter_core::collect::WriteRejected::new(
                    "flow mirror pending rows limit exceeded for instant-TTL insert",
                ))
                .context(WriteRejectedSnafu);
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
    use api::v1::{Row, RowInsertRequest, Rows, Value};
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

    use crate::insert::*;
    use crate::test_util::{
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

    fn make_metric_physical_table_ref_with_time_unit(unit: TimeUnit) -> TableRef {
        let schema = datatypes::schema::SchemaBuilder::try_from_columns(vec![
            ColumnSchema::new(
                greptime_timestamp(),
                ConcreteDataType::timestamp_datatype(unit),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(greptime_value(), ConcreteDataType::float64_datatype(), true),
        ])
        .unwrap()
        .build()
        .unwrap();
        let meta = TableMetaBuilder::empty()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![])
            .value_indices(vec![1])
            .engine("metric")
            .next_column_id(0)
            .options(Default::default())
            .created_on(Default::default())
            .build()
            .unwrap();
        let info = Arc::new(
            TableInfoBuilder::default()
                .table_id(1)
                .table_version(0)
                .name("greptime_physical_table")
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

    fn ms_row_insert_request(timestamp_ms: i64) -> RowInsertRequest {
        ms_row_insert_request_named("my_metric", timestamp_ms)
    }

    fn ms_row_insert_request_named(table: &str, timestamp_ms: i64) -> RowInsertRequest {
        RowInsertRequest {
            table_name: table.to_string(),
            rows: Some(Rows {
                schema: vec![
                    time_index_column_schema(
                        greptime_timestamp(),
                        ColumnDataType::TimestampMillisecond,
                    ),
                    field_column_schema(greptime_value(), ColumnDataType::Float64),
                ],
                rows: vec![Row {
                    values: vec![
                        Value {
                            value_data: Some(ValueData::TimestampMillisecondValue(timestamp_ms)),
                        },
                        Value {
                            value_data: Some(ValueData::F64Value(1.0)),
                        },
                    ],
                }],
            }),
        }
    }

    /// Converts the time index column of each request to `target_unit`.
    fn convert_row_insert_requests_time_unit(
        requests: &mut RowInsertRequests,
        target_unit: TimeUnit,
    ) -> Result<()> {
        for request in &mut requests.inserts {
            let Some(rows) = request.rows.as_mut() else {
                continue;
            };
            convert_rows_time_unit(rows, target_unit)?;
        }
        Ok(())
    }

    #[test]
    fn test_convert_row_insert_requests_time_unit_noop_when_matching() {
        let mut requests = RowInsertRequests {
            inserts: vec![ms_row_insert_request(123)],
        };
        convert_row_insert_requests_time_unit(&mut requests, TimeUnit::Millisecond).unwrap();
        let rows = requests.inserts[0].rows.as_ref().unwrap();
        assert_eq!(
            rows.schema[0].datatype,
            ColumnDataType::TimestampMillisecond as i32
        );
        assert!(matches!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMillisecondValue(123))
        ));
    }

    #[test]
    fn test_convert_row_insert_requests_time_unit_widens_losslessly() {
        let mut requests = RowInsertRequests {
            inserts: vec![ms_row_insert_request(123)],
        };
        convert_row_insert_requests_time_unit(&mut requests, TimeUnit::Microsecond).unwrap();
        let rows = requests.inserts[0].rows.as_ref().unwrap();
        assert_eq!(
            rows.schema[0].datatype,
            ColumnDataType::TimestampMicrosecond as i32
        );
        assert!(matches!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMicrosecondValue(123_000))
        ));
    }

    #[test]
    fn test_convert_row_insert_requests_time_unit_truncates_on_narrowing() {
        // 123_456_789 ns floors to 123_456 us and 123 ms; negative values
        // floor towards negative infinity, matching `Timestamp::convert_to`.
        let requests = |value_ns: i64| RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: "my_metric".to_string(),
                rows: Some(Rows {
                    schema: vec![
                        time_index_column_schema(
                            greptime_timestamp(),
                            ColumnDataType::TimestampNanosecond,
                        ),
                        field_column_schema(greptime_value(), ColumnDataType::Float64),
                    ],
                    rows: vec![Row {
                        values: vec![
                            Value {
                                value_data: Some(ValueData::TimestampNanosecondValue(value_ns)),
                            },
                            Value {
                                value_data: Some(ValueData::F64Value(1.0)),
                            },
                        ],
                    }],
                }),
            }],
        };

        let mut reqs = requests(123_456_789);
        convert_row_insert_requests_time_unit(&mut reqs, TimeUnit::Microsecond).unwrap();
        let rows = reqs.inserts[0].rows.as_ref().unwrap();
        assert!(matches!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMicrosecondValue(123_456))
        ));

        let mut reqs = requests(123_456_789);
        convert_row_insert_requests_time_unit(&mut reqs, TimeUnit::Millisecond).unwrap();
        let rows = reqs.inserts[0].rows.as_ref().unwrap();
        assert!(matches!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMillisecondValue(123))
        ));

        let mut reqs = requests(-123_456_789);
        convert_row_insert_requests_time_unit(&mut reqs, TimeUnit::Microsecond).unwrap();
        let rows = reqs.inserts[0].rows.as_ref().unwrap();
        assert!(matches!(
            rows.rows[0].values[0].value_data,
            Some(ValueData::TimestampMicrosecondValue(-123_457))
        ));
    }

    #[test]
    fn test_convert_row_insert_requests_time_unit_overflow() {
        let mut requests = RowInsertRequests {
            inserts: vec![ms_row_insert_request(i64::MAX)],
        };
        let err =
            convert_row_insert_requests_time_unit(&mut requests, TimeUnit::Nanosecond).unwrap_err();
        assert!(err.to_string().contains("overflows"), "{err}");
    }

    #[test]
    fn test_table_time_index_unit() {
        assert_eq!(
            table_time_index_unit(&make_metric_physical_table_ref_with_time_unit(
                TimeUnit::Microsecond
            )),
            Some(TimeUnit::Microsecond)
        );
        assert_eq!(
            table_time_index_unit(&make_metric_physical_table_ref_with_time_unit(
                TimeUnit::Millisecond
            )),
            Some(TimeUnit::Millisecond)
        );
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

    // Keep global meter registration in one test, isolated by nextest's per-test process.
    #[tokio::test]
    async fn test_write_meter_admission() {
        use std::cell::Cell;
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        use api::region::RegionResponse;
        use api::v1::region::region_request::Body;
        use arrow::array::{Int32Array, TimestampMillisecondArray};
        use arrow::record_batch::RecordBatch;
        use bytes::Bytes;
        use common_error::ext::{ErrorExt, RetryHint};
        use common_error::status_code::StatusCode;
        use common_grpc::flight::{FlightEncoder, FlightMessage};
        use common_meta::ddl::test_util::datanode_handler::DatanodeWatcher;
        use futures::future::BoxFuture;
        use meter_core::ItemCalculator;
        use meter_core::collect::{Collect, WriteRejected};
        use meter_core::data::MeterRecord;
        use meter_core::global::global_registry;
        use session::context::Channel;

        const CATALOG: &str = "write_meter_test";

        #[derive(Default)]
        struct Meter {
            reject: AtomicBool,
            attempts: Mutex<Vec<MeterRecord>>,
            accepted_value: AtomicU64,
        }

        impl Collect for Meter {
            fn on_write(
                &self,
                record: MeterRecord,
            ) -> BoxFuture<'_, std::result::Result<(), WriteRejected>> {
                Box::pin(async move {
                    if record.catalog != CATALOG {
                        return Ok(());
                    }
                    let value = record.value;
                    self.attempts.lock().unwrap().push(record);
                    if self.reject.load(Ordering::Relaxed) {
                        return Err(WriteRejected::new("database row quota exhausted"));
                    }
                    self.accepted_value.fetch_add(value, Ordering::Relaxed);
                    Ok(())
                })
            }

            fn on_read(&self, _: MeterRecord) {}
        }

        impl ItemCalculator<InstantAndNormalInsertRequests> for Meter {
            fn calc(&self, _: &InstantAndNormalInsertRequests) -> u64 {
                17
            }
        }

        let kv_backend = prepare_mocked_backend().await;
        let partition_manager = create_partition_rule_manager(kv_backend.clone()).await;
        let (sender, mut dispatched) = tokio::sync::mpsc::channel(16);
        let watcher = DatanodeWatcher::new(sender).with_handler(|_, request| {
            let rows = match request.body.unwrap() {
                Body::Inserts(requests) => requests
                    .requests
                    .iter()
                    .filter_map(|request| request.rows.as_ref())
                    .map(|rows| rows.rows.len())
                    .sum(),
                // The bulk batches below each contain two rows.
                Body::BulkInsert(_) => 2,
                body => panic!("unexpected request: {body:?}"),
            };
            Ok(RegionResponse::new(rows))
        });
        let flow_cache = Cache::new(100);
        let inserter = Inserter::new(
            catalog::memory::MemoryCatalogManager::new(),
            partition_manager,
            Arc::new(MockDatanodeManager::new(watcher)),
            Arc::new(new_table_flownode_set_cache(
                String::new(),
                flow_cache.clone(),
                kv_backend,
            )),
            true,
        );
        let mut table_info = new_test_table_info(1, "table_1", [1].into_iter());
        table_info.catalog_name = CATALOG.to_string();
        table_info.schema_name = "target_db".to_string();
        let table_info = Arc::new(table_info);
        let table_infos = HashMap::from_iter([(1, table_info.clone())]);
        let ctx = Arc::new(QueryContext::with_channel(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            Channel::Postgres,
        ));
        let rows_request = || {
            let build = |num_rows| RegionInsertRequests {
                requests: vec![RegionInsertRequest {
                    region_id: RegionId::new(1, 1).as_u64(),
                    rows: Some(Rows {
                        schema: vec![],
                        rows: vec![api::v1::Row { values: vec![] }; num_rows],
                    }),
                    ..Default::default()
                }],
            };
            InstantAndNormalInsertRequests {
                normal_requests: build(3),
                instant_requests: build(2),
            }
        };

        // No collector or calculator: ordinary OSS insertion still succeeds.
        let output = inserter
            .do_request(rows_request(), &table_infos, &ctx)
            .await
            .unwrap();
        assert_eq!(output.meta.cost, 0);
        assert!(matches!(output.data, OutputData::AffectedRows(3)));
        dispatched.try_recv().unwrap();

        let meter = Arc::new(Meter::default());
        global_registry().set_collector(meter.clone());
        global_registry().register_calculator(meter.clone());
        // The dependency controls noop mode; exercise both builds with this test.
        let enabled = Cell::new(false);
        write_meter!({
            enabled.set(true);
            MeterRecord::new("probe".into(), "probe".into(), 0, 0, 0)
        })
        .await
        .unwrap();
        let enabled = enabled.get();

        let output = inserter
            .do_request(rows_request(), &table_infos, &ctx)
            .await
            .unwrap();
        assert_eq!(output.meta.cost, if enabled { 17 } else { 0 });
        assert!(matches!(output.data, OutputData::AffectedRows(3)));
        dispatched.try_recv().unwrap();
        assert!(flow_cache.contains_key(&1));
        flow_cache.invalidate_all();
        meter.reject.store(true, Ordering::Relaxed);
        let result = inserter
            .do_request(rows_request(), &table_infos, &ctx)
            .await;
        if enabled {
            let error = result.unwrap_err();
            assert_eq!(error.status_code(), StatusCode::RateLimited);
            assert_eq!(error.retry_hint(), RetryHint::Retryable);
            assert!(error.to_string().contains("database row quota exhausted"));
            assert!(dispatched.try_recv().is_err());
            assert!(
                !flow_cache.contains_key(&1),
                "rejected write reached flow mirroring"
            );
            assert_eq!(meter.accepted_value.load(Ordering::Relaxed), 17);
            let attempts = meter.attempts.lock().unwrap();
            assert_eq!(attempts.len(), 2);
            for record in attempts.iter() {
                assert_eq!(record.catalog, CATALOG);
                assert_eq!(record.schema, "target_db");
                assert_eq!(
                    (record.rows, record.value, record.source),
                    (5, 17, Channel::Postgres as u8)
                );
            }
        } else {
            assert_eq!(result.unwrap().meta.cost, 0);
            dispatched.try_recv().unwrap();
            assert!(meter.attempts.lock().unwrap().is_empty());
        }
        meter.attempts.lock().unwrap().clear();

        // Rejection must also precede admission to the table batcher's queue.
        if enabled {
            let batcher: Arc<dyn PendingRowsBatcher> = Arc::new(UnexpectedBatcher);
            let rows = Rows {
                schema: vec![
                    api::v1::helper::tag_column_schema("a", ColumnDataType::Int32),
                    time_index_column_schema("ts", ColumnDataType::TimestampMillisecond),
                    field_column_schema("b", ColumnDataType::Int32),
                ],
                rows: vec![api::v1::Row {
                    values: vec![
                        api::v1::value::ValueData::I32Value(60).into(),
                        Value {
                            value_data: Some(api::v1::value::ValueData::TimestampMillisecondValue(
                                0,
                            )),
                        },
                        api::v1::value::ValueData::I32Value(0).into(),
                    ],
                }],
            };
            let error = inserter
                .submit_table_rows(rows, table_info.clone(), ctx.clone(), &batcher)
                .await
                .unwrap_err();
            assert_eq!(error.status_code(), StatusCode::RateLimited);
            let mut attempts = meter.attempts.lock().unwrap();
            assert_eq!(attempts.len(), 1);
            assert_eq!(attempts[0].catalog, CATALOG);
            assert_eq!(attempts[0].schema, "target_db");
            assert_eq!(attempts[0].rows, 1);
            attempts.clear();
        }

        let table = Arc::new(table::Table::new(
            table_info.clone(),
            table::metadata::FilterPushDownType::Unsupported,
            Arc::new(DummyDataSource),
        ));
        let batch = RecordBatch::try_new(
            table_info.meta.schema.arrow_schema().clone(),
            vec![
                Arc::new(Int32Array::from(vec![60, 70])),
                Arc::new(TimestampMillisecondArray::from(vec![0, 1])),
                Arc::new(Int32Array::from(vec![0, 0])),
            ],
        )
        .unwrap();
        let bulk_insert = |batch: RecordBatch| {
            let flight_data = FlightEncoder::default()
                .encode(FlightMessage::RecordBatch(batch.clone()))
                .into_iter()
                .next()
                .unwrap();
            inserter.handle_bulk_insert(
                table.clone(),
                flight_data,
                batch,
                Bytes::new(),
                false,
                Channel::Grpc,
            )
        };
        // Empty batches bypass admission even while the collector rejects.
        assert_eq!(bulk_insert(batch.slice(0, 0)).await.unwrap(), 0);
        assert!(meter.attempts.lock().unwrap().is_empty());
        assert!(dispatched.try_recv().is_err());

        // A collector change between batches takes effect on the very next batch.
        for reject in [false, true, false] {
            meter.reject.store(reject, Ordering::Relaxed);
            let result = bulk_insert(batch.clone()).await;
            if enabled && reject {
                let error = result.unwrap_err();
                assert_eq!(error.status_code(), StatusCode::RateLimited);
                assert_eq!(error.retry_hint(), RetryHint::Retryable);
                assert!(dispatched.try_recv().is_err());
            } else {
                assert_eq!(result.unwrap(), 2);
                dispatched.try_recv().unwrap();
            }
        }
        {
            let attempts = meter.attempts.lock().unwrap();
            assert_eq!(attempts.len(), if enabled { 3 } else { 0 });
            for record in attempts.iter() {
                assert_eq!(record.catalog, CATALOG);
                assert_eq!(record.schema, "target_db");
                assert_eq!(
                    (record.rows, record.value, record.source),
                    (2, 0, Channel::Grpc as u8)
                );
            }
        }
        assert_eq!(
            meter.accepted_value.load(Ordering::Relaxed),
            if enabled { 17 } else { 0 }
        );

        // Aggregate repeated database targets and retain admission across nested
        // batching without changing the caller's reusable context.
        meter.attempts.lock().unwrap().clear();
        let original = Arc::new(QueryContext::with_channel(
            CATALOG,
            "a",
            Channel::Prometheus,
        ));
        let mut batches = ["a", "b", "a"].map(|schema| {
            let ctx = if schema == "a" {
                original.clone()
            } else {
                Arc::new(QueryContext::with_channel(
                    CATALOG,
                    schema,
                    Channel::Prometheus,
                ))
            };
            (
                ctx,
                RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: "data".into(),
                        rows: Some(Rows {
                            schema: vec![],
                            rows: vec![api::v1::Row::default(); 2],
                        }),
                    }],
                },
            )
        });
        admit_row_insert_batches(&mut batches).await.unwrap();
        admit_row_insert_batches(&mut batches).await.unwrap();
        assert_eq!(original.write_rows_to_admit(CATALOG, "a", 4), 4);
        for (ctx, _) in &batches {
            assert_eq!(
                ctx.write_rows_to_admit(CATALOG, &ctx.current_schema(), 2),
                0
            );
            assert_eq!(ctx.channel(), Channel::Prometheus);
        }
        {
            let attempts = meter.attempts.lock().unwrap();
            let totals = attempts
                .iter()
                .map(|r| (r.schema.as_str(), r.rows, r.value))
                .collect::<Vec<_>>();
            assert_eq!(
                totals,
                if enabled {
                    vec![("a", 4, 0), ("b", 2, 0)]
                } else {
                    vec![]
                }
            );
        }
        meter.attempts.lock().unwrap().clear();
        meter.reject.store(false, Ordering::Relaxed);
        let ctx = Arc::new(QueryContext::with_channel(
            CATALOG,
            "logical",
            Channel::Otlp,
        ));
        let admitted = admit_write(2, &ctx).await.unwrap();
        let mut requests = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: "metric".to_string(),
                rows: Some(Rows {
                    schema: vec![],
                    rows: vec![api::v1::Row::default(); 2],
                }),
            }],
        };
        let original = requests.clone();
        let cost = Inserter::meter_row_inserts(&mut requests, &admitted)
            .await
            .unwrap();
        assert_eq!(cost, if enabled { 17 } else { 0 });
        assert_eq!(requests, original);
        let records = meter
            .attempts
            .lock()
            .unwrap()
            .iter()
            .map(|record| (record.rows, record.value))
            .collect::<Vec<_>>();
        assert_eq!(
            records,
            if enabled {
                vec![(2, 0), (0, 17)]
            } else {
                vec![]
            }
        );
        meter.reject.store(true, Ordering::Relaxed);
        let result = Inserter::meter_row_inserts(&mut requests, &admitted).await;
        if enabled {
            assert_eq!(result.unwrap_err().status_code(), StatusCode::RateLimited);
        } else {
            assert_eq!(result.unwrap(), 0);
        }
        assert_eq!(requests, original);
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

    struct UnexpectedBatcher;

    #[async_trait::async_trait]
    impl PendingRowsBatcher for UnexpectedBatcher {
        async fn acquire(&self) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
            panic!("empty writes must not acquire batch admission")
        }

        async fn submit(
            &self,
            _table_info: TableInfoRef,
            _batch: arrow::record_batch::RecordBatch,
            _ctx: QueryContextRef,
            _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        ) -> Result<Output> {
            panic!("empty writes must not submit a batch")
        }
    }

    async fn batcher_test_inserter() -> Inserter {
        let kv_backend = prepare_mocked_backend().await;
        Inserter::new(
            catalog::memory::MemoryCatalogManager::new(),
            create_partition_rule_manager(kv_backend.clone()).await,
            Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler)),
            Arc::new(new_table_flownode_set_cache(
                String::new(),
                Cache::new(100),
                kv_backend,
            )),
            true,
        )
    }

    #[tokio::test]
    async fn test_logical_batcher_eligibility() {
        use catalog::RegisterTableRequest;
        use catalog::memory::MemoryCatalogManager;
        use common_meta::instruction::{CacheIdent, CreateFlow};
        use common_meta::kv_backend::KvBackendRef;
        use common_meta::kv_backend::memory::MemoryKvBackend;
        use datatypes::schema::{ColumnDefaultConstraint, SchemaBuilder};
        let requests = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: "test_table".to_string(),
                rows: None,
            }],
        };
        let original =
            make_table_ref_with_schema("ts", "value", ConcreteDataType::float64_datatype())
                .table_info();
        for case in [
            "eligible",
            "physical",
            "ordinary",
            "instant",
            "disabled",
            "hint",
            "flow",
            "required_tag",
            "default",
        ] {
            let mut info = (*original).clone();
            info.meta.engine = METRIC_ENGINE_NAME.to_string();
            info.meta.options.extra_options.insert(
                LOGICAL_TABLE_METADATA_KEY.to_string(),
                "physical".to_string(),
            );
            let mut ctx = QueryContext::arc().fork();
            match case {
                "physical" => {
                    info.meta
                        .options
                        .extra_options
                        .insert(LOGICAL_TABLE_METADATA_KEY.to_string(), "other".to_string());
                }
                "ordinary" => info.meta.engine = "mito".to_string(),
                "instant" => info.meta.options.ttl = Some(common_time::ttl::TimeToLive::Instant),
                "hint" => ctx.set_extension(AUTO_CREATE_TABLE_KEY, "false"),
                "required_tag" | "default" => {
                    let mut columns = info.meta.schema.column_schemas().to_vec();
                    let mut tag = ColumnSchema::new(
                        "tag",
                        ConcreteDataType::string_datatype(),
                        case != "required_tag",
                    );
                    if case == "default" {
                        tag = tag
                            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
                            .unwrap();
                    }
                    columns.push(tag);
                    info.meta.schema = Arc::new(
                        SchemaBuilder::try_from_columns(columns)
                            .unwrap()
                            .build()
                            .unwrap(),
                    );
                    info.meta.primary_key_indices = vec![2];
                }
                _ => {}
            }
            let catalog = MemoryCatalogManager::with_default_setup();
            let table = Arc::new(table::Table::new(
                Arc::new(info),
                table::metadata::FilterPushDownType::Unsupported,
                Arc::new(DummyDataSource),
            ));
            catalog
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "test_table".to_string(),
                    table_id: 1,
                    table,
                })
                .unwrap();
            let mut inserter = batcher_test_inserter().await;
            inserter.catalog_manager = catalog;
            inserter.auto_create_table = case != "disabled";
            let kv_backend: KvBackendRef = Arc::new(MemoryKvBackend::default());
            inserter.table_flownode_set_cache = Arc::new(new_table_flownode_set_cache(
                String::new(),
                Cache::new(10),
                kv_backend,
            ));
            if case == "flow" {
                inserter
                    .table_flownode_set_cache
                    .invalidate(&[CacheIdent::CreateFlow(CreateFlow {
                        flow_id: 1,
                        source_table_ids: vec![1],
                        partition_to_peer_mapping: vec![(0, Peer::empty(1))],
                    })])
                    .await
                    .unwrap();
            }
            assert_eq!(
                inserter
                    .can_batch_metric_rows(&requests, &Arc::new(ctx), "physical")
                    .await
                    .unwrap(),
                case == "eligible",
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn test_batcher_meter_preserves_request() {
        let mut requests = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: "sample".to_string(),
                rows: Some(Rows {
                    schema: vec![],
                    rows: vec![api::v1::Row {
                        values: vec![Value {
                            value_data: Some(api::v1::value::ValueData::F64Value(1.5)),
                        }],
                    }],
                }),
            }],
        };
        let expected = requests.clone();
        Inserter::meter_row_inserts(&mut requests, &QueryContext::arc())
            .await
            .unwrap();
        assert_eq!(requests, expected);
    }

    #[tokio::test]
    async fn test_instant_table_bypasses_batcher() {
        let batcher: Arc<dyn PendingRowsBatcher> = Arc::new(UnexpectedBatcher);
        let inserter = batcher_test_inserter()
            .await
            .with_pending_rows_batcher(Some(batcher));
        let mut ctx = session::context::QueryContextBuilder::default().build();
        ctx.set_batching_enabled(true);
        let ctx = Arc::new(ctx);
        let table = make_table_ref_with_schema("ts", "value", ConcreteDataType::float64_datatype())
            .table_info();
        assert!(inserter.table_batcher(&table, &ctx).is_some());
        let mut instant = (*table).clone();
        instant.meta.options.ttl = Some(common_time::ttl::TimeToLive::Instant);
        assert!(inserter.table_batcher(&Arc::new(instant), &ctx).is_none());
    }

    #[tokio::test]
    async fn test_empty_prepared_rows_skip_batcher() {
        let inserter = batcher_test_inserter().await;
        let table = make_table_ref_with_schema("ts", "value", ConcreteDataType::float64_datatype())
            .table_info();
        let batcher: Arc<dyn PendingRowsBatcher> = Arc::new(UnexpectedBatcher);
        let ctx = QueryContext::arc();
        let output = inserter
            .submit_table_rows(
                Rows {
                    schema: vec![],
                    rows: vec![],
                },
                table.clone(),
                ctx.clone(),
                &batcher,
            )
            .await
            .unwrap();
        assert!(matches!(output.data, OutputData::AffectedRows(0)));
        let output = inserter
            .submit_pending_rows(
                RowInsertRequests {
                    inserts: vec![RowInsertRequest {
                        table_name: table.name.clone(),
                        rows: None,
                    }],
                },
                HashMap::from_iter([(table.table_id(), table)]),
                ctx,
                &batcher,
            )
            .await
            .unwrap();
        assert!(matches!(output.data, OutputData::AffectedRows(0)));
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
        async fn handle(
            &self,
            _request: api::v1::region::RegionRequest,
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
