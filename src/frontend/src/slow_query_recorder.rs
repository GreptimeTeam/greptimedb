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
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema, CreateTableExpr, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
use catalog::CatalogManagerRef;
use common_catalog::consts::{default_engine, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_telemetry::logging::{SlowQueriesRecordType, SlowQueryOptions};
use common_telemetry::{debug, error, info, slow};
use common_time::timestamp::{TimeUnit, Timestamp};
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use query::parser::QueryStatement;
use rand::random;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::ResultExt;
use store_api::mito_engine_options::{APPEND_MODE_KEY, TTL_KEY};
use table::TableRef;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::error::{CatalogSnafu, Result, TableOperationSnafu};

const SLOW_QUERY_TABLE_NAME: &str = "slow_queries";
const SLOW_QUERY_TABLE_COST_COLUMN_NAME: &str = "cost";
const SLOW_QUERY_TABLE_THRESHOLD_COLUMN_NAME: &str = "threshold";
const SLOW_QUERY_TABLE_QUERY_COLUMN_NAME: &str = "query";
const SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME: &str = "timestamp";
const SLOW_QUERY_TABLE_IS_PROMQL_COLUMN_NAME: &str = "is_promql";
const SLOW_QUERY_TABLE_PROMQL_START_COLUMN_NAME: &str = "promql_start";
const SLOW_QUERY_TABLE_PROMQL_END_COLUMN_NAME: &str = "promql_end";
const SLOW_QUERY_TABLE_PROMQL_RANGE_COLUMN_NAME: &str = "promql_range";
const SLOW_QUERY_TABLE_PROMQL_STEP_COLUMN_NAME: &str = "promql_step";

const DEFAULT_SLOW_QUERY_TABLE_TTL: &str = "30d";
const DEFAULT_SLOW_QUERY_EVENTS_CHANNEL_SIZE: usize = 1024;

/// SlowQueryRecorder is responsible for recording slow queries.
#[derive(Clone)]
pub struct SlowQueryRecorder {
    tx: Sender<SlowQueryEvent>,
    slow_query_opts: SlowQueryOptions,
    _handle: Arc<JoinHandle<()>>,
}

#[derive(Debug)]
struct SlowQueryEvent {
    cost: u64,
    threshold: u64,
    query: String,
    is_promql: bool,
    query_ctx: QueryContextRef,
    promql_range: Option<u64>,
    promql_step: Option<u64>,
    promql_start: Option<i64>,
    promql_end: Option<i64>,
}

impl SlowQueryRecorder {
    /// Create a new SlowQueryRecorder.
    pub fn new(
        slow_query_opts: SlowQueryOptions,
        inserter: InserterRef,
        statement_executor: StatementExecutorRef,
        catalog_manager: CatalogManagerRef,
    ) -> Self {
        let (tx, rx) = channel(DEFAULT_SLOW_QUERY_EVENTS_CHANNEL_SIZE);

        let ttl = slow_query_opts
            .ttl
            .clone()
            .unwrap_or(DEFAULT_SLOW_QUERY_TABLE_TTL.to_string());

        // Start a new task to process the slow query events.
        let event_handler = SlowQueryEventHandler {
            inserter,
            statement_executor,
            catalog_manager,
            rx,
            record_type: slow_query_opts.record_type,
            ttl,
        };

        // Start a new background task to process the slow query events.
        let handle = tokio::spawn(async move {
            event_handler.process_slow_query().await;
        });

        Self {
            tx,
            slow_query_opts,
            _handle: Arc::new(handle),
        }
    }

    /// Starts a new SlowQueryTimer. Returns `None` if `slow_query.enable` is false.
    /// The timer sets the start time when created and calculates the elapsed duration when dropped.
    pub fn start(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Option<SlowQueryTimer> {
        if self.slow_query_opts.enable {
            Some(SlowQueryTimer {
                stmt,
                query_ctx,
                start: Instant::now(), // Set the initial start time.
                threshold: self.slow_query_opts.threshold,
                sample_ratio: self.slow_query_opts.sample_ratio,
                tx: self.tx.clone(),
            })
        } else {
            None
        }
    }
}

struct SlowQueryEventHandler {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    catalog_manager: CatalogManagerRef,
    rx: Receiver<SlowQueryEvent>,
    record_type: SlowQueriesRecordType,
    ttl: String,
}

impl SlowQueryEventHandler {
    async fn process_slow_query(mut self) {
        info!(
            "Start the background handler to process slow query events and record them in {:?}.",
            self.record_type
        );
        while let Some(event) = self.rx.recv().await {
            self.record_slow_query(event).await;
        }
    }

    async fn record_slow_query(&self, event: SlowQueryEvent) {
        match self.record_type {
            SlowQueriesRecordType::Log => {
                // Record the slow query in a specific logs file.
                slow!(
                    cost = event.cost,
                    threshold = event.threshold,
                    query = event.query,
                    is_promql = event.is_promql,
                    promql_range = event.promql_range,
                    promql_step = event.promql_step,
                    promql_start = event.promql_start,
                    promql_end = event.promql_end,
                );
            }
            SlowQueriesRecordType::SystemTable => {
                // Record the slow query in a system table that is stored in greptimedb itself.
                if let Err(e) = self.insert_slow_query(&event).await {
                    error!(e; "Failed to insert slow query, query: {:?}", event);
                }
            }
        }
    }

    async fn insert_slow_query(&self, event: &SlowQueryEvent) -> Result<()> {
        debug!("Handle the slow query event: {:?}", event);

        let table = if let Some(table) = self
            .catalog_manager
            .table(
                event.query_ctx.current_catalog(),
                DEFAULT_PRIVATE_SCHEMA_NAME,
                SLOW_QUERY_TABLE_NAME,
                Some(&event.query_ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            table
        } else {
            // Create the system table if it doesn't exist.
            self.create_system_table(event.query_ctx.clone()).await?
        };

        let insert = RowInsertRequest {
            table_name: SLOW_QUERY_TABLE_NAME.to_string(),
            rows: Some(Rows {
                schema: self.build_insert_column_schema(),
                rows: vec![Row {
                    values: vec![
                        ValueData::U64Value(event.cost).into(),
                        ValueData::U64Value(event.threshold).into(),
                        ValueData::StringValue(event.query.to_string()).into(),
                        ValueData::BoolValue(event.is_promql).into(),
                        ValueData::TimestampNanosecondValue(
                            Timestamp::current_time(TimeUnit::Nanosecond).value(),
                        )
                        .into(),
                        ValueData::U64Value(event.promql_range.unwrap_or(0)).into(),
                        ValueData::U64Value(event.promql_step.unwrap_or(0)).into(),
                        ValueData::TimestampMillisecondValue(event.promql_start.unwrap_or(0))
                            .into(),
                        ValueData::TimestampMillisecondValue(event.promql_end.unwrap_or(0)).into(),
                    ],
                }],
            }),
        };

        let requests = RowInsertRequests {
            inserts: vec![insert],
        };

        let table_info = table.table_info();
        let query_ctx = QueryContextBuilder::default()
            .current_catalog(table_info.catalog_name.to_string())
            .current_schema(table_info.schema_name.to_string())
            .build()
            .into();

        self.inserter
            .handle_row_inserts(requests, query_ctx, &self.statement_executor, false)
            .await
            .context(TableOperationSnafu)?;

        Ok(())
    }

    async fn create_system_table(&self, query_ctx: QueryContextRef) -> Result<TableRef> {
        let mut create_table_expr = self.build_create_table_expr(query_ctx.current_catalog());
        if let Some(table) = self
            .catalog_manager
            .table(
                &create_table_expr.catalog_name,
                &create_table_expr.schema_name,
                &create_table_expr.table_name,
                Some(&query_ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            // The table is already created, so we don't need to create it again.
            return Ok(table);
        }

        // Create the `slow_queries` system table.
        let table = self
            .statement_executor
            .create_table_inner(&mut create_table_expr, None, query_ctx.clone())
            .await
            .context(TableOperationSnafu)?;

        info!(
            "Create the {} system table in {:?} successfully.",
            SLOW_QUERY_TABLE_NAME, DEFAULT_PRIVATE_SCHEMA_NAME
        );

        Ok(table)
    }

    fn build_create_table_expr(&self, catalog: &str) -> CreateTableExpr {
        let column_defs = vec![
            ColumnDef {
                name: SLOW_QUERY_TABLE_COST_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The cost of the slow query in milliseconds".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_THRESHOLD_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment:
                    "When the query cost exceeds this value, it will be recorded as a slow query"
                        .to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_QUERY_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The original query statement".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_IS_PROMQL_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Boolean as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "Whether the query is a PromQL query".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::TimestampNanosecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: "The timestamp of the slow query".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_PROMQL_RANGE_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The time range of the PromQL query in milliseconds".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_PROMQL_STEP_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The step of the PromQL query in milliseconds".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_PROMQL_START_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The start timestamp of the PromQL query in milliseconds".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_PROMQL_END_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The end timestamp of the PromQL query in milliseconds".to_string(),
                datatype_extension: None,
                options: None,
            },
        ];

        let table_options = HashMap::from([
            (APPEND_MODE_KEY.to_string(), "true".to_string()),
            (TTL_KEY.to_string(), self.ttl.to_string()),
        ]);

        CreateTableExpr {
            catalog_name: catalog.to_string(),
            schema_name: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(), // Always to store in the `greptime_private` schema.
            table_name: SLOW_QUERY_TABLE_NAME.to_string(),
            desc: "GreptimeDB system table for storing slow queries".to_string(),
            column_defs,
            time_index: SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME.to_string(),
            primary_keys: vec![],
            create_if_not_exists: true,
            table_options,
            table_id: None,
            engine: default_engine().to_string(),
        }
    }

    fn build_insert_column_schema(&self) -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_COST_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_THRESHOLD_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_QUERY_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_IS_PROMQL_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Boolean.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampNanosecond.into(),
                semantic_type: SemanticType::Timestamp.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_RANGE_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_STEP_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_START_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: SLOW_QUERY_TABLE_PROMQL_END_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
        ]
    }
}

/// SlowQueryTimer is used to log slow query when it's dropped.
/// In drop(), it will check if the query is slow and send the slow query event to the handler.
pub struct SlowQueryTimer {
    start: Instant,
    stmt: QueryStatement,
    query_ctx: QueryContextRef,
    threshold: Option<Duration>,
    sample_ratio: Option<f64>,
    tx: Sender<SlowQueryEvent>,
}

impl SlowQueryTimer {
    fn send_slow_query_event(&self, elapsed: Duration, threshold: Duration) {
        let mut slow_query_event = SlowQueryEvent {
            cost: elapsed.as_millis() as u64,
            threshold: threshold.as_millis() as u64,
            query: "".to_string(),
            query_ctx: self.query_ctx.clone(),

            // The following fields are only used for PromQL queries.
            is_promql: false,
            promql_range: None,
            promql_step: None,
            promql_start: None,
            promql_end: None,
        };

        match &self.stmt {
            QueryStatement::Promql(stmt) => {
                slow_query_event.is_promql = true;
                slow_query_event.query = stmt.expr.to_string();
                slow_query_event.promql_step = Some(stmt.interval.as_millis() as u64);

                let start = stmt
                    .start
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                let end = stmt
                    .end
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                slow_query_event.promql_range = Some((end - start) as u64);
                slow_query_event.promql_start = Some(start);
                slow_query_event.promql_end = Some(end);
            }
            QueryStatement::Sql(stmt) => {
                slow_query_event.query = stmt.to_string();
            }
        }

        // Send SlowQueryEvent to the handler.
        if let Err(e) = self.tx.try_send(slow_query_event) {
            error!(e; "Failed to send slow query event");
        }
    }
}

impl Drop for SlowQueryTimer {
    fn drop(&mut self) {
        if let Some(threshold) = self.threshold {
            // Calculate the elaspsed duration since the timer is created.
            let elapsed = self.start.elapsed();
            if elapsed > threshold {
                if let Some(ratio) = self.sample_ratio {
                    // Only capture a portion of slow queries based on sample_ratio.
                    // Generate a random number in [0, 1) and compare it with sample_ratio.
                    if ratio >= 1.0 || random::<f64>() <= ratio {
                        self.send_slow_query_event(elapsed, threshold);
                    }
                } else {
                    // Captures all slow queries if sample_ratio is not set.
                    self.send_slow_query_event(elapsed, threshold);
                }
            }
        }
    }
}
