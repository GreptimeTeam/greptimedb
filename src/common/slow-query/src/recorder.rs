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

use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{
    ColumnDataType, ColumnDef, ColumnSchema, CreateTableExpr, Row, RowInsertRequest,
    RowInsertRequests, Rows, SemanticType,
};
use catalog::CatalogManagerRef;
use common_catalog::consts::{default_engine, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_telemetry::logging::{SlowQueriesRecordType, SlowQueryOptions};
use common_telemetry::{error, slow};
use common_time::timestamp::{TimeUnit, Timestamp};
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use query::parser::QueryStatement;
use rand::random;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::ResultExt;

use crate::error::{CatalogSnafu, CreateTableSnafu, InsertRowsSnafu, Result, TableNotFoundSnafu};

const SLOW_QUERY_TABLE_NAME: &str = "slow_queries";
const SLOW_QUERY_TABLE_COST_COLUMN_NAME: &str = "cost";
const SLOW_QUERY_TABLE_THRESHOLD_COLUMN_NAME: &str = "threshold";
const SLOW_QUERY_TABLE_QUERY_COLUMN_NAME: &str = "query";
const SLOW_QUERY_TABLE_IS_PROMQL_COLUMN_NAME: &str = "is_promql";
const SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME: &str = "timestamp";

/// SlowQueryRecorder is reponsible for recording slow queries.
#[derive(Clone)]
pub struct SlowQueryRecorder {
    slow_query_opts: SlowQueryOptions,
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    catalog_manager: CatalogManagerRef,
}

impl SlowQueryRecorder {
    pub fn new(
        slow_query_opts: SlowQueryOptions,
        inserter: InserterRef,
        statement_executor: StatementExecutorRef,
        catalog_manager: CatalogManagerRef,
    ) -> Self {
        Self {
            slow_query_opts,
            inserter,
            statement_executor,
            catalog_manager,
        }
    }

    pub fn start(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Option<SlowQueryTimer> {
        if self.slow_query_opts.enable {
            Some(SlowQueryTimer {
                start: std::time::Instant::now(),
                stmt,
                threshold: self.slow_query_opts.threshold,
                sample_ratio: self.slow_query_opts.sample_ratio,
                query_ctx,
                inserter: self.inserter.clone(),
                statement_executor: self.statement_executor.clone(),
                catalog_manager: self.catalog_manager.clone(),
                record_type: self.slow_query_opts.record_type,
            })
        } else {
            None
        }
    }
}

/// SlowQueryTimer is used to log slow query when it's dropped.
pub struct SlowQueryTimer {
    start: std::time::Instant,
    stmt: QueryStatement,
    threshold: Option<Duration>,
    sample_ratio: Option<f64>,
    query_ctx: QueryContextRef,
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    catalog_manager: CatalogManagerRef,
    record_type: SlowQueriesRecordType,
}

impl SlowQueryTimer {
    /// Stop the slow query timer and record the slow query.
    pub async fn stop(&self) {
        if let Err(e) = self.create_system_table().await {
            error!(e; "Failed to create system table for slow queries");
            return;
        }

        if let Some(threshold) = self.threshold {
            let elapsed = self.start.elapsed();
            if elapsed > threshold {
                if let Some(ratio) = self.sample_ratio {
                    // Generate a random number in [0, 1) and compare it with sample_ratio.
                    if ratio >= 1.0 || random::<f64>() <= ratio {
                        self.record_slow_query(elapsed, threshold).await;
                    }
                } else {
                    // If sample_ratio is not set, log all slow queries.
                    self.record_slow_query(elapsed, threshold).await;
                }
            }
        }
    }

    async fn record_slow_query(&self, elapsed: Duration, threshold: Duration) {
        let cost = elapsed.as_millis() as u64;
        let threshold = threshold.as_millis() as u64;
        let (query, is_promql) = match &self.stmt {
            QueryStatement::Sql(stmt) => (stmt.to_string(), false),
            QueryStatement::Promql(stmt) => (stmt.to_string(), true),
        };

        match self.record_type {
            SlowQueriesRecordType::Log => {
                // Record the slow query in a specific logs file.
                slow!(
                    cost = cost,
                    threshold = threshold,
                    query = query,
                    is_promql = is_promql
                );
            }
            SlowQueriesRecordType::SystemTable => {
                // Record the slow query in a system table.
                if let Err(e) = self
                    .insert_slow_query(cost, threshold, &query, is_promql)
                    .await
                {
                    error!(e; "Failed to insert slow query, query: {}", query);
                }
            }
        }
    }

    async fn insert_slow_query(
        &self,
        cost: u64,
        threshold: u64,
        query: &str,
        is_promql: bool,
    ) -> Result<()> {
        if let Some(table) = self
            .catalog_manager
            .table(
                &self.query_ctx.current_catalog(),
                DEFAULT_PRIVATE_SCHEMA_NAME,
                SLOW_QUERY_TABLE_NAME,
                Some(&self.query_ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            let insert = RowInsertRequest {
                table_name: SLOW_QUERY_TABLE_NAME.to_string(),
                rows: Some(Rows {
                    schema: self.build_insert_column_schema(),
                    rows: vec![Row {
                        values: vec![
                            ValueData::U64Value(cost).into(),
                            ValueData::U64Value(threshold).into(),
                            ValueData::StringValue(query.to_string()).into(),
                            ValueData::BoolValue(is_promql).into(),
                            ValueData::TimestampNanosecondValue(
                                Timestamp::current_time(TimeUnit::Nanosecond).value(),
                            )
                            .into(),
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
                .handle_row_inserts(requests, query_ctx, &self.statement_executor)
                .await
                .context(InsertRowsSnafu)?;

            Ok(())
        } else {
            TableNotFoundSnafu {
                catalog: self.query_ctx.current_catalog().to_string(),
                schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
                table: SLOW_QUERY_TABLE_NAME.to_string(),
            }
            .fail()
        }
    }

    async fn create_system_table(&self) -> Result<()> {
        let mut create_table_expr = self.build_create_table_expr(self.query_ctx.current_catalog());
        if let Some(_table) = self
            .catalog_manager
            .table(
                &create_table_expr.catalog_name,
                &create_table_expr.schema_name,
                &create_table_expr.table_name,
                Some(&self.query_ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            // The table is already created, so we don't need to create it again.
            return Ok(());
        }

        // Create the `slow_queries` system table.
        self.statement_executor
            .create_table_inner(&mut create_table_expr, None, self.query_ctx.clone())
            .await
            .context(CreateTableSnafu)?;

        Ok(())
    }

    fn build_create_table_expr(&self, catalog: &str) -> CreateTableExpr {
        let column_defs = vec![
            ColumnDef {
                name: SLOW_QUERY_TABLE_COST_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_THRESHOLD_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Uint64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_QUERY_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_IS_PROMQL_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::Boolean as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME.to_string(),
                data_type: ColumnDataType::TimestampNanosecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: "".to_string(),
                datatype_extension: None,
                options: None,
            },
        ];

        CreateTableExpr {
            catalog_name: catalog.to_string(),
            schema_name: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            table_name: SLOW_QUERY_TABLE_NAME.to_string(),
            desc: "GreptimeDB system table for storing slow queries".to_string(),
            column_defs,
            time_index: SLOW_QUERY_TABLE_TIMESTAMP_COLUMN_NAME.to_string(),
            primary_keys: vec![],
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: None, // Should and will be assigned by Meta.
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
        ]
    }
}
