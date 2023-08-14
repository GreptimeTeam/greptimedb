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

use std::any::Any;
use std::sync::Arc;

use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{CreateDatabaseExpr, DdlRequest, DeleteRequest, InsertRequests};
use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_grpc_expr::insert::to_table_insert_request;
use common_query::Output;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogList, CatalogProvider, MemoryCatalogList, MemoryCatalogProvider};
use datafusion::datasource::TableProvider;
use futures::future;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::plan::LogicalPlan;
use query::query_engine::SqlStatementExecutor;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::statements::statement::Statement;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::engine::TableReference;
use table::requests::CreateDatabaseRequest;
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    self, CatalogSnafu, DecodeLogicalPlanSnafu, DeleteExprToRequestSnafu, DeleteSnafu,
    ExecuteLogicalPlanSnafu, ExecuteSqlSnafu, InsertDataSnafu, InsertSnafu, JoinTaskSnafu,
    PlanStatementSnafu, Result, TableNotFoundSnafu, UnsupportedGrpcRequestSnafu,
};
use crate::instance::Instance;

impl Instance {
    pub(crate) async fn handle_create_database(
        &self,
        expr: CreateDatabaseExpr,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let req = CreateDatabaseRequest {
            db_name: expr.database_name,
            create_if_not_exists: expr.create_if_not_exists,
        };
        self.sql_handler.create_database(req, query_ctx).await
    }

    pub(crate) async fn execute_logical(
        &self,
        plan_bytes: Vec<u8>,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog_list = new_dummy_catalog_list(
            ctx.current_catalog(),
            ctx.current_schema(),
            self.catalog_manager.clone(),
        )
        .await?;

        let logical_plan = DFLogicalSubstraitConvertor
            .decode(
                plan_bytes.as_slice(),
                Arc::new(catalog_list) as Arc<_>,
                ctx.current_catalog(),
                ctx.current_schema(),
            )
            .await
            .context(DecodeLogicalPlanSnafu)?;

        self.query_engine
            .execute(LogicalPlan::DfPlan(logical_plan), ctx.clone())
            .await
            .context(ExecuteLogicalPlanSnafu)
    }

    async fn handle_query(&self, query: Query, ctx: QueryContextRef) -> Result<Output> {
        match query {
            Query::Sql(sql) => {
                let stmt = QueryLanguageParser::parse_sql(&sql).context(ExecuteSqlSnafu)?;
                match stmt {
                    // TODO(LFC): Remove SQL execution branch here.
                    // Keep this because substrait can't handle much of SQLs now.
                    QueryStatement::Sql(Statement::Query(_))
                    | QueryStatement::Sql(Statement::Explain(_))
                    | QueryStatement::Promql(_) => {
                        let plan = self
                            .query_engine
                            .planner()
                            .plan(stmt, ctx.clone())
                            .await
                            .context(PlanStatementSnafu)?;
                        self.query_engine
                            .execute(plan, ctx)
                            .await
                            .context(ExecuteLogicalPlanSnafu)
                    }
                    QueryStatement::Sql(stmt) => {
                        self.execute_sql(stmt, ctx).await.context(ExecuteSqlSnafu)
                    }
                }
            }
            Query::LogicalPlan(plan) => self.execute_logical(plan, ctx).await,
            Query::PromRangeQuery(promql) => {
                let prom_query = PromQuery {
                    query: promql.query,
                    start: promql.start,
                    end: promql.end,
                    step: promql.step,
                };
                self.execute_promql(&prom_query, ctx).await
            }
        }
    }

    pub async fn handle_inserts(
        &self,
        requests: InsertRequests,
        ctx: &QueryContextRef,
    ) -> Result<Output> {
        let results = future::try_join_all(requests.inserts.into_iter().map(|insert| {
            let catalog_manager = self.catalog_manager.clone();
            let catalog = ctx.current_catalog().to_owned();
            let schema = ctx.current_schema().to_owned();

            common_runtime::spawn_write(async move {
                let table_name = &insert.table_name.clone();
                let table = catalog_manager
                    .table(&catalog, &schema, table_name)
                    .await
                    .context(CatalogSnafu)?
                    .with_context(|| TableNotFoundSnafu {
                        table_name: common_catalog::format_full_table_name(
                            &catalog, &schema, table_name,
                        ),
                    })?;

                let request =
                    to_table_insert_request(&catalog, &schema, insert).context(InsertDataSnafu)?;

                table.insert(request).await.with_context(|_| InsertSnafu {
                    table_name: common_catalog::format_full_table_name(
                        &catalog, &schema, table_name,
                    ),
                })
            })
        }))
        .await
        .context(JoinTaskSnafu)?;
        let affected_rows = results.into_iter().sum::<Result<usize>>()?;
        Ok(Output::AffectedRows(affected_rows))
    }

    async fn handle_delete(&self, request: DeleteRequest, ctx: QueryContextRef) -> Result<Output> {
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();
        let table_name = &request.table_name.clone();
        let table_ref = TableReference::full(catalog, schema, table_name);

        let table = self
            .catalog_manager
            .table(catalog, schema, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;

        let request = common_grpc_expr::delete::to_table_delete_request(request)
            .context(DeleteExprToRequestSnafu)?;

        let affected_rows = table.delete(request).await.with_context(|_| DeleteSnafu {
            table_name: table_ref.to_string(),
        })?;
        Ok(Output::AffectedRows(affected_rows))
    }

    async fn handle_ddl(&self, request: DdlRequest, query_ctx: QueryContextRef) -> Result<Output> {
        let expr = request.expr.context(error::MissingRequiredFieldSnafu {
            name: "DdlRequest.expr",
        })?;
        match expr {
            DdlExpr::CreateTable(expr) => self.handle_create(expr, query_ctx).await,
            DdlExpr::Alter(expr) => self.handle_alter(expr, query_ctx).await,
            DdlExpr::CreateDatabase(expr) => self.handle_create_database(expr, query_ctx).await,
            DdlExpr::DropTable(expr) => self.handle_drop_table(expr, query_ctx).await,
            DdlExpr::FlushTable(expr) => self.handle_flush_table(expr, query_ctx).await,
            DdlExpr::CompactTable(expr) => self.handle_compact_table(expr, query_ctx).await,
            DdlExpr::TruncateTable(expr) => self.handle_truncate_table(expr, query_ctx).await,
        }
    }
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    type Error = error::Error;

    async fn do_query(&self, request: Request, ctx: QueryContextRef) -> Result<Output> {
        match request {
            Request::Inserts(requests) => self.handle_inserts(requests, &ctx).await,
            Request::Delete(request) => self.handle_delete(request, ctx).await,
            Request::Query(query_request) => {
                let query = query_request
                    .query
                    .context(error::MissingRequiredFieldSnafu {
                        name: "QueryRequest.query",
                    })?;
                self.handle_query(query, ctx).await
            }
            Request::Ddl(request) => self.handle_ddl(request, ctx).await,
            Request::RowInserts(_) | Request::RowDelete(_) => UnsupportedGrpcRequestSnafu {
                kind: "row insert/delete",
            }
            .fail(),
        }
    }
}

struct DummySchemaProvider {
    catalog: String,
    schema: String,
    table_names: Vec<String>,
    catalog_manager: CatalogManagerRef,
}

impl DummySchemaProvider {
    pub async fn try_new(
        catalog_name: String,
        schema_name: String,
        catalog_manager: CatalogManagerRef,
    ) -> Result<Self> {
        let table_names = catalog_manager
            .table_names(&catalog_name, &schema_name)
            .await
            .unwrap();
        Ok(Self {
            catalog: catalog_name,
            schema: schema_name,
            table_names,
            catalog_manager,
        })
    }
}

#[async_trait::async_trait]
impl SchemaProvider for DummySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.catalog_manager
            .table(&self.catalog, &self.schema, name)
            .await
            .context(CatalogSnafu)
            .ok()
            .flatten()
            .map(|t| Arc::new(DfTableProviderAdapter::new(t)) as Arc<_>)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|t| t == name)
    }
}

async fn new_dummy_catalog_list(
    catalog_name: &str,
    schema_name: &str,
    catalog_manager: CatalogManagerRef,
) -> Result<MemoryCatalogList> {
    let schema_provider = DummySchemaProvider::try_new(
        catalog_name.to_string(),
        schema_name.to_string(),
        catalog_manager,
    )
    .await?;
    let catalog_provider = MemoryCatalogProvider::new();
    assert!(catalog_provider
        .register_schema(schema_name, Arc::new(schema_provider) as Arc<_>)
        .is_ok());
    let catalog_list = MemoryCatalogList::new();
    let _ = catalog_list.register_catalog(
        catalog_name.to_string(),
        Arc::new(catalog_provider) as Arc<_>,
    );
    Ok(catalog_list)
}

#[cfg(test)]
mod test {
    use api::v1::add_column::location::LocationType;
    use api::v1::add_column::Location;
    use api::v1::column::Values;
    use api::v1::{
        alter_expr, AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, ColumnDef,
        CreateDatabaseExpr, CreateTableExpr, DropTableExpr, InsertRequest, InsertRequests,
        QueryRequest, RenameTable, SemanticType, TableId, TruncateTableExpr,
    };
    use common_catalog::consts::MITO_ENGINE;
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use common_recordbatch::RecordBatches;
    use datatypes::prelude::*;
    use query::parser::QueryLanguageParser;
    use session::context::QueryContext;

    use super::*;
    use crate::tests::test_util::{self, MockInstance};

    async fn exec_selection(instance: &Instance, sql: &str) -> Output {
        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let engine = instance.query_engine();
        let plan = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();
        engine.execute(plan, QueryContext::arc()).await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_same_table_twice() {
        // It should return TableAlreadyExists(4000)
        let instance = MockInstance::new("test_create_same_table_twice").await;
        let instance = instance.inner();

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                database_name: "my_database".to_string(),
                create_if_not_exists: true,
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                desc: "blabla".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        datatype: ColumnDataType::String as i32,
                        is_nullable: true,
                        default_constraint: vec![],
                    },
                    ColumnDef {
                        name: "ts".to_string(),
                        datatype: ColumnDataType::TimestampMillisecond as i32,
                        is_nullable: false,
                        default_constraint: vec![],
                    },
                ],
                time_index: "ts".to_string(),
                engine: MITO_ENGINE.to_string(),
                ..Default::default()
            })),
        });
        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let err = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap_err();
        assert!(matches!(err.status_code(), StatusCode::TableAlreadyExists));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_same_table_twice() {
        // It should return TableNotFound(4001)
        let instance = MockInstance::new("test_drop_same_table_twice").await;
        let instance = instance.inner();

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                database_name: "my_database".to_string(),
                create_if_not_exists: true,
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                desc: "blabla".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        datatype: ColumnDataType::String as i32,
                        is_nullable: true,
                        default_constraint: vec![],
                    },
                    ColumnDef {
                        name: "ts".to_string(),
                        datatype: ColumnDataType::TimestampMillisecond as i32,
                        is_nullable: false,
                        default_constraint: vec![],
                    },
                ],
                time_index: "ts".to_string(),
                engine: MITO_ENGINE.to_string(),
                ..Default::default()
            })),
        });
        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::DropTable(DropTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                table_id: Some(TableId { id: 1025 }),
            })),
        });

        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let err = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap_err();
        assert!(matches!(err.status_code(), StatusCode::TableNotFound));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_alter_table_twice() {
        let instance = MockInstance::new("test_alter_table_twice").await;
        let instance = instance.inner();

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                database_name: "my_database".to_string(),
                create_if_not_exists: true,
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                desc: "blabla".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        datatype: ColumnDataType::String as i32,
                        is_nullable: true,
                        default_constraint: vec![],
                    },
                    ColumnDef {
                        name: "ts".to_string(),
                        datatype: ColumnDataType::TimestampMillisecond as i32,
                        is_nullable: false,
                        default_constraint: vec![],
                    },
                ],
                time_index: "ts".to_string(),
                engine: MITO_ENGINE.to_string(),
                ..Default::default()
            })),
        });
        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(AlterExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                kind: Some(alter_expr::Kind::AddColumns(AddColumns {
                    add_columns: vec![AddColumn {
                        column_def: Some(ColumnDef {
                            name: "b".to_string(),
                            datatype: ColumnDataType::Int32 as i32,
                            is_nullable: true,
                            default_constraint: vec![],
                        }),
                        is_key: true,
                        location: None,
                    }],
                })),
                ..Default::default()
            })),
        });
        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        // Updates `table_version` to latest.
        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(AlterExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                kind: Some(alter_expr::Kind::AddColumns(AddColumns {
                    add_columns: vec![AddColumn {
                        column_def: Some(ColumnDef {
                            name: "b".to_string(),
                            datatype: ColumnDataType::Int32 as i32,
                            is_nullable: true,
                            default_constraint: vec![],
                        }),
                        is_key: true,
                        location: None,
                    }],
                })),
                table_version: 1,
                ..Default::default()
            })),
        });
        let err = instance
            .do_query(query, QueryContext::arc())
            .await
            .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::TableColumnExists);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rename_table_twice() {
        common_telemetry::init_default_ut_logging();
        let instance = MockInstance::new("test_alter_table_twice").await;
        let instance = instance.inner();

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                database_name: "my_database".to_string(),
                create_if_not_exists: true,
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                desc: "blabla".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        datatype: ColumnDataType::String as i32,
                        is_nullable: true,
                        default_constraint: vec![],
                    },
                    ColumnDef {
                        name: "ts".to_string(),
                        datatype: ColumnDataType::TimestampMillisecond as i32,
                        is_nullable: false,
                        default_constraint: vec![],
                    },
                ],
                time_index: "ts".to_string(),
                engine: MITO_ENGINE.to_string(),
                table_id: Some(TableId { id: 1025 }),
                ..Default::default()
            })),
        });
        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(AlterExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                kind: Some(alter_expr::Kind::RenameTable(RenameTable {
                    new_table_name: "new_my_table".to_string(),
                })),
                table_id: Some(TableId { id: 1025 }),
                ..Default::default()
            })),
        });
        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        // renames it again.
        let output = instance
            .do_query(query.clone(), QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_ddl() {
        let instance = MockInstance::new("test_handle_ddl").await;
        let instance = instance.inner();

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                database_name: "my_database".to_string(),
                create_if_not_exists: true,
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                desc: "blabla".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        datatype: ColumnDataType::String as i32,
                        is_nullable: true,
                        default_constraint: vec![],
                    },
                    ColumnDef {
                        name: "ts".to_string(),
                        datatype: ColumnDataType::TimestampMillisecond as i32,
                        is_nullable: false,
                        default_constraint: vec![],
                    },
                ],
                time_index: "ts".to_string(),
                engine: MITO_ENGINE.to_string(),
                ..Default::default()
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(AlterExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                kind: Some(alter_expr::Kind::AddColumns(AddColumns {
                    add_columns: vec![
                        AddColumn {
                            column_def: Some(ColumnDef {
                                name: "b".to_string(),
                                datatype: ColumnDataType::Int32 as i32,
                                is_nullable: true,
                                default_constraint: vec![],
                            }),
                            is_key: true,
                            location: None,
                        },
                        AddColumn {
                            column_def: Some(ColumnDef {
                                name: "c".to_string(),
                                datatype: ColumnDataType::Int32 as i32,
                                is_nullable: true,
                                default_constraint: vec![],
                            }),
                            is_key: true,
                            location: Some(Location {
                                location_type: LocationType::First.into(),
                                after_cloumn_name: "".to_string(),
                            }),
                        },
                        AddColumn {
                            column_def: Some(ColumnDef {
                                name: "d".to_string(),
                                datatype: ColumnDataType::Int32 as i32,
                                is_nullable: true,
                                default_constraint: vec![],
                            }),
                            is_key: true,
                            location: Some(Location {
                                location_type: LocationType::After.into(),
                                after_cloumn_name: "a".to_string(),
                            }),
                        },
                    ],
                })),
                ..Default::default()
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let Ok(QueryStatement::Sql(stmt)) = QueryLanguageParser::parse_sql(
            "INSERT INTO my_database.my_table (a, b, ts) VALUES ('s', 1, 1672384140000)",
        ) else {
            unreachable!()
        };
        let output = instance
            .execute_sql(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let output = exec_selection(instance, "SELECT * FROM my_database.my_table").await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---+---+---+---------------------+---+
| c | a | d | ts                  | b |
+---+---+---+---------------------+---+
|   | s |   | 2022-12-30T07:09:00 | 1 |
+---+---+---+---------------------+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::DropTable(DropTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "my_database".to_string(),
                table_name: "my_table".to_string(),
                table_id: None,
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));
        assert!(!instance
            .catalog_manager
            .table_exist("greptime", "my_database", "my_table")
            .await
            .unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_truncate_table() {
        let instance = MockInstance::new("test_handle_truncate_table").await;
        let instance = instance.inner();
        assert!(test_util::create_test_table(
            instance,
            ConcreteDataType::timestamp_millisecond_datatype()
        )
        .await
        .is_ok());

        // Insert data.
        let query = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                            ('host1', 66.6, 1024, 1672201025000),\
                            ('host2', 88.8, 333.3, 1672201026000),\
                            ('host3', 88.8, 333.3, 1672201026000)"
                    .to_string(),
            )),
        });

        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(3)));

        let query = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::TruncateTable(TruncateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: "demo".to_string(),
                table_id: None,
            })),
        });

        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));
        // TODO(DevilExileSu): Validate is an empty table.
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_insert() {
        let instance = MockInstance::new("test_handle_insert").await;
        let instance = instance.inner();
        assert!(test_util::create_test_table(
            instance,
            ConcreteDataType::timestamp_millisecond_datatype()
        )
        .await
        .is_ok());

        let insert = InsertRequest {
            table_name: "demo".to_string(),
            columns: vec![
                Column {
                    column_name: "host".to_string(),
                    values: Some(Values {
                        string_values: vec![
                            "host1".to_string(),
                            "host2".to_string(),
                            "host3".to_string(),
                        ],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Tag as i32,
                    datatype: ColumnDataType::String as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "cpu".to_string(),
                    values: Some(Values {
                        f64_values: vec![1.0, 3.0],
                        ..Default::default()
                    }),
                    null_mask: vec![2],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Float64 as i32,
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        ts_millisecond_values: vec![1672384140000, 1672384141000, 1672384142000],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 3,
            ..Default::default()
        };

        let query = Request::Inserts(InsertRequests {
            inserts: vec![insert],
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(3)));

        let output = exec_selection(instance, "SELECT ts, host, cpu FROM demo").await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+-------+-----+
| ts                  | host  | cpu |
+---------------------+-------+-----+
| 2022-12-30T07:09:00 | host1 | 1.0 |
| 2022-12-30T07:09:01 | host2 |     |
| 2022-12-30T07:09:02 | host3 | 3.0 |
+---------------------+-------+-----+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_delete() {
        let instance = MockInstance::new("test_handle_delete").await;
        let instance = instance.inner();
        assert!(test_util::create_test_table(
            instance,
            ConcreteDataType::timestamp_millisecond_datatype()
        )
        .await
        .is_ok());

        let query = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                            ('host1', 66.6, 1024, 1672201025000),\
                            ('host2', 88.8, 333.3, 1672201026000),\
                            ('host3', 88.8, 333.3, 1672201026000)"
                    .to_string(),
            )),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(3)));

        let request = DeleteRequest {
            table_name: "demo".to_string(),
            region_number: 0,
            key_columns: vec![
                Column {
                    column_name: "host".to_string(),
                    values: Some(Values {
                        string_values: vec!["host2".to_string()],
                        ..Default::default()
                    }),
                    datatype: ColumnDataType::String as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        ts_millisecond_values: vec![1672201026000],
                        ..Default::default()
                    }),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 1,
        };

        let request = Request::Delete(request);
        let output = instance
            .do_query(request, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let output = exec_selection(instance, "SELECT ts, host, cpu FROM demo").await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+-------+------+
| ts                  | host  | cpu  |
+---------------------+-------+------+
| 2022-12-28T04:17:05 | host1 | 66.6 |
| 2022-12-28T04:17:06 | host3 | 88.8 |
+---------------------+-------+------+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_query() {
        let instance = MockInstance::new("test_handle_query").await;
        let instance = instance.inner();
        assert!(test_util::create_test_table(
            instance,
            ConcreteDataType::timestamp_millisecond_datatype()
        )
        .await
        .is_ok());

        let query = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                            ('host1', 66.6, 1024, 1672201025000),\
                            ('host2', 88.8, 333.3, 1672201026000)"
                    .to_string(),
            )),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(2)));

        let query = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "SELECT ts, host, cpu, memory FROM demo".to_string(),
            )),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatch = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+-------+------+--------+
| ts                  | host  | cpu  | memory |
+---------------------+-------+------+--------+
| 2022-12-28T04:17:05 | host1 | 66.6 | 1024.0 |
| 2022-12-28T04:17:06 | host2 | 88.8 | 333.3  |
+---------------------+-------+------+--------+";
        let actual = recordbatch.pretty_print().unwrap();
        assert_eq!(actual, expected);
    }
}
