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

use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_request::Request as GrpcRequest;
use api::v1::query_request::Query;
use api::v1::{CreateDatabaseExpr, DdlRequest, InsertRequest};
use async_trait::async_trait;
use common_query::Output;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::plan::LogicalPlan;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use snafu::prelude::*;
use sql::statements::statement::Statement;
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::requests::CreateDatabaseRequest;

use crate::error::{
    self, DecodeLogicalPlanSnafu, ExecuteLogicalPlanSnafu, ExecuteSqlSnafu, PlanStatementSnafu,
    Result,
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

    pub(crate) async fn execute_logical(&self, plan_bytes: Vec<u8>) -> Result<Output> {
        let logical_plan = DFLogicalSubstraitConvertor
            .decode(plan_bytes.as_slice(), self.catalog_manager.clone())
            .await
            .context(DecodeLogicalPlanSnafu)?;

        self.query_engine
            .execute(LogicalPlan::DfPlan(logical_plan), QueryContext::arc())
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
                    QueryStatement::Sql(Statement::Query(_)) => {
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
                    _ => self.execute_stmt(stmt, ctx).await,
                }
            }
            Query::LogicalPlan(plan) => self.execute_logical(plan).await,
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

    pub async fn handle_insert(
        &self,
        request: InsertRequest,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog = &ctx.current_catalog();
        let schema = &ctx.current_schema();
        let table_name = &request.table_name.clone();
        let table = self
            .catalog_manager
            .table(catalog, schema, table_name)
            .await
            .context(error::CatalogSnafu)?
            .context(error::TableNotFoundSnafu { table_name })?;

        let request = common_grpc_expr::insert::to_table_insert_request(catalog, schema, request)
            .context(error::InsertDataSnafu)?;

        let affected_rows = table
            .insert(request)
            .await
            .context(error::InsertSnafu { table_name })?;
        Ok(Output::AffectedRows(affected_rows))
    }

    async fn handle_ddl(&self, request: DdlRequest, query_ctx: QueryContextRef) -> Result<Output> {
        let expr = request.expr.context(error::MissingRequiredFieldSnafu {
            name: "DdlRequest.expr",
        })?;
        match expr {
            DdlExpr::CreateTable(expr) => self.handle_create(expr).await,
            DdlExpr::Alter(expr) => self.handle_alter(expr).await,
            DdlExpr::CreateDatabase(expr) => self.handle_create_database(expr, query_ctx).await,
            DdlExpr::DropTable(expr) => self.handle_drop_table(expr).await,
            DdlExpr::FlushTable(expr) => self.handle_flush_table(expr).await,
        }
    }
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    type Error = error::Error;

    async fn do_query(&self, request: GrpcRequest, ctx: QueryContextRef) -> Result<Output> {
        match request {
            GrpcRequest::Insert(request) => self.handle_insert(request, ctx).await,
            GrpcRequest::Query(query_request) => {
                let query = query_request
                    .query
                    .context(error::MissingRequiredFieldSnafu {
                        name: "QueryRequest.query",
                    })?;
                self.handle_query(query, ctx).await
            }
            GrpcRequest::Ddl(request) => self.handle_ddl(request, ctx).await,
        }
    }
}

#[cfg(test)]
mod test {
    use api::v1::column::{SemanticType, Values};
    use api::v1::{
        alter_expr, AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, ColumnDef,
        CreateDatabaseExpr, CreateTableExpr, QueryRequest,
    };
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
    async fn test_handle_ddl() {
        let instance = MockInstance::new("test_handle_ddl").await;
        let instance = instance.inner();

        let query = GrpcRequest::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                database_name: "my_database".to_string(),
                create_if_not_exists: true,
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let query = GrpcRequest::Ddl(DdlRequest {
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
                ..Default::default()
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let query = GrpcRequest::Ddl(DdlRequest {
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
                    }],
                })),
            })),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        let stmt = QueryLanguageParser::parse_sql(
            "INSERT INTO my_database.my_table (a, b, ts) VALUES ('s', 1, 1672384140000)",
        )
        .unwrap();
        let output = instance
            .execute_stmt(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let output = exec_selection(instance, "SELECT ts, a, b FROM my_database.my_table").await;
        let Output::Stream(stream) = output else { unreachable!() };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2022-12-30T07:09:00 | s | 1 |
+---------------------+---+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_insert() {
        let instance = MockInstance::new("test_handle_insert").await;
        let instance = instance.inner();
        test_util::create_test_table(instance, ConcreteDataType::timestamp_millisecond_datatype())
            .await
            .unwrap();

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

        let query = GrpcRequest::Insert(insert);
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(3)));

        let output = exec_selection(instance, "SELECT ts, host, cpu FROM demo").await;
        let Output::Stream(stream) = output else { unreachable!() };
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
    async fn test_handle_query() {
        let instance = MockInstance::new("test_handle_query").await;
        let instance = instance.inner();
        test_util::create_test_table(instance, ConcreteDataType::timestamp_millisecond_datatype())
            .await
            .unwrap();

        let query = GrpcRequest::Query(QueryRequest {
            query: Some(Query::Sql(
                "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                            ('host1', 66.6, 1024, 1672201025000),\
                            ('host2', 88.8, 333.3, 1672201026000)"
                    .to_string(),
            )),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        assert!(matches!(output, Output::AffectedRows(2)));

        let query = GrpcRequest::Query(QueryRequest {
            query: Some(Query::Sql(
                "SELECT ts, host, cpu, memory FROM demo".to_string(),
            )),
        });
        let output = instance.do_query(query, QueryContext::arc()).await.unwrap();
        let Output::Stream(stream) = output else { unreachable!() };
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
