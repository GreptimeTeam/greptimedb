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

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_query::{Output, OutputData};
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::RecordBatch;
use common_telemetry::{debug, error, tracing};
use datatypes::schema::SchemaRef;
use futures::{future, stream, Stream, StreamExt};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use query::query_engine::DescribeResult;
use session::context::QueryContextRef;
use session::Session;
use sql::dialect::PostgreSqlDialect;
use sql::parser::{ParseOptions, ParserContext};

use super::types::*;
use super::PostgresServerHandler;
use crate::error::Result;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::SqlPlan;

#[async_trait]
impl SimpleQueryHandler for PostgresServerHandler {
    #[tracing::instrument(skip_all, fields(protocol = "postgres"))]
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query_ctx = self.session.new_query_context();
        let db = query_ctx.get_db_string();
        let _timer = crate::metrics::METRIC_POSTGRES_QUERY_TIMER
            .with_label_values(&[crate::metrics::METRIC_POSTGRES_SIMPLE_QUERY, db.as_str()])
            .start_timer();
        let outputs = self.query_handler.do_query(query, query_ctx.clone()).await;

        let mut results = Vec::with_capacity(outputs.len());

        for output in outputs {
            let resp = output_to_query_response(query_ctx.clone(), output, &Format::UnifiedText)?;
            results.push(resp);
        }

        Ok(results)
    }
}

fn output_to_query_response<'a>(
    query_ctx: QueryContextRef,
    output: Result<Output>,
    field_format: &Format,
) -> PgWireResult<Response<'a>> {
    match output {
        Ok(o) => match o.data {
            OutputData::AffectedRows(rows) => {
                Ok(Response::Execution(Tag::new("OK").with_rows(rows)))
            }
            OutputData::Stream(record_stream) => {
                let schema = record_stream.schema();
                recordbatches_to_query_response(query_ctx, record_stream, schema, field_format)
            }
            OutputData::RecordBatches(recordbatches) => {
                let schema = recordbatches.schema();
                recordbatches_to_query_response(
                    query_ctx,
                    recordbatches.as_stream(),
                    schema,
                    field_format,
                )
            }
        },
        Err(e) => {
            let status_code = e.status_code();

            if status_code.should_log_error() {
                let root_error = e.root_cause().unwrap_or(&e);
                error!(e; "Failed to handle postgres query, code: {}, db: {}, error: {}", status_code, query_ctx.get_db_string(), root_error.to_string());
            } else {
                debug!(
                    "Failed to handle postgres query, code: {}, db: {}, error: {:?}",
                    status_code,
                    query_ctx.get_db_string(),
                    e
                );
            };
            Ok(Response::Error(Box::new(
                PgErrorCode::from(status_code).to_err_info(e.output_msg()),
            )))
        }
    }
}

fn recordbatches_to_query_response<'a, S>(
    query_ctx: QueryContextRef,
    recordbatches_stream: S,
    schema: SchemaRef,
    field_format: &Format,
) -> PgWireResult<Response<'a>>
where
    S: Stream<Item = RecordBatchResult<RecordBatch>> + Send + Unpin + 'static,
{
    let pg_schema = Arc::new(
        schema_to_pg(schema.as_ref(), field_format)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?,
    );
    let pg_schema_ref = pg_schema.clone();
    let data_row_stream = recordbatches_stream
        .map(|record_batch_result| match record_batch_result {
            Ok(rb) => stream::iter(
                // collect rows from a single recordbatch into vector to avoid
                // borrowing it
                rb.rows().map(Ok).collect::<Vec<_>>(),
            )
            .boxed(),
            Err(e) => stream::once(future::err(PgWireError::ApiError(Box::new(e)))).boxed(),
        })
        .flatten() // flatten into stream<result<row>>
        .map(move |row| {
            row.and_then(|row| {
                let mut encoder = DataRowEncoder::new(pg_schema_ref.clone());
                for value in row.iter() {
                    encode_value(&query_ctx, value, &mut encoder)?;
                }
                encoder.finish()
            })
        });

    Ok(Response::Query(QueryResponse::new(
        pg_schema,
        data_row_stream,
    )))
}

pub struct DefaultQueryParser {
    query_handler: ServerSqlQueryHandlerRef,
    session: Arc<Session>,
}

impl DefaultQueryParser {
    pub fn new(query_handler: ServerSqlQueryHandlerRef, session: Arc<Session>) -> Self {
        DefaultQueryParser {
            query_handler,
            session,
        }
    }
}

#[async_trait]
impl QueryParser for DefaultQueryParser {
    type Statement = SqlPlan;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        crate::metrics::METRIC_POSTGRES_PREPARED_COUNT.inc();
        let query_ctx = self.session.new_query_context();
        let mut stmts =
            ParserContext::create_with_dialect(sql, &PostgreSqlDialect {}, ParseOptions::default())
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() != 1 {
            Err(PgWireError::UserError(Box::new(ErrorInfo::from(
                PgErrorCode::Ec42P14,
            ))))
        } else {
            let stmt = stmts.remove(0);
            let describe_result = self
                .query_handler
                .do_describe(stmt, query_ctx)
                .await
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let (plan, schema) = if let Some(DescribeResult {
                logical_plan,
                schema,
            }) = describe_result
            {
                (Some(logical_plan), Some(schema))
            } else {
                (None, None)
            };

            Ok(SqlPlan {
                query: sql.to_owned(),
                plan,
                schema,
            })
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresServerHandler {
    type Statement = SqlPlan;
    type QueryParser = DefaultQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query_ctx = self.session.new_query_context();
        let db = query_ctx.get_db_string();
        let _timer = crate::metrics::METRIC_POSTGRES_QUERY_TIMER
            .with_label_values(&[crate::metrics::METRIC_POSTGRES_EXTENDED_QUERY, db.as_str()])
            .start_timer();

        let sql_plan = &portal.statement.statement;

        let output = if let Some(plan) = &sql_plan.plan {
            let plan = plan
                .replace_params_with_values(parameters_to_scalar_values(plan, portal)?.as_ref())
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            self.query_handler
                .do_exec_plan(plan, query_ctx.clone())
                .await
        } else {
            // manually replace variables in prepared statement when no
            // logical_plan is generated. This happens when logical plan is not
            // supported for certain statements.
            let mut sql = sql_plan.query.clone();
            for i in 0..portal.parameter_len() {
                sql = sql.replace(&format!("${}", i + 1), &parameter_to_string(portal, i)?);
            }

            self.query_handler
                .do_query(&sql, query_ctx.clone())
                .await
                .remove(0)
        };

        output_to_query_response(query_ctx, output, &portal.result_column_format)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql_plan = &stmt.statement;
        let (param_types, sql_plan, format) = if let Some(plan) = &sql_plan.plan {
            let param_types = plan
                .get_param_types()
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            let types = param_types_to_pg_types(&param_types)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

            (types, sql_plan, &Format::UnifiedBinary)
        } else {
            let param_types = stmt.parameter_types.clone();
            (param_types, sql_plan, &Format::UnifiedBinary)
        };

        if let Some(schema) = &sql_plan.schema {
            schema_to_pg(schema, format)
                .map(|fields| DescribeStatementResponse::new(param_types, fields))
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        } else {
            Ok(DescribeStatementResponse::new(param_types, vec![]))
        }
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql_plan = &portal.statement.statement;
        let format = &portal.result_column_format;

        if let Some(schema) = &sql_plan.schema {
            schema_to_pg(schema, format)
                .map(DescribePortalResponse::new)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))
        } else {
            Ok(DescribePortalResponse::new(vec![]))
        }
    }
}
