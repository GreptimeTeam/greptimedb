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

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatch;
use common_recordbatch::error::Result as RecordBatchResult;
use common_telemetry::{debug, tracing};
use datafusion_common::ParamValues;
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::SchemaRef;
use futures::{Sink, SinkExt, Stream, StreamExt, future, stream};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ErrorHandler, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use query::query_engine::DescribeResult;
use session::Session;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::dialect::PostgreSqlDialect;
use sql::parser::{ParseOptions, ParserContext};

use crate::SqlPlan;
use crate::error::{DataFusionSnafu, Result};
use crate::postgres::types::*;
use crate::postgres::utils::convert_err;
use crate::postgres::{PostgresServerHandlerInner, fixtures};
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

#[async_trait]
impl SimpleQueryHandler for PostgresServerHandlerInner {
    #[tracing::instrument(skip_all, fields(protocol = "postgres"))]
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query_ctx = self.session.new_query_context();
        let db = query_ctx.get_db_string();
        let _timer = crate::metrics::METRIC_POSTGRES_QUERY_TIMER
            .with_label_values(&[crate::metrics::METRIC_POSTGRES_SIMPLE_QUERY, db.as_str()])
            .start_timer();

        if query.is_empty() {
            // early return if query is empty
            return Ok(vec![Response::EmptyQuery]);
        }

        let query = if let Ok(statements) = self.query_parser.compatibility_parser.parse(query) {
            statements
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(";")
        } else {
            query.to_string()
        };

        if let Some(resps) = fixtures::process(&query, query_ctx.clone()) {
            send_warning_opt(client, query_ctx).await?;
            Ok(resps)
        } else {
            let outputs = self.query_handler.do_query(&query, query_ctx.clone()).await;

            let mut results = Vec::with_capacity(outputs.len());

            for output in outputs {
                let resp =
                    output_to_query_response(query_ctx.clone(), output, &Format::UnifiedText)?;
                results.push(resp);
            }

            send_warning_opt(client, query_ctx).await?;
            Ok(results)
        }
    }
}

async fn send_warning_opt<C>(client: &mut C, query_context: QueryContextRef) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin + Send + Sync,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    if let Some(warning) = query_context.warning() {
        client
            .feed(PgWireBackendMessage::NoticeResponse(
                ErrorInfo::new(
                    PgErrorSeverity::Warning.to_string(),
                    PgErrorCode::Ec01000.code(),
                    warning.clone(),
                )
                .into(),
            ))
            .await?;
    }

    Ok(())
}

pub(crate) fn output_to_query_response(
    query_ctx: QueryContextRef,
    output: Result<Output>,
    field_format: &Format,
) -> PgWireResult<Response> {
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
        Err(e) => Err(convert_err(e)),
    }
}

fn recordbatches_to_query_response<S>(
    query_ctx: QueryContextRef,
    recordbatches_stream: S,
    schema: SchemaRef,
    field_format: &Format,
) -> PgWireResult<Response>
where
    S: Stream<Item = RecordBatchResult<RecordBatch>> + Send + Unpin + 'static,
{
    let pg_schema = Arc::new(schema_to_pg(schema.as_ref(), field_format).map_err(convert_err)?);
    let pg_schema_ref = pg_schema.clone();
    let data_row_stream = recordbatches_stream
        .map(|record_batch_result| match record_batch_result {
            Ok(rb) => stream::iter(
                // collect rows from a single recordbatch into vector to avoid
                // borrowing it
                rb.rows().map(Ok).collect::<Vec<_>>(),
            )
            .boxed(),
            Err(e) => stream::once(future::err(convert_err(e))).boxed(),
        })
        .flatten() // flatten into stream<result<row>>
        .map(move |row| {
            row.and_then(|row| {
                let mut encoder = DataRowEncoder::new(pg_schema_ref.clone());
                for (value, column) in row.into_iter().zip(schema.column_schemas()) {
                    encode_value(&query_ctx, value, &mut encoder, &column.data_type)?;
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
    compatibility_parser: PostgresCompatibilityParser,
}

impl DefaultQueryParser {
    pub fn new(query_handler: ServerSqlQueryHandlerRef, session: Arc<Session>) -> Self {
        DefaultQueryParser {
            query_handler,
            session,
            compatibility_parser: PostgresCompatibilityParser::new(),
        }
    }
}

#[async_trait]
impl QueryParser for DefaultQueryParser {
    type Statement = SqlPlan;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        _types: &[Type],
    ) -> PgWireResult<Self::Statement> {
        crate::metrics::METRIC_POSTGRES_PREPARED_COUNT.inc();
        let query_ctx = self.session.new_query_context();

        // do not parse if query is empty or matches rules
        if sql.is_empty() || fixtures::matches(sql) {
            return Ok(SqlPlan {
                query: sql.to_owned(),
                statement: None,
                plan: None,
                schema: None,
            });
        }

        let sql = if let Ok(mut statements) = self.compatibility_parser.parse(sql) {
            statements.remove(0).to_string()
        } else {
            // bypass the error: it can run into error because of different
            // versions of sqlparser
            sql.to_string()
        };

        let mut stmts = ParserContext::create_with_dialect(
            &sql,
            &PostgreSqlDialect {},
            ParseOptions::default(),
        )
        .map_err(convert_err)?;
        if stmts.len() != 1 {
            Err(PgWireError::UserError(Box::new(ErrorInfo::from(
                PgErrorCode::Ec42P14,
            ))))
        } else {
            let stmt = stmts.remove(0);

            let describe_result = self
                .query_handler
                .do_describe(stmt.clone(), query_ctx)
                .await
                .map_err(convert_err)?;

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
                query: sql.clone(),
                statement: Some(stmt),
                plan,
                schema,
            })
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for PostgresServerHandlerInner {
    type Statement = SqlPlan;
    type QueryParser = DefaultQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let query_ctx = self.session.new_query_context();
        let db = query_ctx.get_db_string();
        let _timer = crate::metrics::METRIC_POSTGRES_QUERY_TIMER
            .with_label_values(&[crate::metrics::METRIC_POSTGRES_EXTENDED_QUERY, db.as_str()])
            .start_timer();

        let sql_plan = &portal.statement.statement;

        if sql_plan.query.is_empty() {
            // early return if query is empty
            return Ok(Response::EmptyQuery);
        }

        if let Some(mut resps) = fixtures::process(&sql_plan.query, query_ctx.clone()) {
            send_warning_opt(client, query_ctx).await?;
            // if the statement matches our predefined rules, return it early
            return Ok(resps.remove(0));
        }

        let output = if let Some(plan) = &sql_plan.plan {
            let plan = plan
                .clone()
                .replace_params_with_values(&ParamValues::List(parameters_to_scalar_values(
                    plan, portal,
                )?))
                .context(DataFusionSnafu)
                .map_err(convert_err)?;
            self.query_handler
                .do_exec_plan(sql_plan.statement.clone(), plan, query_ctx.clone())
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

        send_warning_opt(client, query_ctx.clone()).await?;
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
                .get_parameter_types()
                .context(DataFusionSnafu)
                .map_err(convert_err)?
                .into_iter()
                .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
                .collect();

            let types = param_types_to_pg_types(&param_types).map_err(convert_err)?;

            (types, sql_plan, &Format::UnifiedBinary)
        } else {
            let param_types = stmt.parameter_types.clone();
            (param_types, sql_plan, &Format::UnifiedBinary)
        };

        if let Some(schema) = &sql_plan.schema {
            schema_to_pg(schema, format)
                .map(|fields| DescribeStatementResponse::new(param_types, fields))
                .map_err(convert_err)
        } else {
            if let Some(mut resp) =
                fixtures::process(&sql_plan.query, self.session.new_query_context())
                && let Response::Query(query_response) = resp.remove(0)
            {
                return Ok(DescribeStatementResponse::new(
                    param_types,
                    (*query_response.row_schema()).clone(),
                ));
            }

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
                .map_err(convert_err)
        } else {
            if let Some(mut resp) =
                fixtures::process(&sql_plan.query, self.session.new_query_context())
                && let Response::Query(query_response) = resp.remove(0)
            {
                return Ok(DescribePortalResponse::new(
                    (*query_response.row_schema()).clone(),
                ));
            }

            Ok(DescribePortalResponse::new(vec![]))
        }
    }
}

impl ErrorHandler for PostgresServerHandlerInner {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        debug!("Postgres interface error {}", error)
    }
}
