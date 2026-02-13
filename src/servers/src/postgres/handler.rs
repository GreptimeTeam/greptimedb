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
use datafusion::sql::sqlparser::ast::{CopyOption, CopyTarget, Statement as SqlParserStatement};
use datafusion_common::ParamValues;
use datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::SchemaRef;
use futures::{Sink, SinkExt, Stream, StreamExt, future, stream};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    CopyCsvOptions, CopyEncoder, CopyResponse, CopyTextOptions, DataRowEncoder,
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse, Response, Tag,
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
use sql::statements::statement::Statement;

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

        let parsed_query = self.query_parser.compatibility_parser.parse(query);

        let query = if let Ok(statements) = &parsed_query {
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

            let statements = parsed_query.ok();
            for (idx, output) in outputs.into_iter().enumerate() {
                let copy_format = statements
                    .as_ref()
                    .and_then(|stmts| stmts.get(idx))
                    .and_then(|stmt| check_copy_to_stdout(stmt));
                let resp = if let Some(format) = &copy_format {
                    output_to_copy_response(query_ctx.clone(), output, format)?
                } else {
                    output_to_query_response(query_ctx.clone(), output, &Format::UnifiedText)?
                };
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
    let format_options = format_options_from_query_ctx(&query_ctx);
    let pg_schema = Arc::new(
        schema_to_pg(schema.as_ref(), field_format, Some(format_options)).map_err(convert_err)?,
    );
    let pg_schema_ref = pg_schema.clone();
    let data_row_stream = recordbatches_stream
        .map(move |result| match result {
            Ok(record_batch) => {
                let data_row_encoder = DataRowEncoder::new(pg_schema_ref.clone());
                stream::iter(RecordBatchRowIterator::new(
                    query_ctx.clone(),
                    pg_schema_ref.clone(),
                    record_batch,
                    data_row_encoder,
                ))
                .boxed()
            }
            Err(e) => stream::once(future::err(convert_err(e))).boxed(),
        })
        .flatten();

    Ok(Response::Query(QueryResponse::new(
        pg_schema,
        data_row_stream,
    )))
}

pub(crate) fn output_to_copy_response(
    query_ctx: QueryContextRef,
    output: Result<Output>,
    format: &str,
) -> PgWireResult<Response> {
    match output {
        Ok(o) => match o.data {
            OutputData::AffectedRows(_) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                "42601".to_string(),
                "COPY cannot be used with non-query statements".to_string(),
            )))),
            OutputData::Stream(record_stream) => {
                let schema = record_stream.schema();
                recordbatches_to_copy_response(query_ctx, record_stream, schema, format)
            }
            OutputData::RecordBatches(recordbatches) => {
                let schema = recordbatches.schema();
                recordbatches_to_copy_response(query_ctx, recordbatches.as_stream(), schema, format)
            }
        },
        Err(e) => Err(convert_err(e)),
    }
}

fn recordbatches_to_copy_response<S>(
    query_ctx: QueryContextRef,
    recordbatches_stream: S,
    schema: SchemaRef,
    format: &str,
) -> PgWireResult<Response>
where
    S: Stream<Item = RecordBatchResult<RecordBatch>> + Send + Unpin + 'static,
{
    fn create_encode(format: &str, pg_schema: Arc<Vec<FieldInfo>>) -> CopyEncoder {
        match format {
            "csv" => CopyEncoder::new_csv(pg_schema, CopyCsvOptions::default()),
            "binary" => CopyEncoder::new_binary(pg_schema),
            _ => CopyEncoder::new_text(pg_schema, CopyTextOptions::default()),
        }
    }
    let format_options = format_options_from_query_ctx(&query_ctx);
    let pg_fields = schema_to_pg(schema.as_ref(), &Format::UnifiedText, Some(format_options))
        .map_err(convert_err)?;

    let copy_format = match format.to_lowercase().as_str() {
        "binary" => 1,
        _ => 0,
    };

    let pg_schema = Arc::new(pg_fields);
    let num_columns = pg_schema.len();

    let format_lower = format.to_lowercase();
    let pg_schema_clone = pg_schema.clone();
    let format_lower_for_stream = format_lower.clone();

    let copy_stream = recordbatches_stream
        .map(move |result| match result {
            Ok(record_batch) => {
                let copy_encoder =
                    create_encode(format_lower_for_stream.as_str(), pg_schema_clone.clone());
                stream::iter(RecordBatchRowIterator::new(
                    query_ctx.clone(),
                    pg_schema_clone.clone(),
                    record_batch,
                    copy_encoder,
                ))
                .boxed()
            }
            Err(e) => stream::once(future::err(convert_err(e))).boxed(),
        })
        .flatten();

    let mut final_encoder = create_encode(format_lower.as_str(), pg_schema);
    let final_stream =
        copy_stream.chain(stream::once(async move { Ok(final_encoder.finish_copy()) }));

    Ok(Response::CopyOut(CopyResponse::new(
        copy_format,
        num_columns,
        final_stream,
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
        _types: &[Option<Type>],
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
                copy_to_stdout_format: None,
            });
        }

        let parsed_statements = self.compatibility_parser.parse(sql);
        let (sql, copy_to_stdout_format) = if let Ok(mut statements) = parsed_statements {
            let first_stmt = statements.remove(0);
            let format = check_copy_to_stdout(&first_stmt);
            (first_stmt.to_string(), format)
        } else {
            // bypass the error: it can run into error because of different
            // versions of sqlparser
            (sql.to_string(), None)
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
                copy_to_stdout_format,
            })
        }
    }

    fn get_parameter_types(&self, _stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        // we have our own implementation of describes in ExtendedQueryHandler
        // so we don't use these methods
        Err(PgWireError::ApiError(
            "get_parameter_types is not expected to be called".into(),
        ))
    }

    fn get_result_schema(
        &self,
        _stmt: &Self::Statement,
        _column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        // we have our own implementation of describes in ExtendedQueryHandler
        // so we don't use these methods
        Err(PgWireError::ApiError(
            "get_result_schema is not expected to be called".into(),
        ))
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
            let values = parameters_to_scalar_values(plan, portal)?;
            let plan = plan
                .clone()
                .replace_params_with_values(&ParamValues::List(
                    values.into_iter().map(Into::into).collect(),
                ))
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

        if let Some(format) = &sql_plan.copy_to_stdout_format {
            output_to_copy_response(query_ctx, output, format)
        } else {
            output_to_query_response(query_ctx, output, &portal.result_column_format)
        }
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
        // client provided parameter types, can be empty if client doesn't try to parse statement
        let provided_param_types = &stmt.parameter_types;
        let server_inferenced_types = if let Some(plan) = &sql_plan.plan {
            let param_types = plan
                .get_parameter_types()
                .context(DataFusionSnafu)
                .map_err(convert_err)?
                .into_iter()
                .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
                .collect();

            let types = param_types_to_pg_types(&param_types).map_err(convert_err)?;

            Some(types)
        } else {
            None
        };

        let param_count = if provided_param_types.is_empty() {
            server_inferenced_types
                .as_ref()
                .map(|types| types.len())
                .unwrap_or(0)
        } else {
            provided_param_types.len()
        };

        let param_types = (0..param_count)
            .map(|i| {
                let client_type = provided_param_types.get(i);
                // use server type when client provided type is None (oid: 0 or other invalid values)
                match client_type {
                    Some(Some(client_type)) => client_type.clone(),
                    _ => server_inferenced_types
                        .as_ref()
                        .and_then(|types| types.get(i).cloned())
                        .unwrap_or(Type::UNKNOWN),
                }
            })
            .collect::<Vec<_>>();

        if let Some(schema) = &sql_plan.schema {
            schema_to_pg(schema, &Format::UnifiedBinary, None)
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

        match sql_plan.statement.as_ref() {
            Some(Statement::Query(_)) => {
                // if the query has a schema, it is managed by datafusion, use the schema
                if let Some(schema) = &sql_plan.schema {
                    schema_to_pg(schema, format, None)
                        .map(DescribePortalResponse::new)
                        .map_err(convert_err)
                } else {
                    // fallback to NoData
                    Ok(DescribePortalResponse::new(vec![]))
                }
            }
            // We can cover only part of show statements
            // these show create statements will return 2 columns
            Some(Statement::ShowCreateDatabase(_))
            | Some(Statement::ShowCreateTable(_))
            | Some(Statement::ShowCreateFlow(_))
            | Some(Statement::ShowCreateView(_)) => Ok(DescribePortalResponse::new(vec![
                FieldInfo::new(
                    "name".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    format.format_for(0),
                ),
                FieldInfo::new(
                    "create_statement".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    format.format_for(1),
                ),
            ])),
            // single column show statements
            Some(Statement::ShowTables(_))
            | Some(Statement::ShowFlows(_))
            | Some(Statement::ShowViews(_)) => {
                Ok(DescribePortalResponse::new(vec![FieldInfo::new(
                    "name".to_string(),
                    None,
                    None,
                    Type::TEXT,
                    format.format_for(0),
                )]))
            }
            // we will not support other show statements for extended query protocol at least for now.
            // because the return columns is not predictable at this stage
            _ => {
                // test if query caught by fixture
                if let Some(mut resp) =
                    fixtures::process(&sql_plan.query, self.session.new_query_context())
                    && let Response::Query(query_response) = resp.remove(0)
                {
                    Ok(DescribePortalResponse::new(
                        (*query_response.row_schema()).clone(),
                    ))
                } else {
                    // fallback to NoData
                    Ok(DescribePortalResponse::new(vec![]))
                }
            }
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

fn check_copy_to_stdout(statement: &SqlParserStatement) -> Option<String> {
    if let SqlParserStatement::Copy {
        target, options, ..
    } = statement
    {
        if matches!(target, CopyTarget::Stdout) {
            for opt in options {
                if let CopyOption::Format(format_ident) = opt {
                    return Some(format_ident.value.to_lowercase());
                }
            }
            return Some("txt".to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

    use super::*;

    fn parse_copy_statement(sql: &str) -> SqlParserStatement {
        let parser = PostgresCompatibilityParser::new();
        let statements = parser.parse(sql).unwrap();
        statements.into_iter().next().unwrap()
    }

    #[test]
    fn test_check_copy_out_with_csv_format() {
        let statement = parse_copy_statement("COPY (SELECT 1) TO STDOUT WITH (FORMAT CSV)");
        assert_eq!(check_copy_to_stdout(&statement), Some("csv".to_string()));
    }

    #[test]
    fn test_check_copy_out_with_txt_format() {
        let statement = parse_copy_statement("COPY (SELECT 1) TO STDOUT WITH (FORMAT TXT)");
        assert_eq!(check_copy_to_stdout(&statement), Some("txt".to_string()));
    }

    #[test]
    fn test_check_copy_out_with_binary_format() {
        let statement = parse_copy_statement("COPY (SELECT 1) TO STDOUT WITH (FORMAT BINARY)");
        assert_eq!(check_copy_to_stdout(&statement), Some("binary".to_string()));
    }

    #[test]
    fn test_check_copy_out_without_format() {
        let statement = parse_copy_statement("COPY (SELECT 1) TO STDOUT");
        assert_eq!(check_copy_to_stdout(&statement), Some("txt".to_string()));
    }

    #[test]
    fn test_check_copy_out_to_file() {
        let statement =
            parse_copy_statement("COPY (SELECT 1) TO '/path/to/file.csv' WITH (FORMAT CSV)");
        assert_eq!(check_copy_to_stdout(&statement), None);
    }

    #[test]
    fn test_check_copy_out_case_insensitive() {
        let statement = parse_copy_statement("COPY (SELECT 1) TO STDOUT WITH (FORMAT csv)");
        assert_eq!(check_copy_to_stdout(&statement), Some("csv".to_string()));

        let statement = parse_copy_statement("COPY (SELECT 1) TO STDOUT WITH (FORMAT binary)");
        assert_eq!(check_copy_to_stdout(&statement), Some("binary".to_string()));
    }

    #[test]
    fn test_check_copy_out_with_multiple_options() {
        let statement = parse_copy_statement(
            "COPY (SELECT 1) TO STDOUT WITH (FORMAT csv, DELIMITER ',', HEADER)",
        );
        assert_eq!(check_copy_to_stdout(&statement), Some("csv".to_string()));

        let statement = parse_copy_statement(
            "COPY (SELECT 1) TO STDOUT WITH (DELIMITER ',', HEADER, FORMAT binary)",
        );
        assert_eq!(check_copy_to_stdout(&statement), Some("binary".to_string()));
    }
}
