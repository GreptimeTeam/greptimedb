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

use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use api::v1::auth_header::AuthScheme;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_database_client::GreptimeDatabaseClient;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{
    AlterTableExpr, AuthHeader, Basic, CreateTableExpr, DdlRequest, GreptimeRequest,
    InsertRequests, QueryRequest, RequestHeader, RowInsertRequests,
};
use arrow_flight::{FlightData, Ticket};
use async_stream::stream;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use common_catalog::build_db_string;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::BoxedError;
use common_grpc::flight::do_put::DoPutResponse;
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_query::Output;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper};
use common_telemetry::tracing::Span;
use common_telemetry::tracing_context::W3cTrace;
use common_telemetry::{error, warn};
use futures::future;
use futures_util::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use snafu::{OptionExt, ResultExt, ensure};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap, MetadataValue};
use tonic::transport::Channel;

use crate::error::{
    ConvertFlightDataSnafu, Error, FlightGetSnafu, IllegalFlightMessagesSnafu,
    InvalidTonicMetadataValueSnafu,
};
use crate::{Client, Result, error, from_grpc_response};

type FlightDataStream = Pin<Box<dyn Stream<Item = FlightData> + Send>>;

type DoPutResponseStream = Pin<Box<dyn Stream<Item = Result<DoPutResponse>>>>;

#[derive(Clone, Debug, Default)]
pub struct Database {
    // The "catalog" and "schema" to be used in processing the requests at the server side.
    // They are the "hint" or "context", just like how the "database" in "USE" statement is treated in MySQL.
    // They will be carried in the request header.
    catalog: String,
    schema: String,
    // The dbname follows naming rule as out mysql, postgres and http
    // protocol. The server treat dbname in priority of catalog/schema.
    dbname: String,
    // The time zone indicates the time zone where the user is located.
    // Some queries need to be aware of the user's time zone to perform some specific actions.
    timezone: String,

    client: Client,
    ctx: FlightContext,
}

pub struct DatabaseClient {
    pub addr: String,
    pub inner: GreptimeDatabaseClient<Channel>,
}

impl DatabaseClient {
    /// Returns a closure that logs the error when the request fails.
    pub fn inspect_err<'a>(&'a self, context: &'a str) -> impl Fn(&tonic::Status) + 'a {
        let addr = &self.addr;
        move |status| {
            error!("Failed to {context} request, peer: {addr}, status: {status:?}");
        }
    }
}

fn make_database_client(client: &Client) -> Result<DatabaseClient> {
    let (addr, channel) = client.find_channel()?;
    Ok(DatabaseClient {
        addr,
        inner: GreptimeDatabaseClient::new(channel)
            .max_decoding_message_size(client.max_grpc_recv_message_size())
            .max_encoding_message_size(client.max_grpc_send_message_size()),
    })
}

impl Database {
    /// Create database service client using catalog and schema
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>, client: Client) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            dbname: String::default(),
            timezone: String::default(),
            client,
            ctx: FlightContext::default(),
        }
    }

    /// Create database service client using dbname.
    ///
    /// This API is designed for external usage. `dbname` is:
    ///
    /// - the name of database when using GreptimeDB standalone or cluster
    /// - the name provided by GreptimeCloud or other multi-tenant GreptimeDB
    ///   environment
    pub fn new_with_dbname(dbname: impl Into<String>, client: Client) -> Self {
        Self {
            catalog: String::default(),
            schema: String::default(),
            timezone: String::default(),
            dbname: dbname.into(),
            client,
            ctx: FlightContext::default(),
        }
    }

    /// Set the catalog for the database client.
    pub fn set_catalog(&mut self, catalog: impl Into<String>) {
        self.catalog = catalog.into();
    }

    fn catalog_or_default(&self) -> &str {
        if self.catalog.is_empty() {
            DEFAULT_CATALOG_NAME
        } else {
            &self.catalog
        }
    }

    /// Set the schema for the database client.
    pub fn set_schema(&mut self, schema: impl Into<String>) {
        self.schema = schema.into();
    }

    fn schema_or_default(&self) -> &str {
        if self.schema.is_empty() {
            DEFAULT_SCHEMA_NAME
        } else {
            &self.schema
        }
    }

    /// Set the timezone for the database client.
    pub fn set_timezone(&mut self, timezone: impl Into<String>) {
        self.timezone = timezone.into();
    }

    /// Set the auth scheme for the database client.
    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.ctx.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    /// Make an InsertRequests request to the database.
    pub async fn insert(&self, requests: InsertRequests) -> Result<u32> {
        self.handle(Request::Inserts(requests)).await
    }

    /// Make an InsertRequests request to the database with hints.
    pub async fn insert_with_hints(
        &self,
        requests: InsertRequests,
        hints: &[(&str, &str)],
    ) -> Result<u32> {
        let mut client = make_database_client(&self.client)?;
        let request = self.to_rpc_request(Request::Inserts(requests));

        let mut request = tonic::Request::new(request);
        let metadata = request.metadata_mut();
        Self::put_hints(metadata, hints)?;

        let response = client
            .inner
            .handle(request)
            .await
            .inspect_err(client.inspect_err("insert_with_hints"))?
            .into_inner();
        from_grpc_response(response)
    }

    /// Make a RowInsertRequests request to the database.
    pub async fn row_inserts(&self, requests: RowInsertRequests) -> Result<u32> {
        self.handle(Request::RowInserts(requests)).await
    }

    /// Make a RowInsertRequests request to the database with hints.
    pub async fn row_inserts_with_hints(
        &self,
        requests: RowInsertRequests,
        hints: &[(&str, &str)],
    ) -> Result<u32> {
        let mut client = make_database_client(&self.client)?;
        let request = self.to_rpc_request(Request::RowInserts(requests));

        let mut request = tonic::Request::new(request);
        let metadata = request.metadata_mut();
        Self::put_hints(metadata, hints)?;

        let response = client
            .inner
            .handle(request)
            .await
            .inspect_err(client.inspect_err("row_inserts_with_hints"))?
            .into_inner();
        from_grpc_response(response)
    }

    fn put_hints(metadata: &mut MetadataMap, hints: &[(&str, &str)]) -> Result<()> {
        let Some(value) = hints
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .reduce(|a, b| format!("{},{}", a, b))
        else {
            return Ok(());
        };

        let key = AsciiMetadataKey::from_static("x-greptime-hints");
        let value = AsciiMetadataValue::from_str(&value).context(InvalidTonicMetadataValueSnafu)?;
        metadata.insert(key, value);
        Ok(())
    }

    /// Make a request to the database.
    pub async fn handle(&self, request: Request) -> Result<u32> {
        let mut client = make_database_client(&self.client)?;
        let request = self.to_rpc_request(request);
        let response = client
            .inner
            .handle(request)
            .await
            .inspect_err(client.inspect_err("handle"))?
            .into_inner();
        from_grpc_response(response)
    }

    /// Retry if connection fails, max_retries is the max number of retries, so the total wait time
    /// is `max_retries * GRPC_CONN_TIMEOUT`
    pub async fn handle_with_retry(
        &self,
        request: Request,
        max_retries: u32,
        hints: &[(&str, &str)],
    ) -> Result<u32> {
        let mut client = make_database_client(&self.client)?;
        let mut retries = 0;

        let request = self.to_rpc_request(request);

        loop {
            let mut tonic_request = tonic::Request::new(request.clone());
            let metadata = tonic_request.metadata_mut();
            Self::put_hints(metadata, hints)?;
            let raw_response = client
                .inner
                .handle(tonic_request)
                .await
                .inspect_err(client.inspect_err("handle"));
            match (raw_response, retries < max_retries) {
                (Ok(resp), _) => return from_grpc_response(resp.into_inner()),
                (Err(err), true) => {
                    // determine if the error is retryable
                    if is_grpc_retryable(&err) {
                        // retry
                        retries += 1;
                        warn!("Retrying {} times with error = {:?}", retries, err);
                        continue;
                    } else {
                        error!(
                            err; "Failed to send request to grpc handle, retries = {}, not retryable error, aborting",
                            retries
                        );
                        return Err(err.into());
                    }
                }
                (Err(err), false) => {
                    error!(
                        err; "Failed to send request to grpc handle after {} retries",
                        retries,
                    );
                    return Err(err.into());
                }
            }
        }
    }

    #[inline]
    fn to_rpc_request(&self, request: Request) -> GreptimeRequest {
        GreptimeRequest {
            header: Some(RequestHeader {
                catalog: self.catalog.clone(),
                schema: self.schema.clone(),
                authorization: self.ctx.auth_header.clone(),
                dbname: self.dbname.clone(),
                timezone: self.timezone.clone(),
                // TODO(Taylor-lagrange): add client grpc tracing
                tracing_context: W3cTrace::new(),
            }),
            request: Some(request),
        }
    }

    /// Executes a SQL query without any hints.
    pub async fn sql<S>(&self, sql: S) -> Result<Output>
    where
        S: AsRef<str>,
    {
        self.sql_with_hint(sql, &[]).await
    }

    /// Executes a SQL query with optional hints for query optimization.
    pub async fn sql_with_hint<S>(&self, sql: S, hints: &[(&str, &str)]) -> Result<Output>
    where
        S: AsRef<str>,
    {
        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(sql.as_ref().to_string())),
        });
        self.do_get(request, hints).await
    }

    /// Executes a logical plan directly without SQL parsing.
    pub async fn logical_plan(&self, logical_plan: Vec<u8>) -> Result<Output> {
        let request = Request::Query(QueryRequest {
            query: Some(Query::LogicalPlan(logical_plan)),
        });
        self.do_get(request, &[]).await
    }

    /// Creates a new table using the provided table expression.
    pub async fn create(&self, expr: CreateTableExpr) -> Result<Output> {
        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(expr)),
        });
        self.do_get(request, &[]).await
    }

    /// Alters an existing table using the provided alter expression.
    pub async fn alter(&self, expr: AlterTableExpr) -> Result<Output> {
        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::AlterTable(expr)),
        });
        self.do_get(request, &[]).await
    }

    async fn do_get(&self, request: Request, hints: &[(&str, &str)]) -> Result<Output> {
        let request = self.to_rpc_request(request);
        let request = Ticket {
            ticket: request.encode_to_vec().into(),
        };

        let mut request = tonic::Request::new(request);
        Self::put_hints(request.metadata_mut(), hints)?;

        let mut client = self.client.make_flight_client(false, false)?;

        let response = client.mut_inner().do_get(request).await.or_else(|e| {
            let tonic_code = e.code();
            let e: Error = e.into();
            error!(
                "Failed to do Flight get, addr: {}, code: {}, source: {:?}",
                client.addr(),
                tonic_code,
                e
            );
            Err(BoxedError::new(e)).with_context(|_| FlightGetSnafu {
                addr: client.addr().to_string(),
                tonic_code,
            })
        })?;

        let flight_data_stream = response.into_inner();
        let mut decoder = FlightDecoder::default();

        let mut flight_message_stream = flight_data_stream.map(move |flight_data| {
            flight_data
                .map_err(Error::from)
                .and_then(|data| decoder.try_decode(&data).context(ConvertFlightDataSnafu))?
                .context(IllegalFlightMessagesSnafu {
                    reason: "none message",
                })
        });

        let Some(first_flight_message) = flight_message_stream.next().await else {
            return IllegalFlightMessagesSnafu {
                reason: "Expect the response not to be empty",
            }
            .fail();
        };

        let first_flight_message = first_flight_message?;

        match first_flight_message {
            FlightMessage::AffectedRows(rows) => {
                ensure!(
                    flight_message_stream.next().await.is_none(),
                    IllegalFlightMessagesSnafu {
                        reason: "Expect 'AffectedRows' Flight messages to be the one and the only!"
                    }
                );
                Ok(Output::new_with_affected_rows(rows))
            }
            FlightMessage::RecordBatch(_) | FlightMessage::Metrics(_) => {
                IllegalFlightMessagesSnafu {
                    reason: "The first flight message cannot be a RecordBatch or Metrics message",
                }
                .fail()
            }
            FlightMessage::Schema(schema) => {
                let schema = Arc::new(
                    datatypes::schema::Schema::try_from(schema)
                        .context(error::ConvertSchemaSnafu)?,
                );
                let schema_cloned = schema.clone();
                let stream = Box::pin(stream!({
                    while let Some(flight_message) = flight_message_stream.next().await {
                        let flight_message = flight_message
                            .map_err(BoxedError::new)
                            .context(ExternalSnafu)?;
                        match flight_message {
                            FlightMessage::RecordBatch(arrow_batch) => {
                                yield Ok(RecordBatch::from_df_record_batch(
                                    schema_cloned.clone(),
                                    arrow_batch,
                                ))
                            }
                            FlightMessage::Metrics(_) => {}
                            FlightMessage::AffectedRows(_) | FlightMessage::Schema(_) => {
                                yield IllegalFlightMessagesSnafu {reason: format!("A Schema message must be succeeded exclusively by a set of RecordBatch messages, flight_message: {:?}", flight_message)}
                                        .fail()
                                        .map_err(BoxedError::new)
                                        .context(ExternalSnafu);
                                break;
                            }
                        }
                    }
                }));
                let record_batch_stream = RecordBatchStreamWrapper {
                    schema,
                    stream,
                    output_ordering: None,
                    metrics: Default::default(),
                    span: Span::current(),
                };
                Ok(Output::new_with_stream(Box::pin(record_batch_stream)))
            }
        }
    }

    /// Ingest a stream of [RecordBatch]es that belong to a table, using Arrow Flight's "`DoPut`"
    /// method. The return value is also a stream, produces [DoPutResponse]s.
    pub async fn do_put(&self, stream: FlightDataStream) -> Result<DoPutResponseStream> {
        let mut request = tonic::Request::new(stream);

        if let Some(AuthHeader {
            auth_scheme: Some(AuthScheme::Basic(Basic { username, password })),
        }) = &self.ctx.auth_header
        {
            let encoded = BASE64_STANDARD.encode(format!("{username}:{password}"));
            let value = MetadataValue::from_str(&format!("Basic {encoded}"))
                .context(InvalidTonicMetadataValueSnafu)?;
            request.metadata_mut().insert("x-greptime-auth", value);
        }

        let db_to_put = if !self.dbname.is_empty() {
            &self.dbname
        } else {
            &build_db_string(self.catalog_or_default(), self.schema_or_default())
        };
        request.metadata_mut().insert(
            "x-greptime-db-name",
            MetadataValue::from_str(db_to_put).context(InvalidTonicMetadataValueSnafu)?,
        );

        let mut client = self.client.make_flight_client(false, false)?;
        let response = client.mut_inner().do_put(request).await?;
        let response = response
            .into_inner()
            .map_err(Into::into)
            .and_then(|x| future::ready(DoPutResponse::try_from(x).context(ConvertFlightDataSnafu)))
            .boxed();
        Ok(response)
    }
}

/// by grpc standard, only `Unavailable` is retryable, see: https://github.com/grpc/grpc/blob/master/doc/statuscodes.md#status-codes-and-their-use-in-grpc
pub fn is_grpc_retryable(err: &tonic::Status) -> bool {
    matches!(err.code(), tonic::Code::Unavailable)
}

#[derive(Default, Debug, Clone)]
struct FlightContext {
    auth_header: Option<AuthHeader>,
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use api::v1::auth_header::AuthScheme;
    use api::v1::{AuthHeader, Basic};
    use common_error::status_code::StatusCode;
    use tonic::{Code, Status};

    use super::*;
    use crate::error::TonicSnafu;

    #[test]
    fn test_flight_ctx() {
        let mut ctx = FlightContext::default();
        assert!(ctx.auth_header.is_none());

        let basic = AuthScheme::Basic(Basic {
            username: "u".to_string(),
            password: "p".to_string(),
        });

        ctx.auth_header = Some(AuthHeader {
            auth_scheme: Some(basic),
        });

        assert_matches!(
            ctx.auth_header,
            Some(AuthHeader {
                auth_scheme: Some(AuthScheme::Basic(_)),
            })
        )
    }

    #[test]
    fn test_from_tonic_status() {
        let expected = TonicSnafu {
            code: StatusCode::Internal,
            msg: "blabla".to_string(),
            tonic_code: Code::Internal,
        }
        .build();

        let status = Status::new(Code::Internal, "blabla");
        let actual: Error = status.into();

        assert_eq!(expected.to_string(), actual.to_string());
    }
}
