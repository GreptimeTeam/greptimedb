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

use api::v1::auth_header::AuthScheme;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_database_client::GreptimeDatabaseClient;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{
    AlterExpr, AuthHeader, CreateTableExpr, DdlRequest, GreptimeRequest, InsertRequests,
    QueryRequest, RequestHeader,
};
use arrow_flight::Ticket;
use async_stream::stream;
use common_error::ext::{BoxedError, ErrorExt};
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_query::Output;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::RecordBatchStreamWrapper;
use common_telemetry::error;
use common_telemetry::tracing_context::W3cTrace;
use futures_util::StreamExt;
use prost::Message;
use snafu::{ensure, ResultExt};
use tonic::metadata::AsciiMetadataKey;
use tonic::transport::Channel;

use crate::error::{
    ConvertFlightDataSnafu, Error, IllegalFlightMessagesSnafu, InvalidAsciiSnafu, ServerSnafu,
};
use crate::{from_grpc_response, Client, Result};

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
    pub inner: GreptimeDatabaseClient<Channel>,
}

fn make_database_client(client: &Client) -> Result<DatabaseClient> {
    let (_, channel) = client.find_channel()?;
    Ok(DatabaseClient {
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
    /// environment
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

    pub fn set_catalog(&mut self, catalog: impl Into<String>) {
        self.catalog = catalog.into();
    }

    pub fn catalog(&self) -> &String {
        &self.catalog
    }

    pub fn set_schema(&mut self, schema: impl Into<String>) {
        self.schema = schema.into();
    }

    pub fn schema(&self) -> &String {
        &self.schema
    }

    pub fn set_timezone(&mut self, timezone: impl Into<String>) {
        self.timezone = timezone.into();
    }

    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.ctx.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    pub async fn insert(&self, requests: InsertRequests) -> Result<u32> {
        self.handle(Request::Inserts(requests)).await
    }

    pub async fn insert_with_hints(
        &self,
        requests: InsertRequests,
        hints: &[(&str, &str)],
    ) -> Result<u32> {
        let mut client = make_database_client(&self.client)?.inner;
        let request = self.to_rpc_request(Request::Inserts(requests));

        let mut request = tonic::Request::new(request);
        let metadata = request.metadata_mut();
        for (key, value) in hints {
            let key = AsciiMetadataKey::from_bytes(format!("x-greptime-hint-{}", key).as_bytes())
                .map_err(|_| {
                InvalidAsciiSnafu {
                    value: key.to_string(),
                }
                .build()
            })?;
            let value = value.parse().map_err(|_| {
                InvalidAsciiSnafu {
                    value: value.to_string(),
                }
                .build()
            })?;
            metadata.insert(key, value);
        }
        let response = client.handle(request).await?.into_inner();
        from_grpc_response(response)
    }

    async fn handle(&self, request: Request) -> Result<u32> {
        let mut client = make_database_client(&self.client)?.inner;
        let request = self.to_rpc_request(request);
        let response = client.handle(request).await?.into_inner();
        from_grpc_response(response)
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

    pub async fn sql<S>(&self, sql: S) -> Result<Output>
    where
        S: AsRef<str>,
    {
        self.do_get(Request::Query(QueryRequest {
            query: Some(Query::Sql(sql.as_ref().to_string())),
        }))
        .await
    }

    pub async fn logical_plan(&self, logical_plan: Vec<u8>) -> Result<Output> {
        self.do_get(Request::Query(QueryRequest {
            query: Some(Query::LogicalPlan(logical_plan)),
        }))
        .await
    }

    pub async fn create(&self, expr: CreateTableExpr) -> Result<Output> {
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(expr)),
        }))
        .await
    }

    pub async fn alter(&self, expr: AlterExpr) -> Result<Output> {
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(expr)),
        }))
        .await
    }

    async fn do_get(&self, request: Request) -> Result<Output> {
        let request = self.to_rpc_request(request);
        let request = Ticket {
            ticket: request.encode_to_vec().into(),
        };

        let mut client = self.client.make_flight_client()?;

        let response = client.mut_inner().do_get(request).await.map_err(|e| {
            let tonic_code = e.code();
            let e: Error = e.into();
            let code = e.status_code();
            let msg = e.to_string();
            let error = Error::FlightGet {
                tonic_code,
                addr: client.addr().to_string(),
                source: BoxedError::new(ServerSnafu { code, msg }.build()),
            };
            error!(
                "Failed to do Flight get, addr: {}, code: {}, source: {:?}",
                client.addr(),
                tonic_code,
                error
            );
            error
        })?;

        let flight_data_stream = response.into_inner();
        let mut decoder = FlightDecoder::default();

        let mut flight_message_stream = flight_data_stream.map(move |flight_data| {
            flight_data
                .map_err(Error::from)
                .and_then(|data| decoder.try_decode(data).context(ConvertFlightDataSnafu))
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
            FlightMessage::Recordbatch(_) | FlightMessage::Metrics(_) => {
                IllegalFlightMessagesSnafu {
                    reason: "The first flight message cannot be a RecordBatch or Metrics message",
                }
                .fail()
            }
            FlightMessage::Schema(schema) => {
                let stream = Box::pin(stream!({
                    while let Some(flight_message) = flight_message_stream.next().await {
                        let flight_message = flight_message
                            .map_err(BoxedError::new)
                            .context(ExternalSnafu)?;
                        match flight_message {
                            FlightMessage::Recordbatch(record_batch) => yield Ok(record_batch),
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
                };
                Ok(Output::new_with_stream(Box::pin(record_batch_stream)))
            }
        }
    }
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

    use super::*;

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
}
