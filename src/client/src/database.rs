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
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{
    AlterExpr, AuthHeader, CreateTableExpr, DdlRequest, DeleteRequests, DropTableExpr,
    GreptimeRequest, InsertRequests, PromRangeQuery, QueryRequest, RequestHeader,
    RowInsertRequests, TruncateTableExpr,
};
use arrow_flight::Ticket;
use async_stream::stream;
use common_error::ext::{BoxedError, ErrorExt};
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_query::Output;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::RecordBatchStreamWrapper;
use common_telemetry::logging;
use common_telemetry::tracing_context::W3cTrace;
use futures_util::StreamExt;
use prost::Message;
use snafu::{ensure, ResultExt};

use crate::error::{ConvertFlightDataSnafu, Error, IllegalFlightMessagesSnafu, ServerSnafu};
use crate::{error, from_grpc_response, metrics, Client, Result, StreamInserter};

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

    client: Client,
    ctx: FlightContext,
}

impl Database {
    /// Create database service client using catalog and schema
    pub fn new(catalog: impl Into<String>, schema: impl Into<String>, client: Client) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            dbname: "".to_string(),
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
            catalog: "".to_string(),
            schema: "".to_string(),
            dbname: dbname.into(),
            client,
            ctx: FlightContext::default(),
        }
    }

    pub fn catalog(&self) -> &String {
        &self.catalog
    }

    pub fn set_catalog(&mut self, catalog: impl Into<String>) {
        self.catalog = catalog.into();
    }

    pub fn schema(&self) -> &String {
        &self.schema
    }

    pub fn set_schema(&mut self, schema: impl Into<String>) {
        self.schema = schema.into();
    }

    pub fn dbname(&self) -> &String {
        &self.dbname
    }

    pub fn set_dbname(&mut self, dbname: impl Into<String>) {
        self.dbname = dbname.into();
    }

    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.ctx.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    pub async fn insert(&self, requests: InsertRequests) -> Result<u32> {
        let _timer = metrics::METRIC_GRPC_INSERT.start_timer();
        self.handle(Request::Inserts(requests)).await
    }

    pub async fn row_insert(&self, requests: RowInsertRequests) -> Result<u32> {
        let _timer = metrics::METRIC_GRPC_INSERT.start_timer();
        self.handle(Request::RowInserts(requests)).await
    }

    pub fn streaming_inserter(&self) -> Result<StreamInserter> {
        self.streaming_inserter_with_channel_size(65536)
    }

    pub fn streaming_inserter_with_channel_size(
        &self,
        channel_size: usize,
    ) -> Result<StreamInserter> {
        let client = self.client.make_database_client()?.inner;

        let stream_inserter = StreamInserter::new(
            client,
            self.dbname().to_string(),
            self.ctx.auth_header.clone(),
            channel_size,
        );

        Ok(stream_inserter)
    }

    pub async fn delete(&self, request: DeleteRequests) -> Result<u32> {
        let _timer = metrics::METRIC_GRPC_DELETE.start_timer();
        self.handle(Request::Deletes(request)).await
    }

    async fn handle(&self, request: Request) -> Result<u32> {
        let mut client = self.client.make_database_client()?.inner;
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
        let _timer = metrics::METRIC_GRPC_SQL.start_timer();
        self.do_get(Request::Query(QueryRequest {
            query: Some(Query::Sql(sql.as_ref().to_string())),
        }))
        .await
    }

    pub async fn logical_plan(&self, logical_plan: Vec<u8>) -> Result<Output> {
        let _timer = metrics::METRIC_GRPC_LOGICAL_PLAN.start_timer();
        self.do_get(Request::Query(QueryRequest {
            query: Some(Query::LogicalPlan(logical_plan)),
        }))
        .await
    }

    pub async fn prom_range_query(
        &self,
        promql: &str,
        start: &str,
        end: &str,
        step: &str,
    ) -> Result<Output> {
        let _timer = metrics::METRIC_GRPC_PROMQL_RANGE_QUERY.start_timer();
        self.do_get(Request::Query(QueryRequest {
            query: Some(Query::PromRangeQuery(PromRangeQuery {
                query: promql.to_string(),
                start: start.to_string(),
                end: end.to_string(),
                step: step.to_string(),
            })),
        }))
        .await
    }

    pub async fn create(&self, expr: CreateTableExpr) -> Result<Output> {
        let _timer = metrics::METRIC_GRPC_CREATE_TABLE.start_timer();
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(expr)),
        }))
        .await
    }

    pub async fn alter(&self, expr: AlterExpr) -> Result<Output> {
        let _timer = metrics::METRIC_GRPC_ALTER.start_timer();
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(expr)),
        }))
        .await
    }

    pub async fn drop_table(&self, expr: DropTableExpr) -> Result<Output> {
        let _timer = metrics::METRIC_GRPC_DROP_TABLE.start_timer();
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::DropTable(expr)),
        }))
        .await
    }

    pub async fn truncate_table(&self, expr: TruncateTableExpr) -> Result<Output> {
        let _timer = metrics::METRIC_GRPC_TRUNCATE_TABLE.start_timer();
        self.do_get(Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::TruncateTable(expr)),
        }))
        .await
    }

    async fn do_get(&self, request: Request) -> Result<Output> {
        // FIXME(paomian): should be added some labels for metrics
        let _timer = metrics::METRIC_GRPC_DO_GET.start_timer();
        let request = self.to_rpc_request(request);
        let request = Ticket {
            ticket: request.encode_to_vec().into(),
        };

        let mut client = self.client.make_flight_client()?;

        let response = client.mut_inner().do_get(request).await.map_err(|e| {
            let tonic_code = e.code();
            let e: error::Error = e.into();
            let code = e.status_code();
            let msg = e.to_string();
            let error = Error::FlightGet {
                tonic_code,
                addr: client.addr().to_string(),
                source: BoxedError::new(ServerSnafu { code, msg }.build()),
            };
            logging::error!(
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
                Ok(Output::AffectedRows(rows))
            }
            FlightMessage::Recordbatch(_) => IllegalFlightMessagesSnafu {
                reason: "The first flight message cannot be a RecordBatch message",
            }
            .fail(),
            FlightMessage::Schema(schema) => {
                let stream = Box::pin(stream!({
                    while let Some(flight_message) = flight_message_stream.next().await {
                        let flight_message = flight_message
                            .map_err(BoxedError::new)
                            .context(ExternalSnafu)?;
                        let FlightMessage::Recordbatch(record_batch) = flight_message else {
                            yield IllegalFlightMessagesSnafu {reason: "A Schema message must be succeeded exclusively by a set of RecordBatch messages"}
                                        .fail()
                                        .map_err(BoxedError::new)
                                        .context(ExternalSnafu);
                            break;
                        };
                        yield Ok(record_batch);
                    }
                }));
                let record_batch_stream = RecordBatchStreamWrapper {
                    schema,
                    stream,
                    output_ordering: None,
                };
                Ok(Output::Stream(Box::pin(record_batch_stream)))
            }
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct FlightContext {
    auth_header: Option<AuthHeader>,
}

#[cfg(test)]
mod tests {
    use api::v1::auth_header::AuthScheme;
    use api::v1::{AuthHeader, Basic};

    use crate::database::FlightContext;

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

        assert!(matches!(
            ctx.auth_header,
            Some(AuthHeader {
                auth_scheme: Some(AuthScheme::Basic(_)),
            })
        ))
    }
}
