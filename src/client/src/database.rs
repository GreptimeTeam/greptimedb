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
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use api::v1::auth_header::AuthScheme;
use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::greptime_database_client::GreptimeDatabaseClient;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{
    AlterTableExpr, AuthHeader, Basic, CreateTableExpr, DdlRequest, GreptimeRequest,
    InsertRequests, QueryRequest, RequestHeader, RowInsertRequests,
};
use arc_swap::ArcSwapOption;
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
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, RecordBatchStreamWrapper};
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

#[derive(Debug, Clone, Default)]
pub struct OutputMetrics {
    metrics: Arc<ArcSwapOption<RecordBatchMetrics>>,
    ready: Arc<AtomicBool>,
}

impl OutputMetrics {
    fn new() -> Self {
        Self::default()
    }

    pub fn update(&self, metrics: Option<RecordBatchMetrics>) {
        self.metrics.swap(metrics.map(Arc::new));
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn get(&self) -> Option<RecordBatchMetrics> {
        self.metrics.load().as_ref().map(|m| m.as_ref().clone())
    }

    pub fn region_watermark_map(&self) -> Option<std::collections::HashMap<u64, u64>> {
        self.get()?.region_latest_sequences.map(|sequences| {
            sequences
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>()
        })
    }
}

#[derive(Debug)]
pub struct OutputWithMetrics {
    pub output: Output,
    pub metrics: OutputMetrics,
}

impl OutputWithMetrics {
    pub fn from_output(output: Output) -> Self {
        let terminal_metrics = OutputMetrics::new();
        let output = attach_terminal_metrics(output, &terminal_metrics);
        Self {
            output,
            metrics: terminal_metrics,
        }
    }

    pub fn region_watermark_map(&self) -> Option<std::collections::HashMap<u64, u64>> {
        self.metrics.region_watermark_map()
    }

    pub fn into_output(self) -> Output {
        self.output
    }
}

fn parse_terminal_metrics(metrics_json: &str) -> Option<Arc<RecordBatchMetrics>> {
    serde_json::from_str(metrics_json).ok().map(Arc::new)
}

struct StreamWithMetrics {
    stream: common_recordbatch::SendableRecordBatchStream,
    metrics: OutputMetrics,
}

impl StreamWithMetrics {
    fn new(stream: common_recordbatch::SendableRecordBatchStream, metrics: OutputMetrics) -> Self {
        Self { stream, metrics }
    }

    fn sync_terminal_metrics(&self) {
        self.metrics.update(self.stream.metrics());
    }

    fn mark_ready_if_terminated(&self) {
        self.metrics.mark_ready();
    }
}

impl RecordBatchStream for StreamWithMetrics {
    fn name(&self) -> &str {
        self.stream.name()
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.stream.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.stream.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.sync_terminal_metrics();
        self.metrics.get()
    }
}

impl Stream for StreamWithMetrics {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = Pin::new(&mut self.stream).poll_next(cx);
        match &polled {
            Poll::Ready(Some(_)) => self.sync_terminal_metrics(),
            Poll::Ready(None) => {
                self.sync_terminal_metrics();
                self.mark_ready_if_terminated();
            }
            Poll::Pending => {}
        }
        polled
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

fn attach_terminal_metrics(output: Output, terminal_metrics: &OutputMetrics) -> Output {
    let Output { data, meta } = output;
    let data = match data {
        common_query::OutputData::Stream(stream) => {
            terminal_metrics.update(stream.metrics());
            common_query::OutputData::Stream(Box::pin(StreamWithMetrics::new(
                stream,
                terminal_metrics.clone(),
            )))
        }
        other => {
            terminal_metrics.mark_ready();
            other
        }
    };
    Output::new(data, meta)
}

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
        self.do_get(request, hints)
            .await
            .map(OutputWithMetrics::into_output)
    }

    pub async fn sql_with_terminal_metrics<S>(
        &self,
        sql: S,
        hints: &[(&str, &str)],
    ) -> Result<OutputWithMetrics>
    where
        S: AsRef<str>,
    {
        self.query_with_terminal_metrics(
            QueryRequest {
                query: Some(Query::Sql(sql.as_ref().to_string())),
            },
            hints,
        )
        .await
    }

    /// Executes a logical plan directly without SQL parsing.
    pub async fn logical_plan(&self, logical_plan: Vec<u8>) -> Result<Output> {
        self.query_with_terminal_metrics(
            QueryRequest {
                query: Some(Query::LogicalPlan(logical_plan)),
            },
            &[],
        )
        .await
        .map(OutputWithMetrics::into_output)
    }

    pub async fn query_with_terminal_metrics(
        &self,
        request: QueryRequest,
        hints: &[(&str, &str)],
    ) -> Result<OutputWithMetrics> {
        self.do_get(Request::Query(request), hints).await
    }

    /// Creates a new table using the provided table expression.
    pub async fn create(&self, expr: CreateTableExpr) -> Result<Output> {
        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(expr)),
        });
        self.do_get(request, &[])
            .await
            .map(OutputWithMetrics::into_output)
    }

    /// Alters an existing table using the provided alter expression.
    pub async fn alter(&self, expr: AlterTableExpr) -> Result<Output> {
        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::AlterTable(expr)),
        });
        self.do_get(request, &[])
            .await
            .map(OutputWithMetrics::into_output)
    }

    async fn do_get(&self, request: Request, hints: &[(&str, &str)]) -> Result<OutputWithMetrics> {
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
                let terminal_metrics = OutputMetrics::new();
                let next_message = flight_message_stream.next().await.transpose()?;
                match next_message {
                    None => terminal_metrics.mark_ready(),
                    Some(FlightMessage::Metrics(s)) => {
                        terminal_metrics.update(
                            parse_terminal_metrics(&s).map(|metrics| metrics.as_ref().clone()),
                        );
                        terminal_metrics.mark_ready();
                        ensure!(
                            flight_message_stream.next().await.is_none(),
                            IllegalFlightMessagesSnafu {
                                reason: "Expect 'AffectedRows' Flight messages to be followed by at most one Metrics message"
                            }
                        );
                    }
                    Some(other) => {
                        return IllegalFlightMessagesSnafu {
                            reason: format!(
                                "'AffectedRows' Flight message can only be followed by a Metrics message, got {other:?}"
                            ),
                        }
                        .fail();
                    }
                }
                Ok(OutputWithMetrics {
                    output: Output::new_with_affected_rows(rows),
                    metrics: terminal_metrics,
                })
            }
            FlightMessage::RecordBatch(_) | FlightMessage::Metrics(_) => {
                IllegalFlightMessagesSnafu {
                    reason: "The first flight message cannot be a RecordBatch or Metrics message",
                }
                .fail()
            }
            FlightMessage::Schema(schema) => {
                let metrics = Arc::new(ArcSwapOption::from(None));
                let metrics_ref = metrics.clone();
                let schema = Arc::new(
                    datatypes::schema::Schema::try_from(schema)
                        .context(error::ConvertSchemaSnafu)?,
                );
                let schema_cloned = schema.clone();
                let stream = Box::pin(stream!({
                    let mut buffered_message: Option<FlightMessage> = None;
                    let mut stream_ended = false;

                    while !stream_ended {
                        let flight_message_item = if let Some(msg) = buffered_message.take() {
                            Some(Ok(msg))
                        } else {
                            flight_message_stream.next().await
                        };

                        let flight_message = match flight_message_item {
                            Some(Ok(message)) => message,
                            Some(Err(e)) => {
                                yield Err(BoxedError::new(e)).context(ExternalSnafu);
                                break;
                            }
                            None => break,
                        };

                        match flight_message {
                            FlightMessage::RecordBatch(arrow_batch) => {
                                let result_to_yield = RecordBatch::from_df_record_batch(
                                    schema_cloned.clone(),
                                    arrow_batch,
                                );

                                if let Some(next_flight_message_result) =
                                    flight_message_stream.next().await
                                {
                                    match next_flight_message_result {
                                        Ok(FlightMessage::Metrics(s)) => {
                                            let m = parse_terminal_metrics(&s);
                                            metrics_ref.swap(m);
                                        }
                                        Ok(FlightMessage::RecordBatch(rb)) => {
                                            buffered_message = Some(FlightMessage::RecordBatch(rb));
                                        }
                                        Ok(_) => {
                                            yield IllegalFlightMessagesSnafu {reason: "A RecordBatch message can only be succeeded by a Metrics message or another RecordBatch message"}
                                                .fail()
                                                .map_err(BoxedError::new)
                                                .context(ExternalSnafu);
                                            break;
                                        }
                                        Err(e) => {
                                            yield Err(BoxedError::new(e)).context(ExternalSnafu);
                                            break;
                                        }
                                    }
                                } else {
                                    stream_ended = true;
                                }

                                yield Ok(result_to_yield)
                            }
                            FlightMessage::Metrics(s) => {
                                let m = parse_terminal_metrics(&s);
                                metrics_ref.swap(m);
                                break;
                            }
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
                    metrics,
                    span: Span::current(),
                };
                Ok(OutputWithMetrics::from_output(Output::new_with_stream(
                    Box::pin(record_batch_stream),
                )))
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
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use api::v1::auth_header::AuthScheme;
    use api::v1::{AuthHeader, Basic};
    use common_error::status_code::StatusCode;
    use common_query::OutputData;
    use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::StreamExt;
    use tonic::{Code, Status};

    use super::*;
    use crate::error::TonicSnafu;

    struct MockMetricsStream {
        schema: datatypes::schema::SchemaRef,
        batch: Option<RecordBatch>,
        metrics: RecordBatchMetrics,
        terminal_metrics_only: bool,
    }

    impl Stream for MockMetricsStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.batch.take().map(Ok))
        }
    }

    impl RecordBatchStream for MockMetricsStream {
        fn name(&self) -> &str {
            "MockMetricsStream"
        }

        fn schema(&self) -> datatypes::schema::SchemaRef {
            self.schema.clone()
        }

        fn output_ordering(&self) -> Option<&[OrderOption]> {
            None
        }

        fn metrics(&self) -> Option<RecordBatchMetrics> {
            if self.terminal_metrics_only && self.batch.is_some() {
                return None;
            }
            Some(self.metrics.clone())
        }
    }

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

    #[tokio::test]
    async fn test_query_with_terminal_metrics_tracks_terminal_only_metrics() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1, 2])) as VectorRef],
        )
        .unwrap();
        let output = Output::new_with_stream(Box::pin(MockMetricsStream {
            schema,
            batch: Some(batch),
            metrics: RecordBatchMetrics {
                region_latest_sequences: Some(vec![(7, 42)]),
                ..Default::default()
            },
            terminal_metrics_only: true,
        }));

        let result = OutputWithMetrics::from_output(output);
        let terminal_metrics = result.metrics.clone();
        assert!(!terminal_metrics.is_ready());
        assert!(terminal_metrics.get().is_none());

        let OutputData::Stream(mut stream) = result.output.data else {
            panic!("expected stream output");
        };
        while stream.next().await.is_some() {}

        assert!(terminal_metrics.is_ready());
        assert_eq!(
            terminal_metrics.region_watermark_map(),
            Some(std::collections::HashMap::from([(7_u64, 42_u64)]))
        );
    }

    #[test]
    fn test_parse_terminal_metrics_rejects_invalid_json() {
        assert!(parse_terminal_metrics("{not-json}").is_none());
    }
}
