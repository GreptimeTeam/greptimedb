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

//! Frontend client to run flow as batching task which is time-window-aware normal query triggered every tick set by user

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, Weak};

use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{CreateTableExpr, QueryRequest};
use client::{Client, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, Database, OutputWithMetrics};
use common_error::ext::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager, load_client_tls_config};
use common_meta::peer::{Peer, PeerDiscovery};
use common_query::{Output, OutputData};
use common_telemetry::warn;
use futures::stream::{FuturesUnordered, StreamExt};
use meta_client::client::MetaClient;
use query::datafusion::QUERY_PARALLELISM_HINT;
use query::metrics::terminal_recordbatch_metrics_from_plan;
use query::options::{FlowQueryExtensions, QueryOptions};
use rand::rng;
use rand::seq::SliceRandom;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::{QueryContextBuilder, QueryContextRef};
use session::hints::READ_PREFERENCE_HINT;
use snafu::{OptionExt, ResultExt};
use tokio::sync::SetOnce;

use crate::Error;
use crate::batching_mode::BatchingModeOptions;
use crate::error::{
    CreateSinkTableSnafu, ExternalSnafu, InvalidClientConfigSnafu, InvalidRequestSnafu,
    NoAvailableFrontendSnafu, UnexpectedSnafu,
};

/// Adapter trait for [`GrpcQueryHandler`] that boxes the underlying error into [`BoxedError`].
///
/// This is mainly used by flownode to invoke a frontend instance in standalone mode.
#[async_trait::async_trait]
pub trait GrpcQueryHandlerWithBoxedError: Send + Sync + 'static {
    async fn do_query(
        &self,
        query: Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError>;
}

/// auto impl
#[async_trait::async_trait]
impl<T: GrpcQueryHandler + Send + Sync + 'static> GrpcQueryHandlerWithBoxedError for T {
    async fn do_query(
        &self,
        query: Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        self.do_query(query, ctx).await.map_err(BoxedError::new)
    }
}

#[derive(Debug, Clone)]
pub struct HandlerMutable {
    handler: Arc<Mutex<Option<Weak<dyn GrpcQueryHandlerWithBoxedError>>>>,
    is_initialized: Arc<SetOnce<()>>,
}

impl HandlerMutable {
    pub async fn set_handler(&self, handler: Weak<dyn GrpcQueryHandlerWithBoxedError>) {
        *self.handler.lock().unwrap() = Some(handler);
        // Ignore the error, as we allow the handler to be set multiple times.
        let _ = self.is_initialized.set(());
    }
}

/// A simple frontend client able to execute sql using grpc protocol
///
/// This is for computation-heavy query which need to offload computation to frontend, lifting the load from flownode
#[derive(Debug, Clone)]
pub enum FrontendClient {
    Distributed {
        meta_client: Arc<MetaClient>,
        chnl_mgr: ChannelManager,
        query: QueryOptions,
        batch_opts: BatchingModeOptions,
    },
    Standalone {
        /// for the sake of simplicity still use grpc even in standalone mode
        /// notice the client here should all be lazy, so that can wait after frontend is booted then make conn
        database_client: HandlerMutable,
        query: QueryOptions,
    },
}

impl FrontendClient {
    /// Create a new empty frontend client, with a `HandlerMutable` to set the grpc handler later
    pub fn from_empty_grpc_handler(query: QueryOptions) -> (Self, HandlerMutable) {
        let is_initialized = Arc::new(SetOnce::new());
        let handler = HandlerMutable {
            handler: Arc::new(Mutex::new(None)),
            is_initialized,
        };
        (
            Self::Standalone {
                database_client: handler.clone(),
                query,
            },
            handler,
        )
    }

    /// Waits until the frontend client is initialized.
    pub async fn wait_initialized(&self) {
        if let FrontendClient::Standalone {
            database_client, ..
        } = self
        {
            database_client.is_initialized.wait().await;
        }
    }

    pub fn from_meta_client(
        meta_client: Arc<MetaClient>,
        query: QueryOptions,
        batch_opts: BatchingModeOptions,
    ) -> Result<Self, Error> {
        common_telemetry::info!("Frontend client build without auth");
        Ok(Self::Distributed {
            meta_client,
            chnl_mgr: {
                let cfg = ChannelConfig::new()
                    .connect_timeout(batch_opts.grpc_conn_timeout)
                    .timeout(Some(batch_opts.query_timeout));

                let tls_config = load_client_tls_config(batch_opts.frontend_tls.clone())
                    .context(InvalidClientConfigSnafu)?;
                ChannelManager::with_config(cfg, tls_config)
            },
            query,
            batch_opts,
        })
    }

    pub fn from_grpc_handler(
        grpc_handler: Weak<dyn GrpcQueryHandlerWithBoxedError>,
        query: QueryOptions,
    ) -> Self {
        let is_initialized = Arc::new(SetOnce::new_with(Some(())));
        let handler = HandlerMutable {
            handler: Arc::new(Mutex::new(Some(grpc_handler))),
            is_initialized: is_initialized.clone(),
        };

        Self::Standalone {
            database_client: handler,
            query,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseWithPeer {
    pub database: Database,
    pub peer: Peer,
}

impl DatabaseWithPeer {
    fn new(database: Database, peer: Peer) -> Self {
        Self { database, peer }
    }

    /// Try sending a "SELECT 1" to the database
    async fn try_select_one(&self) -> Result<(), Error> {
        // notice here use `sql` for `SELECT 1` return 1 row
        let _ = self
            .database
            .sql("SELECT 1")
            .await
            .with_context(|_| InvalidRequestSnafu {
                context: format!("Failed to handle `SELECT 1` request at {:?}", self.peer),
            })?;
        Ok(())
    }
}

impl FrontendClient {
    /// scan for available frontend from metadata
    pub(crate) async fn scan_for_frontend(&self) -> Result<Vec<Peer>, Error> {
        let Self::Distributed { meta_client, .. } = self else {
            return Ok(vec![]);
        };

        meta_client
            .active_frontends()
            .await
            .map(|nodes| nodes.into_iter().map(|node| node.peer).collect())
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    /// Probes all discovered frontends without auth.
    ///
    /// Returns non-auth failures to allow callers to retry transient connectivity
    /// errors. Authentication failures are returned immediately because they mean
    /// a frontend advertised an auth-protected endpoint to flownodes.
    pub(crate) async fn check_all_frontends_without_auth(
        &self,
        frontends: &[Peer],
    ) -> Result<Vec<String>, Error> {
        let Self::Distributed {
            chnl_mgr,
            batch_opts,
            ..
        } = self
        else {
            return Ok(vec![]);
        };

        let probe_timeout = batch_opts.grpc_conn_timeout;
        let mut probes = frontends
            .iter()
            .map(|peer| {
                let addr = peer.addr.clone();
                let chnl_mgr = chnl_mgr.clone();

                async move {
                    let client = Client::with_manager_and_urls(chnl_mgr, vec![addr.clone()]);
                    let database = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, client);

                    match tokio::time::timeout(probe_timeout, database.sql("SELECT 1")).await {
                        Ok(Ok(_)) => Ok(None),
                        Ok(Err(err)) if err.tonic_code() == Some(tonic::Code::Unauthenticated) => {
                            Err(err).context(InvalidRequestSnafu {
                                context: format!(
                                    "Frontend {addr} rejected unauthenticated flownode probe; ensure frontend internal_grpc is advertised to metasrv"
                                ),
                            })
                        }
                        Ok(Err(err)) => Ok(Some(format!("{addr}: {err}"))),
                        Err(_) => Ok(Some(format!(
                            "{addr}: health check timed out after {probe_timeout:?}"
                        ))),
                    }
                }
            })
            .collect::<FuturesUnordered<_>>();

        let mut failures = Vec::new();
        while let Some(probe_result) = probes.next().await {
            if let Some(failure) = probe_result? {
                failures.push(failure);
            }
        }

        Ok(failures)
    }

    /// Get a frontend discovered by metasrv and verified with a query probe.
    async fn get_random_active_frontend(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<DatabaseWithPeer, Error> {
        let Self::Distributed {
            meta_client: _,
            chnl_mgr,
            query: _,
            batch_opts,
        } = self
        else {
            return UnexpectedSnafu {
                reason: "Expect distributed mode",
            }
            .fail();
        };

        let mut interval = tokio::time::interval(batch_opts.grpc_conn_timeout);
        interval.tick().await;
        for retry in 0..batch_opts.experimental_grpc_max_retries {
            let mut frontends = self.scan_for_frontend().await?;
            // shuffle the frontends to avoid always pick the same one
            frontends.shuffle(&mut rng());

            for peer in frontends {
                let addr = peer.addr.clone();
                let client = Client::with_manager_and_urls(chnl_mgr.clone(), vec![addr.clone()]);
                let database = Database::new(catalog, schema, client);
                let db = DatabaseWithPeer::new(database, peer);
                match db.try_select_one().await {
                    Ok(_) => return Ok(db),
                    Err(e) => {
                        warn!(
                            "Failed to connect to frontend {} on retry={}: \n{e:?}",
                            addr, retry
                        );
                    }
                }
            }
            // no available frontend
            // sleep and retry
            interval.tick().await;
        }

        NoAvailableFrontendSnafu {
            timeout: batch_opts.grpc_conn_timeout,
            context: "No available frontend found that is able to process query",
        }
        .fail()
    }

    pub async fn create(
        &self,
        create: CreateTableExpr,
        catalog: &str,
        schema: &str,
    ) -> Result<u32, Error> {
        self.handle(
            Request::Ddl(api::v1::DdlRequest {
                expr: Some(api::v1::ddl_request::Expr::CreateTable(create.clone())),
            }),
            catalog,
            schema,
            &mut None,
        )
        .await
        .map_err(BoxedError::new)
        .with_context(|_| CreateSinkTableSnafu {
            create: create.clone(),
        })
    }

    /// Execute a SQL statement on the frontend.
    pub async fn sql(&self, catalog: &str, schema: &str, sql: &str) -> Result<Output, Error> {
        match self {
            FrontendClient::Distributed { .. } => {
                let db = self.get_random_active_frontend(catalog, schema).await?;
                db.database
                    .sql(sql)
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)
            }
            FrontendClient::Standalone {
                database_client, ..
            } => {
                let ctx = QueryContextBuilder::default()
                    .current_catalog(catalog.to_string())
                    .current_schema(schema.to_string())
                    .build();
                let ctx = Arc::new(ctx);
                {
                    let database_client = {
                        database_client
                            .handler
                            .lock()
                            .unwrap()
                            .as_ref()
                            .context(UnexpectedSnafu {
                                reason: "Standalone's frontend instance is not set",
                            })?
                            .upgrade()
                            .context(UnexpectedSnafu {
                                reason: "Failed to upgrade database client",
                            })?
                    };
                    let req = Request::Query(QueryRequest {
                        query: Some(Query::Sql(sql.to_string())),
                    });
                    database_client
                        .do_query(req, ctx)
                        .await
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)
                }
            }
        }
    }

    pub(crate) async fn query_with_terminal_metrics(
        &self,
        catalog: &str,
        schema: &str,
        request: QueryRequest,
        extensions: &[(&str, &str)],
        snapshot_seqs: &HashMap<u64, u64>,
        peer_desc: &mut Option<PeerDesc>,
    ) -> Result<OutputWithMetrics, Error> {
        let flow_extensions = build_flow_extensions(extensions)?;
        match self {
            FrontendClient::Distributed {
                query, batch_opts, ..
            } => {
                let query_parallelism = query.parallelism.to_string();
                let hints = vec![
                    (QUERY_PARALLELISM_HINT, query_parallelism.as_str()),
                    (READ_PREFERENCE_HINT, batch_opts.read_preference.as_ref()),
                ];
                let db = self.get_random_active_frontend(catalog, schema).await?;
                *peer_desc = Some(PeerDesc::Dist {
                    peer: db.peer.clone(),
                });
                db.database
                    .query_with_terminal_metrics_and_flow_extensions(
                        request,
                        &hints,
                        extensions,
                        snapshot_seqs,
                    )
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)
            }
            FrontendClient::Standalone {
                database_client,
                query,
            } => {
                *peer_desc = Some(PeerDesc::Standalone);
                let mut extensions_map = HashMap::from([(
                    QUERY_PARALLELISM_HINT.to_string(),
                    query.parallelism.to_string(),
                )]);
                for (key, value) in extensions {
                    extensions_map.insert((*key).to_string(), (*value).to_string());
                }
                let ctx = QueryContextBuilder::default()
                    .current_catalog(catalog.to_string())
                    .current_schema(schema.to_string())
                    .extensions(extensions_map)
                    .snapshot_seqs(Arc::new(RwLock::new(snapshot_seqs.clone())))
                    .build();
                let ctx = Arc::new(ctx);
                let database_client = {
                    database_client
                        .handler
                        .lock()
                        .map_err(|e| {
                            UnexpectedSnafu {
                                reason: format!("Failed to lock database client: {e}"),
                            }
                            .build()
                        })?
                        .as_ref()
                        .context(UnexpectedSnafu {
                            reason: "Standalone's frontend instance is not set",
                        })?
                        .upgrade()
                        .context(UnexpectedSnafu {
                            reason: "Failed to upgrade database client",
                        })?
                };
                database_client
                    .do_query(Request::Query(request), ctx.clone())
                    .await
                    .map(|output| {
                        wrap_standalone_output_with_terminal_metrics(output, &flow_extensions)
                    })
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)
            }
        }
    }

    /// Handle a request to frontend
    pub(crate) async fn handle(
        &self,
        req: api::v1::greptime_request::Request,
        catalog: &str,
        schema: &str,
        peer_desc: &mut Option<PeerDesc>,
    ) -> Result<u32, Error> {
        match self {
            FrontendClient::Distributed {
                query, batch_opts, ..
            } => {
                let db = self.get_random_active_frontend(catalog, schema).await?;

                *peer_desc = Some(PeerDesc::Dist {
                    peer: db.peer.clone(),
                });

                db.database
                    .handle_with_retry(
                        req.clone(),
                        batch_opts.experimental_grpc_max_retries,
                        &[
                            (QUERY_PARALLELISM_HINT, &query.parallelism.to_string()),
                            (READ_PREFERENCE_HINT, batch_opts.read_preference.as_ref()),
                        ],
                    )
                    .await
                    .with_context(|_| InvalidRequestSnafu {
                        context: format!("Failed to handle request at {:?}: {:?}", db.peer, req),
                    })
            }
            FrontendClient::Standalone {
                database_client,
                query,
            } => {
                let ctx = QueryContextBuilder::default()
                    .current_catalog(catalog.to_string())
                    .current_schema(schema.to_string())
                    .extensions(HashMap::from([(
                        QUERY_PARALLELISM_HINT.to_string(),
                        query.parallelism.to_string(),
                    )]))
                    .build();
                let ctx = Arc::new(ctx);
                {
                    let database_client = {
                        database_client
                            .handler
                            .lock()
                            .unwrap()
                            .as_ref()
                            .context(UnexpectedSnafu {
                                reason: "Standalone's frontend instance is not set",
                            })?
                            .upgrade()
                            .context(UnexpectedSnafu {
                                reason: "Failed to upgrade database client",
                            })?
                    };
                    let resp: common_query::Output = database_client
                        .do_query(req, ctx)
                        .await
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu)?;
                    match resp.data {
                        common_query::OutputData::AffectedRows(rows) => {
                            Ok(rows.try_into().map_err(|_| {
                                UnexpectedSnafu {
                                    reason: format!("Failed to convert rows to u32: {}", rows),
                                }
                                .build()
                            })?)
                        }
                        _ => UnexpectedSnafu {
                            reason: "Unexpected output data",
                        }
                        .fail(),
                    }
                }
            }
        }
    }
}

fn build_flow_extensions(extensions: &[(&str, &str)]) -> Result<FlowQueryExtensions, Error> {
    let flow_extensions = HashMap::from_iter(
        extensions
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
    );
    FlowQueryExtensions::parse_flow_extensions(&flow_extensions)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)
        .map(|extensions| extensions.unwrap_or_default())
}

fn wrap_standalone_output_with_terminal_metrics(
    output: Output,
    flow_extensions: &FlowQueryExtensions,
) -> OutputWithMetrics {
    let should_collect_region_watermark = flow_extensions.should_collect_region_watermark();
    let terminal_metrics =
        if should_collect_region_watermark && !matches!(&output.data, OutputData::Stream(_)) {
            output
                .meta
                .plan
                .clone()
                .and_then(terminal_recordbatch_metrics_from_plan)
        } else {
            None
        };
    let result = OutputWithMetrics::from_output(output);
    if let Some(metrics) = terminal_metrics {
        result.metrics.update(Some(metrics));
    }
    result
}

/// Describe a peer of frontend
#[derive(Debug, Default, Clone)]
pub(crate) enum PeerDesc {
    /// The query failed before a frontend peer was selected.
    #[default]
    Unknown,
    /// Distributed mode's frontend peer address
    Dist {
        /// frontend peer address
        peer: Peer,
    },
    /// Standalone mode
    Standalone,
}

impl std::fmt::Display for PeerDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerDesc::Unknown => write!(f, "unknown"),
            PeerDesc::Dist { peer } => write!(f, "{}", peer.addr),
            PeerDesc::Standalone => write!(f, "standalone"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use arrow_flight::flight_service_server::FlightServiceServer;
    use arrow_flight::{FlightData, Ticket};
    use common_query::{Output, OutputData};
    use common_recordbatch::adapter::RecordBatchMetrics;
    use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures::StreamExt;
    use servers::grpc::flight::{FlightCraft, FlightCraftWrapper, TonicStream};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request as TonicRequest, Response as TonicResponse, Status};

    use super::*;

    #[derive(Debug)]
    struct NoopHandler;

    struct MockMetricsStream {
        schema: datatypes::schema::SchemaRef,
        batch: Option<RecordBatch>,
        metrics: RecordBatchMetrics,
        terminal_metrics_only: bool,
    }

    impl futures::Stream for MockMetricsStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.batch.take().map(Ok))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (
                usize::from(self.batch.is_some()),
                Some(usize::from(self.batch.is_some())),
            )
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

    #[derive(Debug)]
    struct MetricsHandler;

    #[derive(Debug)]
    struct ExtensionAwareHandler;

    #[derive(Debug)]
    struct SnapshotBindingHandler;

    #[derive(Debug)]
    struct RejectUnauthenticatedFlight;

    #[derive(Debug)]
    struct SlowFlight;

    struct WaitForConcurrentFlight {
        barrier: Arc<tokio::sync::Barrier>,
    }

    #[async_trait::async_trait]
    impl GrpcQueryHandlerWithBoxedError for NoopHandler {
        async fn do_query(
            &self,
            _query: Request,
            _ctx: QueryContextRef,
        ) -> std::result::Result<Output, BoxedError> {
            Ok(Output::new_with_affected_rows(0))
        }
    }

    #[async_trait::async_trait]
    impl GrpcQueryHandlerWithBoxedError for MetricsHandler {
        async fn do_query(
            &self,
            _query: Request,
            _ctx: QueryContextRef,
        ) -> std::result::Result<Output, BoxedError> {
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
            Ok(Output::new_with_stream(Box::pin(MockMetricsStream {
                schema,
                batch: Some(batch),
                metrics: RecordBatchMetrics {
                    region_watermarks: vec![common_recordbatch::adapter::RegionWatermarkEntry {
                        region_id: 42,
                        watermark: Some(99),
                    }],
                    ..Default::default()
                },
                terminal_metrics_only: true,
            })))
        }
    }

    #[async_trait::async_trait]
    impl GrpcQueryHandlerWithBoxedError for ExtensionAwareHandler {
        async fn do_query(
            &self,
            _query: Request,
            ctx: QueryContextRef,
        ) -> std::result::Result<Output, BoxedError> {
            assert_eq!(ctx.extension("flow.return_region_seq"), Some("true"));
            Ok(Output::new_with_affected_rows(1))
        }
    }

    #[async_trait::async_trait]
    impl GrpcQueryHandlerWithBoxedError for SnapshotBindingHandler {
        async fn do_query(
            &self,
            _query: Request,
            ctx: QueryContextRef,
        ) -> std::result::Result<Output, BoxedError> {
            assert_eq!(ctx.extension("flow.return_region_seq"), Some("true"));
            assert_eq!(ctx.get_snapshot(7), Some(88));
            ctx.set_snapshot(42, 99);
            Ok(Output::new_with_affected_rows(1))
        }
    }

    #[async_trait::async_trait]
    impl FlightCraft for RejectUnauthenticatedFlight {
        async fn do_get(
            &self,
            _request: TonicRequest<Ticket>,
        ) -> std::result::Result<TonicResponse<TonicStream<FlightData>>, Status> {
            Err(Status::unauthenticated("auth failed"))
        }
    }

    #[async_trait::async_trait]
    impl FlightCraft for SlowFlight {
        async fn do_get(
            &self,
            _request: TonicRequest<Ticket>,
        ) -> std::result::Result<TonicResponse<TonicStream<FlightData>>, Status> {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Err(Status::unavailable("slow response"))
        }
    }

    #[async_trait::async_trait]
    impl FlightCraft for WaitForConcurrentFlight {
        async fn do_get(
            &self,
            _request: TonicRequest<Ticket>,
        ) -> std::result::Result<TonicResponse<TonicStream<FlightData>>, Status> {
            self.barrier.wait().await;
            Err(Status::unavailable("probe started concurrently"))
        }
    }

    async fn start_flight_server<T: FlightCraft>(handler: T) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test flight server");
        let addr = listener.local_addr().expect("local addr").to_string();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(FlightCraftWrapper(handler)))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve test flight server");
        });

        (addr, server)
    }

    #[tokio::test]
    async fn wait_initialized() {
        let (client, handler_mut) =
            FrontendClient::from_empty_grpc_handler(QueryOptions::default());

        assert!(
            timeout(Duration::from_millis(50), client.wait_initialized())
                .await
                .is_err()
        );

        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(NoopHandler);
        handler_mut.set_handler(Arc::downgrade(&handler)).await;

        timeout(Duration::from_secs(1), client.wait_initialized())
            .await
            .expect("wait_initialized should complete after handler is set");

        timeout(Duration::from_millis(10), client.wait_initialized())
            .await
            .expect("wait_initialized should be a no-op once initialized");

        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(NoopHandler);
        let client =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());
        assert!(
            timeout(Duration::from_millis(10), client.wait_initialized())
                .await
                .is_ok()
        );

        let meta_client = Arc::new(MetaClient::new(0, api::v1::meta::Role::Frontend));
        let client = FrontendClient::from_meta_client(
            meta_client,
            QueryOptions::default(),
            BatchingModeOptions::default(),
        )
        .unwrap();
        assert!(
            timeout(Duration::from_millis(10), client.wait_initialized())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_query_with_terminal_metrics_tracks_watermark_in_standalone_mode() {
        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(MetricsHandler);
        let client =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());
        let mut peer_desc = None;

        let result = client
            .query_with_terminal_metrics(
                "greptime",
                "public",
                QueryRequest {
                    query: Some(Query::Sql("select 1".to_string())),
                },
                &[],
                &HashMap::new(),
                &mut peer_desc,
            )
            .await
            .unwrap();
        assert!(matches!(peer_desc, Some(PeerDesc::Standalone)));

        let terminal_metrics = result.metrics.clone();
        assert!(!result.metrics.is_ready());
        assert!(terminal_metrics.get().is_none());

        let OutputData::Stream(mut stream) = result.output.data else {
            panic!("expected stream output");
        };
        while stream.next().await.is_some() {}

        assert!(terminal_metrics.is_ready());
        assert_eq!(
            terminal_metrics.region_watermark_map(),
            Some(HashMap::from([(42_u64, 99_u64)]))
        );
    }

    #[tokio::test]
    async fn test_query_with_terminal_metrics_forwards_flow_extensions_in_standalone_mode() {
        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(ExtensionAwareHandler);
        let client =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());
        let mut peer_desc = None;

        let result = client
            .query_with_terminal_metrics(
                "greptime",
                "public",
                QueryRequest {
                    query: Some(Query::Sql("insert into t select 1".to_string())),
                },
                &[("flow.return_region_seq", "true")],
                &HashMap::new(),
                &mut peer_desc,
            )
            .await
            .unwrap();
        assert!(matches!(peer_desc, Some(PeerDesc::Standalone)));

        assert!(result.metrics.is_ready());
        assert!(result.region_watermark_map().is_none());
    }

    #[tokio::test]
    async fn test_query_with_terminal_metrics_uses_standalone_snapshot_bounds() {
        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(SnapshotBindingHandler);
        let client =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());
        let mut peer_desc = None;

        let result = client
            .query_with_terminal_metrics(
                "greptime",
                "public",
                QueryRequest {
                    query: Some(Query::Sql("insert into t select * from src".to_string())),
                },
                &[("flow.return_region_seq", "true")],
                &HashMap::from([(7, 88)]),
                &mut peer_desc,
            )
            .await
            .unwrap();
        assert!(matches!(peer_desc, Some(PeerDesc::Standalone)));

        assert!(result.metrics.is_ready());
        assert_eq!(result.region_watermark_map(), None);
    }

    #[tokio::test]
    async fn test_query_with_terminal_metrics_rejects_invalid_flow_extensions() {
        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(NoopHandler);
        let client =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());
        let mut peer_desc = None;

        let err = client
            .query_with_terminal_metrics(
                "greptime",
                "public",
                QueryRequest {
                    query: Some(Query::Sql("select 1".to_string())),
                },
                &[("flow.return_region_seq", "not-a-bool")],
                &HashMap::new(),
                &mut peer_desc,
            )
            .await
            .unwrap_err();

        assert!(format!("{err:?}").contains("Invalid value for flow.return_region_seq"));
    }

    #[tokio::test]
    async fn test_check_all_frontends_without_auth_fails_fast_on_unauthenticated_frontend() {
        let (addr, server) = start_flight_server(RejectUnauthenticatedFlight).await;
        let client = FrontendClient::from_meta_client(
            Arc::new(MetaClient::new(0, api::v1::meta::Role::Frontend)),
            QueryOptions::default(),
            BatchingModeOptions::default(),
        )
        .unwrap();

        let err = client
            .check_all_frontends_without_auth(&[Peer {
                id: 1,
                addr: addr.clone(),
            }])
            .await
            .unwrap_err();
        server.abort();

        let Error::InvalidRequest {
            context, source, ..
        } = err
        else {
            panic!("expected InvalidRequest, got {err:?}");
        };
        assert!(context.contains(&addr));
        assert!(context.contains("rejected unauthenticated flownode probe"));
        assert_eq!(source.tonic_code(), Some(tonic::Code::Unauthenticated));
    }

    #[tokio::test]
    async fn test_check_all_frontends_without_auth_uses_grpc_connection_timeout() {
        let (addr, server) = start_flight_server(SlowFlight).await;
        let client = FrontendClient::from_meta_client(
            Arc::new(MetaClient::new(0, api::v1::meta::Role::Frontend)),
            QueryOptions::default(),
            BatchingModeOptions {
                grpc_conn_timeout: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .unwrap();

        let failures = client
            .check_all_frontends_without_auth(&[Peer {
                id: 1,
                addr: addr.clone(),
            }])
            .await
            .unwrap();
        server.abort();

        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains(&addr));
        assert!(failures[0].contains("health check timed out"));
    }

    #[tokio::test]
    async fn test_check_all_frontends_without_auth_checks_frontends_concurrently() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let (addr1, server1) = start_flight_server(WaitForConcurrentFlight {
            barrier: barrier.clone(),
        })
        .await;
        let (addr2, server2) = start_flight_server(WaitForConcurrentFlight { barrier }).await;
        let client = FrontendClient::from_meta_client(
            Arc::new(MetaClient::new(0, api::v1::meta::Role::Frontend)),
            QueryOptions::default(),
            BatchingModeOptions {
                grpc_conn_timeout: Duration::from_millis(500),
                ..Default::default()
            },
        )
        .unwrap();

        let failures = timeout(
            Duration::from_secs(2),
            client.check_all_frontends_without_auth(&[
                Peer {
                    id: 1,
                    addr: addr1.clone(),
                },
                Peer {
                    id: 2,
                    addr: addr2.clone(),
                },
            ]),
        )
        .await
        .expect("concurrent probes should complete before per-peer timeouts")
        .unwrap();
        server1.abort();
        server2.abort();

        assert_eq!(failures.len(), 2);
        assert!(failures.iter().any(|failure| failure.contains(&addr1)));
        assert!(failures.iter().any(|failure| failure.contains(&addr2)));
        assert!(
            failures
                .iter()
                .all(|failure| !failure.contains("health check timed out")),
            "sequential probes would time out before both requests reach the barrier: {failures:?}"
        );
    }
}
