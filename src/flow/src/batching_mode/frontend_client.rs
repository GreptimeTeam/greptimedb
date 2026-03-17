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
use std::sync::{Arc, Mutex, Weak};
use std::time::SystemTime;

use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{CreateTableExpr, QueryRequest};
use client::{Client, Database, OutputWithMetrics};
use common_error::ext::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager, load_client_tls_config};
use common_meta::cluster::{NodeInfo, NodeInfoKey, Role};
use common_meta::peer::Peer;
use common_meta::rpc::store::RangeRequest;
use common_query::{Output, OutputData};
use common_telemetry::warn;
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

use crate::batching_mode::BatchingModeOptions;
use crate::error::{
    CreateSinkTableSnafu, ExternalSnafu, InvalidClientConfigSnafu, InvalidRequestSnafu,
    NoAvailableFrontendSnafu, UnexpectedSnafu,
};
use crate::{Error, FlowAuthHeader};

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
        auth: Option<FlowAuthHeader>,
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
        auth: Option<FlowAuthHeader>,
        query: QueryOptions,
        batch_opts: BatchingModeOptions,
    ) -> Result<Self, Error> {
        common_telemetry::info!("Frontend client build with auth={:?}", auth);
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
            auth,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlowQueryFailure {
    pub stale_cursor: Option<FlowStaleCursorDetail>,
}

impl FlowQueryFailure {
    pub fn is_stale_cursor(&self) -> bool {
        self.stale_cursor.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlowStaleCursorDetail {
    pub region_id: Option<String>,
    pub given_seq: Option<u64>,
    pub min_readable_seq: Option<u64>,
}

const STALE_CURSOR_TOKEN: &str = "STALE_CURSOR";
const STALE_CURSOR_RETRY_HINT: &str = "FALLBACK_FULL_RECOMPUTE";

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
    /// TODO(discord9): better way to detect stale cursor error instead of parsing the error message
    pub fn inspect_query_error(err: &Error) -> FlowQueryFailure {
        let debug = format!("{err:?}");
        let stale_cursor = parse_stale_cursor_detail(&debug);
        FlowQueryFailure { stale_cursor }
    }

    /// scan for available frontend from metadata
    pub(crate) async fn scan_for_frontend(&self) -> Result<Vec<(NodeInfoKey, NodeInfo)>, Error> {
        let Self::Distributed { meta_client, .. } = self else {
            return Ok(vec![]);
        };
        let cluster_client = meta_client
            .cluster_client()
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let prefix = NodeInfoKey::key_prefix_with_role(Role::Frontend);
        let req = RangeRequest::new().with_prefix(prefix);
        let resp = cluster_client
            .range(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let mut res = Vec::with_capacity(resp.kvs.len());
        for kv in resp.kvs {
            let key = NodeInfoKey::try_from(kv.key)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;

            let val = NodeInfo::try_from(kv.value)
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
            res.push((key, val));
        }
        Ok(res)
    }

    /// Get the frontend with recent enough(less than 1 minute from now) `last_activity_ts`
    /// and is able to process query
    async fn get_random_active_frontend(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<DatabaseWithPeer, Error> {
        let Self::Distributed {
            meta_client: _,
            chnl_mgr,
            auth,
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
            let now_in_ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            // shuffle the frontends to avoid always pick the same one
            frontends.shuffle(&mut rng());

            // found node with maximum last_activity_ts
            for (_, node_info) in frontends
                .iter()
                // filter out frontend that have been down for more than 1 min
                .filter(|(_, node_info)| {
                    node_info.last_activity_ts
                        + batch_opts
                            .experimental_frontend_activity_timeout
                            .as_millis() as i64
                        > now_in_ms
                })
            {
                let addr = &node_info.peer.addr;
                let client = Client::with_manager_and_urls(chnl_mgr.clone(), vec![addr.clone()]);
                let database = {
                    let mut db = Database::new(catalog, schema, client);
                    if let Some(auth) = auth {
                        db.set_auth(auth.auth().clone());
                    }
                    db
                };
                let db = DatabaseWithPeer::new(database, node_info.peer.clone());
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

    pub async fn query_with_terminal_metrics(
        &self,
        catalog: &str,
        schema: &str,
        request: QueryRequest,
        extensions: &[(&str, &str)],
    ) -> Result<OutputWithMetrics, Error> {
        let flow_extensions = build_flow_extensions(extensions)?;
        match self {
            FrontendClient::Distributed {
                query, batch_opts, ..
            } => {
                let query_parallelism = query.parallelism.to_string();
                let mut hints = vec![
                    (QUERY_PARALLELISM_HINT, query_parallelism.as_str()),
                    (READ_PREFERENCE_HINT, batch_opts.read_preference.as_ref()),
                ];
                hints.extend_from_slice(extensions);
                let db = self.get_random_active_frontend(catalog, schema).await?;
                db.database
                    .query_with_terminal_metrics(request, &hints)
                    .await
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)
            }
            FrontendClient::Standalone {
                database_client,
                query,
            } => {
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
                    .do_query(Request::Query(request), ctx)
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
    FlowQueryExtensions::from_extensions(&flow_extensions)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)
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
#[derive(Debug, Default)]
pub(crate) enum PeerDesc {
    /// Distributed mode's frontend peer address
    Dist {
        /// frontend peer address
        peer: Peer,
    },
    /// Standalone mode
    #[default]
    Standalone,
}

impl std::fmt::Display for PeerDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PeerDesc::Dist { peer } => write!(f, "{}", peer.addr),
            PeerDesc::Standalone => write!(f, "standalone"),
        }
    }
}

fn parse_stale_cursor_detail(message: &str) -> Option<FlowStaleCursorDetail> {
    if !message.contains(STALE_CURSOR_TOKEN) || !message.contains(STALE_CURSOR_RETRY_HINT) {
        return None;
    }

    Some(FlowStaleCursorDetail {
        region_id: extract_segment(message, "region: ", ", given_seq:"),
        given_seq: extract_u64_segment(message, "given_seq: ", ", min_readable_seq:"),
        min_readable_seq: extract_u64_segment(message, "min_readable_seq: ", ", retry_hint:"),
    })
}

fn extract_segment(message: &str, start: &str, end: &str) -> Option<String> {
    let start_idx = message.find(start)? + start.len();
    let tail = &message[start_idx..];
    let end_idx = tail.find(end)?;
    Some(tail[..end_idx].trim().to_string())
}

fn extract_u64_segment(message: &str, start: &str, end: &str) -> Option<u64> {
    extract_segment(message, start, end)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use common_error::ext::PlainError;
    use common_error::status_code::StatusCode;
    use common_query::{Output, OutputData};
    use common_recordbatch::adapter::RecordBatchMetrics;
    use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures::StreamExt;
    use snafu::GenerateImplicitData;
    use tokio::time::timeout;

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
                    region_latest_sequences: Some(vec![(42, 99)]),
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
            None,
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

    #[test]
    fn test_inspect_query_error_detects_stale_cursor() {
        let err = Error::External {
            source: BoxedError::new(PlainError::new(
                "STALE_CURSOR: incremental query stale, region: 4398046511104(1024, 0), given_seq: 9, min_readable_seq: 18, retry_hint: FALLBACK_FULL_RECOMPUTE".to_string(),
                StatusCode::EngineExecuteQuery,
            )),
            location: snafu::Location::generate(),
        };

        let failure = FrontendClient::inspect_query_error(&err);
        assert!(failure.is_stale_cursor());
        assert_eq!(
            failure.stale_cursor,
            Some(FlowStaleCursorDetail {
                region_id: Some("4398046511104(1024, 0)".to_string()),
                given_seq: Some(9),
                min_readable_seq: Some(18),
            })
        );
    }

    #[test]
    fn test_inspect_query_error_ignores_non_stale_error() {
        let err = Error::External {
            source: BoxedError::new(PlainError::new(
                "ordinary query failure".to_string(),
                StatusCode::EngineExecuteQuery,
            )),
            location: snafu::Location::generate(),
        };

        let failure = FrontendClient::inspect_query_error(&err);
        assert!(!failure.is_stale_cursor());
        assert_eq!(failure.stale_cursor, None);
    }

    #[tokio::test]
    async fn test_query_with_terminal_metrics_tracks_watermark_in_standalone_mode() {
        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(MetricsHandler);
        let client =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());

        let result = client
            .query_with_terminal_metrics(
                "greptime",
                "public",
                QueryRequest {
                    query: Some(Query::Sql("select 1".to_string())),
                },
                &[],
            )
            .await
            .unwrap();

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

        let result = client
            .query_with_terminal_metrics(
                "greptime",
                "public",
                QueryRequest {
                    query: Some(Query::Sql("insert into t select 1".to_string())),
                },
                &[("flow.return_region_seq", "true")],
            )
            .await
            .unwrap();

        assert!(result.metrics.is_ready());
        assert!(result.region_watermark_map().is_none());
    }

    #[tokio::test]
    async fn test_query_with_terminal_metrics_rejects_invalid_flow_extensions() {
        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(NoopHandler);
        let client =
            FrontendClient::from_grpc_handler(Arc::downgrade(&handler), QueryOptions::default());

        let err = client
            .query_with_terminal_metrics(
                "greptime",
                "public",
                QueryRequest {
                    query: Some(Query::Sql("select 1".to_string())),
                },
                &[("flow.return_region_seq", "not-a-bool")],
            )
            .await
            .unwrap_err();

        assert!(format!("{err:?}").contains("Invalid value for flow.return_region_seq"));
    }
}
