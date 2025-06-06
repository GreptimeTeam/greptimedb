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
use std::time::{Duration, Instant, SystemTime};

use api::v1::greptime_request::Request;
use api::v1::CreateTableExpr;
use client::{Client, Database};
use common_error::ext::{BoxedError, ErrorExt};
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cluster::{NodeInfo, NodeInfoKey, Role};
use common_meta::peer::Peer;
use common_meta::rpc::store::RangeRequest;
use common_query::Output;
use common_telemetry::{debug, warn};
use itertools::Itertools;
use meta_client::client::MetaClient;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};

use crate::batching_mode::task::BatchingTask;
use crate::batching_mode::{
    DEFAULT_BATCHING_ENGINE_QUERY_TIMEOUT, FRONTEND_ACTIVITY_TIMEOUT, GRPC_CONN_TIMEOUT,
    GRPC_MAX_RETRIES,
};
use crate::error::{ExternalSnafu, InvalidRequestSnafu, NoAvailableFrontendSnafu, UnexpectedSnafu};
use crate::metrics::METRIC_FLOW_BATCHING_ENGINE_GUESS_FE_LOAD;
use crate::{Error, FlowAuthHeader, FlowId};

/// Just like [`GrpcQueryHandler`] but use BoxedError
///
/// basically just a specialized `GrpcQueryHandler<Error=BoxedError>`
///
/// this is only useful for flownode to
/// invoke frontend Instance in standalone mode
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
impl<
        E: ErrorExt + Send + Sync + 'static,
        T: GrpcQueryHandler<Error = E> + Send + Sync + 'static,
    > GrpcQueryHandlerWithBoxedError for T
{
    async fn do_query(
        &self,
        query: Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        self.do_query(query, ctx).await.map_err(BoxedError::new)
    }
}

type HandlerMutable = Arc<std::sync::Mutex<Option<Weak<dyn GrpcQueryHandlerWithBoxedError>>>>;

/// Statistics about running query on this frontend from flownode
#[derive(Debug, Default, Clone)]
struct FrontendStat {
    /// The query for flow id has been running since this timestamp
    since: HashMap<FlowId, Instant>,
    /// The average query time for each flow id
    /// This is used to calculate the average query time for each flow id
    past_query_avg: HashMap<FlowId, (usize, Duration)>,
}

#[derive(Debug, Default, Clone)]
pub struct FrontendStats {
    /// The statistics for each flow id
    stats: Arc<Mutex<HashMap<String, FrontendStat>>>,
}

impl FrontendStats {
    pub fn observe(&self, frontend_addr: &str, flow_id: FlowId) -> FrontendStatsGuard {
        let mut stats = self.stats.lock().expect("Failed to lock frontend stats");
        let stat = stats.entry(frontend_addr.to_string()).or_default();
        stat.since.insert(flow_id, Instant::now());

        FrontendStatsGuard {
            stats: self.stats.clone(),
            frontend_addr: frontend_addr.to_string(),
            cur: flow_id,
        }
    }

    /// return frontend addrs sorted by load, from lightest to heaviest
    /// The load is calculated as the total average query time for each flow id plus running query's total running time elapsed
    pub fn sort_by_load(&self) -> Vec<String> {
        let stats = self.stats.lock().expect("Failed to lock frontend stats");
        let fe_load_factor = stats
            .iter()
            .map(|(node_addr, stat)| {
                // total expected avg running time for all currently running queries
                let total_expect_avg_run_time = stat
                    .since
                    .keys()
                    .map(|f| {
                        let (count, total_duration) =
                            stat.past_query_avg.get(f).unwrap_or(&(0, Duration::ZERO));
                        if *count == 0 {
                            0.0
                        } else {
                            total_duration.as_secs_f64() / *count as f64
                        }
                    })
                    .sum::<f64>();
                let total_cur_running_time = stat
                    .since
                    .values()
                    .map(|since| since.elapsed().as_secs_f64())
                    .sum::<f64>();
                (
                    node_addr.to_string(),
                    total_expect_avg_run_time + total_cur_running_time,
                )
            })
            .sorted_by(|(_, load_a), (_, load_b)| {
                load_a
                    .partial_cmp(load_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .collect::<Vec<_>>();
        debug!("Frontend load factor: {:?}", fe_load_factor);
        for (node_addr, load) in &fe_load_factor {
            METRIC_FLOW_BATCHING_ENGINE_GUESS_FE_LOAD
                .with_label_values(&[&node_addr.to_string()])
                .observe(*load);
        }
        fe_load_factor
            .into_iter()
            .map(|(addr, _)| addr)
            .collect::<Vec<_>>()
    }
}

pub struct FrontendStatsGuard {
    stats: Arc<Mutex<HashMap<String, FrontendStat>>>,
    frontend_addr: String,
    cur: FlowId,
}

impl Drop for FrontendStatsGuard {
    fn drop(&mut self) {
        let mut stats = self.stats.lock().expect("Failed to lock frontend stats");
        if let Some(stat) = stats.get_mut(&self.frontend_addr) {
            if let Some(since) = stat.since.remove(&self.cur) {
                let elapsed = since.elapsed();
                let (count, total_duration) = stat.past_query_avg.entry(self.cur).or_default();
                *count += 1;
                *total_duration += elapsed;
            }
        }
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
        fe_stats: FrontendStats,
    },
    Standalone {
        /// for the sake of simplicity still use grpc even in standalone mode
        /// notice the client here should all be lazy, so that can wait after frontend is booted then make conn
        database_client: HandlerMutable,
    },
}

impl FrontendClient {
    /// Create a new empty frontend client, with a `HandlerMutable` to set the grpc handler later
    pub fn from_empty_grpc_handler() -> (Self, HandlerMutable) {
        let handler = Arc::new(std::sync::Mutex::new(None));
        (
            Self::Standalone {
                database_client: handler.clone(),
            },
            handler,
        )
    }

    pub fn from_meta_client(meta_client: Arc<MetaClient>, auth: Option<FlowAuthHeader>) -> Self {
        common_telemetry::info!("Frontend client build with auth={:?}", auth);
        Self::Distributed {
            meta_client,
            chnl_mgr: {
                let cfg = ChannelConfig::new()
                    .connect_timeout(GRPC_CONN_TIMEOUT)
                    .timeout(DEFAULT_BATCHING_ENGINE_QUERY_TIMEOUT);
                ChannelManager::with_config(cfg)
            },
            auth,
            fe_stats: Default::default(),
        }
    }

    pub fn from_grpc_handler(grpc_handler: Weak<dyn GrpcQueryHandlerWithBoxedError>) -> Self {
        Self::Standalone {
            database_client: Arc::new(std::sync::Mutex::new(Some(grpc_handler))),
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
            fe_stats,
        } = self
        else {
            return UnexpectedSnafu {
                reason: "Expect distributed mode",
            }
            .fail();
        };

        let mut interval = tokio::time::interval(GRPC_CONN_TIMEOUT);
        interval.tick().await;
        for retry in 0..GRPC_MAX_RETRIES {
            let mut frontends = self.scan_for_frontend().await?;
            let now_in_ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            let node_addrs_by_load = fe_stats.sort_by_load();
            // index+1 to load order asc, so that the lightest node has load 1 and non-existent node has load 0
            let addr2load = node_addrs_by_load
                .iter()
                .enumerate()
                .map(|(i, id)| (id.clone(), i + 1))
                .collect::<HashMap<_, _>>();
            // sort frontends by load, from lightest to heaviest
            frontends.sort_by(|(_, a), (_, b)| {
                // if not even in stats, treat as 0 load since never been queried
                let load_a = addr2load.get(&a.peer.addr).unwrap_or(&0);
                let load_b = addr2load.get(&b.peer.addr).unwrap_or(&0);
                load_a.cmp(load_b)
            });
            debug!("Frontend nodes sorted by load: {:?}", frontends);

            // found node with maximum last_activity_ts
            for (_, node_info) in frontends
                .iter()
                // filter out frontend that have been down for more than 1 min
                .filter(|(_, node_info)| {
                    node_info.last_activity_ts + FRONTEND_ACTIVITY_TIMEOUT.as_millis() as i64
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
            timeout: GRPC_CONN_TIMEOUT,
            context: "No available frontend found that is able to process query",
        }
        .fail()
    }

    pub async fn create(
        &self,
        create: CreateTableExpr,
        catalog: &str,
        schema: &str,
        task: Option<&BatchingTask>,
    ) -> Result<u32, Error> {
        self.handle(
            Request::Ddl(api::v1::DdlRequest {
                expr: Some(api::v1::ddl_request::Expr::CreateTable(create)),
            }),
            catalog,
            schema,
            &mut None,
            task,
        )
        .await
    }

    /// Handle a request to frontend
    pub(crate) async fn handle(
        &self,
        req: api::v1::greptime_request::Request,
        catalog: &str,
        schema: &str,
        peer_desc: &mut Option<PeerDesc>,
        task: Option<&BatchingTask>,
    ) -> Result<u32, Error> {
        match self {
            FrontendClient::Distributed { fe_stats, .. } => {
                let db = self.get_random_active_frontend(catalog, schema).await?;

                *peer_desc = Some(PeerDesc::Dist {
                    peer: db.peer.clone(),
                });

                let flow_id = task.map(|t| t.config.flow_id).unwrap_or_default();
                let _guard = fe_stats.observe(&db.peer.addr, flow_id);

                db.database
                    .handle_with_retry(req.clone(), GRPC_MAX_RETRIES)
                    .await
                    .with_context(|_| InvalidRequestSnafu {
                        context: format!("Failed to handle request at {:?}: {:?}", db.peer, req),
                    })
            }
            FrontendClient::Standalone { database_client } => {
                let ctx = QueryContextBuilder::default()
                    .current_catalog(catalog.to_string())
                    .current_schema(schema.to_string())
                    .build();
                let ctx = Arc::new(ctx);
                {
                    let database_client = {
                        database_client
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
                        .do_query(req.clone(), ctx)
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
