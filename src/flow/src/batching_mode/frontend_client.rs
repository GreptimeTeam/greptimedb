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

use std::sync::{Arc, Weak};

use api::v1::greptime_request::Request;
use api::v1::CreateTableExpr;
use client::{Client, Database};
use common_error::ext::{BoxedError, ErrorExt};
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cluster::{NodeInfo, NodeInfoKey, Role};
use common_meta::peer::Peer;
use common_meta::rpc::store::RangeRequest;
use common_query::Output;
use meta_client::client::MetaClient;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};

use crate::batching_mode::DEFAULT_BATCHING_ENGINE_QUERY_TIMEOUT;
use crate::error::{ExternalSnafu, InvalidRequestSnafu, UnexpectedSnafu};
use crate::Error;

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

/// A simple frontend client able to execute sql using grpc protocol
///
/// This is for computation-heavy query which need to offload computation to frontend, lifting the load from flownode
#[derive(Debug, Clone)]
pub enum FrontendClient {
    Distributed {
        meta_client: Arc<MetaClient>,
        chnl_mgr: ChannelManager,
    },
    Standalone {
        /// for the sake of simplicity still use grpc even in standalone mode
        /// notice the client here should all be lazy, so that can wait after frontend is booted then make conn
        /// TODO(discord9): not use grpc under standalone mode
        database_client: HandlerMutable,
    },
}

impl FrontendClient {
    pub fn from_empty_grpc_handler() -> (Self, HandlerMutable) {
        let handler = Arc::new(std::sync::Mutex::new(None));
        (
            Self::Standalone {
                database_client: handler.clone(),
            },
            handler,
        )
    }

    pub fn from_meta_client(meta_client: Arc<MetaClient>) -> Self {
        Self::Distributed {
            meta_client,
            chnl_mgr: {
                let cfg = ChannelConfig::new().timeout(DEFAULT_BATCHING_ENGINE_QUERY_TIMEOUT);
                ChannelManager::with_config(cfg)
            },
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
}

impl FrontendClient {
    async fn scan_for_frontend(&self) -> Result<Vec<(NodeInfoKey, NodeInfo)>, Error> {
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

    /// Get the database with max `last_activity_ts`
    async fn get_last_active_frontend(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<DatabaseWithPeer, Error> {
        let Self::Distributed {
            meta_client: _,
            chnl_mgr,
        } = self
        else {
            return UnexpectedSnafu {
                reason: "Expect distributed mode",
            }
            .fail();
        };

        let frontends = self.scan_for_frontend().await?;
        let mut peer = None;

        if let Some((_, val)) = frontends.iter().max_by_key(|(_, val)| val.last_activity_ts) {
            peer = Some(val.peer.clone());
        }

        let Some(peer) = peer else {
            UnexpectedSnafu {
                reason: format!("No frontend available: {:?}", frontends),
            }
            .fail()?
        };
        let client = Client::with_manager_and_urls(chnl_mgr.clone(), vec![peer.addr.clone()]);
        let database = Database::new(catalog, schema, client);
        Ok(DatabaseWithPeer::new(database, peer))
    }

    pub async fn create(
        &self,
        create: CreateTableExpr,
        catalog: &str,
        schema: &str,
    ) -> Result<u32, Error> {
        self.handle(
            Request::Ddl(api::v1::DdlRequest {
                expr: Some(api::v1::ddl_request::Expr::CreateTable(create)),
            }),
            catalog,
            schema,
            &mut None,
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
    ) -> Result<u32, Error> {
        match self {
            FrontendClient::Distributed { .. } => {
                let db = self.get_last_active_frontend(catalog, schema).await?;

                *peer_desc = Some(PeerDesc::Dist {
                    peer: db.peer.clone(),
                });

                db.database
                    .handle(req.clone())
                    .await
                    .with_context(|_| InvalidRequestSnafu {
                        context: format!("Failed to handle request: {:?}", req),
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
