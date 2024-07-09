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

//! Frontend Client for flownode, used for writing result back to database

use std::fmt::Debug;
use std::time::Duration;

use api::v1::greptime_database_client::GreptimeDatabaseClient;
use api::v1::greptime_request::Request;
use api::v1::{
    GreptimeRequest, GreptimeResponse, RequestHeader, RowDeleteRequests, RowInsertRequests,
};
use client::frontend::FrontendRequester;
use client::Client;
use common_error::ext::BoxedError;
use common_frontend::handler::FrontendInvoker;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cluster::{ClusterInfo, Role};
use common_meta::peer::Peer;
use common_query::Output;
use common_telemetry::tracing_context::{TracingContext, W3cTrace};
use futures::FutureExt;
use itertools::Itertools;
use meta_client::MetaClientRef;
use moka::future::{Cache, CacheBuilder};
use session::context::{QueryContext, QueryContextRef};
use snafu::{ensure, IntoError, ResultExt};
use tokio::sync::Mutex;

use crate::error::{GetFrontendRequesterSnafu, ListFrontendNodesSnafu};
use crate::{Error, Result};

#[derive(Clone)]
struct FrontendPeers {
    meta_client: MetaClientRef,
}

impl FrontendPeers {
    pub fn new(meta_client: MetaClientRef) -> Self {
        Self { meta_client }
    }

    pub async fn get_peers(&self) -> Result<Vec<Peer>> {
        let mut node_infos = self
            .meta_client
            .list_nodes(Some(Role::Frontend))
            .await
            .context(ListFrontendNodesSnafu)?;
        node_infos.sort_by(|a, b| b.last_activity_ts.cmp(&a.last_activity_ts));
        if let Some(last_activity_ts) = node_infos.first().map(|node| node.last_activity_ts) {
            // Filter out nodes that have been inactive for a long time, as they are likely dead.
            node_infos.retain(|node| {
                node.last_activity_ts
                    >= last_activity_ts - Duration::from_secs(5 * 60).as_millis() as i64
            })
        }

        ensure!(
            !node_infos.is_empty(),
            GetFrontendRequesterSnafu {
                reason: "empty frontend nodes"
            }
        );

        Ok(node_infos
            .into_iter()
            .map(|node_info| node_info.peer)
            .collect())
    }
}

pub struct FrontendClient {
    channel_manager: ChannelManager,
    frontend_peers: FrontendPeers,
    requesters: Cache<String, FrontendRequester>,
}

impl FrontendClient {
    pub fn new(config: ChannelConfig, meta_client: MetaClientRef) -> Self {
        Self {
            channel_manager: ChannelManager::with_config(config),
            frontend_peers: FrontendPeers::new(meta_client),
            requesters: CacheBuilder::new(1)
                .time_to_live(Duration::from_secs(5 * 60))
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        }
    }

    pub async fn get_requester(&self) -> Result<FrontendRequester> {
        let init = async move {
            let addrs = self
                .frontend_peers
                .get_peers()
                .await?
                .into_iter()
                .map(|peer| peer.addr)
                .collect::<Vec<_>>();
            let client = Client::with_manager_and_urls(self.channel_manager.clone(), addrs);
            Ok(FrontendRequester::new(client))
        };

        self.requesters
            .try_get_with_by_ref::<_, Error, _>("single_requester", init)
            .await
            .map_err(|e| Error::GetFrontendRequester {
                reason: e.to_string(),
            })
    }
}

fn to_rpc_request(request: Request, ctx: &QueryContext) -> GreptimeRequest {
    let header = RequestHeader {
        catalog: ctx.current_catalog().to_string(),
        schema: ctx.current_schema().to_string(),
        authorization: None,
        // dbname is empty so that header use catalog+schema to determine the database
        // see `create_query_context` in `greptime_handler.rs`
        dbname: "".to_string(),
        timezone: ctx.timezone().to_string(),
        tracing_context: TracingContext::from_current_span().to_w3c(),
    };
    GreptimeRequest {
        header: Some(header),
        request: Some(request),
    }
}

fn from_rpc_error(e: client::error::Error) -> common_frontend::error::Error {
    common_frontend::error::ExternalSnafu {}.into_error(BoxedError::new(e))
}

fn from_requester_error(e: Error) -> common_frontend::error::Error {
    common_frontend::error::ExternalSnafu {}.into_error(BoxedError::new(e))
}

#[async_trait::async_trait]
impl FrontendInvoker for FrontendClient {
    async fn row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> common_frontend::error::Result<Output> {
        let requester = self.get_requester().await.map_err(from_requester_error)?;
        let req = to_rpc_request(Request::RowInserts(requests), &ctx);
        let affect_rows = requester.handle(req).await.map_err(from_rpc_error)?;

        Ok(Output::new_with_affected_rows(affect_rows as usize))
    }

    async fn row_deletes(
        &self,
        requests: RowDeleteRequests,
        ctx: QueryContextRef,
    ) -> common_frontend::error::Result<Output> {
        let requester = self.get_requester().await.map_err(from_requester_error)?;
        let req = to_rpc_request(Request::RowDeletes(requests), &ctx);
        let affect_rows = requester.handle(req).await.map_err(from_rpc_error)?;

        Ok(Output::new_with_affected_rows(affect_rows as usize))
    }
}
