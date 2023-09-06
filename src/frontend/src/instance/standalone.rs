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

use std::sync::Arc;

use api::v1::meta::Partition;
use api::v1::region::{region_request, QueryRequest, RegionRequest};
use async_trait::async_trait;
use client::error::{HandleRequestSnafu, Result as ClientResult};
use client::region::check_response_header;
use client::region_handler::RegionRequestHandler;
use common_error::ext::BoxedError;
use common_meta::datanode_manager::{AffectedRows, Datanode, DatanodeManager, DatanodeRef};
use common_meta::ddl::{TableCreator, TableCreatorContext};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::{Sequence, SequenceRef};
use common_recordbatch::SendableRecordBatchStream;
use datanode::region_server::RegionServer;
use servers::grpc::region_server::RegionServerHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use table::metadata::RawTableInfo;

use crate::error::InvokeRegionServerSnafu;

const TABLE_ID_SEQ: &str = "table_id";

pub(crate) struct StandaloneRegionRequestHandler {
    region_server: RegionServer,
}

impl StandaloneRegionRequestHandler {
    pub fn arc(region_server: RegionServer) -> Arc<Self> {
        Arc::new(Self { region_server })
    }
}

#[async_trait]
impl RegionRequestHandler for StandaloneRegionRequestHandler {
    async fn handle(
        &self,
        request: region_request::Body,
        _ctx: QueryContextRef,
    ) -> ClientResult<AffectedRows> {
        let response = self
            .region_server
            .handle(request)
            .await
            .context(InvokeRegionServerSnafu)
            .map_err(BoxedError::new)
            .context(HandleRequestSnafu)?;

        check_response_header(response.header)?;
        Ok(response.affected_rows)
    }

    async fn do_get(&self, request: QueryRequest) -> ClientResult<SendableRecordBatchStream> {
        self.region_server
            .handle_read(request)
            .await
            .map_err(BoxedError::new)
            .context(HandleRequestSnafu)
    }
}

pub(crate) struct StandaloneDatanode(pub(crate) RegionServer);

#[async_trait]
impl Datanode for StandaloneDatanode {
    async fn handle(&self, request: RegionRequest) -> MetaResult<AffectedRows> {
        let body = request.body.context(meta_error::UnexpectedSnafu {
            err_msg: "body not found",
        })?;
        let resp = self
            .0
            .handle(body)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;

        Ok(resp.affected_rows)
    }
}

pub(crate) struct StandaloneDatanodeManager(pub(crate) RegionServer);

#[async_trait]
impl DatanodeManager for StandaloneDatanodeManager {
    async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
        Arc::new(StandaloneDatanode(self.0.clone()))
    }
}

pub(crate) struct StandaloneTableCreator {
    table_id_sequence: SequenceRef,
}

impl StandaloneTableCreator {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            table_id_sequence: Arc::new(Sequence::new(TABLE_ID_SEQ, 1024, 10, kv_backend)),
        }
    }
}

#[async_trait]
impl TableCreator for StandaloneTableCreator {
    async fn create(
        &self,
        _ctx: &TableCreatorContext,
        _raw_table_info: &mut RawTableInfo,
        partitions: &[Partition],
    ) -> MetaResult<(TableId, Vec<RegionRoute>)> {
        let table_id = self.table_id_sequence.next().await? as u32;
        let region_routes = partitions
            .iter()
            .enumerate()
            .map(|(i, partition)| {
                let region = Region {
                    id: RegionId::new(table_id, i as u32),
                    partition: Some(partition.clone().into()),
                    ..Default::default()
                };
                // It's only a placeholder.
                let peer = Peer::default();
                RegionRoute {
                    region,
                    leader_peer: Some(peer),
                    follower_peers: vec![],
                }
            })
            .collect::<Vec<_>>();

        Ok((table_id, region_routes))
    }
}
