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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::region::{QueryRequest, RegionRequest, RegionResponse};
use async_trait::async_trait;
use client::region::check_response_header;
use common_catalog::consts::METRIC_ENGINE;
use common_error::ext::BoxedError;
use common_meta::datanode_manager::{AffectedRows, Datanode, DatanodeManager, DatanodeRef};
use common_meta::ddl::{TableMetadata, TableMetadataAllocator, TableMetadataAllocatorContext};
use common_meta::error::{self as meta_error, Result as MetaResult, UnsupportedSnafu};
use common_meta::key::table_route::{
    LogicalTableRouteValue, PhysicalTableRouteValue, TableRouteValue,
};
use common_meta::peer::Peer;
use common_meta::rpc::ddl::CreateTableTask;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::SequenceRef;
use common_meta::wal::options_allocator::allocate_region_wal_options;
use common_meta::wal::WalOptionsAllocatorRef;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, info, tracing};
use datanode::region_server::RegionServer;
use servers::grpc::region_server::RegionServerHandler;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{RegionId, RegionNumber, TableId};

use crate::error::{InvalidRegionRequestSnafu, InvokeRegionServerSnafu, Result};

pub struct StandaloneDatanodeManager(pub RegionServer);

#[async_trait]
impl DatanodeManager for StandaloneDatanodeManager {
    async fn datanode(&self, _datanode: &Peer) -> DatanodeRef {
        RegionInvoker::arc(self.0.clone())
    }
}

/// Relative to [client::region::RegionRequester]
struct RegionInvoker {
    region_server: RegionServer,
}

impl RegionInvoker {
    pub fn arc(region_server: RegionServer) -> Arc<Self> {
        Arc::new(Self { region_server })
    }

    async fn handle_inner(&self, request: RegionRequest) -> Result<RegionResponse> {
        let body = request.body.with_context(|| InvalidRegionRequestSnafu {
            reason: "body not found",
        })?;

        self.region_server
            .handle(body)
            .await
            .context(InvokeRegionServerSnafu)
    }
}

#[async_trait]
impl Datanode for RegionInvoker {
    async fn handle(&self, request: RegionRequest) -> MetaResult<AffectedRows> {
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("RegionInvoker::handle_region_request"));
        let response = self
            .handle_inner(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        check_response_header(response.header)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?;
        Ok(response.affected_rows)
    }

    async fn handle_query(&self, request: QueryRequest) -> MetaResult<SendableRecordBatchStream> {
        let span = request
            .header
            .as_ref()
            .map(|h| TracingContext::from_w3c(&h.tracing_context))
            .unwrap_or_default()
            .attach(tracing::info_span!("RegionInvoker::handle_query"));
        self.region_server
            .handle_read(request)
            .trace(span)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}

pub struct StandaloneTableMetadataAllocator {
    table_id_sequence: SequenceRef,
    wal_options_allocator: WalOptionsAllocatorRef,
}

impl StandaloneTableMetadataAllocator {
    pub fn new(
        table_id_sequence: SequenceRef,
        wal_options_allocator: WalOptionsAllocatorRef,
    ) -> Self {
        Self {
            table_id_sequence,
            wal_options_allocator,
        }
    }

    async fn allocate_table_id(&self, task: &CreateTableTask) -> MetaResult<TableId> {
        let table_id = if let Some(table_id) = &task.create_table.table_id {
            let table_id = table_id.id;

            ensure!(
                !self
                    .table_id_sequence
                    .min_max()
                    .await
                    .contains(&(table_id as u64)),
                UnsupportedSnafu {
                    operation: format!(
                        "create table by id {} that is reserved in this node",
                        table_id
                    )
                }
            );

            info!(
                "Received explicitly allocated table id {}, will use it directly.",
                table_id
            );

            table_id
        } else {
            self.table_id_sequence.next().await? as TableId
        };
        Ok(table_id)
    }

    fn create_wal_options(
        &self,
        table_route: &TableRouteValue,
    ) -> MetaResult<HashMap<RegionNumber, String>> {
        match table_route {
            TableRouteValue::Physical(x) => {
                let region_numbers = x
                    .region_routes
                    .iter()
                    .map(|route| route.region.id.region_number())
                    .collect();
                allocate_region_wal_options(region_numbers, &self.wal_options_allocator)
            }
            TableRouteValue::Logical(_) => Ok(HashMap::new()),
        }
    }
}

fn create_table_route(table_id: TableId, task: &CreateTableTask) -> TableRouteValue {
    if task.create_table.engine == METRIC_ENGINE {
        TableRouteValue::Logical(LogicalTableRouteValue {})
    } else {
        let region_routes = task
            .partitions
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
                    leader_status: None,
                }
            })
            .collect::<Vec<_>>();
        TableRouteValue::Physical(PhysicalTableRouteValue::new(region_routes))
    }
}

#[async_trait]
impl TableMetadataAllocator for StandaloneTableMetadataAllocator {
    async fn create(
        &self,
        _ctx: &TableMetadataAllocatorContext,
        task: &CreateTableTask,
    ) -> MetaResult<TableMetadata> {
        let table_id = self.allocate_table_id(task).await?;

        let table_route = create_table_route(table_id, task);

        let region_wal_options = self.create_wal_options(&table_route)?;

        debug!(
            "Allocated region wal options {:?} for table {}",
            region_wal_options, table_id
        );

        Ok(TableMetadata {
            table_id,
            table_route,
            region_wal_options,
        })
    }
}
