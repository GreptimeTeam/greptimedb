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

use api::v1::meta::{
    ddl_task_server, Partition, Region, RegionRoute, SubmitDdlTaskRequest, SubmitDdlTaskResponse,
    Table, TableRoute,
};
use api::v1::TableId;
use common_meta::rpc::ddl::{CreateTableTask, DdlTask, DropTableTask};
use common_meta::rpc::router;
use common_meta::table_name::TableName;
use common_telemetry::{info, warn};
use snafu::{OptionExt, ResultExt};
use table::metadata::RawTableInfo;
use tonic::{Request, Response};

use super::store::kv::KvStoreRef;
use super::GrpcResult;
use crate::ddl::DdlManagerRef;
use crate::error::{self, Result};
use crate::metasrv::{MetaSrv, SelectorContext, SelectorRef};
use crate::sequence::SequenceRef;
use crate::service::router::fetch_table;

#[async_trait::async_trait]
impl ddl_task_server::DdlTask for MetaSrv {
    async fn submit_ddl_task(
        &self,
        request: Request<SubmitDdlTaskRequest>,
    ) -> GrpcResult<SubmitDdlTaskResponse> {
        let SubmitDdlTaskRequest { header, task, .. } = request.into_inner();

        let header = header.context(error::MissingRequestHeaderSnafu)?;
        let task: DdlTask = task
            .context(error::MissingRequiredParameterSnafu { param: "task" })?
            .try_into()
            .context(error::ConvertProtoDataSnafu)?;

        let ctx = SelectorContext {
            datanode_lease_secs: self.options().datanode_lease_secs,
            server_addr: self.options().server_addr.clone(),
            kv_store: self.kv_store(),
            meta_peer_client: self.meta_peer_client(),
            catalog: None,
            schema: None,
            table: None,
        };

        let resp = match task {
            DdlTask::CreateTable(create_table_task) => {
                handle_create_table_task(
                    header.cluster_id,
                    create_table_task,
                    ctx,
                    self.selector().clone(),
                    self.table_id_sequence().clone(),
                    self.ddl_manager().clone(),
                )
                .await?
            }
            DdlTask::DropTable(drop_table_task) => {
                handle_drop_table_task(
                    header.cluster_id,
                    drop_table_task,
                    self.kv_store().clone(),
                    self.ddl_manager().clone(),
                )
                .await?
            }
        };

        Ok(Response::new(resp))
    }
}

async fn handle_create_table_task(
    cluster_id: u64,
    mut create_table_task: CreateTableTask,
    ctx: SelectorContext,
    selector: SelectorRef,
    table_id_sequence: SequenceRef,
    ddl_manager: DdlManagerRef,
) -> Result<SubmitDdlTaskResponse> {
    let table_name = create_table_task.table_name();

    let ctx = SelectorContext {
        datanode_lease_secs: ctx.datanode_lease_secs,
        server_addr: ctx.server_addr,
        kv_store: ctx.kv_store,
        meta_peer_client: ctx.meta_peer_client,
        catalog: Some(table_name.catalog_name.clone()),
        schema: Some(table_name.schema_name.clone()),
        table: Some(table_name.table_name.clone()),
    };

    let partitions = create_table_task
        .partitions
        .clone()
        .into_iter()
        .map(Into::into)
        .collect();

    let table_route = handle_create_table_route(
        cluster_id,
        table_name,
        partitions,
        &mut create_table_task.table_info,
        ctx,
        selector,
        table_id_sequence,
    )
    .await?;
    let table_id = table_route.table.id;

    let id = ddl_manager
        .submit_create_table_task(cluster_id, create_table_task, table_route)
        .await?;

    info!("Table: {table_id} created via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        table_id: Some(TableId {
            id: table_id as u32,
        }),
        ..Default::default()
    })
}

/// pre-calculates create table task's metadata.
async fn handle_create_table_route(
    cluster_id: u64,
    table_name: TableName,
    partitions: Vec<Partition>,
    table_info: &mut RawTableInfo,
    ctx: SelectorContext,
    selector: SelectorRef,
    table_id_sequence: SequenceRef,
) -> Result<router::TableRoute> {
    let mut peers = selector.select(cluster_id, &ctx).await?;

    if peers.len() < partitions.len() {
        warn!("Create table failed due to no enough available datanodes, table: {table_name:?}, partition number: {}, datanode number: {}", partitions.len(), peers.len());
        return error::NoEnoughAvailableDatanodeSnafu {
            expected: partitions.len(),
            available: peers.len(),
        }
        .fail();
    }

    // We don't need to keep all peers, just truncate it to the number of partitions.
    // If the peers are not enough, some peers will be used for multiple partitions.
    peers.truncate(partitions.len());

    let id = table_id_sequence.next().await?;
    table_info.ident.table_id = id as u32;

    let table = Table {
        id,
        table_name: Some(table_name.into()),
        ..Default::default()
    };

    let region_routes = partitions
        .into_iter()
        .enumerate()
        .map(|(i, partition)| {
            let region = Region {
                id: i as u64,
                partition: Some(partition),
                ..Default::default()
            };
            RegionRoute {
                region: Some(region),
                leader_peer_index: (i % peers.len()) as u64,
                follower_peer_indexes: vec![], // follower_peers is not supported at the moment
            }
        })
        .collect::<Vec<_>>();

    let table_route = TableRoute {
        table: Some(table),
        region_routes,
    };

    router::TableRoute::try_from_raw(&peers, table_route).context(error::TableRouteConversionSnafu)
}

async fn handle_drop_table_task(
    cluster_id: u64,
    drop_table_task: DropTableTask,
    kv_store: KvStoreRef,
    ddl_manager: DdlManagerRef,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = drop_table_task.table_id;

    let (table_global_value, table_route_value) =
        fetch_table(&kv_store, &drop_table_task.table_ref())
            .await?
            .with_context(|| error::TableNotFoundSnafu {
                name: drop_table_task.table_ref().to_string(),
            })?;

    let table_route = router::TableRoute::try_from_raw(
        &table_route_value.peers,
        table_route_value
            .table_route
            .context(error::UnexpectedSnafu {
                violated: "expected table_route",
            })?,
    )
    .context(error::TableRouteConversionSnafu)?;

    let table_info = table_global_value.table_info;

    let id = ddl_manager
        .submit_drop_table_task(cluster_id, drop_table_task, table_route, table_info)
        .await?;

    info!("Table: {table_id} created via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}
