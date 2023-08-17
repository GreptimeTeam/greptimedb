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
    ddl_task_server, Partition, SubmitDdlTaskRequest, SubmitDdlTaskResponse, TableId,
};
use common_grpc_expr::alter_expr_to_request;
use common_meta::rpc::ddl::{
    AlterTableTask, CreateTableTask, DdlTask, DropTableTask, TruncateTableTask,
};
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::table_name::TableName;
use common_telemetry::{info, warn};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::{RegionId, MAX_REGION_SEQ};
use table::metadata::RawTableInfo;
use tonic::{Request, Response};

use super::GrpcResult;
use crate::ddl::DdlManagerRef;
use crate::error::{self, Result, TableMetadataManagerSnafu, TooManyPartitionsSnafu};
use crate::metasrv::{MetaSrv, SelectorContext, SelectorRef};
use crate::sequence::SequenceRef;

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
            kv_store: self.kv_store().clone(),
            meta_peer_client: self.meta_peer_client().clone(),
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
                    self.ddl_manager().clone(),
                )
                .await?
            }
            DdlTask::AlterTable(alter_table_task) => {
                handle_alter_table_task(
                    header.cluster_id,
                    alter_table_task,
                    self.ddl_manager().clone(),
                )
                .await?
            }
            DdlTask::TruncateTable(truncate_table_task) => {
                handle_truncate_table_task(
                    header.cluster_id,
                    truncate_table_task,
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

    let region_routes = handle_create_region_routes(
        cluster_id,
        table_name,
        partitions,
        &mut create_table_task.table_info,
        ctx,
        selector,
        table_id_sequence,
    )
    .await?;

    let table_id = create_table_task.table_info.ident.table_id;

    // TODO(weny): refactor the table route.
    let id = ddl_manager
        .submit_create_table_task(cluster_id, create_table_task, region_routes)
        .await?;

    info!("Table: {table_id} is created via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        table_id: Some(TableId { id: table_id }),
        ..Default::default()
    })
}

/// pre-calculates create table task's metadata.
async fn handle_create_region_routes(
    cluster_id: u64,
    table_name: TableName,
    partitions: Vec<Partition>,
    table_info: &mut RawTableInfo,
    ctx: SelectorContext,
    selector: SelectorRef,
    table_id_sequence: SequenceRef,
) -> Result<Vec<RegionRoute>> {
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

    let table_id = table_id_sequence.next().await? as u32;
    table_info.ident.table_id = table_id;

    ensure!(
        partitions.len() <= MAX_REGION_SEQ as usize,
        TooManyPartitionsSnafu
    );

    let region_routes = partitions
        .into_iter()
        .enumerate()
        .map(|(i, partition)| {
            let region = Region {
                id: RegionId::new(table_id, i as u32),
                partition: Some(partition.into()),
                ..Default::default()
            };
            let peer = peers[i % peers.len()].clone();
            RegionRoute {
                region,
                leader_peer: Some(peer.into()),
                follower_peers: vec![], // follower_peers is not supported at the moment
            }
        })
        .collect::<Vec<_>>();

    Ok(region_routes)
}

async fn handle_drop_table_task(
    cluster_id: u64,
    drop_table_task: DropTableTask,
    ddl_manager: DdlManagerRef,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = drop_table_task.table_id;
    let table_metadata_manager = &ddl_manager.table_metadata_manager;
    let table_ref = drop_table_task.table_ref();

    let (table_info_value, table_route_value) = table_metadata_manager
        .get_full_table_info(table_id)
        .await
        .context(error::TableMetadataManagerSnafu)?;

    let table_info_value = table_info_value.with_context(|| error::TableInfoNotFoundSnafu {
        table_name: table_ref.to_string(),
    })?;

    let table_route_value = table_route_value.with_context(|| error::TableRouteNotFoundSnafu {
        table_name: table_ref.to_string(),
    })?;

    let id = ddl_manager
        .submit_drop_table_task(
            cluster_id,
            drop_table_task,
            table_info_value,
            table_route_value,
        )
        .await?;

    info!("Table: {table_id} is dropped via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_alter_table_task(
    cluster_id: u64,
    mut alter_table_task: AlterTableTask,
    ddl_manager: DdlManagerRef,
) -> Result<SubmitDdlTaskResponse> {
    let table_id = alter_table_task
        .alter_table
        .table_id
        .as_ref()
        .context(error::UnexpectedSnafu {
            violated: "expected table id ",
        })?
        .id;

    let mut alter_table_request =
        alter_expr_to_request(table_id, alter_table_task.alter_table.clone())
            .context(error::ConvertGrpcExprSnafu)?;

    let table_ref = alter_table_task.table_ref();

    let table_info_value = ddl_manager
        .table_metadata_manager
        .table_info_manager()
        .get(table_id)
        .await
        .context(TableMetadataManagerSnafu)?
        .with_context(|| error::TableInfoNotFoundSnafu {
            table_name: table_ref.to_string(),
        })?;

    let table_info = &table_info_value.table_info;

    // Sets alter_table's table_version
    alter_table_task.alter_table.table_version = table_info.ident.version;
    alter_table_request.table_version = Some(table_info.ident.version);

    let id = ddl_manager
        .submit_alter_table_task(
            cluster_id,
            alter_table_task,
            alter_table_request,
            table_info_value,
        )
        .await?;

    info!("Table: {table_id} is altered via procedure_id {id:?}");

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}

async fn handle_truncate_table_task(
    cluster_id: u64,
    truncate_table_task: TruncateTableTask,
    ddl_manager: DdlManagerRef,
) -> Result<SubmitDdlTaskResponse> {
    let truncate_table = &truncate_table_task.truncate_table;
    let table_id = truncate_table
        .table_id
        .as_ref()
        .context(error::UnexpectedSnafu {
            violated: "expected table id ",
        })?
        .id;

    let table_ref = truncate_table_task.table_ref();

    let table_route_value = ddl_manager
        .table_metadata_manager
        .table_route_manager()
        .get(table_id)
        .await
        .context(TableMetadataManagerSnafu)?
        .with_context(|| error::TableRouteNotFoundSnafu {
            table_name: table_ref.to_string(),
        })?;

    let region_routes = table_route_value.region_routes;

    let id = ddl_manager
        .submit_truncate_table_task(cluster_id, truncate_table_task, region_routes)
        .await?;

    Ok(SubmitDdlTaskResponse {
        key: id.to_string().into(),
        ..Default::default()
    })
}
