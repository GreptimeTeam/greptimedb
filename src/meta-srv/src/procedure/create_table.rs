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

use api::v1::region::region_request::Body as PbRegionRequest;
use api::v1::region::{ColumnDef, CreateRequest as PbCreateRegionRequest};
use api::v1::SemanticType;
use async_trait::async_trait;
use client::region::RegionClientBuilder;
use client::Database;
use common_catalog::consts::MITO2_ENGINE;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::key::table_name::TableNameKey;
use common_meta::rpc::ddl::CreateTableTask;
use common_meta::rpc::router::{find_leader_regions, find_leaders, RegionRoute};
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::info;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use strum::AsRefStr;
use table::engine::{region_dir, TableReference};
use table::metadata::{RawTableInfo, TableId};

use super::utils::{handle_request_datanode_error, handle_retry_error};
use crate::ddl::DdlContext;
use crate::error::{
    self, BuildRegionClientSnafu, PrimaryKeyNotFoundSnafu, Result, TableMetadataManagerSnafu,
};
use crate::metrics;

pub struct CreateTableProcedure {
    context: DdlContext,
    creator: TableCreator,
}

impl CreateTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::CreateTable";

    pub(crate) fn new(
        cluster_id: u64,
        task: CreateTableTask,
        region_routes: Vec<RegionRoute>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            creator: TableCreator::new(cluster_id, task, region_routes),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(CreateTableProcedure {
            context,
            creator: TableCreator { data },
        })
    }

    fn table_name(&self) -> TableName {
        self.creator.data.task.table_name()
    }

    pub fn table_info(&self) -> &RawTableInfo {
        &self.creator.data.task.table_info
    }

    fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }

    pub fn region_routes(&self) -> &Vec<RegionRoute> {
        &self.creator.data.region_routes
    }

    /// Checks whether the table exists.
    async fn on_prepare(&mut self) -> Result<Status> {
        let expr = &self.creator.data.task.create_table;
        let exist = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                &expr.catalog_name,
                &expr.schema_name,
                &expr.table_name,
            ))
            .await
            .context(TableMetadataManagerSnafu)?;

        if exist {
            ensure!(
                self.creator.data.task.create_table.create_if_not_exists,
                error::TableAlreadyExistsSnafu {
                    table_name: self.creator.data.table_ref().to_string(),
                }
            );

            return Ok(Status::Done);
        }

        self.creator.data.state = if expr.engine == MITO2_ENGINE {
            CreateTableState::DatanodeCreateRegions
        } else {
            CreateTableState::DatanodeCreateTable
        };

        Ok(Status::executing(true))
    }

    fn create_region_request_template(&self) -> Result<PbCreateRegionRequest> {
        let create_table_expr = &self.creator.data.task.create_table;

        let column_defs = create_table_expr
            .column_defs
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let semantic_type = if create_table_expr.time_index == c.name {
                    SemanticType::Timestamp
                } else if create_table_expr.primary_keys.contains(&c.name) {
                    SemanticType::Tag
                } else {
                    SemanticType::Field
                };

                ColumnDef {
                    name: c.name.clone(),
                    column_id: i as u32,
                    datatype: c.datatype,
                    is_nullable: c.is_nullable,
                    default_constraint: c.default_constraint.clone(),
                    semantic_type: semantic_type as i32,
                }
            })
            .collect::<Vec<_>>();

        let primary_key = create_table_expr
            .primary_keys
            .iter()
            .map(|key| {
                column_defs
                    .iter()
                    .find_map(|c| {
                        if &c.name == key {
                            Some(c.column_id)
                        } else {
                            None
                        }
                    })
                    .context(PrimaryKeyNotFoundSnafu { key })
            })
            .collect::<Result<_>>()?;

        Ok(PbCreateRegionRequest {
            region_id: 0,
            engine: create_table_expr.engine.to_string(),
            column_defs,
            primary_key,
            create_if_not_exists: true,
            region_dir: "".to_string(),
            options: create_table_expr.table_options.clone(),
        })
    }

    async fn on_datanode_create_regions(&mut self) -> Result<Status> {
        let create_table_data = &self.creator.data;
        let region_routes = &create_table_data.region_routes;

        let create_table_expr = &create_table_data.task.create_table;
        let catalog = &create_table_expr.catalog_name;
        let schema = &create_table_expr.schema_name;

        let request_template = self.create_region_request_template()?;

        let leaders = find_leaders(region_routes);
        let mut create_table_tasks = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let client = self.context.datanode_clients.get_client(&datanode).await;

            let mut builder = RegionClientBuilder::default();
            builder.catalog(catalog);
            builder.schema(schema);
            builder.client(client);
            let client = builder.build().with_context(|_| BuildRegionClientSnafu {
                peer: datanode.clone(),
            })?;

            let regions = find_leader_regions(region_routes, &datanode);
            let requests = regions
                .iter()
                .map(|region_number| {
                    let region_id = RegionId::new(self.table_id(), *region_number);

                    let mut create_table_request = request_template.clone();
                    create_table_request.region_id = region_id.as_u64();
                    create_table_request.region_dir = region_dir(catalog, schema, region_id);

                    PbRegionRequest::Create(create_table_request)
                })
                .collect::<Vec<_>>();

            create_table_tasks.push(common_runtime::spawn_bg(async move {
                for request in requests {
                    if let Err(err) = client.handle(request).await {
                        return Err(handle_request_datanode_error(datanode)(err));
                    }
                }
                Ok(())
            }));
        }

        join_all(create_table_tasks)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu).flatten())
            .collect::<Result<Vec<_>>>()?;

        self.creator.data.state = CreateTableState::CreateMetadata;

        Ok(Status::executing(true))
    }

    async fn on_create_metadata(&self) -> Result<Status> {
        let table_id = self.table_id();
        let manager = &self.context.table_metadata_manager;

        let raw_table_info = self.table_info().clone();
        let region_routes = self.region_routes().clone();
        manager
            .create_table_metadata(raw_table_info, region_routes)
            .await
            .context(TableMetadataManagerSnafu)?;
        info!("Created table metadata for table {table_id}");

        Ok(Status::Done)
    }

    async fn on_datanode_create_table(&mut self) -> Result<Status> {
        let region_routes = &self.creator.data.region_routes;
        let table_name = self.table_name();
        let clients = self.context.datanode_clients.clone();
        let leaders = find_leaders(region_routes);
        let mut joins = Vec::with_capacity(leaders.len());
        let table_id = self.table_id();

        for datanode in leaders {
            let client = clients.get_client(&datanode).await;
            let client = Database::new(&table_name.catalog_name, &table_name.schema_name, client);

            let regions = find_leader_regions(region_routes, &datanode);
            let mut create_expr_for_region = self.creator.data.task.create_table.clone();
            create_expr_for_region.region_numbers = regions;
            create_expr_for_region.table_id = Some(api::v1::TableId { id: table_id });

            joins.push(common_runtime::spawn_bg(async move {
                if let Err(err) = client.create(create_expr_for_region).await {
                    if err.status_code() != StatusCode::TableAlreadyExists {
                        return Err(handle_request_datanode_error(datanode)(err));
                    }
                }
                Ok(())
            }));
        }

        let _r = join_all(joins)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu).flatten())
            .collect::<Result<Vec<_>>>()?;

        self.creator.data.state = CreateTableState::CreateMetadata;

        Ok(Status::executing(true))
    }
}

#[async_trait]
impl Procedure for CreateTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.creator.data.state;

        let _timer = common_telemetry::timer!(
            metrics::METRIC_META_PROCEDURE_CREATE_TABLE,
            &[("step", state.as_ref().to_string())]
        );

        match state {
            CreateTableState::Prepare => self.on_prepare().await,
            CreateTableState::DatanodeCreateTable => self.on_datanode_create_table().await,
            CreateTableState::DatanodeCreateRegions => self.on_datanode_create_regions().await,
            CreateTableState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.creator.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.creator.data.table_ref();
        let key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );

        LockKey::single(key)
    }
}

pub struct TableCreator {
    data: CreateTableData,
}

impl TableCreator {
    pub fn new(cluster_id: u64, task: CreateTableTask, region_routes: Vec<RegionRoute>) -> Self {
        Self {
            data: CreateTableData {
                state: CreateTableState::Prepare,
                cluster_id,
                task,
                region_routes,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr)]
enum CreateTableState {
    /// Prepares to create the table
    Prepare,
    /// Datanode creates the table
    DatanodeCreateTable,
    /// Create regions on the Datanode
    DatanodeCreateRegions,
    /// Creates metadata
    CreateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableData {
    state: CreateTableState,
    task: CreateTableTask,
    region_routes: Vec<RegionRoute>,
    cluster_id: u64,
}

impl CreateTableData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};

    use api::v1::region::region_server::RegionServer;
    use api::v1::region::RegionResponse;
    use api::v1::{
        ColumnDataType, ColumnDef as PbColumnDef, CreateTableExpr, ResponseHeader,
        Status as PbStatus,
    };
    use chrono::DateTime;
    use client::client_manager::DatanodeClients;
    use client::Client;
    use common_grpc::channel_manager::ChannelManager;
    use common_meta::key::TableMetadataManager;
    use common_meta::peer::Peer;
    use common_runtime::{Builder as RuntimeBuilder, Runtime};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema};
    use servers::grpc::region_server::{RegionServerHandler, RegionServerRequestHandler};
    use table::metadata::{RawTableMeta, TableIdent, TableType};
    use table::requests::TableOptions;
    use tokio::sync::mpsc;
    use tonic::transport::Server;
    use tower::service_fn;

    use super::*;
    use crate::handler::{HeartbeatMailbox, Pushers};
    use crate::sequence::Sequence;
    use crate::service::store::kv::KvBackendAdapter;
    use crate::service::store::memory::MemStore;
    use crate::test_util::new_region_route;

    fn create_table_procedure() -> CreateTableProcedure {
        let create_table_expr = CreateTableExpr {
            catalog_name: "my_catalog".to_string(),
            schema_name: "my_schema".to_string(),
            table_name: "my_table".to_string(),
            desc: "blabla".to_string(),
            column_defs: vec![
                PbColumnDef {
                    name: "ts".to_string(),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                },
                PbColumnDef {
                    name: "my_tag1".to_string(),
                    datatype: ColumnDataType::String as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                },
                PbColumnDef {
                    name: "my_tag2".to_string(),
                    datatype: ColumnDataType::String as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                },
                PbColumnDef {
                    name: "my_field_column".to_string(),
                    datatype: ColumnDataType::Int32 as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                },
            ],
            time_index: "ts".to_string(),
            primary_keys: vec!["my_tag2".to_string(), "my_tag1".to_string()],
            create_if_not_exists: false,
            table_options: HashMap::new(),
            table_id: None,
            region_numbers: vec![1, 2, 3],
            engine: MITO2_ENGINE.to_string(),
        };

        let raw_table_info = RawTableInfo {
            ident: TableIdent::new(42),
            name: "my_table".to_string(),
            desc: Some("blabla".to_string()),
            catalog_name: "my_catalog".to_string(),
            schema_name: "my_schema".to_string(),
            meta: RawTableMeta {
                schema: RawSchema {
                    column_schemas: vec![
                        ColumnSchema::new(
                            "ts".to_string(),
                            ConcreteDataType::timestamp_millisecond_datatype(),
                            false,
                        ),
                        ColumnSchema::new(
                            "my_tag1".to_string(),
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        ColumnSchema::new(
                            "my_tag2".to_string(),
                            ConcreteDataType::string_datatype(),
                            true,
                        ),
                        ColumnSchema::new(
                            "my_field_column".to_string(),
                            ConcreteDataType::int32_datatype(),
                            true,
                        ),
                    ],
                    timestamp_index: Some(0),
                    version: 0,
                },
                primary_key_indices: vec![1, 2],
                value_indices: vec![2],
                engine: MITO2_ENGINE.to_string(),
                next_column_id: 3,
                region_numbers: vec![1, 2, 3],
                engine_options: HashMap::new(),
                options: TableOptions::default(),
                created_on: DateTime::default(),
                partition_key_indices: vec![],
            },
            table_type: TableType::Base,
        };

        let peers = vec![
            Peer::new(1, "127.0.0.1:4001"),
            Peer::new(2, "127.0.0.1:4002"),
            Peer::new(3, "127.0.0.1:4003"),
        ];
        let region_routes = vec![
            new_region_route(1, &peers, 3),
            new_region_route(2, &peers, 2),
            new_region_route(3, &peers, 1),
        ];

        let kv_store = Arc::new(MemStore::new());

        let mailbox_sequence = Sequence::new("test_heartbeat_mailbox", 0, 100, kv_store.clone());
        let mailbox = HeartbeatMailbox::create(Pushers::default(), mailbox_sequence);

        CreateTableProcedure::new(
            1,
            CreateTableTask::new(create_table_expr, vec![], raw_table_info),
            region_routes,
            DdlContext {
                datanode_clients: Arc::new(DatanodeClients::default()),
                mailbox,
                server_addr: "127.0.0.1:4321".to_string(),
                table_metadata_manager: Arc::new(TableMetadataManager::new(
                    KvBackendAdapter::wrap(kv_store),
                )),
            },
        )
    }

    #[test]
    fn test_create_region_request_template() {
        let procedure = create_table_procedure();

        let template = procedure.create_region_request_template().unwrap();

        let expected = PbCreateRegionRequest {
            region_id: 0,
            engine: MITO2_ENGINE.to_string(),
            column_defs: vec![
                ColumnDef {
                    name: "ts".to_string(),
                    column_id: 0,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    is_nullable: false,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Timestamp as i32,
                },
                ColumnDef {
                    name: "my_tag1".to_string(),
                    column_id: 1,
                    datatype: ColumnDataType::String as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                },
                ColumnDef {
                    name: "my_tag2".to_string(),
                    column_id: 2,
                    datatype: ColumnDataType::String as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Tag as i32,
                },
                ColumnDef {
                    name: "my_field_column".to_string(),
                    column_id: 3,
                    datatype: ColumnDataType::Int32 as i32,
                    is_nullable: true,
                    default_constraint: vec![],
                    semantic_type: SemanticType::Field as i32,
                },
            ],
            primary_key: vec![2, 1],
            create_if_not_exists: true,
            region_dir: "".to_string(),
            options: HashMap::new(),
        };
        assert_eq!(template, expected);
    }

    #[derive(Clone)]
    struct TestingRegionServerHandler {
        runtime: Arc<Runtime>,
        create_region_notifier: mpsc::Sender<RegionId>,
    }

    impl TestingRegionServerHandler {
        fn new(create_region_notifier: mpsc::Sender<RegionId>) -> Self {
            Self {
                runtime: Arc::new(RuntimeBuilder::default().worker_threads(2).build().unwrap()),
                create_region_notifier,
            }
        }

        fn new_client(&self, datanode: &Peer) -> Client {
            let (client, server) = tokio::io::duplex(1024);

            let handler =
                RegionServerRequestHandler::new(Arc::new(self.clone()), None, self.runtime.clone());

            tokio::spawn(async move {
                Server::builder()
                    .add_service(RegionServer::new(handler))
                    .serve_with_incoming(futures::stream::iter(vec![Ok::<_, std::io::Error>(
                        server,
                    )]))
                    .await
            });

            let channel_manager = ChannelManager::new();
            let mut client = Some(client);
            channel_manager
                .reset_with_connector(
                    datanode.addr.clone(),
                    service_fn(move |_| {
                        let client = client.take().unwrap();
                        async move { Ok::<_, std::io::Error>(client) }
                    }),
                )
                .unwrap();
            Client::with_manager_and_urls(channel_manager, vec![datanode.addr.clone()])
        }
    }

    #[async_trait]
    impl RegionServerHandler for TestingRegionServerHandler {
        async fn handle(&self, request: PbRegionRequest) -> servers::error::Result<RegionResponse> {
            let PbRegionRequest::Create(request) = request else {
                unreachable!()
            };
            let region_id = request.region_id.into();

            self.create_region_notifier.send(region_id).await.unwrap();

            Ok(RegionResponse {
                header: Some(ResponseHeader {
                    status: Some(PbStatus {
                        status_code: 0,
                        err_msg: "".to_string(),
                    }),
                }),
                affected_rows: 0,
            })
        }
    }

    #[tokio::test]
    async fn test_on_datanode_create_regions() {
        let mut procedure = create_table_procedure();

        let (tx, mut rx) = mpsc::channel(10);

        let region_server = TestingRegionServerHandler::new(tx);

        let datanodes = find_leaders(&procedure.creator.data.region_routes);
        for peer in datanodes {
            let client = region_server.new_client(&peer);
            procedure
                .context
                .datanode_clients
                .insert_client(peer, client)
                .await;
        }

        let expected_created_regions = Arc::new(Mutex::new(HashSet::from([
            RegionId::new(42, 1),
            RegionId::new(42, 2),
            RegionId::new(42, 3),
        ])));
        let handle = tokio::spawn({
            let expected_created_regions = expected_created_regions.clone();
            let mut max_recv = expected_created_regions.lock().unwrap().len();
            async move {
                while let Some(region_id) = rx.recv().await {
                    expected_created_regions.lock().unwrap().remove(&region_id);

                    max_recv -= 1;
                    if max_recv == 0 {
                        break;
                    }
                }
            }
        });

        let status = procedure.on_datanode_create_regions().await.unwrap();
        assert!(matches!(status, Status::Executing { persist: true }));
        assert!(matches!(
            procedure.creator.data.state,
            CreateTableState::CreateMetadata
        ));

        handle.await.unwrap();

        assert!(expected_created_regions.lock().unwrap().is_empty());
    }
}
