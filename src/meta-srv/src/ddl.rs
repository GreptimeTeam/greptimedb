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

use client::client_manager::DatanodeClients;
use common_meta::key::TableMetadataManagerRef;
use common_meta::rpc::ddl::{AlterTableTask, CreateTableTask, DropTableTask};
use common_meta::rpc::router::TableRoute;
use common_procedure::{watcher, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use snafu::ResultExt;
use table::metadata::RawTableInfo;
use table::requests::AlterTableRequest;

use crate::error::{self, Result};
use crate::procedure::alter_table::AlterTableProcedure;
use crate::procedure::create_table::CreateTableProcedure;
use crate::procedure::drop_table::DropTableProcedure;
use crate::service::mailbox::MailboxRef;
use crate::service::store::kv::KvStoreRef;

pub type DdlManagerRef = Arc<DdlManager>;

pub struct DdlManager {
    procedure_manager: ProcedureManagerRef,
    kv_store: KvStoreRef,
    datanode_clients: Arc<DatanodeClients>,
    pub(crate) mailbox: MailboxRef,
    pub(crate) server_addr: String,
    table_metadata_manager: TableMetadataManagerRef,
}

#[derive(Clone)]
pub(crate) struct DdlContext {
    pub(crate) kv_store: KvStoreRef,
    pub(crate) datanode_clients: Arc<DatanodeClients>,
    pub(crate) mailbox: MailboxRef,
    pub(crate) server_addr: String,
    #[allow(unused)]
    pub(crate) table_metadata_manager: TableMetadataManagerRef,
}

impl DdlManager {
    pub(crate) fn new(
        procedure_manager: ProcedureManagerRef,
        kv_store: KvStoreRef,
        datanode_clients: Arc<DatanodeClients>,
        mailbox: MailboxRef,
        server_addr: String,
        table_metadata_manager: TableMetadataManagerRef,
    ) -> Self {
        Self {
            procedure_manager,
            kv_store,
            datanode_clients,
            mailbox,
            server_addr,
            table_metadata_manager,
        }
    }

    pub(crate) fn create_context(&self) -> DdlContext {
        DdlContext {
            kv_store: self.kv_store.clone(),
            datanode_clients: self.datanode_clients.clone(),
            mailbox: self.mailbox.clone(),
            server_addr: self.server_addr.clone(),
            table_metadata_manager: self.table_metadata_manager.clone(),
        }
    }

    pub(crate) fn try_start(&self) -> Result<()> {
        let context = self.create_context();

        self.procedure_manager
            .register_loader(
                CreateTableProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    CreateTableProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: CreateTableProcedure::TYPE_NAME,
            })?;

        let context = self.create_context();

        self.procedure_manager
            .register_loader(
                DropTableProcedure::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    DropTableProcedure::from_json(json, context).map(|p| Box::new(p) as _)
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: DropTableProcedure::TYPE_NAME,
            })
    }

    pub async fn submit_alter_table_task(
        &self,
        cluster_id: u64,
        alter_table_task: AlterTableTask,
        alter_table_request: AlterTableRequest,
        table_info: RawTableInfo,
    ) -> Result<ProcedureId> {
        let context = self.create_context();

        let procedure = AlterTableProcedure::new(
            cluster_id,
            alter_table_task,
            alter_table_request,
            table_info,
            context,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    pub async fn submit_create_table_task(
        &self,
        cluster_id: u64,
        create_table_task: CreateTableTask,
        table_route: TableRoute,
    ) -> Result<ProcedureId> {
        let context = self.create_context();

        let procedure =
            CreateTableProcedure::new(cluster_id, create_table_task, table_route, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    pub async fn submit_drop_table_task(
        &self,
        cluster_id: u64,
        drop_table_task: DropTableTask,
        table_route: TableRoute,
    ) -> Result<ProcedureId> {
        let context = self.create_context();

        let procedure = DropTableProcedure::new(cluster_id, drop_table_task, table_route, context);

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        self.submit_procedure(procedure_with_id).await
    }

    async fn submit_procedure(&self, procedure_with_id: ProcedureWithId) -> Result<ProcedureId> {
        let procedure_id = procedure_with_id.id;

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;

        watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu)?;

        Ok(procedure_id)
    }
}
