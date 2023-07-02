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
use common_meta::rpc::ddl::DdlTask;
use common_procedure::{watcher, ProcedureId, ProcedureManagerRef, ProcedureWithId};
use snafu::ResultExt;
use tokio::sync::oneshot;

use crate::error::{self, Result};
use crate::metasrv::{SelectorContext, SelectorRef};
use crate::procedure::create_table::{CreateTableProcedure, CreateTableProcedureStatus};
use crate::sequence::SequenceRef;
use crate::service::store::kv::KvStoreRef;

pub type DdlManagerRef = Arc<DdlManager>;

pub struct DdlManager {
    procedure_manager: ProcedureManagerRef,
    selector: SelectorRef,
    selector_ctx: SelectorContext,
    table_id_sequence: SequenceRef,
    kv_store: KvStoreRef,
    datanode_clients: Arc<DatanodeClients>,
}

// TODO(weny): removes in following PRs.
#[allow(unused)]
pub(crate) struct DdlContext {
    pub(crate) selector: SelectorRef,
    pub(crate) selector_ctx: SelectorContext,
    pub(crate) table_id_sequence: SequenceRef,
    pub(crate) kv_store: KvStoreRef,
    pub(crate) datanode_clients: Arc<DatanodeClients>,
    /// Only notifies upper if `notify` presence.
    pub(crate) notifier: Option<oneshot::Sender<ProcedureStatus>>,
}

impl Clone for DdlContext {
    fn clone(&self) -> Self {
        Self {
            selector: self.selector.clone(),
            selector_ctx: self.selector_ctx.clone(),
            table_id_sequence: self.table_id_sequence.clone(),
            kv_store: self.kv_store.clone(),
            datanode_clients: self.datanode_clients.clone(),
            notifier: None,
        }
    }
}

impl DdlContext {
    pub fn with_notifier(&mut self, notifier: oneshot::Sender<ProcedureStatus>) {
        self.notifier = Some(notifier)
    }
}

#[derive(Debug, Clone)]
pub enum ProcedureStatus {
    CreateTable(CreateTableProcedureStatus),
}

impl DdlManager {
    pub(crate) fn new(
        procedure_manager: ProcedureManagerRef,
        selector: SelectorRef,
        selector_ctx: SelectorContext,
        table_id_sequence: SequenceRef,
        kv_store: KvStoreRef,
        datanode_clients: Arc<DatanodeClients>,
    ) -> Self {
        Self {
            procedure_manager,
            selector,
            selector_ctx,
            table_id_sequence,
            kv_store,
            datanode_clients,
        }
    }

    pub(crate) fn create_context(&self) -> DdlContext {
        DdlContext {
            selector: self.selector.clone(),
            selector_ctx: self.selector_ctx.clone(),
            table_id_sequence: self.table_id_sequence.clone(),
            kv_store: self.kv_store.clone(),
            datanode_clients: self.datanode_clients.clone(),
            notifier: None,
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
            })
    }

    pub(crate) async fn execute_procedure_task(
        &self,
        cluster_id: u64,
        task: DdlTask,
    ) -> Result<(ProcedureId, ProcedureStatus)> {
        let (tx, mut rx) = oneshot::channel();

        let procedure_with_id = match task {
            DdlTask::CreateTable(create_table_task) => {
                let mut context = self.create_context();
                context.with_notifier(tx);

                let procedure = CreateTableProcedure::new(cluster_id, create_table_task, context);
                ProcedureWithId::with_random_id(Box::new(procedure))
            }
        };

        let procedure_id = procedure_with_id.id;

        let mut watcher = self
            .procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;

        watcher::wait(&mut watcher)
            .await
            .context(error::WaitProcedureSnafu)?;

        let status = rx.try_recv().context(error::TryReceiveStatusSnafu)?;

        Ok((procedure_id, status))
    }
}
