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

use std::time::Duration;

use api::v1::meta::reconcile_request::Target;
use api::v1::meta::{
    DdlTaskRequest as PbDdlTaskRequest, DdlTaskResponse as PbDdlTaskResponse, MigrateRegionRequest,
    MigrateRegionResponse, ProcedureDetailRequest, ProcedureDetailResponse, ProcedureStateResponse,
    QueryProcedureRequest, ReconcileCatalog, ReconcileDatabase, ReconcileRequest,
    ReconcileResponse, ReconcileTable, ResolveStrategy, procedure_service_server,
};
use common_meta::procedure_executor::ExecutorContext;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest};
use common_meta::rpc::procedure;
use snafu::{OptionExt, ResultExt};
use table::table_reference::TableReference;
use tonic::Request;

use crate::metasrv::Metasrv;
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};
use crate::service::GrpcResult;
use crate::{check_leader, error};

#[async_trait::async_trait]
impl procedure_service_server::ProcedureService for Metasrv {
    async fn query(
        &self,
        request: Request<QueryProcedureRequest>,
    ) -> GrpcResult<ProcedureStateResponse> {
        check_leader!(
            self,
            request,
            ProcedureStateResponse,
            "`query procedure state`"
        );

        let QueryProcedureRequest { header, pid, .. } = request.into_inner();
        let _header = header.context(error::MissingRequestHeaderSnafu)?;
        let pid = pid.context(error::MissingRequiredParameterSnafu { param: "pid" })?;
        let pid = procedure::pb_pid_to_pid(&pid).context(error::ConvertProtoDataSnafu)?;

        let state = self
            .procedure_manager()
            .procedure_state(pid)
            .await
            .context(error::QueryProcedureSnafu)?
            .context(error::ProcedureNotFoundSnafu {
                pid: pid.to_string(),
            })?;

        Ok(Response::new(procedure::procedure_state_to_pb_response(
            &state,
        )))
    }

    async fn ddl(&self, request: Request<PbDdlTaskRequest>) -> GrpcResult<PbDdlTaskResponse> {
        check_leader!(self, request, PbDdlTaskResponse, "`ddl`");

        let PbDdlTaskRequest {
            header,
            query_context,
            task,
            wait,
            timeout_secs,
        } = request.into_inner();

        let header = header.context(error::MissingRequestHeaderSnafu)?;
        let query_context = query_context
            .context(error::MissingRequiredParameterSnafu {
                param: "query_context",
            })?
            .into();
        let task: DdlTask = task
            .context(error::MissingRequiredParameterSnafu { param: "task" })?
            .try_into()
            .context(error::ConvertProtoDataSnafu)?;

        let resp = self
            .ddl_manager()
            .submit_ddl_task(
                &ExecutorContext {
                    tracing_context: Some(header.tracing_context),
                },
                SubmitDdlTaskRequest {
                    query_context,
                    wait,
                    timeout: Duration::from_secs(timeout_secs.into()),
                    task,
                },
            )
            .await
            .context(error::SubmitDdlTaskSnafu)?
            .into();

        Ok(Response::new(resp))
    }

    async fn migrate(
        &self,
        request: Request<MigrateRegionRequest>,
    ) -> GrpcResult<MigrateRegionResponse> {
        check_leader!(self, request, MigrateRegionResponse, "`migrate`");

        let MigrateRegionRequest {
            header,
            region_id,
            from_peer,
            to_peer,
            timeout_secs,
        } = request.into_inner();

        let _header = header.context(error::MissingRequestHeaderSnafu)?;
        let from_peer = self
            .lookup_datanode_peer(from_peer)
            .await?
            .context(error::PeerUnavailableSnafu { peer_id: from_peer })?;
        let to_peer = self
            .lookup_datanode_peer(to_peer)
            .await?
            .context(error::PeerUnavailableSnafu { peer_id: to_peer })?;

        let pid = self
            .region_migration_manager()
            .submit_procedure(RegionMigrationProcedureTask {
                region_id: region_id.into(),
                from_peer,
                to_peer,
                timeout: Duration::from_secs(timeout_secs.into()),
                trigger_reason: RegionMigrationTriggerReason::Manual,
            })
            .await?
            .map(procedure::pid_to_pb_pid);

        let resp = MigrateRegionResponse {
            pid,
            ..Default::default()
        };

        Ok(Response::new(resp))
    }

    async fn reconcile(&self, request: Request<ReconcileRequest>) -> GrpcResult<ReconcileResponse> {
        check_leader!(self, request, ReconcileResponse, "`reconcile`");

        let ReconcileRequest { header, target } = request.into_inner();
        let _header = header.context(error::MissingRequestHeaderSnafu)?;
        let target = target.context(error::MissingRequiredParameterSnafu { param: "target" })?;
        let parse_resolve_strategy = |resolve_strategy: i32| {
            ResolveStrategy::try_from(resolve_strategy)
                .ok()
                .context(error::UnexpectedSnafu {
                    violated: format!("Invalid resolve strategy: {}", resolve_strategy),
                })
        };
        let procedure_id = match target {
            Target::ReconcileTable(table) => {
                let ReconcileTable {
                    catalog_name,
                    schema_name,
                    table_name,
                    resolve_strategy,
                } = table;
                let resolve_strategy = parse_resolve_strategy(resolve_strategy)?;
                let table_ref = TableReference::full(&catalog_name, &schema_name, &table_name);
                self.reconciliation_manager()
                    .reconcile_table(table_ref, resolve_strategy.into())
                    .await
                    .context(error::SubmitReconcileProcedureSnafu)?
            }
            Target::ReconcileDatabase(database) => {
                let ReconcileDatabase {
                    catalog_name,
                    database_name,
                    resolve_strategy,
                    parallelism,
                } = database;
                let resolve_strategy = parse_resolve_strategy(resolve_strategy)?;
                self.reconciliation_manager()
                    .reconcile_database(
                        catalog_name,
                        database_name,
                        resolve_strategy.into(),
                        parallelism as usize,
                    )
                    .await
                    .context(error::SubmitReconcileProcedureSnafu)?
            }
            Target::ReconcileCatalog(catalog) => {
                let ReconcileCatalog {
                    catalog_name,
                    resolve_strategy,
                    parallelism,
                } = catalog;
                let resolve_strategy = parse_resolve_strategy(resolve_strategy)?;
                self.reconciliation_manager()
                    .reconcile_catalog(catalog_name, resolve_strategy.into(), parallelism as usize)
                    .await
                    .context(error::SubmitReconcileProcedureSnafu)?
            }
        };
        Ok(Response::new(ReconcileResponse {
            pid: Some(procedure::pid_to_pb_pid(procedure_id)),
            ..Default::default()
        }))
    }

    async fn details(
        &self,
        request: Request<ProcedureDetailRequest>,
    ) -> GrpcResult<ProcedureDetailResponse> {
        check_leader!(
            self,
            request,
            ProcedureDetailResponse,
            "`procedure details`"
        );

        let ProcedureDetailRequest { header } = request.into_inner();
        let _header = header.context(error::MissingRequestHeaderSnafu)?;
        let metas = self
            .procedure_manager()
            .list_procedures()
            .await
            .context(error::QueryProcedureSnafu)?;
        Ok(Response::new(procedure::procedure_details_to_pb_response(
            metas,
        )))
    }
}
