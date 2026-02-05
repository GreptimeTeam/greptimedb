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
    DdlTaskRequest as PbDdlTaskRequest, DdlTaskResponse as PbDdlTaskResponse, GcRegionsRequest,
    GcRegionsResponse, GcStats, GcTableRequest, GcTableResponse, MigrateRegionRequest,
    MigrateRegionResponse, ProcedureDetailRequest, ProcedureDetailResponse, ProcedureStateResponse,
    QueryProcedureRequest, ReconcileCatalog, ReconcileDatabase, ReconcileRequest,
    ReconcileResponse, ReconcileTable, ResolveStrategy, procedure_service_server,
};
use common_meta::key::TableMetadataManagerRef;
use common_meta::key::table_name::TableNameKey;
use common_meta::procedure_executor::ExecutorContext;
use common_meta::rpc::ddl::{DdlTask, SubmitDdlTaskRequest};
use common_meta::rpc::procedure::{
    self, GcRegionsRequest as MetaGcRegionsRequest, GcResponse,
    GcTableRequest as MetaGcTableRequest,
};
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::table_reference::TableReference;
use tonic::Request;

use crate::error::{TableMetadataManagerSnafu, TableNotFoundSnafu};
use crate::metasrv::Metasrv;
use crate::procedure::region_migration::manager::{
    RegionMigrationProcedureTask, RegionMigrationTriggerReason,
};
use crate::service::GrpcResult;
use crate::{check_leader, error, gc};

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

    async fn gc_regions(
        &self,
        request: Request<GcRegionsRequest>,
    ) -> GrpcResult<GcRegionsResponse> {
        check_leader!(self, request, GcRegionsResponse, "`gc_regions`");

        let GcRegionsRequest {
            header,
            region_ids,
            full_file_listing,
            timeout_secs,
        } = request.into_inner();

        let _header = header.context(error::MissingRequestHeaderSnafu)?;

        let response = self
            .handle_gc_regions(MetaGcRegionsRequest {
                region_ids,
                full_file_listing,
                timeout: Duration::from_secs(timeout_secs as u64),
            })
            .await?;

        Ok(Response::new(gc_response_to_regions_pb(response)))
    }

    async fn gc_table(&self, request: Request<GcTableRequest>) -> GrpcResult<GcTableResponse> {
        check_leader!(self, request, GcTableResponse, "`gc_table`");

        let GcTableRequest {
            header,
            catalog_name,
            schema_name,
            table_name,
            full_file_listing,
            timeout_secs,
        } = request.into_inner();

        let _header = header.context(error::MissingRequestHeaderSnafu)?;

        let response = self
            .handle_gc_table(MetaGcTableRequest {
                catalog_name,
                schema_name,
                table_name,
                full_file_listing,
                timeout: Duration::from_secs(timeout_secs as u64),
            })
            .await?;

        Ok(Response::new(gc_response_to_table_pb(response)))
    }
}

impl Metasrv {
    async fn handle_gc_regions(&self, request: MetaGcRegionsRequest) -> error::Result<GcResponse> {
        let region_ids: Vec<RegionId> = request
            .region_ids
            .into_iter()
            .map(RegionId::from_u64)
            .collect();
        self.trigger_gc_for_regions(region_ids, request.full_file_listing, request.timeout)
            .await
    }

    async fn handle_gc_table(&self, request: MetaGcTableRequest) -> error::Result<GcResponse> {
        let table_name_key = TableNameKey::new(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        );

        let table_metadata_manager: &TableMetadataManagerRef = self.table_metadata_manager();
        let table_id = table_metadata_manager
            .table_name_manager()
            .get(table_name_key)
            .await
            .context(TableMetadataManagerSnafu)?
            .context(TableNotFoundSnafu {
                name: request.table_name.clone(),
            })?
            .table_id();

        let (_phy_table_id, route) = table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await
            .context(TableMetadataManagerSnafu)?;

        let region_ids: Vec<RegionId> = route.region_routes.iter().map(|r| r.region.id).collect();
        self.trigger_gc_for_regions(region_ids, request.full_file_listing, request.timeout)
            .await
    }

    /// Triggers manual GC for specified regions and returns the GC response.
    async fn trigger_gc_for_regions(
        &self,
        region_ids: Vec<RegionId>,
        full_file_listing: bool,
        timeout: Duration,
    ) -> error::Result<GcResponse> {
        let gc_ticker = self.gc_ticker().context(error::UnexpectedSnafu {
            violated: "GC ticker not available".to_string(),
        })?;

        let (tx, rx) = tokio::sync::oneshot::channel();
        gc_ticker
            .sender
            .send(gc::Event::Manually {
                sender: tx,
                region_ids: Some(region_ids),
                full_file_listing: Some(full_file_listing),
                timeout: Some(timeout),
            })
            .await
            .map_err(|_| {
                error::UnexpectedSnafu {
                    violated: "Failed to send GC event".to_string(),
                }
                .build()
            })?;

        let job_report = rx.await.map_err(|_| {
            error::UnexpectedSnafu {
                violated: "GC job channel closed unexpectedly".to_string(),
            }
            .build()
        })?;

        let report = gc_job_report_to_gc_report(job_report);

        Ok(gc_report_to_response(&report))
    }
}

fn gc_job_report_to_gc_report(job_report: crate::gc::GcJobReport) -> store_api::storage::GcReport {
    // Merge all datanode reports into a single GcReport
    let mut gc_report = store_api::storage::GcReport::default();
    for (_datanode_id, report) in job_report.per_datanode_reports {
        gc_report.merge(report);
    }
    gc_report
}

fn gc_report_to_response(report: &store_api::storage::GcReport) -> GcResponse {
    let deleted_files = report.deleted_files.values().map(|v| v.len() as u64).sum();
    let deleted_indexes = report
        .deleted_indexes
        .values()
        .map(|v| v.len() as u64)
        .sum();
    GcResponse {
        processed_regions: report.processed_regions.len() as u64,
        need_retry_regions: report
            .need_retry_regions
            .iter()
            .map(|id| id.as_u64())
            .collect(),
        deleted_files,
        deleted_indexes,
    }
}

fn gc_response_to_regions_pb(resp: GcResponse) -> GcRegionsResponse {
    GcRegionsResponse {
        stats: Some(GcStats {
            processed_regions: resp.processed_regions,
            need_retry_regions: resp.need_retry_regions,
            deleted_files: resp.deleted_files,
            deleted_indexes: resp.deleted_indexes,
        }),
        ..Default::default()
    }
}

fn gc_response_to_table_pb(resp: GcResponse) -> GcTableResponse {
    GcTableResponse {
        stats: Some(GcStats {
            processed_regions: resp.processed_regions,
            need_retry_regions: resp.need_retry_regions,
            deleted_files: resp.deleted_files,
            deleted_indexes: resp.deleted_indexes,
        }),
        ..Default::default()
    }
}
