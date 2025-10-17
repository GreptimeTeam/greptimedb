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

use catalog::information_schema::{DatanodeInspectRequest, InformationExtension};
use client::SendableRecordBatchStream;
use client::api::v1::meta::RegionRole;
use common_error::ext::BoxedError;
use common_meta::cluster::{NodeInfo, NodeStatus};
use common_meta::datanode::RegionStat;
use common_meta::key::flow::flow_state::FlowStat;
use common_meta::peer::Peer;
use common_procedure::{ProcedureInfo, ProcedureManagerRef};
use common_query::request::QueryRequest;
use common_stat::{ResourceStatImpl, ResourceStatRef};
use datanode::region_server::RegionServer;
use flow::StreamingEngine;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::RwLock;

pub struct StandaloneInformationExtension {
    region_server: RegionServer,
    procedure_manager: ProcedureManagerRef,
    start_time_ms: u64,
    flow_streaming_engine: RwLock<Option<Arc<StreamingEngine>>>,
    resource_stat: ResourceStatRef,
}

impl StandaloneInformationExtension {
    pub fn new(region_server: RegionServer, procedure_manager: ProcedureManagerRef) -> Self {
        let mut resource_stat = ResourceStatImpl::default();
        resource_stat.start_collect_cpu_usage();
        Self {
            region_server,
            procedure_manager,
            start_time_ms: common_time::util::current_time_millis() as u64,
            flow_streaming_engine: RwLock::new(None),
            resource_stat: Arc::new(resource_stat),
        }
    }

    /// Set the flow streaming engine for the standalone instance.
    pub async fn set_flow_streaming_engine(&self, flow_streaming_engine: Arc<StreamingEngine>) {
        let mut guard = self.flow_streaming_engine.write().await;
        *guard = Some(flow_streaming_engine);
    }
}

#[async_trait::async_trait]
impl InformationExtension for StandaloneInformationExtension {
    type Error = catalog::error::Error;

    async fn nodes(&self) -> std::result::Result<Vec<NodeInfo>, Self::Error> {
        let build_info = common_version::build_info();
        let node_info = NodeInfo {
            // For the standalone:
            // - id always 0
            // - empty string for peer_addr
            peer: Peer {
                id: 0,
                addr: "".to_string(),
            },
            last_activity_ts: -1,
            status: NodeStatus::Standalone,
            version: build_info.version.to_string(),
            git_commit: build_info.commit_short.to_string(),
            // Use `self.start_time_ms` instead.
            // It's not precise but enough.
            start_time_ms: self.start_time_ms,
            total_cpu_millicores: self.resource_stat.get_total_cpu_millicores(),
            total_memory_bytes: self.resource_stat.get_total_memory_bytes(),
            cpu_usage_millicores: self.resource_stat.get_cpu_usage_millicores(),
            memory_usage_bytes: self.resource_stat.get_memory_usage_bytes(),
            hostname: hostname::get()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        };
        Ok(vec![node_info])
    }

    async fn procedures(&self) -> std::result::Result<Vec<(String, ProcedureInfo)>, Self::Error> {
        self.procedure_manager
            .list_procedures()
            .await
            .map_err(BoxedError::new)
            .map(|procedures| {
                procedures
                    .into_iter()
                    .map(|procedure| {
                        let status = procedure.state.as_str_name().to_string();
                        (status, procedure)
                    })
                    .collect::<Vec<_>>()
            })
            .context(catalog::error::ListProceduresSnafu)
    }

    async fn region_stats(&self) -> std::result::Result<Vec<RegionStat>, Self::Error> {
        let stats = self
            .region_server
            .reportable_regions()
            .into_iter()
            .map(|stat| {
                let region_stat = self
                    .region_server
                    .region_statistic(stat.region_id)
                    .unwrap_or_default();
                RegionStat {
                    id: stat.region_id,
                    rcus: 0,
                    wcus: 0,
                    approximate_bytes: region_stat.estimated_disk_size(),
                    engine: stat.engine,
                    role: RegionRole::from(stat.role).into(),
                    num_rows: region_stat.num_rows,
                    memtable_size: region_stat.memtable_size,
                    manifest_size: region_stat.manifest_size,
                    sst_size: region_stat.sst_size,
                    sst_num: region_stat.sst_num,
                    index_size: region_stat.index_size,
                    region_manifest: region_stat.manifest.into(),
                    data_topic_latest_entry_id: region_stat.data_topic_latest_entry_id,
                    metadata_topic_latest_entry_id: region_stat.metadata_topic_latest_entry_id,
                    written_bytes: region_stat.written_bytes,
                }
            })
            .collect::<Vec<_>>();
        Ok(stats)
    }

    async fn flow_stats(&self) -> std::result::Result<Option<FlowStat>, Self::Error> {
        Ok(Some(
            self.flow_streaming_engine
                .read()
                .await
                .as_ref()
                .unwrap()
                .gen_state_report()
                .await,
        ))
    }

    async fn inspect_datanode(
        &self,
        request: DatanodeInspectRequest,
    ) -> std::result::Result<SendableRecordBatchStream, Self::Error> {
        let req = QueryRequest {
            plan: request
                .build_plan()
                .context(catalog::error::DatafusionSnafu)?,
            region_id: RegionId::default(),
            header: None,
        };

        self.region_server
            .handle_read(req)
            .await
            .map_err(BoxedError::new)
            .context(catalog::error::InternalSnafu)
    }
}
