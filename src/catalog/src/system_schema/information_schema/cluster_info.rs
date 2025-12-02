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

use std::sync::{Arc, Weak};
use std::time::Duration;

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_CLUSTER_INFO_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::cluster::{DatanodeStatus, NodeInfo, NodeStatus};
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_time::timestamp::Timestamp;
use common_workload::DatanodeWorkloadType;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::timestamp::TimestampMillisecond;
use datatypes::value::Value;
use datatypes::vectors::{
    Int64VectorBuilder, StringVectorBuilder, TimestampMillisecondVectorBuilder,
};
use serde::Serialize;
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use crate::CatalogManager;
use crate::error::{CreateRecordBatchSnafu, InternalSnafu, Result};
use crate::system_schema::information_schema::{CLUSTER_INFO, InformationTable, Predicates};
use crate::system_schema::utils;

const PEER_TYPE_FRONTEND: &str = "FRONTEND";
const PEER_TYPE_METASRV: &str = "METASRV";

const PEER_ID: &str = "peer_id";
const PEER_TYPE: &str = "peer_type";
const PEER_ADDR: &str = "peer_addr";
const PEER_HOSTNAME: &str = "peer_hostname";
const TOTAL_CPU_MILLICORES: &str = "total_cpu_millicores";
const TOTAL_MEMORY_BYTES: &str = "total_memory_bytes";
const CPU_USAGE_MILLICORES: &str = "cpu_usage_millicores";
const MEMORY_USAGE_BYTES: &str = "memory_usage_bytes";
const VERSION: &str = "version";
const GIT_COMMIT: &str = "git_commit";
const START_TIME: &str = "start_time";
const UPTIME: &str = "uptime";
const ACTIVE_TIME: &str = "active_time";
const NODE_STATUS: &str = "node_status";

const INIT_CAPACITY: usize = 42;

/// The `CLUSTER_INFO` table provides information about the current topology information of the cluster.
///
/// - `peer_id`: the peer server id.
/// - `peer_type`: the peer type, such as `datanode`, `frontend`, `metasrv` etc.
/// - `peer_addr`: the peer gRPC address.
/// - `peer_hostname`: the hostname of the peer.
/// - `total_cpu_millicores`: the total CPU millicores of the peer.
/// - `total_memory_bytes`: the total memory bytes of the peer.
/// - `cpu_usage_millicores`: the CPU usage millicores of the peer.
/// - `memory_usage_bytes`: the memory usage bytes of the peer.
/// - `version`: the build package version of the peer.
/// - `git_commit`: the build git commit hash of the peer.
/// - `start_time`: the starting time of the peer.
/// - `uptime`: the uptime of the peer.
/// - `active_time`: the time since the last activity of the peer.
/// - `node_status`: the status info of the peer.
///
#[derive(Debug)]
pub(super) struct InformationSchemaClusterInfo {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaClusterInfo {
    pub(super) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(PEER_ID, ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new(PEER_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(PEER_ADDR, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(PEER_HOSTNAME, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                TOTAL_CPU_MILLICORES,
                ConcreteDataType::int64_datatype(),
                false,
            ),
            ColumnSchema::new(
                TOTAL_MEMORY_BYTES,
                ConcreteDataType::int64_datatype(),
                false,
            ),
            ColumnSchema::new(
                CPU_USAGE_MILLICORES,
                ConcreteDataType::int64_datatype(),
                false,
            ),
            ColumnSchema::new(
                MEMORY_USAGE_BYTES,
                ConcreteDataType::int64_datatype(),
                false,
            ),
            ColumnSchema::new(VERSION, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(GIT_COMMIT, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                START_TIME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(UPTIME, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(ACTIVE_TIME, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(NODE_STATUS, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaClusterInfoBuilder {
        InformationSchemaClusterInfoBuilder::new(self.schema.clone(), self.catalog_manager.clone())
    }
}

impl InformationTable for InformationSchemaClusterInfo {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_CLUSTER_INFO_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        CLUSTER_INFO
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn to_stream(&self, request: ScanRequest) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        let stream = Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_cluster_info(Some(request))
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ));
        Ok(Box::pin(
            RecordBatchStreamAdapter::try_new(stream)
                .map_err(BoxedError::new)
                .context(InternalSnafu)?,
        ))
    }
}

struct InformationSchemaClusterInfoBuilder {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,

    peer_ids: Int64VectorBuilder,
    peer_types: StringVectorBuilder,
    peer_addrs: StringVectorBuilder,
    peer_hostnames: StringVectorBuilder,
    total_cpu_millicores: Int64VectorBuilder,
    total_memory_bytes: Int64VectorBuilder,
    cpu_usage_millicores: Int64VectorBuilder,
    memory_usage_bytes: Int64VectorBuilder,
    versions: StringVectorBuilder,
    git_commits: StringVectorBuilder,
    start_times: TimestampMillisecondVectorBuilder,
    uptimes: StringVectorBuilder,
    active_times: StringVectorBuilder,
    node_status: StringVectorBuilder,
}

impl InformationSchemaClusterInfoBuilder {
    fn new(schema: SchemaRef, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema,
            catalog_manager,
            peer_ids: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            peer_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            peer_addrs: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            peer_hostnames: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            total_cpu_millicores: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            total_memory_bytes: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            cpu_usage_millicores: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            memory_usage_bytes: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            versions: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            git_commits: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            start_times: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            uptimes: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            active_times: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            node_status: StringVectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.cluster_info` virtual table
    async fn make_cluster_info(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let predicates = Predicates::from_scan_request(&request);
        let information_extension = utils::information_extension(&self.catalog_manager)?;
        let node_infos = information_extension.nodes().await?;
        for node_info in node_infos {
            self.add_node_info(&predicates, node_info);
        }
        self.finish()
    }

    fn add_node_info(&mut self, predicates: &Predicates, node_info: NodeInfo) {
        let peer_type = node_info.status.role_name();
        let peer_id = peer_id(peer_type, node_info.peer.id);

        let row = [
            (PEER_ID, &Value::from(peer_id)),
            (PEER_TYPE, &Value::from(peer_type)),
            (PEER_ADDR, &Value::from(node_info.peer.addr.as_str())),
            (PEER_HOSTNAME, &Value::from(node_info.hostname.as_str())),
            (VERSION, &Value::from(node_info.version.as_str())),
            (GIT_COMMIT, &Value::from(node_info.git_commit.as_str())),
        ];

        if !predicates.eval(&row) {
            return;
        }

        self.peer_ids.push(Some(peer_id));
        self.peer_types.push(Some(peer_type));
        self.peer_addrs.push(Some(&node_info.peer.addr));
        self.peer_hostnames.push(Some(&node_info.hostname));
        self.versions.push(Some(&node_info.version));
        self.git_commits.push(Some(&node_info.git_commit));
        if node_info.start_time_ms > 0 {
            self.start_times
                .push(Some(TimestampMillisecond(Timestamp::new_millisecond(
                    node_info.start_time_ms as i64,
                ))));
            self.uptimes.push(Some(
                Self::format_duration_since(node_info.start_time_ms).as_str(),
            ));
        } else {
            self.start_times.push(None);
            self.uptimes.push(None);
        }
        self.total_cpu_millicores
            .push(Some(node_info.total_cpu_millicores));
        self.total_memory_bytes
            .push(Some(node_info.total_memory_bytes));
        self.cpu_usage_millicores
            .push(Some(node_info.cpu_usage_millicores));
        self.memory_usage_bytes
            .push(Some(node_info.memory_usage_bytes));

        if node_info.last_activity_ts > 0 {
            self.active_times.push(Some(
                Self::format_duration_since(node_info.last_activity_ts as u64).as_str(),
            ));
        } else {
            self.active_times.push(None);
        }
        self.node_status
            .push(format_node_status(&node_info).as_deref());
    }

    fn format_duration_since(ts: u64) -> String {
        let now = common_time::util::current_time_millis() as u64;
        let duration_since = now - ts;
        humantime::format_duration(Duration::from_millis(duration_since)).to_string()
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.peer_ids.finish()),
            Arc::new(self.peer_types.finish()),
            Arc::new(self.peer_addrs.finish()),
            Arc::new(self.peer_hostnames.finish()),
            Arc::new(self.total_cpu_millicores.finish()),
            Arc::new(self.total_memory_bytes.finish()),
            Arc::new(self.cpu_usage_millicores.finish()),
            Arc::new(self.memory_usage_bytes.finish()),
            Arc::new(self.versions.finish()),
            Arc::new(self.git_commits.finish()),
            Arc::new(self.start_times.finish()),
            Arc::new(self.uptimes.finish()),
            Arc::new(self.active_times.finish()),
            Arc::new(self.node_status.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaClusterInfo {
    fn schema(&self) -> &ArrowSchemaRef {
        self.schema.arrow_schema()
    }

    fn execute(&self, _: Arc<TaskContext>) -> DfSendableRecordBatchStream {
        let schema = self.schema.arrow_schema().clone();
        let mut builder = self.builder();
        Box::pin(DfRecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                builder
                    .make_cluster_info(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}

fn peer_id(peer_type: &str, peer_id: u64) -> i64 {
    if peer_type == PEER_TYPE_FRONTEND || peer_type == PEER_TYPE_METASRV {
        -1
    } else {
        peer_id as i64
    }
}

#[derive(Serialize)]
struct DisplayMetasrvStatus {
    is_leader: bool,
}

#[derive(Serialize)]
struct DisplayDatanodeStatus {
    workloads: Vec<DatanodeWorkloadType>,
    leader_regions: usize,
    follower_regions: usize,
}

impl From<&DatanodeStatus> for DisplayDatanodeStatus {
    fn from(status: &DatanodeStatus) -> Self {
        Self {
            workloads: status
                .workloads
                .types
                .iter()
                .flat_map(|w| DatanodeWorkloadType::from_i32(*w))
                .collect(),
            leader_regions: status.leader_regions,
            follower_regions: status.follower_regions,
        }
    }
}

fn format_node_status(node_info: &NodeInfo) -> Option<String> {
    match &node_info.status {
        NodeStatus::Datanode(datanode_status) => {
            serde_json::to_string(&DisplayDatanodeStatus::from(datanode_status)).ok()
        }
        NodeStatus::Frontend(_) => None,
        NodeStatus::Flownode(_) => None,
        NodeStatus::Metasrv(metasrv_status) => {
            if metasrv_status.is_leader {
                serde_json::to_string(&DisplayMetasrvStatus { is_leader: true }).ok()
            } else {
                None
            }
        }
        NodeStatus::Standalone => None,
    }
}
