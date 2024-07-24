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
use common_config::Mode;
use common_error::ext::BoxedError;
use common_meta::cluster::{ClusterInfo, NodeInfo, NodeStatus};
use common_meta::peer::Peer;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_telemetry::warn;
use common_time::timestamp::Timestamp;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::timestamp::TimestampMillisecond;
use datatypes::value::Value;
use datatypes::vectors::{
    Int64VectorBuilder, StringVectorBuilder, TimestampMillisecondVectorBuilder,
};
use snafu::ResultExt;
use store_api::storage::{ScanRequest, TableId};

use super::CLUSTER_INFO;
use crate::error::{CreateRecordBatchSnafu, InternalSnafu, ListNodesSnafu, Result};
use crate::system_schema::information_schema::{InformationTable, Predicates};
use crate::system_schema::utils;
use crate::CatalogManager;

const PEER_ID: &str = "peer_id";
const PEER_TYPE: &str = "peer_type";
const PEER_ADDR: &str = "peer_addr";
const VERSION: &str = "version";
const GIT_COMMIT: &str = "git_commit";
const START_TIME: &str = "start_time";
const UPTIME: &str = "uptime";
const ACTIVE_TIME: &str = "active_time";

const INIT_CAPACITY: usize = 42;

/// The `CLUSTER_INFO` table provides information about the current topology information of the cluster.
///
/// - `peer_id`: the peer server id.
/// - `peer_type`: the peer type, such as `datanode`, `frontend`, `metasrv` etc.
/// - `peer_addr`: the peer gRPC address.
/// - `version`: the build package version of the peer.
/// - `git_commit`: the build git commit hash of the peer.
/// - `start_time`: the starting time of the peer.
/// - `uptime`: the uptime of the peer.
/// - `active_time`: the time since the last activity of the peer.
///
pub(super) struct InformationSchemaClusterInfo {
    schema: SchemaRef,
    catalog_manager: Weak<dyn CatalogManager>,
    start_time_ms: u64,
}

impl InformationSchemaClusterInfo {
    pub(super) fn new(catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_manager,
            start_time_ms: common_time::util::current_time_millis() as u64,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(PEER_ID, ConcreteDataType::int64_datatype(), false),
            ColumnSchema::new(PEER_TYPE, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(PEER_ADDR, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(VERSION, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(GIT_COMMIT, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                START_TIME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new(UPTIME, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(ACTIVE_TIME, ConcreteDataType::string_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaClusterInfoBuilder {
        InformationSchemaClusterInfoBuilder::new(
            self.schema.clone(),
            self.catalog_manager.clone(),
            self.start_time_ms,
        )
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
    start_time_ms: u64,
    catalog_manager: Weak<dyn CatalogManager>,

    peer_ids: Int64VectorBuilder,
    peer_types: StringVectorBuilder,
    peer_addrs: StringVectorBuilder,
    versions: StringVectorBuilder,
    git_commits: StringVectorBuilder,
    start_times: TimestampMillisecondVectorBuilder,
    uptimes: StringVectorBuilder,
    active_times: StringVectorBuilder,
}

impl InformationSchemaClusterInfoBuilder {
    fn new(
        schema: SchemaRef,
        catalog_manager: Weak<dyn CatalogManager>,
        start_time_ms: u64,
    ) -> Self {
        Self {
            schema,
            catalog_manager,
            peer_ids: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
            peer_types: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            peer_addrs: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            versions: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            git_commits: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            start_times: TimestampMillisecondVectorBuilder::with_capacity(INIT_CAPACITY),
            uptimes: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            active_times: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            start_time_ms,
        }
    }

    /// Construct the `information_schema.cluster_info` virtual table
    async fn make_cluster_info(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let predicates = Predicates::from_scan_request(&request);
        let mode = utils::running_mode(&self.catalog_manager)?.unwrap_or(Mode::Standalone);

        match mode {
            Mode::Standalone => {
                let build_info = common_version::build_info();

                self.add_node_info(
                    &predicates,
                    NodeInfo {
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
                    },
                );
            }
            Mode::Distributed => {
                if let Some(meta_client) = utils::meta_client(&self.catalog_manager)? {
                    let node_infos = meta_client
                        .list_nodes(None)
                        .await
                        .map_err(BoxedError::new)
                        .context(ListNodesSnafu)?;

                    for node_info in node_infos {
                        self.add_node_info(&predicates, node_info);
                    }
                } else {
                    warn!("Could not find meta client in distributed mode.");
                }
            }
        }

        self.finish()
    }

    fn add_node_info(&mut self, predicates: &Predicates, node_info: NodeInfo) {
        let peer_type = node_info.status.role_name();

        let row = [
            (PEER_ID, &Value::from(node_info.peer.id)),
            (PEER_TYPE, &Value::from(peer_type)),
            (PEER_ADDR, &Value::from(node_info.peer.addr.as_str())),
            (VERSION, &Value::from(node_info.version.as_str())),
            (GIT_COMMIT, &Value::from(node_info.git_commit.as_str())),
        ];

        if !predicates.eval(&row) {
            return;
        }

        if peer_type == "FRONTEND" || peer_type == "METASRV" {
            // Always set peer_id to be -1 for frontends and metasrvs
            self.peer_ids.push(Some(-1));
        } else {
            self.peer_ids.push(Some(node_info.peer.id as i64));
        }

        self.peer_types.push(Some(peer_type));
        self.peer_addrs.push(Some(&node_info.peer.addr));
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

        if node_info.last_activity_ts > 0 {
            self.active_times.push(Some(
                Self::format_duration_since(node_info.last_activity_ts as u64).as_str(),
            ));
        } else {
            self.active_times.push(None);
        }
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
            Arc::new(self.versions.finish()),
            Arc::new(self.git_commits.finish()),
            Arc::new(self.start_times.finish()),
            Arc::new(self.uptimes.finish()),
            Arc::new(self.active_times.finish()),
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
