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

use core::pin::pin;
use std::sync::{Arc, Weak};

use arrow_schema::SchemaRef as ArrowSchemaRef;
use common_catalog::consts::INFORMATION_SCHEMA_REGION_PEERS_TABLE_ID;
use common_error::ext::BoxedError;
use common_meta::rpc::router::RegionRoute;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion::common::HashMap;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter as DfRecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream as DfPartitionStream;
use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::value::Value;
use datatypes::vectors::{Int64VectorBuilder, StringVectorBuilder, UInt64VectorBuilder};
use futures::{StreamExt, TryStreamExt};
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, ScanRequest, TableId};
use table::metadata::TableType;

use super::REGION_PEERS;
use crate::error::{
    CreateRecordBatchSnafu, FindRegionRoutesSnafu, InternalSnafu, Result,
    UpgradeWeakCatalogManagerRefSnafu,
};
use crate::kvbackend::KvBackendCatalogManager;
use crate::system_schema::information_schema::{InformationTable, Predicates};
use crate::CatalogManager;

pub const TABLE_CATALOG: &str = "table_catalog";
pub const TABLE_SCHEMA: &str = "table_schema";
pub const TABLE_NAME: &str = "table_name";
pub const REGION_ID: &str = "region_id";
pub const PEER_ID: &str = "peer_id";
const PEER_ADDR: &str = "peer_addr";
pub const IS_LEADER: &str = "is_leader";
const STATUS: &str = "status";
const DOWN_SECONDS: &str = "down_seconds";
const INIT_CAPACITY: usize = 42;

/// The `REGION_PEERS` table provides information about the region distribution and routes. Including fields:
///
/// - `table_catalog`: the table catalog name
/// - `table_schema`: the table schema name
/// - `table_name`: the table name
/// - `region_id`: the region id
/// - `peer_id`: the region storage datanode peer id
/// - `peer_addr`: the region storage datanode gRPC peer address
/// - `is_leader`: whether the peer is the leader
/// - `status`: the region status, `ALIVE` or `DOWNGRADED`.
/// - `down_seconds`: the duration of being offline, in seconds.
#[derive(Debug)]
pub(super) struct InformationSchemaRegionPeers {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,
}

impl InformationSchemaRegionPeers {
    pub(super) fn new(catalog_name: String, catalog_manager: Weak<dyn CatalogManager>) -> Self {
        Self {
            schema: Self::schema(),
            catalog_name,
            catalog_manager,
        }
    }

    pub(crate) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ColumnSchema::new(TABLE_CATALOG, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_SCHEMA, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(TABLE_NAME, ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(REGION_ID, ConcreteDataType::uint64_datatype(), false),
            ColumnSchema::new(PEER_ID, ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new(PEER_ADDR, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(IS_LEADER, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(STATUS, ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(DOWN_SECONDS, ConcreteDataType::int64_datatype(), true),
        ]))
    }

    fn builder(&self) -> InformationSchemaRegionPeersBuilder {
        InformationSchemaRegionPeersBuilder::new(
            self.schema.clone(),
            self.catalog_name.clone(),
            self.catalog_manager.clone(),
        )
    }
}

impl InformationTable for InformationSchemaRegionPeers {
    fn table_id(&self) -> TableId {
        INFORMATION_SCHEMA_REGION_PEERS_TABLE_ID
    }

    fn table_name(&self) -> &'static str {
        REGION_PEERS
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
                    .make_region_peers(Some(request))
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

struct InformationSchemaRegionPeersBuilder {
    schema: SchemaRef,
    catalog_name: String,
    catalog_manager: Weak<dyn CatalogManager>,

    table_catalogs: StringVectorBuilder,
    table_schemas: StringVectorBuilder,
    table_names: StringVectorBuilder,
    region_ids: UInt64VectorBuilder,
    peer_ids: UInt64VectorBuilder,
    peer_addrs: StringVectorBuilder,
    is_leaders: StringVectorBuilder,
    statuses: StringVectorBuilder,
    down_seconds: Int64VectorBuilder,
}

impl InformationSchemaRegionPeersBuilder {
    fn new(
        schema: SchemaRef,
        catalog_name: String,
        catalog_manager: Weak<dyn CatalogManager>,
    ) -> Self {
        Self {
            schema,
            catalog_name,
            catalog_manager,
            table_catalogs: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_schemas: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            table_names: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            region_ids: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            peer_ids: UInt64VectorBuilder::with_capacity(INIT_CAPACITY),
            peer_addrs: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            is_leaders: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            statuses: StringVectorBuilder::with_capacity(INIT_CAPACITY),
            down_seconds: Int64VectorBuilder::with_capacity(INIT_CAPACITY),
        }
    }

    /// Construct the `information_schema.region_peers` virtual table
    async fn make_region_peers(&mut self, request: Option<ScanRequest>) -> Result<RecordBatch> {
        let catalog_name = self.catalog_name.clone();
        let catalog_manager = self
            .catalog_manager
            .upgrade()
            .context(UpgradeWeakCatalogManagerRefSnafu)?;

        let partition_manager = catalog_manager
            .as_any()
            .downcast_ref::<KvBackendCatalogManager>()
            .map(|catalog_manager| catalog_manager.partition_manager());

        let predicates = Predicates::from_scan_request(&request);

        for schema_name in catalog_manager.schema_names(&catalog_name, None).await? {
            let table_id_stream = catalog_manager
                .tables(&catalog_name, &schema_name, None)
                .try_filter_map(|t| async move {
                    let table_info = t.table_info();
                    if table_info.table_type == TableType::Temporary {
                        Ok(None)
                    } else {
                        Ok(Some((
                            table_info.ident.table_id,
                            table_info.name.to_string(),
                        )))
                    }
                });

            const BATCH_SIZE: usize = 128;

            // Split table ids into chunks
            let mut table_id_chunks = pin!(table_id_stream.ready_chunks(BATCH_SIZE));

            while let Some(table_ids) = table_id_chunks.next().await {
                let tables = table_ids.into_iter().collect::<Result<HashMap<_, _>>>()?;
                let table_ids = tables.keys().cloned().collect::<Vec<_>>();

                let table_routes = if let Some(partition_manager) = &partition_manager {
                    partition_manager
                        .batch_find_region_routes(&table_ids)
                        .await
                        .context(FindRegionRoutesSnafu)?
                } else {
                    table_ids.into_iter().map(|id| (id, vec![])).collect()
                };

                for (table_id, routes) in table_routes {
                    // Safety: table_id is guaranteed to be in the map
                    let table_name = tables.get(&table_id).unwrap();
                    self.add_region_peers(
                        &catalog_name,
                        &schema_name,
                        table_name,
                        &predicates,
                        table_id,
                        &routes,
                    );
                }
            }
        }

        self.finish()
    }

    fn add_region_peers(
        &mut self,
        table_catalog: &str,
        table_schema: &str,
        table_name: &str,
        predicates: &Predicates,
        table_id: TableId,
        routes: &[RegionRoute],
    ) {
        for route in routes {
            let region_id = RegionId::new(table_id, route.region.id.region_number()).as_u64();
            let peer_id = route.leader_peer.clone().map(|p| p.id);
            let peer_addr = route.leader_peer.clone().map(|p| p.addr);
            let state = if let Some(state) = route.leader_state {
                Some(state.as_ref().to_string())
            } else {
                // Alive by default
                Some("ALIVE".to_string())
            };

            let row = [
                (TABLE_CATALOG, &Value::from(table_catalog)),
                (TABLE_SCHEMA, &Value::from(table_schema)),
                (TABLE_NAME, &Value::from(table_name)),
                (REGION_ID, &Value::from(region_id)),
            ];

            if !predicates.eval(&row) {
                return;
            }

            self.table_catalogs.push(Some(table_catalog));
            self.table_schemas.push(Some(table_schema));
            self.table_names.push(Some(table_name));
            self.region_ids.push(Some(region_id));
            self.peer_ids.push(peer_id);
            self.peer_addrs.push(peer_addr.as_deref());
            self.is_leaders.push(Some("Yes"));
            self.statuses.push(state.as_deref());
            self.down_seconds
                .push(route.leader_down_millis().map(|m| m / 1000));

            for follower in &route.follower_peers {
                self.table_catalogs.push(Some(table_catalog));
                self.table_schemas.push(Some(table_schema));
                self.table_names.push(Some(table_name));
                self.region_ids.push(Some(region_id));
                self.peer_ids.push(Some(follower.id));
                self.peer_addrs.push(Some(follower.addr.as_str()));
                self.is_leaders.push(Some("No"));
                self.statuses.push(None);
                self.down_seconds.push(None);
            }
        }
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let columns: Vec<VectorRef> = vec![
            Arc::new(self.table_catalogs.finish()),
            Arc::new(self.table_schemas.finish()),
            Arc::new(self.table_names.finish()),
            Arc::new(self.region_ids.finish()),
            Arc::new(self.peer_ids.finish()),
            Arc::new(self.peer_addrs.finish()),
            Arc::new(self.is_leaders.finish()),
            Arc::new(self.statuses.finish()),
            Arc::new(self.down_seconds.finish()),
        ];
        RecordBatch::new(self.schema.clone(), columns).context(CreateRecordBatchSnafu)
    }
}

impl DfPartitionStream for InformationSchemaRegionPeers {
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
                    .make_region_peers(None)
                    .await
                    .map(|x| x.into_df_record_batch())
                    .map_err(Into::into)
            }),
        ))
    }
}
