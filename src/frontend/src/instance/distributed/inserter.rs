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

use std::collections::HashMap;

use api::v1::region::{region_request, InsertRequests, RegionRequest, RegionRequestHeader};
use common_meta::datanode_manager::DatanodeManager;
use common_meta::peer::Peer;
use futures_util::future;
use metrics::counter;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    FindDatanodeSnafu, FindTableRouteSnafu, JoinTaskSnafu, RequestInsertsSnafu, Result,
    SplitInsertSnafu,
};

/// A distributed inserter. It ingests gRPC [InsertRequests].
///
/// Table data partitioning and Datanode requests batching are handled inside.
pub struct DistInserter<'a> {
    catalog_manager: &'a FrontendCatalogManager,
    trace_id: Option<u64>,
    span_id: Option<u64>,
}

impl<'a> DistInserter<'a> {
    pub fn new(catalog_manager: &'a FrontendCatalogManager) -> Self {
        Self {
            catalog_manager,
            trace_id: None,
            span_id: None,
        }
    }

    pub fn with_trace_id(mut self, trace_id: u64) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    #[allow(dead_code)]
    pub fn with_span_id(mut self, span_id: u64) -> Self {
        self.span_id = Some(span_id);
        self
    }

    pub(crate) async fn insert(&self, requests: InsertRequests) -> Result<u64> {
        let requests = self.split(requests).await?;
        let trace_id = self.trace_id.unwrap_or_default();
        let span_id = self.span_id.unwrap_or_default();
        let results = future::try_join_all(requests.into_iter().map(|(peer, inserts)| {
            let datanode_clients = self.catalog_manager.datanode_clients();
            common_runtime::spawn_write(async move {
                let request = RegionRequest {
                    header: Some(RegionRequestHeader { trace_id, span_id }),
                    body: Some(region_request::Body::Inserts(inserts)),
                };
                datanode_clients
                    .datanode(&peer)
                    .await
                    .handle(request)
                    .await
                    .context(RequestInsertsSnafu)
            })
        }))
        .await
        .context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u64>>()?;
        counter!(crate::metrics::DIST_INGEST_ROW_COUNT, affected_rows);
        Ok(affected_rows)
    }

    /// Splits gRPC [InsertRequests] into multiple gRPC [InsertRequests]s, each of which
    /// is grouped by the peer of Datanode, so we can batch them together when invoking gRPC write
    /// method in Datanode.
    async fn split(&self, requests: InsertRequests) -> Result<HashMap<Peer, InsertRequests>> {
        let partition_manager = self.catalog_manager.partition_manager();
        let mut inserts: HashMap<Peer, InsertRequests> = HashMap::new();

        for req in requests.requests {
            let table_id = RegionId::from_u64(req.region_id).table_id();

            let req_splits = partition_manager
                .split_insert_request(table_id, req)
                .await
                .context(SplitInsertSnafu)?;
            let table_route = partition_manager
                .find_table_route(table_id)
                .await
                .context(FindTableRouteSnafu { table_id })?;

            for (region_number, insert) in req_splits {
                let peer =
                    table_route
                        .find_region_leader(region_number)
                        .context(FindDatanodeSnafu {
                            region: region_number,
                        })?;
                inserts
                    .entry(peer.clone())
                    .or_default()
                    .requests
                    .push(insert);
            }
        }

        Ok(inserts)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::helper::vectors_to_rows;
    use api::v1::region::InsertRequest;
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema, Row, Rows, SemanticType, Value};
    use client::client_manager::DatanodeClients;
    use common_meta::key::catalog_name::{CatalogManager, CatalogNameKey};
    use common_meta::key::schema_name::{SchemaManager, SchemaNameKey};
    use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::KvBackendRef;
    use common_meta::rpc::router::{Region, RegionRoute};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema as DtColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use table::metadata::{RawTableInfo, TableInfoBuilder, TableMetaBuilder};

    use super::*;
    use crate::heartbeat::handler::tests::MockKvCacheInvalidator;
    use crate::table::test::create_partition_rule_manager;

    async fn prepare_mocked_backend() -> KvBackendRef {
        let backend = Arc::new(MemoryKvBackend::default());

        let catalog_manager = CatalogManager::new(backend.clone());
        let schema_manager = SchemaManager::new(backend.clone());

        catalog_manager
            .create(CatalogNameKey::default())
            .await
            .unwrap();
        schema_manager
            .create(SchemaNameKey::default(), None)
            .await
            .unwrap();

        backend
    }

    async fn create_testing_table(
        table_name: &str,
        table_metadata_manager: &TableMetadataManagerRef,
    ) {
        let schema = Arc::new(Schema::new(vec![
            DtColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
                .with_time_index(true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                    "current_timestamp()".to_string(),
                )))
                .unwrap(),
            DtColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
        ]));

        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![])
            .next_column_id(1)
            .build()
            .unwrap();

        let table_id = 1;
        let table_info: RawTableInfo = TableInfoBuilder::new(table_name, table_meta)
            .table_id(table_id)
            .build()
            .unwrap()
            .into();

        let region_route_factory = |region_id: u64, peer: u64| RegionRoute {
            region: Region {
                id: region_id.into(),
                ..Default::default()
            },
            leader_peer: Some(Peer {
                id: peer,
                addr: String::new(),
            }),
            follower_peers: vec![],
        };

        let region_routes = vec![
            region_route_factory(1, 1),
            region_route_factory(2, 2),
            region_route_factory(3, 3),
        ];
        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_split_inserts() {
        let backend = prepare_mocked_backend().await;

        let table_metadata_manager = Arc::new(TableMetadataManager::new(backend.clone()));
        let table_name = "one_column_partitioning_table";
        create_testing_table(table_name, &table_metadata_manager).await;

        let catalog_manager = Arc::new(FrontendCatalogManager::new(
            backend,
            Arc::new(MockKvCacheInvalidator::default()),
            create_partition_rule_manager().await,
            Arc::new(DatanodeClients::default()),
            table_metadata_manager,
        ));

        let inserter = DistInserter::new(&catalog_manager);

        let new_insert_request = |vector: VectorRef| -> InsertRequest {
            let row_count = vector.len();
            InsertRequest {
                region_id: RegionId::new(1, 0).into(),
                rows: Some(Rows {
                    schema: vec![ColumnSchema {
                        column_name: "a".to_string(),
                        datatype: ColumnDataType::Int32 as i32,
                        semantic_type: SemanticType::Field as i32,
                    }],
                    rows: vectors_to_rows([vector].iter(), row_count),
                }),
            }
        };

        let requests = InsertRequests {
            requests: vec![
                new_insert_request(Arc::new(Int32Vector::from(vec![
                    Some(1),
                    None,
                    Some(11),
                    Some(101),
                ]))),
                new_insert_request(Arc::new(Int32Vector::from(vec![
                    Some(2),
                    Some(12),
                    None,
                    Some(102),
                ]))),
            ],
        };

        let mut inserts = inserter.split(requests).await.unwrap();

        assert_eq!(inserts.len(), 3);

        let new_split_insert_request =
            |rows: Vec<Option<i32>>, region_id: RegionId| -> InsertRequest {
                InsertRequest {
                    region_id: region_id.into(),
                    rows: Some(Rows {
                        schema: vec![ColumnSchema {
                            column_name: "a".to_string(),
                            datatype: ColumnDataType::Int32 as i32,
                            semantic_type: SemanticType::Field as i32,
                        }],
                        rows: rows
                            .into_iter()
                            .map(|v| Row {
                                values: vec![Value {
                                    value_data: v.map(ValueData::I32Value),
                                }],
                            })
                            .collect(),
                    }),
                }
            };

        // region to datanode placement:
        // 1 -> 1
        // 2 -> 2
        // 3 -> 3
        //
        // region value ranges:
        // 1 -> [50, max)
        // 2 -> [10, 50)
        // 3 -> (min, 10)

        let datanode_inserts = inserts.remove(&Peer::new(1, "")).unwrap().requests;
        assert_eq!(datanode_inserts.len(), 2);
        assert_eq!(
            datanode_inserts[0],
            new_split_insert_request(vec![Some(101)], RegionId::new(1, 1))
        );
        assert_eq!(
            datanode_inserts[1],
            new_split_insert_request(vec![Some(102)], RegionId::new(1, 1))
        );

        let datanode_inserts = inserts.remove(&Peer::new(2, "")).unwrap().requests;
        assert_eq!(datanode_inserts.len(), 2);
        assert_eq!(
            datanode_inserts[0],
            new_split_insert_request(vec![Some(11)], RegionId::new(1, 2))
        );
        assert_eq!(
            datanode_inserts[1],
            new_split_insert_request(vec![Some(12)], RegionId::new(1, 2))
        );

        let datanode_inserts = inserts.remove(&Peer::new(3, "")).unwrap().requests;
        assert_eq!(datanode_inserts.len(), 2);
        assert_eq!(
            datanode_inserts[0],
            new_split_insert_request(vec![Some(1), None], RegionId::new(1, 3))
        );
        assert_eq!(
            datanode_inserts[1],
            new_split_insert_request(vec![Some(2), None], RegionId::new(1, 3))
        );
    }
}
