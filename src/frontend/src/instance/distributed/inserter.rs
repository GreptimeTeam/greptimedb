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
use std::sync::Arc;

use api::v1::InsertRequests;
use catalog::CatalogManager;
use client::Database;
use common_grpc_expr::insert::to_table_insert_request;
use common_meta::peer::Peer;
use common_meta::table_name::TableName;
use futures::future;
use metrics::counter;
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableInfoRef;
use table::meter_insert_request;
use table::requests::InsertRequest;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    CatalogSnafu, FindDatanodeSnafu, FindTableRouteSnafu, JoinTaskSnafu, NotSupportedSnafu,
    RequestDatanodeSnafu, Result, SplitInsertSnafu, TableNotFoundSnafu, ToTableInsertRequestSnafu,
};
use crate::table::insert::to_grpc_insert_request;

/// A distributed inserter. It ingests GRPC [InsertRequests] or table [InsertRequest] (so it can be
/// used in protocol handlers or table insertion API).
///
/// Table data partitioning and Datanode requests batching are handled inside.
///
/// Note that the inserter is confined to a single catalog and schema. I.e., it cannot handle
/// multiple insert requests with different catalog or schema (will throw "NotSupported" error).
/// This is because we currently do not have this kind of requirements. Let's keep it simple for now.
pub(crate) struct DistInserter {
    catalog: String,
    schema: String,
    catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistInserter {
    pub(crate) fn new(
        catalog: String,
        schema: String,
        catalog_manager: Arc<FrontendCatalogManager>,
    ) -> Self {
        Self {
            catalog,
            schema,
            catalog_manager,
        }
    }

    pub(crate) async fn grpc_insert(&self, requests: InsertRequests) -> Result<u32> {
        let inserts = requests
            .inserts
            .into_iter()
            .map(|x| {
                to_table_insert_request(&self.catalog, &self.schema, x)
                    .context(ToTableInsertRequestSnafu)
            })
            .collect::<Result<Vec<_>>>()?;

        self.insert(inserts).await
    }

    pub(crate) async fn insert(&self, requests: Vec<InsertRequest>) -> Result<u32> {
        ensure!(
            requests
                .iter()
                .all(|x| x.catalog_name == self.catalog && x.schema_name == self.schema),
            NotSupportedSnafu {
                feat: "insert requests with different catalog or schema",
            }
        );

        let inserts = self.split_inserts(requests).await?;

        self.request_datanodes(inserts).await
    }

    /// Splits multiple table [InsertRequest]s into multiple GRPC [InsertRequests]s, each of which
    /// is grouped by the peer of Datanode, so we can batch them together when invoking gRPC write
    /// method in Datanode.
    async fn split_inserts(
        &self,
        requests: Vec<InsertRequest>,
    ) -> Result<HashMap<Peer, InsertRequests>> {
        let partition_manager = self.catalog_manager.partition_manager();

        let mut inserts = HashMap::new();

        for request in requests {
            meter_insert_request!(request);

            let table_name = TableName::new(&self.catalog, &self.schema, &request.table_name);
            let table_info = self.find_table_info(&request.table_name).await?;
            let table_meta = &table_info.meta;

            let split = partition_manager
                .split_insert_request(&table_name, request, table_meta.schema.as_ref())
                .await
                .context(SplitInsertSnafu)?;

            let table_route = partition_manager
                .find_table_route(&table_name)
                .await
                .with_context(|_| FindTableRouteSnafu {
                    table_name: table_name.to_string(),
                })?;

            for (region_number, insert) in split {
                let datanode =
                    table_route
                        .find_region_leader(region_number)
                        .context(FindDatanodeSnafu {
                            region: region_number,
                        })?;

                let insert = to_grpc_insert_request(table_meta, region_number, insert)?;

                inserts
                    .entry(datanode.clone())
                    .or_insert_with(|| InsertRequests { inserts: vec![] })
                    .inserts
                    .push(insert);
            }
        }
        Ok(inserts)
    }

    async fn find_table_info(&self, table_name: &str) -> Result<TableInfoRef> {
        let table = self
            .catalog_manager
            .table(&self.catalog, &self.schema, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: common_catalog::format_full_table_name(
                    &self.catalog,
                    &self.schema,
                    table_name,
                ),
            })?;
        Ok(table.table_info())
    }

    async fn request_datanodes(&self, inserts: HashMap<Peer, InsertRequests>) -> Result<u32> {
        let results = future::try_join_all(inserts.into_iter().map(|(peer, inserts)| {
            let datanode_clients = self.catalog_manager.datanode_clients();
            let catalog = self.catalog.clone();
            let schema = self.schema.clone();

            common_runtime::spawn_write(async move {
                let client = datanode_clients.get_client(&peer).await;
                let database = Database::new(&catalog, &schema, client);
                database.insert(inserts).await.context(RequestDatanodeSnafu)
            })
        }))
        .await
        .context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u32>>()?;
        counter!(crate::metrics::DIST_INGEST_ROW_COUNT, affected_rows as u64);
        Ok(affected_rows)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::column::{SemanticType, Values};
    use api::v1::{Column, ColumnDataType, InsertRequest as GrpcInsertRequest};
    use catalog::helper::{
        CatalogKey, CatalogValue, SchemaKey, SchemaValue, TableGlobalKey, TableGlobalValue,
    };
    use catalog::remote::mock::MockKvBackend;
    use catalog::remote::{KvBackend, KvBackendRef};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use super::*;
    use crate::datanode::DatanodeClients;
    use crate::heartbeat::handler::tests::MockKvCacheInvalidator;
    use crate::table::test::create_partition_rule_manager;

    async fn prepare_mocked_backend() -> KvBackendRef {
        let backend = Arc::new(MockKvBackend::default());

        let default_catalog = CatalogKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        }
        .to_string();
        backend
            .set(
                default_catalog.as_bytes(),
                CatalogValue.as_bytes().unwrap().as_slice(),
            )
            .await
            .unwrap();

        let default_schema = SchemaKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        }
        .to_string();
        backend
            .set(
                default_schema.as_bytes(),
                SchemaValue.as_bytes().unwrap().as_slice(),
            )
            .await
            .unwrap();

        backend
    }

    async fn create_testing_table(backend: &KvBackendRef, table_name: &str) {
        let table_global_key = TableGlobalKey {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
        };

        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
                .with_time_index(true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                    "current_timestamp()".to_string(),
                )))
                .unwrap(),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
        ]));

        let mut builder = TableMetaBuilder::default();
        builder.schema(schema);
        builder.primary_key_indices(vec![]);
        builder.next_column_id(1);
        let table_meta = builder.build().unwrap();

        let table_info = TableInfoBuilder::new(table_name, table_meta)
            .build()
            .unwrap();

        let table_global_value = TableGlobalValue {
            node_id: 1,
            regions_id_map: HashMap::from([(1, vec![1]), (2, vec![2]), (3, vec![3])]),
            table_info: table_info.into(),
        };

        backend
            .set(
                table_global_key.to_string().as_bytes(),
                table_global_value.as_bytes().unwrap().as_slice(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_split_inserts() {
        let backend = prepare_mocked_backend().await;

        let table_name = "one_column_partitioning_table";
        create_testing_table(&backend, table_name).await;

        let catalog_manager = Arc::new(FrontendCatalogManager::new(
            backend,
            Arc::new(MockKvCacheInvalidator::default()),
            create_partition_rule_manager().await,
            Arc::new(DatanodeClients::default()),
        ));

        let inserter = DistInserter::new(
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            catalog_manager,
        );

        let new_insert_request = |vector: VectorRef| -> InsertRequest {
            InsertRequest {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: table_name.to_string(),
                columns_values: HashMap::from([("a".to_string(), vector)]),
                region_number: 0,
            }
        };
        let requests = vec![
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
        ];

        let mut inserts = inserter.split_inserts(requests).await.unwrap();
        assert_eq!(inserts.len(), 3);

        let new_grpc_insert_request = |column_values: Vec<i32>,
                                       null_mask: Vec<u8>,
                                       row_count: u32,
                                       region_number: u32|
         -> GrpcInsertRequest {
            GrpcInsertRequest {
                table_name: table_name.to_string(),
                columns: vec![Column {
                    column_name: "a".to_string(),
                    semantic_type: SemanticType::Field as i32,
                    values: Some(Values {
                        i32_values: column_values,
                        ..Default::default()
                    }),
                    null_mask,
                    datatype: ColumnDataType::Int32 as i32,
                }],
                row_count,
                region_number,
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

        let datanode_inserts = inserts.remove(&Peer::new(1, "")).unwrap().inserts;
        assert_eq!(datanode_inserts.len(), 2);
        assert_eq!(
            datanode_inserts[0],
            new_grpc_insert_request(vec![101], vec![0], 1, 1)
        );
        assert_eq!(
            datanode_inserts[1],
            new_grpc_insert_request(vec![102], vec![0], 1, 1)
        );

        let datanode_inserts = inserts.remove(&Peer::new(2, "")).unwrap().inserts;
        assert_eq!(datanode_inserts.len(), 2);
        assert_eq!(
            datanode_inserts[0],
            new_grpc_insert_request(vec![11], vec![0], 1, 2)
        );
        assert_eq!(
            datanode_inserts[1],
            new_grpc_insert_request(vec![12], vec![0], 1, 2)
        );

        let datanode_inserts = inserts.remove(&Peer::new(3, "")).unwrap().inserts;
        assert_eq!(datanode_inserts.len(), 2);
        assert_eq!(
            datanode_inserts[0],
            new_grpc_insert_request(vec![1], vec![2], 2, 3)
        );
        assert_eq!(
            datanode_inserts[1],
            new_grpc_insert_request(vec![2], vec![2], 2, 3)
        );
    }
}
