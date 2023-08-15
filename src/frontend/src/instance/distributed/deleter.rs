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
use std::iter;
use std::sync::Arc;

use api::v1::DeleteRequests;
use catalog::CatalogManager;
use client::Database;
use common_grpc_expr::delete::to_table_delete_request;
use common_meta::peer::Peer;
use common_meta::table_name::TableName;
use futures::future;
use snafu::{OptionExt, ResultExt};
use table::requests::DeleteRequest;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    CatalogSnafu, FindDatanodeSnafu, FindTableRouteSnafu, JoinTaskSnafu,
    MissingTimeIndexColumnSnafu, RequestDatanodeSnafu, Result, SplitDeleteSnafu,
    TableNotFoundSnafu, ToTableDeleteRequestSnafu,
};
use crate::table::delete::to_grpc_delete_request;

/// A distributed deleter. It ingests GRPC [DeleteRequests] or table [DeleteRequest] (so it can be
/// used in protocol handlers or table deletion API).
///
/// Table data partitioning and Datanode requests batching are handled inside.
///
/// Note that the deleter is confined to a single catalog and schema. I.e., it cannot handle
/// multiple deletes requests with different catalog or schema (will throw "NotSupported" error).
/// This is because we currently do not have this kind of requirements. Let's keep it simple for now.
pub(crate) struct DistDeleter {
    catalog: String,
    schema: String,
    catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistDeleter {
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

    pub async fn grpc_delete(&self, requests: DeleteRequests) -> Result<usize> {
        let deletes = requests
            .deletes
            .into_iter()
            .map(|delete| {
                to_table_delete_request(&self.catalog, &self.schema, delete)
                    .context(ToTableDeleteRequestSnafu)
            })
            .collect::<Result<Vec<_>>>()?;
        self.delete(deletes).await
    }

    pub(crate) async fn delete(&self, requests: Vec<DeleteRequest>) -> Result<usize> {
        debug_assert!(requests
            .iter()
            .all(|x| x.catalog_name == self.catalog && x.schema_name == self.schema));
        let deletes = self.split_deletes(requests).await?;
        self.request_datanodes(deletes).await
    }

    async fn split_deletes(
        &self,
        requests: Vec<DeleteRequest>,
    ) -> Result<HashMap<Peer, DeleteRequests>> {
        let partition_manager = self.catalog_manager.partition_manager();

        let mut deletes = HashMap::new();

        for request in requests {
            let table_name = &request.table_name;
            let table = self
                .catalog_manager
                .table(&self.catalog, &self.schema, table_name)
                .await
                .context(CatalogSnafu)?
                .with_context(|| TableNotFoundSnafu {
                    table_name: table_name.to_string(),
                })?;
            let table_info = table.table_info();
            let table_meta = &table_info.meta;

            let table_id = table_info.table_id();
            let table_name = &request.table_name;
            let schema = table.schema();
            let time_index = &schema
                .timestamp_column()
                .with_context(|| table::error::MissingTimeIndexColumnSnafu {
                    table_name: table_name.to_string(),
                })
                .context(MissingTimeIndexColumnSnafu)?
                .name;
            let primary_key_column_names = table_info
                .meta
                .row_key_column_names()
                .chain(iter::once(time_index))
                .collect::<Vec<_>>();
            let table_name = request.table_name.clone();
            let split = partition_manager
                .split_delete_request(table_id, request, primary_key_column_names)
                .await
                .context(SplitDeleteSnafu)?;
            let table_route = partition_manager
                .find_table_route(table_id)
                .await
                .with_context(|_| FindTableRouteSnafu {
                    table_name: table_name.to_string(),
                })?;

            for (region_number, delete) in split {
                let datanode =
                    table_route
                        .find_region_leader(region_number)
                        .context(FindDatanodeSnafu {
                            region: region_number,
                        })?;
                let table_name = TableName::new(&self.catalog, &self.schema, &table_name);
                let delete =
                    to_grpc_delete_request(table_meta, &table_name, region_number, delete)?;
                deletes
                    .entry(datanode.clone())
                    .or_insert_with(|| DeleteRequests { deletes: vec![] })
                    .deletes
                    .push(delete);
            }
        }
        Ok(deletes)
    }

    async fn request_datanodes(&self, deletes: HashMap<Peer, DeleteRequests>) -> Result<usize> {
        let results = future::try_join_all(deletes.into_iter().map(|(peer, deletes)| {
            let datanode_clients = self.catalog_manager.datanode_clients();
            let catalog = self.catalog.clone();
            let schema = self.schema.clone();

            common_runtime::spawn_write(async move {
                let client = datanode_clients.get_client(&peer).await;
                let database = Database::new(&catalog, &schema, client);
                database.delete(deletes).await.context(RequestDatanodeSnafu)
            })
        }))
        .await
        .context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u32>>()?;
        Ok(affected_rows as usize)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::column::Values;
    use api::v1::{Column, ColumnDataType, DeleteRequest as GrpcDeleteRequest, SemanticType};
    use client::client_manager::DatanodeClients;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::helper::{CatalogValue, SchemaValue};
    use common_meta::key::catalog_name::CatalogNameKey;
    use common_meta::key::schema_name::SchemaNameKey;
    use common_meta::key::table_name::TableNameKey;
    use common_meta::key::table_region::RegionDistribution;
    use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::{KvBackend, KvBackendRef};
    use common_meta::rpc::store::PutRequest;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use table::metadata::{RawTableInfo, TableInfoBuilder, TableMetaBuilder};

    use super::*;
    use crate::heartbeat::handler::tests::MockKvCacheInvalidator;
    use crate::table::test::create_partition_rule_manager;

    async fn prepare_mocked_backend() -> KvBackendRef {
        let backend = Arc::new(MemoryKvBackend::default());

        let default_catalog = CatalogNameKey {
            catalog: DEFAULT_CATALOG_NAME,
        }
        .to_string();
        let req = PutRequest::new()
            .with_key(default_catalog.as_bytes())
            .with_value(CatalogValue.as_bytes().unwrap());
        backend.put(req).await.unwrap();

        let default_schema = SchemaNameKey {
            catalog: DEFAULT_CATALOG_NAME,
            schema: DEFAULT_SCHEMA_NAME,
        }
        .to_string();
        let req = PutRequest::new()
            .with_key(default_schema.as_bytes())
            .with_value(SchemaValue.as_bytes().unwrap());
        backend.put(req).await.unwrap();

        backend
    }

    async fn create_testing_table(
        table_name: &str,
        table_metadata_manager: &TableMetadataManagerRef,
    ) {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
                .with_time_index(true)
                .with_default_constraint(Some(ColumnDefaultConstraint::Function(
                    "current_timestamp()".to_string(),
                )))
                .unwrap(),
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::int32_datatype(), false),
        ]));

        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![1])
            .next_column_id(1)
            .build()
            .unwrap();

        let table_id = 1;
        let table_info: RawTableInfo = TableInfoBuilder::new(table_name, table_meta)
            .table_id(table_id)
            .build()
            .unwrap()
            .into();

        let key = TableNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, table_name);
        assert!(table_metadata_manager
            .table_name_manager()
            .create(&key, table_id)
            .await
            .is_ok());

        assert!(table_metadata_manager
            .table_info_manager()
            .compare_and_put(table_id, None, table_info)
            .await
            .is_ok());

        let _ = table_metadata_manager
            .table_region_manager()
            .compare_and_put(
                1,
                None,
                RegionDistribution::from([(1, vec![1]), (2, vec![2]), (3, vec![3])]),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_split_deletes() {
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

        let new_delete_request = |vector: VectorRef| -> DeleteRequest {
            DeleteRequest {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: table_name.to_string(),
                key_column_values: HashMap::from([("a".to_string(), vector)]),
            }
        };
        let requests = vec![
            new_delete_request(Arc::new(Int32Vector::from(vec![
                Some(1),
                Some(11),
                Some(50),
            ]))),
            new_delete_request(Arc::new(Int32Vector::from(vec![
                Some(2),
                Some(12),
                Some(102),
            ]))),
        ];

        let deleter = DistDeleter::new(
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            catalog_manager,
        );
        let mut deletes = deleter.split_deletes(requests).await.unwrap();

        assert_eq!(deletes.len(), 3);

        let new_grpc_delete_request = |column_values: Vec<i32>,
                                       null_mask: Vec<u8>,
                                       row_count: u32,
                                       region_number: u32|
         -> GrpcDeleteRequest {
            GrpcDeleteRequest {
                table_name: table_name.to_string(),
                key_columns: vec![Column {
                    column_name: "a".to_string(),
                    semantic_type: SemanticType::Tag as i32,
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

        let datanode_deletes = deletes.remove(&Peer::new(1, "")).unwrap().deletes;
        assert_eq!(datanode_deletes.len(), 2);

        assert_eq!(
            datanode_deletes[0],
            new_grpc_delete_request(vec![50], vec![0], 1, 1)
        );
        assert_eq!(
            datanode_deletes[1],
            new_grpc_delete_request(vec![102], vec![0], 1, 1)
        );

        let datanode_deletes = deletes.remove(&Peer::new(2, "")).unwrap().deletes;
        assert_eq!(datanode_deletes.len(), 2);
        assert_eq!(
            datanode_deletes[0],
            new_grpc_delete_request(vec![11], vec![0], 1, 2)
        );
        assert_eq!(
            datanode_deletes[1],
            new_grpc_delete_request(vec![12], vec![0], 1, 2)
        );

        let datanode_deletes = deletes.remove(&Peer::new(3, "")).unwrap().deletes;
        assert_eq!(datanode_deletes.len(), 2);
        assert_eq!(
            datanode_deletes[0],
            new_grpc_delete_request(vec![1], vec![0], 1, 3)
        );
        assert_eq!(
            datanode_deletes[1],
            new_grpc_delete_request(vec![2], vec![0], 1, 3)
        );
    }
}
