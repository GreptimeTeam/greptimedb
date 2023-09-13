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

use api::v1::region::DeleteRequests as RegionDeleteRequests;
use api::v1::Rows;
use partition::manager::PartitionRuleManager;
use table::metadata::TableInfo;
use table::requests::DeleteRequest as TableDeleteRequest;

use crate::error::Result;
use crate::req_convert::common::partitioner::Partitioner;
use crate::req_convert::common::{column_schema, row_count};

pub struct TableToRegion<'a> {
    table_info: &'a TableInfo,
    partition_manager: &'a PartitionRuleManager,
}

impl<'a> TableToRegion<'a> {
    pub fn new(table_info: &'a TableInfo, partition_manager: &'a PartitionRuleManager) -> Self {
        Self {
            table_info,
            partition_manager,
        }
    }

    pub async fn convert(&self, request: TableDeleteRequest) -> Result<RegionDeleteRequests> {
        let row_count = row_count(&request.key_column_values)?;
        let schema = column_schema(self.table_info, &request.key_column_values)?;
        let rows = api::helper::vectors_to_rows(request.key_column_values.values(), row_count);
        let rows = Rows { schema, rows };

        let requests = Partitioner::new(self.partition_manager)
            .partition_delete_requests(self.table_info.table_id(), rows)
            .await?;
        Ok(RegionDeleteRequests { requests })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::region::DeleteRequest as RegionDeleteRequest;
    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, ColumnSchema, Row, SemanticType, Value};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_meta::key::catalog_name::{CatalogManager, CatalogNameKey};
    use common_meta::key::schema_name::{SchemaManager, SchemaNameKey};
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::kv_backend::KvBackendRef;
    use datatypes::vectors::{Int32Vector, VectorRef};
    use store_api::storage::RegionId;

    use super::*;
    use crate::table::test::{create_partition_rule_manager, new_test_table_info};

    async fn prepare_mocked_backend() -> KvBackendRef {
        let backend = Arc::new(MemoryKvBackend::default());

        let catalog_manager = CatalogManager::new(backend.clone());
        let schema_manager = SchemaManager::new(backend.clone());

        catalog_manager
            .create(CatalogNameKey::default(), false)
            .await
            .unwrap();
        schema_manager
            .create(SchemaNameKey::default(), None, false)
            .await
            .unwrap();

        backend
    }

    #[tokio::test]
    async fn test_delete_request_table_to_region() {
        // region to datanode placement:
        // 1 -> 1
        // 2 -> 2
        // 3 -> 3
        //
        // region value ranges:
        // 1 -> [50, max)
        // 2 -> [10, 50)
        // 3 -> (min, 10)

        let backend = prepare_mocked_backend().await;
        let partition_manager = create_partition_rule_manager(backend.clone()).await;
        let table_info = new_test_table_info(1, "table_1", vec![0u32, 1, 2].into_iter());

        let converter = TableToRegion::new(&table_info, &partition_manager);

        let table_request = build_table_request(Arc::new(Int32Vector::from(vec![
            Some(1),
            None,
            Some(11),
            Some(101),
        ])));

        let region_requests = converter.convert(table_request).await.unwrap();
        let mut region_id_to_region_requests = region_requests
            .requests
            .into_iter()
            .map(|r| (r.region_id, r))
            .collect::<HashMap<_, _>>();

        let region_id = RegionId::new(1, 1).as_u64();
        let region_request = region_id_to_region_requests.remove(&region_id).unwrap();
        assert_eq!(
            region_request,
            build_region_request(vec![Some(101)], region_id)
        );

        let region_id = RegionId::new(1, 2).as_u64();
        let region_request = region_id_to_region_requests.remove(&region_id).unwrap();
        assert_eq!(
            region_request,
            build_region_request(vec![Some(11)], region_id)
        );

        let region_id = RegionId::new(1, 3).as_u64();
        let region_request = region_id_to_region_requests.remove(&region_id).unwrap();
        assert_eq!(
            region_request,
            build_region_request(vec![Some(1), None], region_id)
        );
    }

    fn build_table_request(vector: VectorRef) -> TableDeleteRequest {
        TableDeleteRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "table_1".to_string(),
            key_column_values: HashMap::from([("a".to_string(), vector)]),
        }
    }

    fn build_region_request(rows: Vec<Option<i32>>, region_id: u64) -> RegionDeleteRequest {
        RegionDeleteRequest {
            region_id,
            rows: Some(Rows {
                schema: vec![ColumnSchema {
                    column_name: "a".to_string(),
                    datatype: ColumnDataType::Int32 as i32,
                    semantic_type: SemanticType::Tag as i32,
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
    }
}
