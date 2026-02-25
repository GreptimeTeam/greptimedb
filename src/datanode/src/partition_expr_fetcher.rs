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

use common_meta::key::table_route::TableRouteManager;
use common_meta::kv_backend::KvBackendRef;
use mito2::region::opener::PartitionExprFetcher;
use store_api::storage::RegionId;

/// A fetcher to fetch partition exprs from the table route.
pub struct MetaPartitionExprFetcher {
    table_route_manager: TableRouteManager,
}

impl MetaPartitionExprFetcher {
    /// Creates a new [MetaPartitionExprFetcher].
    pub fn new(kv: KvBackendRef) -> Self {
        Self {
            table_route_manager: TableRouteManager::new(kv),
        }
    }
}

#[async_trait::async_trait]
impl PartitionExprFetcher for MetaPartitionExprFetcher {
    async fn fetch_expr(&self, region_id: RegionId) -> Option<String> {
        let table_id = region_id.table_id();
        let Ok((_, route)) = self
            .table_route_manager
            .get_physical_table_route(table_id)
            .await
        else {
            return None;
        };
        let region_number = region_id.region_number();
        let rr = route
            .region_routes
            .iter()
            .find(|r| r.region.id.region_number() == region_number)?;
        Some(rr.region.partition_expr())
    }
}

#[cfg(test)]
mod tests {
    use common_meta::key::table_route::{TableRouteStorage, TableRouteValue};
    use common_meta::kv_backend::TxnService;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::rpc::router::{Region, RegionRoute};

    use super::*;

    #[tokio::test]
    async fn test_fetch_expr_json() {
        let kv = std::sync::Arc::new(MemoryKvBackend::new());
        let storage = TableRouteStorage::new(kv.clone());
        let table_id_u32 = 42;
        let region_number = 7;
        let region_id = RegionId::new(table_id_u32, region_number);
        let expr_json =
            r#"{"Expr":{"lhs":{"Column":"a"},"op":"GtEq","rhs":{"Value":{"UInt32":10}}}}"#;

        let region_route = RegionRoute {
            region: Region {
                id: region_id,
                name: "r".to_string(),
                attrs: Default::default(),
                partition_expr: expr_json.to_string(),
            },
            ..Default::default()
        };
        let trv = TableRouteValue::physical(vec![region_route]);
        let (txn, _dec) = storage.build_create_txn(table_id_u32, &trv).unwrap();
        kv.txn(txn).await.unwrap();

        let fetcher = MetaPartitionExprFetcher::new(kv);
        let fetched = fetcher.fetch_expr(region_id).await;
        assert_eq!(fetched.as_deref(), Some(expr_json));
    }
}
