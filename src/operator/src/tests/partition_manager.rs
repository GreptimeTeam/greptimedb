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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_base::hash::partition_expr_version;
use common_meta::cache::{TableRouteCacheRef, new_table_route_cache};
use common_meta::key::TableMetadataManager;
use common_meta::key::table_route::TableRouteValue;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use moka::future::CacheBuilder;
use partition::cache::{PartitionInfoCacheRef, new_partition_info_cache};
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use store_api::storage::RegionNumber;
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder};

pub fn new_test_table_info(
    table_id: u32,
    table_name: &str,
    _region_numbers: impl Iterator<Item = u32>,
) -> TableInfo {
    let column_schemas = vec![
        ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new("b", ConcreteDataType::int32_datatype(), true),
    ];
    let schema = SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .version(123)
        .build()
        .unwrap();

    let meta = TableMetaBuilder::empty()
        .schema(Arc::new(schema))
        .primary_key_indices(vec![0])
        .engine("engine")
        .next_column_id(3)
        .partition_key_indices(vec![0])
        .build()
        .unwrap();
    TableInfoBuilder::default()
        .table_id(table_id)
        .table_version(5)
        .name(table_name)
        .meta(meta)
        .build()
        .unwrap()
}

fn new_test_region_wal_options(regions: Vec<RegionNumber>) -> HashMap<RegionNumber, String> {
    // TODO(niebayes): construct region wal options for test.
    let _ = regions;
    HashMap::default()
}

fn test_new_table_route_cache(kv_backend: KvBackendRef) -> TableRouteCacheRef {
    let cache = CacheBuilder::new(128).build();
    Arc::new(new_table_route_cache(
        "table_route_cache".to_string(),
        cache,
        kv_backend.clone(),
    ))
}

fn test_new_partition_info_cache(table_route_cache: TableRouteCacheRef) -> PartitionInfoCacheRef {
    let cache = CacheBuilder::new(128).build();
    Arc::new(new_partition_info_cache(
        "partition_info_cache".to_string(),
        cache,
        table_route_cache,
    ))
}

/// Create a partition rule manager with two tables, one is partitioned by single column, and
/// the other one is two. The tables are under default catalog and schema.
///
/// Table named "one_column_partitioning_table" is partitioned by column "a" like this:
/// PARTITION BY RANGE (a) (
///   PARTITION r1 VALUES LESS THAN (10),
///   PARTITION r2 VALUES LESS THAN (50),
///   PARTITION r3 VALUES LESS THAN (MAXVALUE),
/// )
///
/// Table named "two_column_partitioning_table" is partitioned by columns "a" and "b" like this:
/// PARTITION BY RANGE (a, b) (
///   PARTITION r1 VALUES LESS THAN (10, 'hz'),
///   PARTITION r2 VALUES LESS THAN (50, 'sh'),
///   PARTITION r3 VALUES LESS THAN (MAXVALUE, MAXVALUE),
/// )
pub(crate) async fn create_partition_rule_manager(
    kv_backend: KvBackendRef,
) -> PartitionRuleManagerRef {
    let table_metadata_manager = TableMetadataManager::new(kv_backend.clone());
    let table_route_cache = test_new_table_route_cache(kv_backend.clone());
    let partition_info_cache = test_new_partition_info_cache(table_route_cache.clone());
    let partition_manager = Arc::new(PartitionRuleManager::new(
        kv_backend,
        table_route_cache,
        partition_info_cache,
    ));
    let regions = vec![1u32, 2, 3];
    let region_wal_options = new_test_region_wal_options(regions.clone());
    let expr_str = serde_json::json!({
        "Expr": {
            "lhs": {"Column": "a"},
            "op": "GtEq",
            "rhs": {"Value": {"Int32": 50}}
        }
    })
    .to_string();
    table_metadata_manager
        .create_table_metadata(
            new_test_table_info(1, "table_1", regions.clone().into_iter()),
            TableRouteValue::physical(vec![
                RegionRoute {
                    region: Region {
                        id: 3.into(),
                        name: "r1".to_string(),
                        attrs: BTreeMap::new(),
                        partition_expr: PartitionExpr::new(
                            Operand::Column("a".to_string()),
                            RestrictedOp::Lt,
                            Operand::Value(datatypes::value::Value::Int32(10)),
                        )
                        .as_json_str()
                        .unwrap(),
                    },
                    leader_peer: Some(Peer::new(3, "")),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
                RegionRoute {
                    region: Region {
                        id: 2.into(),
                        name: "r2".to_string(),
                        attrs: BTreeMap::new(),
                        partition_expr: PartitionExpr::new(
                            Operand::Expr(PartitionExpr::new(
                                Operand::Column("a".to_string()),
                                RestrictedOp::GtEq,
                                Operand::Value(datatypes::value::Value::Int32(10)),
                            )),
                            RestrictedOp::And,
                            Operand::Expr(PartitionExpr::new(
                                Operand::Column("a".to_string()),
                                RestrictedOp::Lt,
                                Operand::Value(datatypes::value::Value::Int32(50)),
                            )),
                        )
                        .as_json_str()
                        .unwrap(),
                    },
                    leader_peer: Some(Peer::new(2, "")),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
                RegionRoute {
                    // Keep legacy `partition` payload to test compatibility.
                    region: serde_json::from_value(serde_json::json!({
                        "id": 1,
                        "name": "r3",
                        "partition": {
                            "column_list": ["a"],
                            "value_list": [expr_str]
                        },
                        "attrs": {},
                        "partition_expr": ""
                    }))
                    .unwrap(),
                    leader_peer: Some(Peer::new(1, "")),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
            ]),
            region_wal_options.clone(),
        )
        .await
        .unwrap();
    partition_manager
}

#[tokio::test]
async fn test_partition_expr_version_cache() {
    let kv_backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
    let partition_manager = create_partition_rule_manager(kv_backend).await;
    let partitions = partition_manager
        .find_physical_partition_info(1)
        .await
        .unwrap()
        .partitions
        .clone();

    let mut version_by_region = HashMap::new();
    for partition in partitions {
        let expected = partition
            .partition_expr
            .as_ref()
            .map(|expr| expr.as_json_str().unwrap())
            .map(|expr_json| partition_expr_version(Some(expr_json.as_str())))
            .unwrap_or_default();
        assert_eq!(Some(expected), partition.partition_expr_version);
        version_by_region.insert(
            partition.id.region_number(),
            partition.partition_expr_version,
        );
    }

    assert_eq!(3, version_by_region.len());
    assert_ne!(None, *version_by_region.get(&1).unwrap());
    assert_ne!(None, *version_by_region.get(&2).unwrap());
    assert_ne!(None, *version_by_region.get(&3).unwrap());
}
