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

#[cfg(test)]
use api::v1::helper::tag_column_schema;
#[cfg(test)]
use api::v1::value::ValueData;
#[cfg(test)]
use api::v1::{ColumnDataType, Row, Rows};
#[cfg(test)]
use common_base::hash::partition_expr_version;
use common_meta::cache::{TableRouteCacheRef, new_table_route_cache};
use common_meta::key::TableMetadataManager;
use common_meta::key::table_route::TableRouteValue;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::wal_provider::RegionWalOptions;
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
    new_test_table_info_with_columns(table_id, table_name, test_column_schemas(true), vec![0])
}

fn test_column_schemas(include_b: bool) -> Vec<ColumnSchema> {
    let mut column_schemas = vec![
        ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
    ];
    if include_b {
        column_schemas.push(ColumnSchema::new(
            "b",
            ConcreteDataType::int32_datatype(),
            true,
        ));
    }
    column_schemas
}

fn new_test_table_info_with_columns(
    table_id: u32,
    table_name: &str,
    column_schemas: Vec<ColumnSchema>,
    partition_key_indices: Vec<usize>,
) -> TableInfo {
    let next_column_id = column_schemas.len() as u32;
    let schema = SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .version(123)
        .build()
        .unwrap();

    let meta = TableMetaBuilder::empty()
        .schema(Arc::new(schema))
        .primary_key_indices(partition_key_indices.clone())
        .engine("engine")
        .next_column_id(next_column_id)
        .partition_key_indices(partition_key_indices)
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

#[cfg(test)]
fn new_physical_test_table_info(table_id: u32, table_name: &str) -> TableInfo {
    new_test_table_info_with_columns(table_id, table_name, test_column_schemas(true), vec![0, 2])
}

#[cfg(test)]
fn new_logical_test_table_info(table_id: u32, table_name: &str) -> TableInfo {
    new_test_table_info_with_columns(table_id, table_name, test_column_schemas(false), vec![0])
}

fn new_test_region_wal_options(regions: Vec<RegionNumber>) -> RegionWalOptions {
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
pub async fn create_partition_rule_manager(kv_backend: KvBackendRef) -> PartitionRuleManagerRef {
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

#[tokio::test]
async fn test_logical_split_rows_rejects_physical_only_partition_column() {
    let kv_backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
    let table_metadata_manager = TableMetadataManager::new(kv_backend.clone());
    let table_route_cache = test_new_table_route_cache(kv_backend.clone());
    let partition_info_cache = test_new_partition_info_cache(table_route_cache.clone());
    let partition_manager =
        PartitionRuleManager::new(kv_backend, table_route_cache, partition_info_cache);
    let physical_table_info = new_physical_test_table_info(1024, "physical");
    let logical_table_info = new_logical_test_table_info(1025, "logical");
    assert_eq!(
        vec!["a", "b"],
        physical_table_info
            .meta
            .partition_column_names()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        vec!["a"],
        logical_table_info
            .meta
            .partition_column_names()
            .map(|name| name.as_str())
            .collect::<Vec<_>>()
    );

    table_metadata_manager
        .create_table_metadata(
            physical_table_info,
            TableRouteValue::physical(vec![RegionRoute {
                region: Region {
                    id: 1.into(),
                    name: "r1".to_string(),
                    attrs: BTreeMap::new(),
                    partition_expr: PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("a".to_string()),
                            RestrictedOp::GtEq,
                            Operand::Value(datatypes::value::Value::Int32(10)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("b".to_string()),
                            RestrictedOp::Lt,
                            Operand::Value(datatypes::value::Value::Int32(50)),
                        )),
                    )
                    .as_json_str()
                    .unwrap(),
                },
                leader_peer: Some(Peer::new(1, "")),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            new_test_region_wal_options(vec![1]),
        )
        .await
        .unwrap();
    table_metadata_manager
        .create_table_metadata(
            logical_table_info.clone(),
            TableRouteValue::logical(1024),
            new_test_region_wal_options(vec![]),
        )
        .await
        .unwrap();

    let (partition_rule, _) = partition_manager
        .find_table_partition_rule(&logical_table_info)
        .await
        .unwrap();
    assert_eq!(&["a"], partition_rule.partition_columns());

    let error = partition_manager
        .split_rows(
            &logical_table_info,
            Rows {
                schema: vec![tag_column_schema("a", ColumnDataType::Int32)],
                rows: vec![Row {
                    values: vec![ValueData::I32Value(100).into()],
                }],
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        partition::error::Error::UndefinedColumn { ref column, .. } if column == "b"
    ));
}

#[tokio::test]
async fn test_partition_routing_with_empty_default_expression() {
    use datatypes::arrow::array::{ArrayRef, RecordBatch, StringArray};
    use datatypes::value::Value;
    use partition::expr::col;
    use store_api::storage::RegionId;

    for (transition, target_count) in [(true, 1), (true, 3), (false, 3)] {
        let backend = Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new());
        let route_cache = test_new_table_route_cache(backend.clone());
        let manager = PartitionRuleManager::new(
            backend.clone(),
            route_cache.clone(),
            test_new_partition_info_cache(route_cache),
        );
        let value = |s: &str| Value::String(s.into());
        let mut exprs = vec![
            col("host").lt(value("36z0")),
            col("host")
                .gt_eq(value("36z0"))
                .and(col("host").lt(value("7Ez0"))),
            col("host")
                .gt_eq(value("7Ez0"))
                .and(col("host").lt(value("BMz0"))),
            col("host").gt_eq(value("BMz0")),
        ];
        if target_count == 1 {
            exprs = vec![
                col("host").lt(value("36z0")),
                col("host").gt_eq(value("36z0")),
            ];
        }
        let routes = exprs
            .into_iter()
            .enumerate()
            .map(|(i, expr)| RegionRoute {
                region: Region {
                    id: RegionId::new(1027, i as u32),
                    partition_expr: if transition && i == 0 {
                        String::new()
                    } else {
                        expr.as_json_str().unwrap()
                    },
                    ..Default::default()
                },
                leader_peer: Some(Peer::new(1, "")),
                ..Default::default()
            })
            .collect();
        let info = new_test_table_info_with_columns(
            1027,
            "physical",
            vec![
                ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            ],
            vec![0],
        );
        let metadata_manager = TableMetadataManager::new(backend);
        metadata_manager
            .create_table_metadata(
                info.clone(),
                TableRouteValue::physical(routes),
                new_test_region_wal_options((0..=target_count).collect()),
            )
            .await
            .unwrap();
        let mut logical_info = info.clone();
        logical_info.ident.table_id = 1028;
        logical_info.name = "logical".into();
        metadata_manager
            .create_table_metadata(
                logical_info.clone(),
                TableRouteValue::logical(1027),
                HashMap::new(),
            )
            .await
            .unwrap();

        let hosts = [None, Some("1"), Some("5"), Some("8"), Some("animi")];
        let expected_regions = if target_count == 1 {
            [0, 0, 1, 1, 1]
        } else {
            [0, 0, 1, 2, 3]
        };
        let batch = RecordBatch::try_from_iter([(
            "host",
            Arc::new(StringArray::from(hosts.to_vec())) as ArrayRef,
        )])
        .unwrap();
        for table_info in [&info, &logical_info] {
            let (rule, _) = manager.find_table_partition_rule(table_info).await.unwrap();
            for (host, expected) in hosts.into_iter().zip(expected_regions) {
                assert_eq!(
                    rule.find_region(&[host.map(value).unwrap_or(Value::Null)])
                        .unwrap(),
                    expected
                );
            }
            let splits = rule.split_record_batch(&batch).unwrap();
            assert_eq!(splits.len(), target_count as usize + 1);
            for (region, mask) in splits {
                let selected = mask
                    .array()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, selected)| (selected == Some(true)).then_some(i))
                    .collect::<Vec<_>>();
                let expected = expected_regions
                    .iter()
                    .enumerate()
                    .filter_map(|(i, expected)| (*expected == region).then_some(i))
                    .collect::<Vec<_>>();
                assert_eq!(selected, expected);
            }
        }
    }
}
