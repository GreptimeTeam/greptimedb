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

use catalog::kvbackend::MetaKvBackend;
use common_meta::cache::{new_table_route_cache, TableRouteCacheRef};
use common_meta::key::table_route::TableRouteValue;
use common_meta::key::TableMetadataManager;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use datafusion_expr::expr::Expr;
use datafusion_expr::expr_fn::{and, binary_expr, col, or};
use datafusion_expr::{lit, Operator};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaBuilder};
use meta_client::client::MetaClient;
use moka::future::CacheBuilder;
use partition::columns::RangeColumnsPartitionRule;
use partition::expr::{Operand, PartitionExpr, RestrictedOp};
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use partition::partition::{PartitionBound, PartitionDef};
use partition::range::RangePartitionRule;
use partition::PartitionRuleRef;
use store_api::storage::RegionNumber;
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder};

pub fn new_test_table_info(
    table_id: u32,
    table_name: &str,
    region_numbers: impl Iterator<Item = u32>,
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

    let meta = TableMetaBuilder::default()
        .schema(Arc::new(schema))
        .primary_key_indices(vec![0])
        .engine("engine")
        .next_column_id(3)
        .region_numbers(region_numbers.collect::<Vec<_>>())
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
    let partition_manager = Arc::new(PartitionRuleManager::new(kv_backend, table_route_cache));

    let regions = vec![1u32, 2, 3];
    let region_wal_options = new_test_region_wal_options(regions.clone());

    table_metadata_manager
        .create_table_metadata(
            new_test_table_info(1, "table_1", regions.clone().into_iter()).into(),
            TableRouteValue::physical(vec![
                RegionRoute {
                    region: Region {
                        id: 3.into(),
                        name: "r1".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::Expr(PartitionExpr::new(
                                    Operand::Column("a".to_string()),
                                    RestrictedOp::Lt,
                                    Operand::Value(datatypes::value::Value::Int32(10)),
                                ))],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(3, "")),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region {
                        id: 2.into(),
                        name: "r2".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::Expr(PartitionExpr::new(
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
                                ))],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(2, "")),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region {
                        id: 1.into(),
                        name: "r3".to_string(),
                        partition: Some(
                            PartitionDef::new(
                                vec!["a".to_string()],
                                vec![PartitionBound::Expr(PartitionExpr::new(
                                    Operand::Column("a".to_string()),
                                    RestrictedOp::GtEq,
                                    Operand::Value(datatypes::value::Value::Int32(50)),
                                ))],
                            )
                            .try_into()
                            .unwrap(),
                        ),
                        attrs: BTreeMap::new(),
                    },
                    leader_peer: Some(Peer::new(1, "")),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                },
            ]),
            region_wal_options.clone(),
        )
        .await
        .unwrap();

    partition_manager
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "TODO(ruihang, weny): WIP new partition rule"]
async fn test_find_partition_rule() {
    let partition_manager =
        create_partition_rule_manager(Arc::new(MemoryKvBackend::default())).await;

    // "one_column_partitioning_table" has id 1
    let partition_rule = partition_manager
        .find_table_partition_rule(1)
        .await
        .unwrap();
    let range_rule = partition_rule
        .as_any()
        .downcast_ref::<RangePartitionRule>()
        .unwrap();
    assert_eq!(range_rule.column_name(), "a");
    assert_eq!(range_rule.all_regions(), &vec![3, 2, 1]);
    assert_eq!(range_rule.bounds(), &vec![10_i32.into(), 50_i32.into()]);

    // "two_column_partitioning_table" has table 2
    let partition_rule = partition_manager
        .find_table_partition_rule(2)
        .await
        .unwrap();
    let range_columns_rule = partition_rule
        .as_any()
        .downcast_ref::<RangeColumnsPartitionRule>()
        .unwrap();
    assert_eq!(range_columns_rule.column_list(), &vec!["a", "b"]);
    assert_eq!(
        range_columns_rule.value_lists(),
        &vec![
            vec![
                PartitionBound::Value(10_i32.into()),
                PartitionBound::Value("hz".into()),
            ],
            vec![
                PartitionBound::Value(50_i32.into()),
                PartitionBound::Value("sh".into()),
            ],
            vec![PartitionBound::MaxValue, PartitionBound::MaxValue]
        ]
    );
    assert_eq!(range_columns_rule.regions(), &vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_find_regions() {
    let kv_backend = Arc::new(MetaKvBackend {
        client: Arc::new(MetaClient::default()),
    });
    let table_route_cache = test_new_table_route_cache(kv_backend.clone());
    let partition_manager = Arc::new(PartitionRuleManager::new(kv_backend, table_route_cache));

    // PARTITION BY RANGE (a) (
    //   PARTITION r1 VALUES LESS THAN (10),
    //   PARTITION r2 VALUES LESS THAN (20),
    //   PARTITION r3 VALUES LESS THAN (50),
    //   PARTITION r4 VALUES LESS THAN (MAXVALUE),
    // )
    let partition_rule: PartitionRuleRef = Arc::new(RangePartitionRule::new(
        "a",
        vec![10_i32.into(), 20_i32.into(), 50_i32.into()],
        vec![0_u32, 1, 2, 3],
    )) as _;

    let partition_rule_clone = partition_rule.clone();
    let test = |filters: Vec<Expr>, expect_regions: Vec<RegionNumber>| {
        let mut regions = partition_manager
            .find_regions_by_filters(partition_rule_clone.clone(), filters.as_slice())
            .unwrap();
        regions.sort();
        assert_eq!(regions, expect_regions);
    };

    // test simple filter
    test(
        vec![binary_expr(col("a"), Operator::Lt, lit(10))], // a < 10
        vec![0],
    );
    test(
        vec![binary_expr(col("a"), Operator::LtEq, lit(10))], // a <= 10
        vec![0, 1],
    );
    test(
        vec![binary_expr(lit(20), Operator::Gt, col("a"))], // 20 > a
        vec![0, 1],
    );
    test(
        vec![binary_expr(lit(20), Operator::GtEq, col("a"))], // 20 >= a
        vec![0, 1, 2],
    );
    test(
        vec![binary_expr(lit(45), Operator::Eq, col("a"))], // 45 == a
        vec![2],
    );
    test(
        vec![binary_expr(col("a"), Operator::NotEq, lit(45))], // a != 45
        vec![0, 1, 2, 3],
    );
    test(
        vec![binary_expr(col("a"), Operator::Gt, lit(50))], // a > 50
        vec![3],
    );

    // test multiple filters
    test(
        vec![
            binary_expr(col("a"), Operator::Gt, lit(10)),
            binary_expr(col("a"), Operator::Gt, lit(50)),
        ], // [a > 10, a > 50]
        vec![3],
    );

    // test finding all regions when provided with not supported filters or not partition column
    test(
        vec![binary_expr(col("row_id"), Operator::LtEq, lit(123))], // row_id <= 123
        vec![0, 1, 2, 3],
    );
    test(
        vec![binary_expr(col("c"), Operator::Gt, lit(123))], // c > 789
        vec![0, 1, 2, 3],
    );

    // test complex "AND" or "OR" filters
    test(
        vec![and(
            binary_expr(col("row_id"), Operator::Lt, lit(1)),
            or(
                binary_expr(col("row_id"), Operator::Lt, lit(1)),
                binary_expr(col("a"), Operator::Lt, lit(1)),
            ),
        )], // row_id < 1 OR (row_id < 1 AND a > 1)
        vec![0, 1, 2, 3],
    );
    test(
        vec![or(
            binary_expr(col("a"), Operator::Lt, lit(20)),
            binary_expr(col("a"), Operator::GtEq, lit(20)),
        )], // a < 20 OR a >= 20
        vec![0, 1, 2, 3],
    );
    test(
        vec![and(
            binary_expr(col("a"), Operator::Lt, lit(20)),
            binary_expr(col("a"), Operator::Lt, lit(50)),
        )], // a < 20 AND a < 50
        vec![0, 1],
    );

    // test failed to find regions by contradictory filters
    let regions = partition_manager.find_regions_by_filters(
        partition_rule,
        vec![and(
            binary_expr(col("a"), Operator::Lt, lit(20)),
            binary_expr(col("a"), Operator::GtEq, lit(20)),
        )]
        .as_slice(),
    ); // a < 20 AND a >= 20
    assert!(matches!(
        regions.unwrap_err(),
        partition::error::Error::FindRegions { .. }
    ));
}
