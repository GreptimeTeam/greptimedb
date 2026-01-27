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

use std::collections::BTreeSet;
use std::time::Duration;

use common_meta::key::table_repart::TableRepartValue;
use common_procedure::{Procedure, Status};
use common_procedure_test::new_test_procedure_context;
use meta_srv::gc::BatchGcProcedure;
use store_api::storage::{FileRefsManifest, RegionId};

use crate::test_util::{StorageType, execute_sql};
use crate::tests::gc::{distributed_with_gc, get_table_route};

#[tokio::test]
async fn test_cleanup_region_repartition_update_tombstone_remove() {
    let _ = dotenv::dotenv();
    let (test_context, _guard) = distributed_with_gc(&StorageType::File).await;
    let instance = test_context.frontend();
    let metasrv = test_context.metasrv();

    let create_table_sql = r#"
CREATE TABLE test_cleanup_repartition (
    ts TIMESTAMP TIME INDEX,
    val DOUBLE,
    host STRING
)PARTITION ON COLUMNS (host) (
  host < 'a',
  host >= 'a' AND host < 'm',
  host >= 'm'
) WITH (append_mode = 'true')"#;
    execute_sql(&instance, create_table_sql).await;

    let table = instance
        .catalog_manager()
        .table("greptime", "public", "test_cleanup_repartition", None)
        .await
        .unwrap()
        .unwrap();
    let table_id = table.table_info().table_id();

    let (_routes, regions) = get_table_route(metasrv.table_metadata_manager(), table_id).await;
    let base_region = *regions.first().expect("table has at least one region");

    let region_a = base_region;
    let region_b = RegionId::new(table_id, base_region.region_number() + 1);
    let region_c = RegionId::new(table_id, base_region.region_number() + 2);

    let dst_a_initial = RegionId::new(table_id, base_region.region_number() + 10);
    let dst_b_initial = RegionId::new(table_id, base_region.region_number() + 20);
    let dst_c_initial = RegionId::new(table_id, base_region.region_number() + 30);
    let dst_a_new = RegionId::new(table_id, base_region.region_number() + 40);

    let repart_mgr = metasrv.table_metadata_manager().table_repart_manager();
    let current = repart_mgr.get_with_raw_bytes(table_id).await.unwrap();
    let mut initial_value = TableRepartValue::new();
    initial_value.update_mappings(region_a, &[dst_a_initial]);
    initial_value.update_mappings(region_b, &[dst_b_initial]);
    initial_value.update_mappings(region_c, &[dst_c_initial]);
    repart_mgr
        .upsert_value(table_id, current, &initial_value)
        .await
        .unwrap();

    let mut manifest = FileRefsManifest::default();
    manifest
        .cross_region_refs
        .insert(region_a, [dst_a_new].into());
    manifest.file_refs.insert(region_b, Default::default());

    let regions = vec![region_a, region_b, region_c];

    let mut procedure = BatchGcProcedure::new_update_repartition_for_test(
        metasrv.mailbox().clone(),
        metasrv.table_metadata_manager().clone(),
        metasrv.options().grpc.server_addr.clone(),
        regions,
        manifest,
        Duration::from_secs(5),
    );

    let procedure_ctx = new_test_procedure_context();
    let status = procedure.execute(&procedure_ctx).await.unwrap();
    assert!(matches!(status, Status::Done { .. }));

    let repart_after = repart_mgr.get(table_id).await.unwrap().unwrap();
    assert_eq!(
        repart_after.src_to_dst.get(&region_a),
        Some(&BTreeSet::from([dst_a_new]))
    );
    assert_eq!(
        repart_after.src_to_dst.get(&region_b),
        Some(&BTreeSet::new())
    );
    assert!(!repart_after.src_to_dst.contains_key(&region_c));
}

#[tokio::test]
async fn test_cleanup_region_repartition_preserve_uninvolved_entries() {
    let _ = dotenv::dotenv();
    let (test_context, _guard) = distributed_with_gc(&StorageType::File).await;
    let instance = test_context.frontend();
    let metasrv = test_context.metasrv();

    let create_table_sql = r#"
        CREATE TABLE test_cleanup_repartition_preserve (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            host STRING
        )PARTITION ON COLUMNS (host) (
  host < 'a',
  host >= 'a' AND host < 'm',
  host >= 'm' AND host < 'x',
  host >= 'x'
) WITH (append_mode = 'true')
    "#;
    execute_sql(&instance, create_table_sql).await;

    let table = instance
        .catalog_manager()
        .table(
            "greptime",
            "public",
            "test_cleanup_repartition_preserve",
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let table_id = table.table_info().table_id();

    let (_routes, regions) = get_table_route(metasrv.table_metadata_manager(), table_id).await;
    let base_region = *regions.first().expect("table has at least one region");

    let region_a = base_region;
    let region_b = RegionId::new(table_id, base_region.region_number() + 1);
    let region_c = RegionId::new(table_id, base_region.region_number() + 2);
    let region_d = RegionId::new(table_id, base_region.region_number() + 3);

    let dst_a_initial = RegionId::new(table_id, base_region.region_number() + 10);
    let dst_b_initial = RegionId::new(table_id, base_region.region_number() + 20);
    let dst_c_initial = RegionId::new(table_id, base_region.region_number() + 30);
    let dst_d_initial = RegionId::new(table_id, base_region.region_number() + 50);
    let dst_a_new = RegionId::new(table_id, base_region.region_number() + 60);

    let repart_mgr = metasrv.table_metadata_manager().table_repart_manager();
    let current = repart_mgr.get_with_raw_bytes(table_id).await.unwrap();
    let mut initial_value = TableRepartValue::new();
    initial_value.update_mappings(region_a, &[dst_a_initial]);
    initial_value.update_mappings(region_b, &[dst_b_initial]);
    initial_value.update_mappings(region_c, &[dst_c_initial]);
    initial_value.update_mappings(region_d, &[dst_d_initial]);
    repart_mgr
        .upsert_value(table_id, current, &initial_value)
        .await
        .unwrap();

    let mut manifest = FileRefsManifest::default();
    manifest
        .cross_region_refs
        .insert(region_a, [dst_a_new].into());
    manifest.file_refs.insert(region_b, Default::default());

    let regions = vec![region_a, region_b, region_c];

    let mut procedure = BatchGcProcedure::new_update_repartition_for_test(
        metasrv.mailbox().clone(),
        metasrv.table_metadata_manager().clone(),
        metasrv.options().grpc.server_addr.clone(),
        regions,
        manifest,
        Duration::from_secs(5),
    );

    let procedure_ctx = new_test_procedure_context();
    let status = procedure.execute(&procedure_ctx).await.unwrap();
    assert!(matches!(status, Status::Done { .. }));

    let repart_after = repart_mgr.get(table_id).await.unwrap().unwrap();
    assert_eq!(
        repart_after.src_to_dst.get(&region_a),
        Some(&BTreeSet::from([dst_a_new]))
    );
    assert_eq!(
        repart_after.src_to_dst.get(&region_b),
        Some(&BTreeSet::new())
    );
    assert!(!repart_after.src_to_dst.contains_key(&region_c));
    assert_eq!(
        repart_after.src_to_dst.get(&region_d),
        Some(&BTreeSet::from([dst_d_initial]))
    );
}
