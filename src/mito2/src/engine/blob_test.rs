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

//! Tests for external storage of large field values.

use std::time::Duration;

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{Rows, SemanticType};
use common_recordbatch::RecordBatches;
use datafusion_expr::{col, lit};
use datatypes::arrow::array::AsArray;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::StorageOptions;
use store_api::region_engine::{MitoCopyRegionFromRequest, RegionEngine};
use store_api::region_request::{RegionCreateRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::compact;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, delete_rows, delete_rows_schema, flush_region, put_rows,
    rows_schema,
};

/// Long values on even rows go to the sidecar; odd rows stay inline, and one of them looks
/// like a reference.
fn payload(i: usize, version: usize) -> String {
    match i % 4 {
        1 => format!("short-{i}-{version}"),
        3 => format!("\0greptime-blob:{i}:{version}:0"),
        _ => format!("{i:06}-{version}-").repeat(16 * 1024),
    }
}

async fn put(
    engine: &MitoEngine,
    region_id: RegionId,
    schema: &[api::v1::ColumnSchema],
    ts: &[usize],
    version: usize,
) {
    let rows = ts
        .iter()
        .map(|i| {
            row(vec![
                ValueData::StringValue("a".to_string()),
                ValueData::StringValue(payload(*i, version)),
                ValueData::TimestampMillisecondValue(*i as i64 * 1000),
            ])
        })
        .collect();
    put_rows(
        engine,
        region_id,
        Rows {
            schema: schema.to_vec(),
            rows,
        },
    )
    .await;
}

/// Returns `(ts, payload)` of every visible row, ordered by ts.
async fn scan(
    engine: &MitoEngine,
    region_id: RegionId,
    request: ScanRequest,
) -> Vec<(i64, String)> {
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let ts = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<datatypes::arrow::datatypes::TimestampMillisecondType>();
        let values = batch.column_by_name("field_0").unwrap().as_string::<i32>();
        for i in 0..batch.num_rows() {
            rows.push((ts.value(i), values.value(i).to_string()));
        }
    }
    rows.sort();
    rows
}

async fn blob_files(env: &TestEnv) -> usize {
    let store = env.get_object_store().unwrap();
    store
        .list_with("")
        .recursive(true)
        .await
        .unwrap()
        .iter()
        .filter(|e| e.path().ends_with(".blob"))
        .count()
}

fn create_request(external: bool) -> RegionCreateRequest {
    let mut request = CreateRequestBuilder::new()
        .tag_num(1)
        .field_num(1)
        .field_datatype(ConcreteDataType::string_datatype())
        .build();
    if external {
        let field = request
            .column_metadatas
            .iter_mut()
            .find(|c| c.semantic_type == SemanticType::Field)
            .unwrap();
        field.column_schema = field
            .column_schema
            .clone()
            .with_storage_options(StorageOptions::default())
            .unwrap();
    }
    request
}

#[tokio::test]
async fn test_large_values_stay_in_sst_without_external_storage() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(false);
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put(&engine, region_id, &schema, &[0, 1], 0).await;
    flush_region(&engine, region_id, None).await;
    assert_eq!(0, blob_files(&env).await);
}

#[tokio::test]
async fn test_large_values_survive_flush_filter_and_compaction() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    let delete_schema = delete_rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put(&engine, region_id, &schema, &[0, 1, 2, 3], 0).await;
    flush_region(&engine, region_id, None).await;
    put(&engine, region_id, &schema, &[4, 5, 6, 7], 0).await;
    flush_region(&engine, region_id, None).await;
    assert_eq!(2, blob_files(&env).await);
    // Size estimates (compaction batch size and memory) must count values in the sidecar.
    // Each file holds two long values.
    {
        // Scoped: the version holds file handles, which would block the purge checked below.
        let version = engine.get_region(region_id).unwrap().version();
        for level in version.ssts.levels() {
            for file in level.files.values() {
                assert!(
                    file.meta_ref().max_row_group_uncompressed_size
                        >= 2 * payload(0, 0).len() as u64
                );
            }
        }
    }

    let expected: Vec<_> = (0..8).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );

    // The predicate must see values, not references, in every filter path.
    let filtered = scan(
        &engine,
        region_id,
        ScanRequest {
            filters: vec![col("field_0").eq(lit(payload(4, 0)))],
            ..Default::default()
        },
    )
    .await;
    assert_eq!(vec![(4000, payload(4, 0))], filtered);

    // Overwrite and delete across files, then merge everything.
    put(&engine, region_id, &schema, &[1, 6], 1).await;
    let rows = Rows {
        schema: delete_schema,
        rows: vec![row(vec![
            ValueData::StringValue("a".to_string()),
            ValueData::TimestampMillisecondValue(2000),
        ])],
    };
    delete_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    compact(&engine, region_id).await;

    let expected: Vec<_> = [0, 1, 3, 4, 5, 6, 7]
        .into_iter()
        .map(|i| (i as i64 * 1000, payload(i, usize::from(i == 1 || i == 6))))
        .collect();
    assert_eq!(
        expected,
        scan(&engine, region_id, ScanRequest::default()).await
    );

    // Compacted inputs take their sidecars with them.
    let ssts = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .map(|l| l.files.len())
        .sum::<usize>();
    let mut blobs = blob_files(&env).await;
    for _ in 0..50 {
        if blobs == ssts {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        blobs = blob_files(&env).await;
    }
    assert_eq!(ssts, blobs);
}

#[tokio::test]
async fn test_copy_region_requires_declared_sidecars() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let source_id = RegionId::new(1, 1);
    let request = create_request(true);
    let schema = rows_schema(&request);
    for id in [source_id, RegionId::new(1, 2), RegionId::new(1, 3)] {
        engine
            .handle_request(id, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
    }
    put(&engine, source_id, &schema, &[0, 1, 2, 3], 0).await;
    flush_region(&engine, source_id, None).await;
    let copy = |target_id| {
        engine.copy_region_from(
            target_id,
            MitoCopyRegionFromRequest {
                source_region_id: source_id,
                parallelism: 1,
            },
        )
    };

    copy(RegionId::new(1, 2)).await.unwrap();
    let expected: Vec<_> = (0..4).map(|i| (i as i64 * 1000, payload(i, 0))).collect();
    assert_eq!(
        expected,
        scan(&engine, RegionId::new(1, 2), ScanRequest::default()).await
    );

    let store = env.get_object_store().unwrap();
    for entry in store.list_with("").recursive(true).await.unwrap() {
        if entry.path().contains("1_0000000001") && entry.path().ends_with(".blob") {
            store.delete(entry.path()).await.unwrap();
        }
    }
    copy(RegionId::new(1, 3)).await.unwrap_err();
}
