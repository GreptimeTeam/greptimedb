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

use std::sync::Arc;

use common_time::Timestamp;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{MemTable, TableProvider};
use datafusion::prelude::{SessionConfig, SessionContext};
use mito2::config::MitoConfig;
use mito2::engine::MitoEngine;
use mito2::manifest::action::RegionEdit;
use mito2::sst::file::FileMeta;
use mito2::test_util::{CreateRequestBuilder, TestEnv};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::sst_entry::ManifestSstEntry;
use store_api::storage::{FileId, RegionId};

use crate::region_server::catalog::inspect::{InspectTable, ManifestSstSource};

/// Installs deterministic metadata only. No SST object is read by inspection.
async fn manifest_engine(regions: u32, files_per_region: usize) -> (TestEnv, MitoEngine) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    for number in 0..regions {
        let region_id = RegionId::new(11, number);
        engine
            .handle_request(
                region_id,
                RegionRequest::Create(CreateRequestBuilder::new().build()),
            )
            .await
            .unwrap();
        engine
            .edit_region(
                region_id,
                RegionEdit {
                    files_to_add: (0..files_per_region)
                        .map(|index| FileMeta {
                            region_id,
                            file_id: FileId::from_bytes(
                                (((number as u128) << 64) | index as u128).to_be_bytes(),
                            ),
                            time_range: (
                                Timestamp::new_millisecond(index as i64),
                                Timestamp::new_millisecond(index as i64 + 1),
                            ),
                            file_size: 1024 + index as u64,
                            sequence: None,
                            ..Default::default()
                        })
                        .collect(),
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                },
            )
            .await
            .unwrap();
    }
    (env, engine)
}

/// Baseline preserves eager full-row materialization, then applies SQL. Both
/// variants use the current engine enumerator, so this does not measure removal
/// of the old manifest-map cloning independently.
async fn inspect_query(engine: &MitoEngine, streaming: bool, sql: &str) -> RecordBatch {
    let schema = ManifestSstEntry::schema().arrow_schema().clone();
    let table: Arc<dyn TableProvider> = if streaming {
        Arc::new(InspectTable::new(Arc::new(ManifestSstSource::new(
            engine.clone(),
        ))))
    } else {
        let entries = engine.all_ssts_from_manifest().await;
        let batch = ManifestSstEntry::to_record_batch(&entries).unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    };
    let context = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(17)
            .with_target_partitions(1),
    );
    context.register_table("manifest", table).unwrap();
    let frame = context.sql(sql).await.unwrap();
    let schema = frame.schema().as_arrow().clone();
    let batches = frame.collect().await.unwrap();
    concat_batches(&Arc::new(schema), &batches).unwrap()
}

#[tokio::test]
async fn test_manifest_provider_matches_eager_scan() {
    let (_env, engine) = manifest_engine(3, 64).await;
    let region = RegionId::new(11, 1).as_u64();
    for sql in [
        "SELECT file_id, min_ts FROM manifest WHERE table_id = 11 ORDER BY min_ts, file_id LIMIT 7"
            .to_string(),
        "SELECT file_id FROM manifest WHERE table_id = 12 LIMIT 7".to_string(),
        format!(
            "SELECT file_id FROM manifest WHERE table_id = 11 AND region_id = {region} ORDER BY min_ts DESC LIMIT 7"
        ),
        format!("SELECT * FROM manifest WHERE region_id = {region} ORDER BY file_id"),
        format!(
            "SELECT min_ts, max_ts FROM manifest WHERE region_id = {region} ORDER BY min_ts ASC NULLS LAST, file_id LIMIT 7"
        ),
        format!(
            "SELECT file_size FROM manifest WHERE region_id = {region} ORDER BY max_ts DESC NULLS FIRST, file_id LIMIT 7 OFFSET 3"
        ),
        // Arithmetic is deliberately left as a residual predicate. Limiting
        // metadata enumeration before it would return too few rows.
        format!(
            "SELECT file_id FROM manifest WHERE region_id = {region} AND file_size % 3 = 0 ORDER BY min_ts, file_id LIMIT 7"
        ),
        format!(
            "SELECT file_id FROM manifest WHERE region_id = {region} AND min_ts >= TIMESTAMP '1970-01-01 00:00:00.020' ORDER BY min_ts, file_id LIMIT 7"
        ),
        // Variable-sized metadata needed only by a residual filter must still
        // be materialized even when it is absent from the SELECT list.
        format!(
            "SELECT file_id FROM manifest WHERE region_id = {region} AND length(file_path) > 0 ORDER BY min_ts, file_id LIMIT 7"
        ),
        // Repeated path predicates reuse metadata while both path columns stay
        // hidden from the output. The range selects only region 1's 64 files.
        "SELECT file_id FROM manifest WHERE table_dir >= 'test' AND table_dir < 'tesu' AND file_path >= 'test/11_0000000001/' AND file_path < 'test/11_0000000002/' ORDER BY file_id LIMIT 7".to_string(),
        // Visibility snapshots are needed for residual predicates even when
        // the SELECT list does not include visible.
        format!(
            "SELECT file_id FROM manifest WHERE region_id = {region} AND visible ORDER BY file_id LIMIT 7"
        ),
        format!(
            "SELECT file_id FROM manifest WHERE region_id = {region} AND NOT visible ORDER BY file_id LIMIT 7"
        ),
        "SELECT file_id FROM manifest WHERE region_id = 0 ORDER BY min_ts LIMIT 1".to_string(),
        "SELECT min_ts FROM manifest ORDER BY min_ts LIMIT 0".to_string(),
        "SELECT count(*) FROM manifest".to_string(),
        // Unsupported compound predicates must not prune matching regions.
        format!(
            "SELECT file_id FROM manifest WHERE region_id = {region} OR file_size = 1024 ORDER BY file_id"
        ),
    ] {
        let expected = inspect_query(&engine, false, &sql).await;
        let actual = inspect_query(&engine, true, &sql).await;
        assert_eq!(actual, expected, "{sql}");
    }
}

#[tokio::test]
async fn test_manifest_referring_region_and_origin_path() {
    use datafusion::arrow::array::{StringArray, UInt64Array};

    let (_env, engine) = manifest_engine(2, 2).await;
    let referring = RegionId::new(11, 1);
    // The creating region need not be open on this datanode.
    let origin = RegionId::new(11, 99);
    let file_id = FileId::from_bytes(999u128.to_be_bytes());
    engine
        .edit_region(
            referring,
            RegionEdit {
                files_to_add: vec![FileMeta {
                    region_id: origin,
                    file_id,
                    file_size: 4096,
                    ..Default::default()
                }],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
        )
        .await
        .unwrap();
    let sql = format!(
        "SELECT region_id, origin_region_id, file_path FROM manifest WHERE table_id = 11 AND region_id = {} AND origin_region_id = {} AND file_path LIKE '%/11_0000000099/%' LIMIT 1",
        referring.as_u64(),
        origin.as_u64(),
    );
    // Check source pruning without SQL residuals: the origin is not a loaded
    // region, but its file must be found in the referring region's manifest.
    use datafusion_expr::{col, lit};
    use futures::StreamExt;
    for (predicate, expected) in [
        (col("origin_region_id").eq(lit(origin.as_u64())), 1),
        (
            col("file_path").eq(lit(format!("test/11_0000000099/{file_id}.parquet"))),
            1,
        ),
        (col("file_path").eq(lit("missing/file.parquet")), 0),
        (col("table_dir").eq(lit("missing/")), 0),
        (col("node_id").is_not_null(), 0),
    ] {
        let batches = engine
            .scan_ssts_from_manifest_projected(std::slice::from_ref(&predicate), 2, None)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            batches.iter().map(Vec::len).sum::<usize>(),
            expected,
            "{predicate}"
        );
    }
    let actual = inspect_query(&engine, true, &sql).await;
    assert_eq!(actual, inspect_query(&engine, false, &sql).await);
    assert_eq!(actual.num_rows(), 1);
    assert_eq!(
        actual
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        referring.as_u64()
    );
    assert_eq!(
        actual
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        origin.as_u64()
    );
    assert!(
        actual
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .ends_with(&format!("11_0000000099/{file_id}.parquet"))
    );
    let missing = sql.replace("%/11_0000000099/%", "%/11_0000000001/%");
    assert_eq!(inspect_query(&engine, true, &missing).await.num_rows(), 0);
}
