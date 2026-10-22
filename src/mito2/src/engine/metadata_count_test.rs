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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use datatypes::arrow::datatypes::SchemaRef;
use object_store::layers::mock::MockLayerBuilder;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionDeleteRequest, RegionPutRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use table::table::scan::RegionScanExec;

use crate::config::MitoConfig;
use crate::engine::compaction_test::{compact, delete_and_flush, put_and_flush};
use crate::test_util::{self, CreateRequestBuilder, TestEnv};

/// Exercises DataFusion's normal SQL physical optimizer over a real Mito scan.
/// GreptimeDB's SQL-to-count hint is covered separately by ScanHintRule tests.
#[derive(Debug)]
struct CountTable {
    plan: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl TableProvider for CountTable {
    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if let Some(projection) = projection {
            let schema = self.plan.schema();
            let exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> = projection
                .iter()
                .map(|index| {
                    let name = schema.field(*index).name();
                    (Arc::new(Column::new(name, *index)) as _, name.clone())
                })
                .collect();
            Ok(Arc::new(ProjectionExec::try_new(exprs, self.plan.clone())?))
        } else {
            Ok(self.plan.clone())
        }
    }
}

#[tokio::test]
async fn test_metadata_count_compaction_sql_and_fallback() {
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            if path.ends_with(".parquet") {
                observed.fetch_add(1, Ordering::Relaxed);
            }
            inner
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(layer);
    let config = MitoConfig {
        experimental_metadata_count: true,
        min_compaction_interval: Duration::from_secs(3600),
        schedule_compaction_after_edit: false,
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "count_test",
            "greptime",
            "public",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .build();
    let schema = test_util::rows_schema(&create);
    let table_dir = create.table_dir.clone();
    let options = create.options.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    for range in [0..5, 2..7, 0..3] {
        put_and_flush(&engine, region_id, &schema, range).await;
    }
    delete_and_flush(&engine, region_id, &schema, 1..2).await;
    let request = ScanRequest {
        metadata_count_hint: true,
        ..Default::default()
    };
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(
        scanner.properties().exact_num_rows(),
        None,
        "flush does not certify logical rows"
    );
    compact(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert!(files[0].meta_ref().num_rows_is_logical);
    assert_eq!(files[0].meta_ref().num_rows, 6);
    drop(version);
    drop(region);

    // Verify the capability survives manifest persistence and reopening.
    test_util::reopen_region(&engine, region_id, table_dir, true, options).await;
    reads.store(0, Ordering::Relaxed);
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(scanner.properties().exact_num_rows(), Some(6));
    let scan = RegionScanExec::new(scanner, request.clone(), None).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(CountTable {
            plan: Arc::new(scan),
        }),
    )
    .unwrap();
    let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(!text.contains("RegionScanExec"), "{text}");
    let batches = df.collect().await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Int64(Some(6))
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "optimized COUNT must not open any SST"
    );

    let ts = |v| lit(ScalarValue::TimestampMillisecond(Some(v), None));
    for (filters, expected) in [
        (
            vec![col("ts").gt_eq(ts(0)).and(col("ts").lt(ts(7000)))],
            Some(6),
        ),
        (vec![col("ts").lt(ts(3000))], None),
        (vec![col("ts").gt(ts(7000))], Some(0)),
    ] {
        let scanner = engine
            .handle_query(
                region_id,
                ScanRequest {
                    filters,
                    ..request.clone()
                },
            )
            .await
            .unwrap();
        assert_eq!(scanner.properties().exact_num_rows(), expected);
    }

    // A new deletion in memory invalidates the complete count; ordinary scan
    // still resolves the tombstone against the compacted file.
    engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest {
                rows: api::v1::Rows {
                    schema: schema.clone(),
                    rows: test_util::build_rows_for_key("a", 2, 3, 0),
                },
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(scanner.properties().exact_num_rows(), None);
    let baseline = common_recordbatch::RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        baseline.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        5
    );
    // Ordinary writes retain their original behavior.
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: api::v1::Rows {
                    schema,
                    rows: test_util::build_rows_for_key("a", 2, 3, 0),
                },
                hint: None,
                partition_expr_version: None,
                skip_wal: false,
            }),
        )
        .await
        .unwrap();
    engine.stop().await.unwrap();
}
