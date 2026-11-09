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
use std::time::Duration;

use api::v1::Rows;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;
use mito2::config::MitoConfig;
use mito2::engine::listener::EventListener;
use mito2::test_util::{
    CreateRequestBuilder, TestEnv, build_rows_for_key, flush_region, put_rows, rows_schema,
};
use store_api::compaction_task::CompactionTaskEntry;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::RegionId;
use tokio::sync::Semaphore;

use crate::region_server::catalog::inspect::{CompactionTasksSource, InspectTable};

struct MergeGate {
    entered: Semaphore,
    resume: Semaphore,
}

#[async_trait::async_trait]
impl EventListener for MergeGate {
    async fn on_compaction_unit_merge_begin(&self, _: RegionId, _: u64) {
        self.entered.add_permits(1);
        if let Ok(permit) = self.resume.acquire().await {
            permit.forget();
        }
    }
}

struct ReleaseGate(Arc<MergeGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.resume.close();
    }
}

async fn query(context: &SessionContext, sql: &str) -> RecordBatch {
    let frame = context.sql(sql).await.unwrap();
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.unwrap();
    concat_batches(&schema, &batches).unwrap()
}

/// Ablates lazy inspection against eager snapshots under the same gated workload.
#[tokio::test]
async fn test_compaction_tasks_live_scan_and_pushdown() {
    let gate = Arc::new(MergeGate {
        entered: Semaphore::new(0),
        resume: Semaphore::new(0),
    });
    let guard = ReleaseGate(gate.clone());
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine_with(
            MitoConfig {
                max_background_compactions: 2,
                min_compaction_interval: Duration::from_secs(3600),
                ..Default::default()
            },
            None,
            Some(gate.clone()),
            None,
        )
        .await;
    let mut jobs = Vec::new();
    for table_id in [11, 12] {
        let region_id = RegionId::new(table_id, 1);
        let create = CreateRequestBuilder::new()
            .insert_option("compaction.type", "twcs")
            .insert_option("compaction.twcs.trigger_file_num", "100")
            .build();
        let schema = rows_schema(&create);
        engine
            .handle_request(region_id, RegionRequest::Create(create))
            .await
            .unwrap();
        for _ in 0..2 {
            put_rows(
                &engine,
                region_id,
                Rows {
                    schema: schema.clone(),
                    rows: build_rows_for_key("a", 0, 10, 0),
                },
            )
            .await;
            flush_region(&engine, region_id, None).await;
        }
        let engine = engine.clone();
        jobs.push(tokio::spawn(async move {
            engine
                .handle_request(
                    region_id,
                    RegionRequest::Compact(RegionCompactRequest {
                        options: api::v1::region::compact_request::Options::StrictWindow(
                            api::v1::region::StrictWindow {
                                window_seconds: 3600,
                            },
                        ),
                        ..Default::default()
                    }),
                )
                .await
                .unwrap();
        }));
    }
    tokio::time::timeout(Duration::from_secs(20), gate.entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    let entries = engine.inspect_compaction_tasks(&[]).await.unwrap();
    assert_eq!(entries.len(), 2);
    for entry in &entries {
        assert_eq!(entry.state.as_ref(), "running");
        assert_eq!(entry.trigger_type.as_ref(), "manual");
        assert_eq!(entry.input_files, Some(2));
        let options: serde_json::Value = serde_json::from_str(&entry.request_options).unwrap();
        assert_eq!(options["window_seconds"], 3600);
        assert!(entry.effective_options.is_some());
    }
    let lazy = SessionContext::new();
    lazy.register_table(
        "tasks",
        Arc::new(InspectTable::new(Arc::new(CompactionTasksSource::new(
            engine.clone(),
        )))),
    )
    .unwrap();
    let eager = SessionContext::new();
    let batch = CompactionTaskEntry::to_record_batch(&entries).unwrap();
    eager
        .register_table(
            "tasks",
            Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    // Repeat with identical state and workload; only the scan implementation differs.
    for _ in 0..3 {
        for sql in [
            "SELECT task_id, request_options, effective_options FROM tasks ORDER BY task_id",
            "SELECT task_id, input_time_start, input_time_end, output_time_start, output_time_end FROM tasks ORDER BY task_id",
            "SELECT task_id FROM tasks WHERE input_time_start <= CAST('1970-01-01T00:00:01Z' AS TIMESTAMP(9)) AND output_time_end > CAST('1970-01-01T00:00:01Z' AS TIMESTAMP(9)) ORDER BY task_id",
            "SELECT task_id FROM tasks WHERE output_time_end IS NULL ORDER BY task_id",
            "SELECT task_id FROM tasks WHERE table_id = 12 AND state = 'running' LIMIT 1",
            "SELECT count(*) FROM tasks WHERE table_id = 12",
            "SELECT task_id FROM tasks WHERE table_id = 99 LIMIT 1",
            "SELECT task_id FROM tasks WHERE table_id = 99 OR trigger_type = 'manual' ORDER BY task_id",
            "SELECT task_id FROM tasks WHERE length(request_options) > 2 ORDER BY task_id LIMIT 1",
            "SELECT task_id FROM tasks WHERE table_id = 12 AND length(request_options) < 2 LIMIT 1",
            "SELECT task_id FROM tasks WHERE table_id = 99 OR length(request_options) > 2 ORDER BY task_id LIMIT 1",
            "SELECT table_id FROM tasks ORDER BY table_id DESC LIMIT 1",
            "SELECT task_id FROM tasks WHERE node_id IS NULL ORDER BY task_id",
            "SELECT task_id FROM tasks LIMIT 0",
        ] {
            assert_eq!(query(&lazy, sql).await, query(&eager, sql).await, "{sql}");
        }
    }
    drop(guard);
    for job in jobs {
        tokio::time::timeout(Duration::from_secs(20), job)
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if engine
                .inspect_compaction_tasks(&[])
                .await
                .unwrap()
                .is_empty()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        query(&lazy, "SELECT task_id FROM tasks").await.num_rows(),
        0
    );
}
