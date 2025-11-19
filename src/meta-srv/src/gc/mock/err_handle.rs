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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common_meta::datanode::RegionManifestInfo;
use common_meta::peer::Peer;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionRole;
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{MockSchedulerCtx, mock_region_stat, new_empty_report_with};
use crate::gc::{GcScheduler, GcSchedulerOptions};

/// Error Handling Tests
#[tokio::test]
async fn test_gc_regions_failure_handling() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    // Create region stat with proper size and file_removed_cnt to ensure it gets selected as candidate
    let mut region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut region_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    // Create a context that will return an error for gc_regions
    let mut gc_reports = HashMap::new();
    gc_reports.insert(region_id, GcReport::default());

    // Inject an error for gc_regions method
    let gc_error = crate::error::UnexpectedSnafu {
        violated: "Simulated GC failure for testing".to_string(),
    }
    .build();

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        file_refs: HashMap::from([(region_id, HashSet::from([FileId::random()]))]),
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer)]),
        )]))
        .with_gc_regions_error(gc_error),
    );

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions::default(),
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // This should handle the failure gracefully
    let report = scheduler.handle_tick().await.unwrap();

    // Validate the report shows the failure handling
    assert_eq!(
        report.per_datanode_reports.len(),
        1,
        "Should process 1 datanode despite failure"
    );
    assert_eq!(
        report.failed_datanodes.len(),
        0,
        "Should have 0 failed datanodes (failure handled via need_retry_regions)"
    );

    // Check that the region is in need_retry_regions due to the failure
    let datanode_report = report.per_datanode_reports.values().next().unwrap();
    assert_eq!(
        datanode_report.need_retry_regions.len(),
        1,
        "Should have 1 region in need_retry_regions due to failure"
    );
    assert!(
        datanode_report.need_retry_regions.contains(&region_id),
        "Region should be in need_retry_regions"
    );

    // Verify that calls were made despite potential failures
    assert_eq!(
        *ctx.get_table_to_region_stats_calls.lock().unwrap(),
        1,
        "Expected 1 call to get_table_to_region_stats"
    );
    assert!(
        *ctx.get_file_references_calls.lock().unwrap() >= 1,
        "Expected at least 1 call to get_file_references"
    );
    assert!(
        *ctx.gc_regions_calls.lock().unwrap() >= 1,
        "Expected at least 1 call to gc_regions"
    );
}

#[tokio::test]
async fn test_get_file_references_failure() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    // Create region stat with proper size and file_removed_cnt to ensure it gets selected as candidate
    let mut region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut region_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    // Create context with empty file refs (simulating failure)
    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            file_refs: Arc::new(Mutex::new(Some(FileRefsManifest::default()))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(
                region_id,
                new_empty_report_with([region_id]),
            )]))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer)]),
        )])),
    );

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions {
            retry_backoff_duration: Duration::from_millis(10), // shorten for test
            ..Default::default()
        },
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let report = scheduler.handle_tick().await.unwrap();

    // Validate the report shows the expected results
    // In the new implementation, even if get_file_references fails, we still create a datanode report
    assert_eq!(
        report.per_datanode_reports.len(),
        1,
        "Should process 1 datanode"
    );
    assert_eq!(
        report.failed_datanodes.len(),
        0,
        "Should have 0 failed datanodes (failure handled gracefully)"
    );

    // The region should be processed but may have empty results due to file refs failure
    let datanode_report = report.per_datanode_reports.values().next().unwrap();
    // The current implementation still processes the region even with file refs failure
    // and creates an empty entry in deleted_files
    assert!(
        datanode_report.deleted_files.contains_key(&region_id),
        "Should have region in deleted_files (even if empty)"
    );
    assert!(
        datanode_report.deleted_files[&region_id].is_empty(),
        "Should have empty deleted files due to file refs failure"
    );

    // Should still attempt to get file references (may be called multiple times due to retry logic)
    assert!(
        *ctx.get_file_references_calls.lock().unwrap() >= 1,
        "Expected at least 1 call to get_file_references, got {}",
        *ctx.get_file_references_calls.lock().unwrap()
    );
}

#[tokio::test]
async fn test_get_table_route_failure() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);

    // Create region stat with proper size and file_removed_cnt to ensure it gets selected as candidate
    let mut region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut region_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    // Inject an error for get_table_route method to simulate failure
    let route_error = crate::error::UnexpectedSnafu {
        violated: "Simulated table route failure for testing".to_string(),
    }
    .build();

    // Create context with table route error injection
    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });
    ctx.set_table_route_error(route_error);

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions::default(),
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Get candidates first
    let stats = &ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();
    let candidates = scheduler.select_gc_candidates(stats).await.unwrap();

    // Convert table-based candidates to datanode-based candidates
    let datanode_to_candidates = HashMap::from([(
        Peer::new(1, ""),
        candidates
            .into_iter()
            .flat_map(|(table_id, candidates)| candidates.into_iter().map(move |c| (table_id, c)))
            .collect(),
    )]);

    // This should handle table route failure gracefully
    let report = scheduler
        .process_datanodes_with_retry(datanode_to_candidates)
        .await;

    // Should process the datanode but handle route error gracefully
    assert_eq!(
        report.per_datanode_reports.len(),
        0,
        "Expected 0 datanode report"
    );
    assert_eq!(
        report.failed_datanodes.len(),
        1,
        "Expected 1 failed datanodes (route error handled gracefully)"
    );
    assert!(
        report.failed_datanodes.contains_key(&1),
        "Failed datanodes should contain the datanode with route error"
    );
}
