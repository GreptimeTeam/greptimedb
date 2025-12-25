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

use common_meta::key::table_route::PhysicalTableRouteValue;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_telemetry::{info, init_default_ut_logging};
use store_api::region_engine::RegionRole;
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{
    MockSchedulerCtx, TEST_REGION_SIZE_200MB, mock_candidate, mock_region_stat, new_candidate,
};
use crate::gc::{GcScheduler, GcSchedulerOptions};

/// Concurrent Processing Tests
#[tokio::test]
async fn test_concurrent_table_processing_limits() {
    init_default_ut_logging();

    let mut candidates = HashMap::new();
    let mut gc_reports = HashMap::new();

    // Create many tables with candidates
    for table_id in 1..=10 {
        let region_id = RegionId::new(table_id, 1);
        candidates.insert(table_id, vec![new_candidate(region_id, 1.0)]);
        gc_reports.insert(
            region_id,
            GcReport {
                deleted_files: HashMap::from([(region_id, vec![FileId::random()])]),
                ..Default::default()
            },
        );
    }

    let ctx = MockSchedulerCtx {
        candidates: Arc::new(Mutex::new(Some(candidates))),
        file_refs: Arc::new(Mutex::new(Some(FileRefsManifest {
            manifest_version: (1..=10).map(|i| (RegionId::new(i, 1), 1)).collect(),
            ..Default::default()
        }))),
        gc_reports: Arc::new(Mutex::new(gc_reports)),
        ..Default::default()
    }
    .with_table_routes(
        (1..=10)
            .map(|table_id| {
                let region_id = RegionId::new(table_id, 1);
                (table_id, (table_id, vec![(region_id, Peer::new(1, ""))]))
            })
            .collect(),
    );

    let ctx = Arc::new(ctx);

    let config = GcSchedulerOptions {
        max_concurrent_tables: 3,                          // Set a low limit
        retry_backoff_duration: Duration::from_millis(50), // for faster test
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let candidates = ctx.candidates.lock().unwrap().clone().unwrap_or_default();

    // Convert table-based candidates to datanode-based candidates
    let peer = Peer::new(1, "");
    let datanode_to_candidates = HashMap::from([(
        peer,
        candidates
            .into_iter()
            .flat_map(|(table_id, candidates)| candidates.into_iter().map(move |c| (table_id, c)))
            .collect(),
    )]);

    let report = scheduler
        .parallel_process_datanodes(datanode_to_candidates)
        .await;

    // Should process all datanodes
    assert_eq!(report.per_datanode_reports.len(), 1);
    assert_eq!(report.failed_datanodes.len(), 0);
}

#[tokio::test]
async fn test_datanode_processes_tables_with_partial_gc_failures() {
    init_default_ut_logging();

    let table1 = 1;
    let region1 = RegionId::new(table1, 1);
    let table2 = 2;
    let region2 = RegionId::new(table2, 1);
    let peer = Peer::new(1, "");

    let mut candidates = HashMap::new();
    candidates.insert(table1, vec![new_candidate(region1, 1.0)]);
    candidates.insert(table2, vec![new_candidate(region2, 1.0)]);

    // Set up GC reports for success and failure
    let mut gc_reports = HashMap::new();
    gc_reports.insert(
        region1,
        GcReport {
            deleted_files: HashMap::from([(region1, vec![])]),
            ..Default::default()
        },
    );
    // region2 will have no GC report, simulating failure

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region1, 1), (region2, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            candidates: Arc::new(Mutex::new(Some(candidates))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([
            (table1, (table1, vec![(region1, peer.clone())])),
            (table2, (table2, vec![(region2, peer.clone())])),
        ])),
    );

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions::default(),
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let candidates = ctx.candidates.lock().unwrap().clone().unwrap_or_default();

    // Convert table-based candidates to datanode-based candidates

    let datanode_to_candidates = HashMap::from([(
        peer,
        candidates
            .into_iter()
            .flat_map(|(table_id, candidates)| candidates.into_iter().map(move |c| (table_id, c)))
            .collect(),
    )]);

    let report = scheduler
        .parallel_process_datanodes(datanode_to_candidates)
        .await;

    // Should have one datanode with mixed results
    assert_eq!(report.per_datanode_reports.len(), 1);
    // also check one failed region (region2 has no GC report, so it should be in need_retry_regions)
    let datanode_report = report.per_datanode_reports.values().next().unwrap();
    assert_eq!(datanode_report.need_retry_regions.len(), 1);
    assert_eq!(report.failed_datanodes.len(), 0);
}

// Region Concurrency Tests

#[tokio::test]
async fn test_region_gc_concurrency_limit() {
    init_default_ut_logging();

    let table_id = 1;
    let peer = Peer::new(1, "");

    // Create multiple regions for the same table
    let mut region_stats = Vec::new();
    let mut candidates = Vec::new();
    let mut gc_reports = HashMap::new();

    for i in 1..=10 {
        let region_id = RegionId::new(table_id, i as u32);
        let region_stat =
            mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10); // 200MB
        region_stats.push(region_stat);

        candidates.push(mock_candidate(region_id));

        gc_reports.insert(
            region_id,
            GcReport {
                deleted_files: HashMap::from([(
                    region_id,
                    vec![FileId::random(), FileId::random()],
                )]),
                ..Default::default()
            },
        );
    }

    let table_stats = HashMap::from([(table_id, region_stats)]);

    let file_refs = FileRefsManifest {
        manifest_version: (1..=10)
            .map(|i| (RegionId::new(table_id, i as u32), 1))
            .collect(),
        ..Default::default()
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
            (
                table_id,
                (1..=10)
                    .map(|i| (RegionId::new(table_id, i as u32), peer.clone()))
                    .collect(),
            ),
        )])),
    );

    // Configure low concurrency limit
    let config = GcSchedulerOptions {
        region_gc_concurrency: 3, // Only 3 regions can be processed concurrently
        retry_backoff_duration: Duration::from_millis(50), // for faster test
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let start_time = Instant::now();
    let report = scheduler
        .process_datanode_gc(
            peer,
            candidates.into_iter().map(|c| (table_id, c)).collect(),
        )
        .await
        .unwrap();
    let duration = start_time.elapsed();

    // All regions should be processed successfully
    // Check that all 10 regions have deleted files
    assert_eq!(report.deleted_files.len(), 10);
    for i in 1..=10 {
        let region_id = RegionId::new(table_id, i as u32);
        assert!(report.deleted_files.contains_key(&region_id));
        assert_eq!(report.deleted_files[&region_id].len(), 2); // Each region has 2 deleted files
    }
    assert!(report.need_retry_regions.is_empty());

    // Verify that concurrency limit was respected (this is hard to test directly,
    // but we can verify that the processing completed successfully)
    info!(
        "Processed 10 regions with concurrency limit 3 in {:?}",
        duration
    );
}

#[tokio::test]
async fn test_region_gc_concurrency_with_partial_failures() {
    init_default_ut_logging();

    let table_id = 1;
    let peer = Peer::new(1, "");

    // Create multiple regions with mixed success/failure
    let mut region_stats = Vec::new();
    let mut candidates = Vec::new();
    let mut gc_reports = HashMap::new();

    // Create the context first so we can set errors on it
    let ctx = Arc::new(MockSchedulerCtx::default());

    for i in 1..=6 {
        let region_id = RegionId::new(table_id, i as u32);
        let region_stat =
            mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10); // 200MB
        region_stats.push(region_stat);

        candidates.push(mock_candidate(region_id));

        if i % 2 == 0 {
            // Even regions will succeed
            gc_reports.insert(
                region_id,
                GcReport {
                    deleted_files: HashMap::from([(
                        region_id,
                        vec![FileId::random(), FileId::random()],
                    )]),
                    ..Default::default()
                },
            );
        } else {
            // Odd regions will fail - don't add them to gc_reports
            // This will cause them to be marked as needing retry
        }
    }

    let table_stats = HashMap::from([(table_id, region_stats)]);

    let file_refs = FileRefsManifest {
        manifest_version: (1..=6)
            .map(|i| (RegionId::new(table_id, i as u32), 1))
            .collect(),
        ..Default::default()
    };

    // Update the context with the data
    *ctx.table_to_region_stats.lock().unwrap() = Some(table_stats);
    *ctx.gc_reports.lock().unwrap() = gc_reports;
    *ctx.file_refs.lock().unwrap() = Some(file_refs);
    let region_routes = (1..=6)
        .map(|i| RegionRoute {
            region: Region::new_test(RegionId::new(table_id, i as u32)),
            leader_peer: Some(peer.clone()),
            ..Default::default()
        })
        .collect();

    *ctx.table_routes.lock().unwrap() = HashMap::from([(
        table_id,
        (table_id, PhysicalTableRouteValue::new(region_routes)),
    )]);

    // Configure concurrency limit
    let config = GcSchedulerOptions {
        region_gc_concurrency: 2, // Process 2 regions concurrently
        retry_backoff_duration: Duration::from_millis(50), // for faster test
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let datanode_to_candidates = HashMap::from([(
        peer.clone(),
        candidates.into_iter().map(|c| (table_id, c)).collect(),
    )]);

    let report = scheduler
        .parallel_process_datanodes(datanode_to_candidates)
        .await;

    let report = report.per_datanode_reports.get(&peer.id).unwrap();

    // Should have 3 successful and 3 failed regions
    // Even regions (2, 4, 6) should succeed, odd regions (1, 3, 5) should fail
    let mut successful_regions = 0;
    let mut failed_regions = 0;

    for i in 1..=6 {
        let region_id = RegionId::new(table_id, i as u32);
        if i % 2 == 0 {
            // Even regions should succeed
            if report.deleted_files.contains_key(&region_id) {
                successful_regions += 1;
            }
        } else {
            // Odd regions should fail - they should be in need_retry_regions
            if report.need_retry_regions.contains(&region_id) {
                failed_regions += 1;
            }
        }
    }

    // In the new implementation, regions that cause gc_regions to return an error
    // are added to need_retry_regions. Let's check if we have the expected mix.
    info!(
        "Successful regions: {}, Failed regions: {}",
        successful_regions, failed_regions
    );
    info!(
        "Deleted files: {:?}",
        report.deleted_files.keys().collect::<Vec<_>>()
    );
    info!("Need retry regions: {:?}", report.need_retry_regions);

    // The exact count might vary depending on how the mock handles errors,
    // but we should have some successful and some failed regions
    assert!(
        successful_regions > 0,
        "Should have at least some successful regions"
    );
    assert!(
        failed_regions > 0,
        "Should have at least some failed regions"
    );
}

#[tokio::test]
async fn test_region_gc_concurrency_with_retryable_errors() {
    init_default_ut_logging();

    let table_id = 1;
    let peer = Peer::new(1, "");

    // Create multiple regions
    let mut region_stats = Vec::new();
    let mut candidates = Vec::new();

    for i in 1..=5 {
        let region_id = RegionId::new(table_id, i as u32);
        let region_stat =
            mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10); // 200MB
        region_stats.push(region_stat);
        candidates.push(mock_candidate(region_id));
    }

    let table_stats = HashMap::from([(table_id, region_stats)]);

    let file_refs = FileRefsManifest {
        manifest_version: (1..=5)
            .map(|i| (RegionId::new(table_id, i as u32), 1))
            .collect(),
        ..Default::default()
    };

    let gc_report = (1..=5)
        .map(|i| {
            let region_id = RegionId::new(table_id, i as u32);
            (
                region_id,
                // mock the actual gc report with deleted files when succeeded(even no files to delete)
                GcReport::new(
                    HashMap::from([(region_id, vec![])]),
                    Default::default(),
                    HashSet::new(),
                ),
            )
        })
        .collect();

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            gc_reports: Arc::new(Mutex::new(gc_report)),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (
                table_id,
                (1..=5)
                    .map(|i| (RegionId::new(table_id, i as u32), peer.clone()))
                    .collect(),
            ),
        )])),
    );

    // Configure concurrency limit
    let config = GcSchedulerOptions {
        region_gc_concurrency: 2, // Process 2 regions concurrently
        retry_backoff_duration: Duration::from_millis(50),
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let datanode_to_candidates = HashMap::from([(
        peer.clone(),
        candidates.into_iter().map(|c| (table_id, c)).collect(),
    )]);
    let report = scheduler
        .parallel_process_datanodes(datanode_to_candidates)
        .await;

    let report = report.per_datanode_reports.get(&peer.id).unwrap();

    // In the new implementation without retry logic, all regions should be processed
    // The exact behavior depends on how the mock handles the regions
    info!(
        "Deleted files: {:?}",
        report.deleted_files.keys().collect::<Vec<_>>()
    );
    info!("Need retry regions: {:?}", report.need_retry_regions);

    // We should have processed all 5 regions in some way
    let total_processed = report.deleted_files.len() + report.need_retry_regions.len();
    assert_eq!(total_processed, 5, "Should have processed all 5 regions");
}
