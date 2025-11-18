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

use crate::gc::mock::{MockSchedulerCtx, mock_candidate, mock_region_stat, new_candidate};
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
        .process_datanodes_with_retry(datanode_to_candidates)
        .await;

    // Should process all datanodes
    assert_eq!(report.per_datanode_reports.len(), 1);
    assert_eq!(report.failed_datanodes.len(), 0);
}

#[tokio::test]
async fn test_mixed_success_failure_tables() {
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
            (table2, (table2, vec![(region2, peer)])),
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
    let peer = Peer::new(1, "");
    let datanode_to_candidates = HashMap::from([(
        peer,
        candidates
            .into_iter()
            .flat_map(|(table_id, candidates)| candidates.into_iter().map(move |c| (table_id, c)))
            .collect(),
    )]);

    let report = scheduler
        .process_datanodes_with_retry(datanode_to_candidates)
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
        let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
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
async fn test_region_gc_concurrency_with_mixed_results() {
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
        let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
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
            // Odd regions will fail - set non-retryable error for specific region
            ctx.set_gc_regions_error_for_region(
                region_id,
                crate::error::RegionRouteNotFoundSnafu { region_id }.build(),
            );
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

    let dn2candidates = HashMap::from([(
        peer.clone(),
        candidates.into_iter().map(|c| (table_id, c)).collect(),
    )]);

    let report = scheduler.process_datanodes_with_retry(dn2candidates).await;

    let report = report.per_datanode_reports.get(&peer.id).unwrap();

    // Should have 3 successful and 3 failed regions
    // Even regions (2, 4, 6) should succeed, odd regions (1, 3, 5) should fail
    let mut successful_regions = 0;
    let mut failed_regions = 0;

    for i in 1..=6 {
        let region_id = RegionId::new(table_id, i as u32);
        if i % 2 == 0 {
            // Even regions should succeed
            assert!(
                report.deleted_files.contains_key(&region_id),
                "Even region {} should succeed",
                i
            );
            successful_regions += 1;
        } else {
            // Odd regions should fail
            assert!(
                report.need_retry_regions.contains(&region_id),
                "Odd region {} should fail",
                i
            );
            failed_regions += 1;
        }
    }

    assert_eq!(successful_regions, 3, "Should have 3 successful regions");
    assert_eq!(failed_regions, 3, "Should have 3 failed regions");
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
        let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
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
                GcReport::new(HashMap::from([(region_id, vec![])]), HashSet::new()),
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

    // Configure concurrency limit and retry settings
    let config = GcSchedulerOptions {
        region_gc_concurrency: 2, // Process 2 regions concurrently
        max_retries_per_region: 2,
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

    // Set up retryable errors for all regions (they'll succeed after 1 retry)
    for i in 1..=5 {
        let region_id = RegionId::new(table_id, i as u32);
        ctx.set_gc_regions_success_after_retries(region_id, 1);
    }

    let dn2candidates = HashMap::from([(
        peer.clone(),
        candidates.into_iter().map(|c| (table_id, c)).collect(),
    )]);
    let report = scheduler.process_datanodes_with_retry(dn2candidates).await;

    let report = report.per_datanode_reports.get(&peer.id).unwrap();

    // All regions should eventually succeed after retries
    assert_eq!(report.deleted_files.len(), 5);
    for i in 1..=5 {
        let region_id = RegionId::new(table_id, i as u32);
        assert!(
            report.deleted_files.contains_key(&region_id),
            "Region {} should succeed",
            i
        );
    }
    assert!(report.need_retry_regions.is_empty());

    // Verify that retries were attempted for all regions
    for i in 1..=5 {
        let region_id = RegionId::new(table_id, i as u32);
        let retry_count = ctx.get_retry_count(region_id);
        assert!(
            retry_count >= 1,
            "Region {} should have at least 1 attempt",
            region_id
        );
    }
}
