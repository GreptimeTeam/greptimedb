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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common_meta::peer::Peer;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionRole;
use store_api::storage::{FileId, FileRefsManifest, GcReport, RegionId};

use crate::gc::mock::{MockSchedulerCtx, mock_candidate, mock_region_stat, new_empty_report_with};
use crate::gc::{GcScheduler, GcSchedulerOptions};

/// Retry Logic Tests
#[tokio::test]
async fn test_retry_logic_with_retryable_errors() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let gc_report = GcReport {
        deleted_files: HashMap::from([(region_id, vec![FileId::random(), FileId::random()])]),
        ..Default::default()
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(region_id, gc_report)]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    // Configure retry settings
    let config = GcSchedulerOptions {
        max_retries_per_region: 3,
        retry_backoff_duration: Duration::from_millis(100), // Short backoff for testing
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Test 1: Success after 2 retryable errors
    ctx.reset_retry_tracking();
    ctx.set_gc_regions_success_after_retries(region_id, 2);

    let start_time = Instant::now();
    let mut datanode_to_candidates = HashMap::new();
    datanode_to_candidates.insert(peer.clone(), vec![(table_id, mock_candidate(region_id))]);

    let job_report = scheduler
        .process_datanodes_with_retry(datanode_to_candidates)
        .await;
    let duration = start_time.elapsed();

    // Extract the datanode report from the job report
    let report = job_report.per_datanode_reports.get(&peer.id).unwrap();

    // Check that the report has deleted files for the region
    assert!(report.deleted_files.contains_key(&region_id));
    assert_eq!(report.deleted_files[&region_id].len(), 2); // We created 2 files in the mock
    // The current implementation retries all errors up to max_retries_per_region
    // so the region may still be in need_retry_regions after all retries are exhausted
    assert_eq!(ctx.get_retry_count(region_id), 3); // 2 retries + 1 success (handled by mock)

    // Verify backoff was applied (should take at least 200ms for 2 retries with 100ms backoff)
    assert!(
        duration >= Duration::from_millis(200),
        "Expected backoff duration not met: {:?}",
        duration
    );

    // Test 2: Exceed max retries
    ctx.reset_retry_tracking();
    ctx.set_gc_regions_success_after_retries(region_id, 5); // More than max_retries_per_region (3)

    let dn2candidates =
        HashMap::from([(peer.clone(), vec![(table_id, mock_candidate(region_id))])]);
    let report = scheduler.process_datanodes_with_retry(dn2candidates).await;

    let report = report.per_datanode_reports.get(&peer.id).unwrap();

    // Since the region exceeded max retries, it should be in need_retry_regions
    assert!(report.need_retry_regions.contains(&region_id));
    assert_eq!(ctx.get_retry_count(region_id), 4); // 1 initial + 3 retries (mock tracks all attempts)
}

#[tokio::test]
async fn test_retry_logic_with_error_sequence() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let gc_report = GcReport {
        deleted_files: HashMap::from([(region_id, vec![FileId::random(), FileId::random()])]),
        ..Default::default()
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(region_id, gc_report)]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    let config = GcSchedulerOptions {
        max_retries_per_region: 5,
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

    // Test with sequence: retryable error -> retryable error -> success
    let retryable_error1 = crate::error::RetryLaterSnafu {
        reason: "Test retryable error 1".to_string(),
    }
    .build();

    let retryable_error2 = crate::error::MailboxTimeoutSnafu { id: 123u64 }.build();

    ctx.reset_retry_tracking();
    ctx.set_gc_regions_error_sequence(vec![retryable_error1, retryable_error2]);

    let mut datanode_to_candidates = HashMap::new();
    datanode_to_candidates.insert(peer.clone(), vec![(table_id, mock_candidate(region_id))]);

    let job_report = scheduler
        .process_datanodes_with_retry(datanode_to_candidates)
        .await;

    // Extract the datanode report from the job report
    let report = job_report.per_datanode_reports.get(&peer.id).unwrap();

    // Check that the report has deleted files for the region
    assert!(report.deleted_files.contains_key(&region_id));
    assert_eq!(report.deleted_files[&region_id].len(), 2); // We created 2 files in the mock
    // The current implementation retries all errors up to max_retries_per_region
    // so the region may still be in need_retry_regions after all retries are exhausted
    assert_eq!(ctx.get_retry_count(region_id), 3); // 2 errors + 1 success (handled by mock)
}

#[tokio::test]
async fn test_retry_logic_non_retryable_error() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10); // 200MB
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(
                region_id,
                new_empty_report_with([region_id]),
            )]))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    let config = GcSchedulerOptions {
        max_retries_per_region: 3,
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

    // Test with non-retryable error (should fail immediately)
    let non_retryable_error = crate::error::RegionRouteNotFoundSnafu { region_id }.build();

    ctx.reset_retry_tracking();
    ctx.gc_regions_error
        .lock()
        .unwrap()
        .replace(non_retryable_error);

    let mut datanode_to_candidates = HashMap::new();
    datanode_to_candidates.insert(peer.clone(), vec![(table_id, mock_candidate(region_id))]);

    let job_report = scheduler
        .process_datanodes_with_retry(datanode_to_candidates)
        .await;

    // Extract the datanode report from the job report
    let report = job_report.per_datanode_reports.get(&peer.id).unwrap();

    // Non-retryable error should still retry(then success)
    assert!(report.deleted_files.contains_key(&region_id));
    assert_eq!(ctx.get_retry_count(region_id), 2); // Should retry up to max_retries_per_region (3 retry + 1 success)
}

/// Test need_retry_regions functionality with multi-round retry
#[tokio::test]
async fn test_need_retry_regions_multi_round() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id1 = RegionId::new(table_id, 1);
    let region_id2 = RegionId::new(table_id, 2);
    let peer1 = Peer::new(1, "");
    let peer2 = Peer::new(2, "");

    let region_stat1 = mock_region_stat(region_id1, RegionRole::Leader, 200_000_000, 10);
    let region_stat2 = mock_region_stat(region_id2, RegionRole::Leader, 150_000_000, 8);
    let table_stats = HashMap::from([(table_id, vec![region_stat1, region_stat2])]);

    // Create GC reports with need_retry_regions
    let gc_report1 = GcReport {
        deleted_files: HashMap::from([(region_id1, vec![FileId::random()])]),
        need_retry_regions: std::collections::HashSet::from([region_id1]), // This region needs retry
    };

    let gc_report2 = GcReport {
        deleted_files: HashMap::from([(region_id2, vec![FileId::random()])]),
        need_retry_regions: std::collections::HashSet::new(), // This region succeeds
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id1, 1), (region_id2, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([
                (region_id1, gc_report1),
                (region_id2, gc_report2),
            ]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (
                table_id,
                vec![(region_id1, peer1.clone()), (region_id2, peer2.clone())],
            ),
        )])),
    );

    let config = GcSchedulerOptions {
        max_retries_per_region: 3,
        retry_backoff_duration: Duration::from_millis(100),
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Set up success after 1 retry for region1 (which needs retry)
    ctx.reset_retry_tracking();
    ctx.set_gc_regions_success_after_retries(region_id1, 1);

    let start_time = Instant::now();
    // Use peer1 since both regions belong to the same table and we're testing single datanode scenario
    let report1 = scheduler
        .process_datanodes_with_retry(HashMap::from([(
            peer1.clone(),
            vec![(table_id, mock_candidate(region_id1))],
        )]))
        .await;
    let report2 = scheduler
        .process_datanodes_with_retry(HashMap::from([(
            peer2.clone(),
            vec![(table_id, mock_candidate(region_id2))],
        )]))
        .await;
    let duration = start_time.elapsed();

    let report1 = report1.per_datanode_reports.get(&peer1.id).unwrap();
    let report2 = report2.per_datanode_reports.get(&peer2.id).unwrap();

    // Verify results - both reports should have their respective regions
    assert!(report1.deleted_files.contains_key(&region_id1));
    assert_eq!(report1.deleted_files[&region_id1].len(), 1); // We created 1 file in the mock
    assert!(report1.need_retry_regions.is_empty());

    assert!(report2.deleted_files.contains_key(&region_id2));
    assert_eq!(report2.deleted_files[&region_id2].len(), 1); // We created 1 file in the mock
    assert!(report2.need_retry_regions.is_empty());

    // Verify retry count for region1 (should be 2: 1 initial failure + 1 retry success)
    assert_eq!(ctx.get_retry_count(region_id1), 2);
    // region2 should only be processed once (no retry needed)
    assert_eq!(ctx.get_retry_count(region_id2), 1);

    // Verify exponential backoff was applied (should take at least 100ms for 1 retry round)
    assert!(
        duration >= Duration::from_millis(100),
        "Expected backoff duration not met: {:?}",
        duration
    );
}

/// Test need_retry_regions with exponential backoff across multiple rounds
#[tokio::test]
async fn test_need_retry_regions_exponential_backoff() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10);
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    // Create GC report that will need multiple retry rounds
    let gc_report = GcReport {
        deleted_files: HashMap::from([(region_id, vec![FileId::random()])]),
        need_retry_regions: std::collections::HashSet::from([]),
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(region_id, gc_report)]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    let config = GcSchedulerOptions {
        max_retries_per_region: 4,
        retry_backoff_duration: Duration::from_millis(50), // Short base duration for testing
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Set up success after 3 retries (so 4 total rounds: initial + 3 retries)
    ctx.reset_retry_tracking();
    ctx.set_gc_regions_success_after_retries(region_id, 3);

    let start_time = Instant::now();
    let mut datanode_to_candidates = HashMap::new();
    datanode_to_candidates.insert(peer.clone(), vec![(table_id, mock_candidate(region_id))]);
    let job_report = scheduler
        .process_datanodes_with_retry(datanode_to_candidates)
        .await;
    let duration = start_time.elapsed();

    // Extract the datanode report from the job report
    let report = job_report.per_datanode_reports.get(&peer.id).unwrap();

    // Verify results
    assert!(report.deleted_files.contains_key(&region_id));
    assert!(report.need_retry_regions.is_empty());
    assert_eq!(ctx.get_retry_count(region_id), 4); // 1 initial + 3 retries (mock tracks all attempts)

    // Verify exponential backoff was applied
    // Expected backoff: 50ms + 100ms = 150ms minimum
    assert!(
        duration >= Duration::from_millis(150),
        "Expected exponential backoff duration not met: {:?}",
        duration
    );
}

/// Test need_retry_regions with table route rediscovery failure
#[tokio::test]
async fn test_need_retry_regions_table_route_failure() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id = RegionId::new(table_id, 1);
    let peer = Peer::new(1, "");

    let region_stat = mock_region_stat(region_id, RegionRole::Leader, 200_000_000, 10);
    let table_stats = HashMap::from([(table_id, vec![region_stat])]);

    // Create GC report with need_retry_regions
    let gc_report = GcReport {
        deleted_files: HashMap::from([(region_id, vec![FileId::random()])]),
        need_retry_regions: std::collections::HashSet::from([region_id]),
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id, 1)]),
        ..Default::default()
    };

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([(region_id, gc_report)]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(
            table_id,
            (table_id, vec![(region_id, peer.clone())]),
        )])),
    );

    // Inject table route error to simulate rediscovery failure
    let table_route_error = crate::error::RegionRouteNotFoundSnafu { region_id }.build();
    ctx.set_table_route_error(table_route_error);

    let config = GcSchedulerOptions {
        max_retries_per_region: 3,
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

    let report = scheduler
        .process_datanodes_with_retry(HashMap::from([(
            peer.clone(),
            vec![(table_id, mock_candidate(region_id))],
        )]))
        .await;
    assert!(report.per_datanode_reports.is_empty());
    assert!(report.failed_datanodes.contains_key(&peer.id));
}

/// Test need_retry_regions with mixed success/failure scenarios
#[tokio::test]
async fn test_need_retry_regions_mixed_scenarios() {
    init_default_ut_logging();

    let table_id = 1;
    let region_id1 = RegionId::new(table_id, 1); // Will succeed after retry
    let region_id2 = RegionId::new(table_id, 2); // Will fail after max retries
    let region_id3 = RegionId::new(table_id, 3); // Will succeed immediately
    let peer = Peer::new(1, "");

    let region_stat1 = mock_region_stat(region_id1, RegionRole::Leader, 200_000_000, 10);
    let region_stat2 = mock_region_stat(region_id2, RegionRole::Leader, 150_000_000, 8);
    let region_stat3 = mock_region_stat(region_id3, RegionRole::Leader, 100_000_000, 5);
    let table_stats = HashMap::from([(table_id, vec![region_stat1, region_stat2, region_stat3])]);

    // Create GC reports
    let gc_report1 = GcReport {
        deleted_files: HashMap::from([(region_id1, vec![FileId::random()])]),
        need_retry_regions: std::collections::HashSet::from([region_id1]),
    };

    let gc_report2 = GcReport {
        deleted_files: HashMap::from([(region_id2, vec![FileId::random()])]),
        need_retry_regions: std::collections::HashSet::from([region_id2]),
    };

    let gc_report3 = GcReport {
        deleted_files: HashMap::from([(region_id3, vec![FileId::random()])]),
        need_retry_regions: std::collections::HashSet::new(),
    };

    let file_refs = FileRefsManifest {
        manifest_version: HashMap::from([(region_id1, 1), (region_id2, 1), (region_id3, 1)]),
        ..Default::default()
    };

    let routes = vec![
        (region_id1, peer.clone()),
        (region_id2, peer.clone()),
        (region_id3, peer),
    ];

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([
                (region_id1, gc_report1),
                (region_id2, gc_report2),
                (region_id3, gc_report3),
            ]))),
            file_refs: Arc::new(Mutex::new(Some(file_refs))),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(table_id, (table_id, routes.clone()))])),
    );

    let config = GcSchedulerOptions {
        max_retries_per_region: 2, // Low limit to test failure case
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

    // Set up different retry scenarios
    ctx.reset_retry_tracking();
    ctx.set_gc_regions_success_after_retries(region_id1, 1); // Succeed after 1 retry
    ctx.set_gc_regions_success_after_retries(region_id2, 5); // Needs more retries than limit (2)
    // region3 succeeds immediately (default behavior)

    // Process each region separately with its corresponding peer and check results directly
    for (region_id, peer) in routes {
        let dn2candidate =
            HashMap::from([(peer.clone(), vec![(table_id, mock_candidate(region_id))])]);
        let report = scheduler.process_datanodes_with_retry(dn2candidate).await;

        let report = report.per_datanode_reports.get(&peer.id).unwrap();

        if region_id == region_id1 {
            // region1 should succeed after 1 retry
            assert!(report.deleted_files.contains_key(&region_id1));
            assert!(report.need_retry_regions.is_empty());
        } else if region_id == region_id2 {
            // region2 should fail due to retry limit
            assert!(!report.deleted_files.contains_key(&region_id2));
            assert!(report.need_retry_regions.contains(&region_id2));
        } else if region_id == region_id3 {
            // region3 should succeed immediately
            assert!(report.deleted_files.contains_key(&region_id3));
            assert!(report.need_retry_regions.is_empty());
        }
    }

    // Verify retry counts
    assert_eq!(ctx.get_retry_count(region_id1), 2); // Succeeds immediately
    assert_eq!(ctx.get_retry_count(region_id2), 3); // Stops at max_retries_per_region
    assert_eq!(ctx.get_retry_count(region_id3), 1); // Succeeds immediately
}
