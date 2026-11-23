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
use store_api::storage::{GcReport, RegionId};
use table::metadata::TableId;

use crate::gc::mock::{
    MockSchedulerCtx, TEST_REGION_SIZE_200MB, mock_region_stat, new_empty_report_with,
};
use crate::gc::options::TICKER_INTERVAL;
use crate::gc::scheduler::GcJobReport;
use crate::gc::{GcScheduler, GcSchedulerOptions};

/// Candidate Selection Tests
#[tokio::test]
async fn test_gc_candidate_filtering_by_role() {
    init_default_ut_logging();

    let table_id = 1;
    let leader_region = RegionId::new(table_id, 1);
    let follower_region = RegionId::new(table_id, 2);

    let mut leader_stat = mock_region_stat(
        leader_region,
        RegionRole::Leader,
        TEST_REGION_SIZE_200MB,
        10,
    ); // 200MB

    let mut follower_stat = mock_region_stat(
        follower_region,
        RegionRole::Follower,
        TEST_REGION_SIZE_200MB,
        10,
    ); // 200MB

    // Set up manifest info for scoring
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut leader_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut follower_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![leader_stat.clone(), follower_stat.clone()])]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config: GcSchedulerOptions::default(),
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let stats = ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();

    let candidates = scheduler.select_gc_candidates(&stats).await.unwrap();

    // Should only select leader regions
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );
    if let Some(table_candidates) = candidates.get(&table_id) {
        assert_eq!(
            table_candidates.len(),
            1,
            "Expected 1 candidate for table {}, got {}",
            table_id,
            table_candidates.len()
        );
        assert_eq!(
            table_candidates[0].region_id, leader_region,
            "Expected leader region {}, got {}",
            leader_region, table_candidates[0].region_id
        );
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

#[tokio::test]
async fn test_gc_candidate_size_threshold() {
    init_default_ut_logging();

    let table_id = 1;
    let small_region = RegionId::new(table_id, 1);
    let large_region = RegionId::new(table_id, 2);

    let mut small_stat = mock_region_stat(small_region, RegionRole::Leader, 50_000_000, 5); // 50MB
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut small_stat.region_manifest
    {
        *file_removed_cnt = 3;
    }

    let mut large_stat =
        mock_region_stat(large_region, RegionRole::Leader, TEST_REGION_SIZE_200MB, 20); // 200MB
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut large_stat.region_manifest
    {
        *file_removed_cnt = 5;
    }

    let table_stats = HashMap::from([(table_id, vec![small_stat, large_stat])]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    let config = GcSchedulerOptions {
        min_region_size_threshold: 100 * 1024 * 1024, // 100MB (default)
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let stats = ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();

    let candidates = scheduler.select_gc_candidates(&stats).await.unwrap();

    // Should only select large region
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );
    if let Some(table_candidates) = candidates.get(&table_id) {
        assert_eq!(
            table_candidates.len(),
            1,
            "Expected 1 candidate for table {}, got {}",
            table_id,
            table_candidates.len()
        );
        assert_eq!(
            table_candidates[0].region_id, large_region,
            "Expected large region {}, got {}",
            large_region, table_candidates[0].region_id
        );
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

#[tokio::test]
async fn test_gc_candidate_scoring() {
    init_default_ut_logging();

    let table_id = 1;
    let low_score_region = RegionId::new(table_id, 1);
    let high_score_region = RegionId::new(table_id, 2);

    let mut low_stat = mock_region_stat(
        low_score_region,
        RegionRole::Leader,
        TEST_REGION_SIZE_200MB,
        5,
    ); // 200MB
    // Set low file removal rate for low_score_region
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut low_stat.region_manifest
    {
        *file_removed_cnt = 2;
    }

    let mut high_stat = mock_region_stat(
        high_score_region,
        RegionRole::Leader,
        TEST_REGION_SIZE_200MB,
        50,
    ); // 200MB
    // Set high file removal rate for high_score_region
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut high_stat.region_manifest
    {
        *file_removed_cnt = 20;
    }

    let table_stats = HashMap::from([(table_id, vec![low_stat, high_stat])]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    let config = GcSchedulerOptions {
        sst_count_weight: 1.0,
        file_removed_count_weight: 0.5,
        min_region_size_threshold: 100 * 1024 * 1024, // 100MB (default)
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let stats = ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();

    let candidates = scheduler.select_gc_candidates(&stats).await.unwrap();

    // Should select both regions but high score region should be first
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );
    if let Some(table_candidates) = candidates.get(&table_id) {
        assert_eq!(
            table_candidates.len(),
            2,
            "Expected 2 candidates for table {}, got {}",
            table_id,
            table_candidates.len()
        );
        // Higher score region should come first (sorted by score descending)
        assert_eq!(
            table_candidates[0].region_id, high_score_region,
            "High score region should be first"
        );
        assert!(
            table_candidates[0].score > table_candidates[1].score,
            "High score region should have higher score: {} > {}",
            table_candidates[0].score,
            table_candidates[1].score
        );
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

#[tokio::test]
async fn test_gc_candidate_regions_per_table_threshold() {
    init_default_ut_logging();

    let table_id = 1;
    // Create 10 regions for the same table
    let mut region_stats = Vec::new();

    for i in 0..10 {
        let region_id = RegionId::new(table_id, i + 1);
        let mut stat = mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 20); // 200MB

        // Set different file removal rates to create different scores
        // Higher region IDs get higher scores (better GC candidates)
        if let RegionManifestInfo::Mito {
            file_removed_cnt, ..
        } = &mut stat.region_manifest
        {
            *file_removed_cnt = (i as u64 + 1) * 2; // Region 1: 2, Region 2: 4, ..., Region 10: 20
        }

        region_stats.push(stat);
    }

    let table_stats = HashMap::from([(table_id, region_stats)]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats))),
        ..Default::default()
    });

    // Set regions_per_table_threshold to 3
    let config = GcSchedulerOptions {
        regions_per_table_threshold: 3,
        min_region_size_threshold: 100 * 1024 * 1024, // 100MB (default)
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let stats = ctx
        .table_to_region_stats
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();

    let candidates = scheduler.select_gc_candidates(&stats).await.unwrap();

    // Should have 1 table with candidates
    assert_eq!(
        candidates.len(),
        1,
        "Expected 1 table with candidates, got {}",
        candidates.len()
    );

    if let Some(table_candidates) = candidates.get(&table_id) {
        // `regions_per_table_threshold` bounds the score channel only; the
        // listing-staleness fallback appends one more slice of the same size, so a
        // table of 10 qualified regions yields at most 2 * 3 candidates.
        assert_eq!(
            table_candidates.len(),
            6,
            "Expected 2 * regions_per_table_threshold candidates for table {}, got {}",
            table_id,
            table_candidates.len()
        );

        // Verify that the top 3 scoring regions are selected
        // Regions 8, 9, 10 should have the highest scores (file_removed_cnt: 16, 18, 20)
        // They should be returned first, in descending order by score
        let expected_top_regions = vec![10, 9, 8];
        let actual_top_regions: Vec<u32> = table_candidates
            .iter()
            .take(3)
            .map(|c| c.region_id.region_number())
            .collect();

        assert_eq!(
            actual_top_regions, expected_top_regions,
            "Expected regions {:?} to be selected by score, got {:?}",
            expected_top_regions, actual_top_regions
        );

        // The appended fallback slice holds regions outside the top 3, never a
        // duplicate of a score-selected one. None of the regions was ever tracked as
        // fully listed, so all of them are equally stale and the tie-break is score:
        // ranks 7, 6, 5 are the best-scoring of the remaining 7.
        let fallback_regions: Vec<u32> = table_candidates
            .iter()
            .skip(3)
            .map(|c| c.region_id.region_number())
            .collect();
        assert_eq!(
            fallback_regions,
            vec![7, 6, 5],
            "Expected the fallback slice to be the most overdue non-top-3 ranks, got {:?}",
            fallback_regions
        );

        let mut unique_regions: Vec<u32> = actual_top_regions;
        unique_regions.extend(fallback_regions);
        unique_regions.sort_unstable();
        unique_regions.dedup();
        assert_eq!(
            unique_regions.len(),
            6,
            "a region must not be selected by both channels"
        );

        // Verify they are sorted by score in descending order
        for i in 0..table_candidates.len() - 1 {
            assert!(
                table_candidates[i].score >= table_candidates[i + 1].score,
                "Candidates should be sorted by score descending: {} >= {}",
                table_candidates[i].score,
                table_candidates[i + 1].score
            );
        }
    } else {
        panic!("Expected table {} to have candidates", table_id);
    }
}

// ---------------------------------------------------------------------------
// GC candidate selection boundedness ("GC 选区有界性")
// ---------------------------------------------------------------------------

/// How the wall clock lines up between two consecutive ticks.
///
/// Production ticks fire every `TICKER_INTERVAL` while a dispatched region stays
/// in cooldown for `gc_cooldown_period`, and both are 300s. The elapsed time
/// between two ticks therefore jitters around exactly one cooldown period, which
/// yields the two phases a steady-state scheduler has to survive.
#[derive(Debug, Clone, Copy)]
enum TickPhase {
    /// No predecessor: nothing was ever GC'd, so every ranked region qualifies.
    FirstTick,
    /// Phase A: the previous round's group finished just before the tick boundary,
    /// so it has cleared its cooldown and the top ranks win the slots again.
    PreviousGroupPastCooldown,
    /// Phase B: the previous round finished slightly after the tick boundary, so
    /// its group is still cooling down and the next ranks take the slots.
    PreviousRoundFinishedLate,
}

/// Logical-clock shim for the gap between two ticks.
///
/// A unit test cannot spend [`TICKER_INTERVAL`] of wall time per round, and the
/// scheduler reads the real [`Instant`]. So instead of waiting, this rewrites the
/// in-memory tracker to the state the scheduler would observe at the next tick
/// boundary: every tracked region that is not in `cooling` is pinned to
/// `gc_cooldown_period + 1s` of elapsed time (eligible), and every region in
/// `cooling` is pinned to zero elapsed time (still cooling down). Only the
/// elapse-time *values* are modelled; selection, dispatch and bookkeeping still go
/// through the real `handle_tick` -> `process_datanode_gc` path.
async fn model_tick_clock(scheduler: &GcScheduler, cooling: &HashSet<RegionId>) {
    let now = Instant::now();
    let elapsed_enough = now
        .checked_sub(scheduler.config.gc_cooldown_period + Duration::from_secs(1))
        .expect("the model needs monotonic uptime longer than gc_cooldown_period");

    let mut tracker = scheduler.region_gc_tracker.lock().await;
    for (region_id, info) in tracker.iter_mut() {
        let last_gc_time = if cooling.contains(region_id) {
            now
        } else {
            elapsed_enough
        };
        info.last_gc_time = last_gc_time;
        // Keep every region on fast listing: a real tick would not reach
        // `full_file_listing_interval` (24h) either, and fast listing keeps one
        // dispatch per tick instead of a fast/full split.
        info.last_full_listing_time = Some(last_gc_time);
    }
}

/// Leader peer of `region_id` as seen through the mock's table route.
fn route_leader(ctx: &MockSchedulerCtx, table_id: TableId, region_id: RegionId) -> Option<Peer> {
    let routes = ctx.table_routes.lock().unwrap();
    let (_physical_table_id, route) = routes.get(&table_id)?;
    route
        .region_routes
        .iter()
        .find(|region_route| region_route.region.id == region_id)
        .and_then(|region_route| region_route.leader_peer.clone())
}

/// Boundedness contract for GC candidate selection.
///
/// Scenario: one table with 41 regions that are all equally reachable leaders over
/// `min_region_size_threshold`, with stable, pairwise distinct, positive scores, and
/// `regions_per_table_threshold == 20`. Because `gc_cooldown_period` equals
/// `TICKER_INTERVAL` (300s), the elapsed time between ticks lands on either side of
/// exactly one cooldown period. The contract under test is that within a bounded
/// number of ticks every legitimate candidate gets dispatched at least once (an
/// equivalent and stronger form is "every region is dispatched at least once in any
/// 3 consecutive rounds"); a fixed top-N slice must not monopolise the slots forever.
///
/// The driver is the real `handle_tick` path (candidate selection, route resolution,
/// `process_datanode_gc`, receipt handling); only the stats, routes, per-region
/// reports and the inter-tick elapsed time are injected.
///
/// Without the listing-staleness fallback channel this scenario is red: the top 20 are
/// re-selected whenever their cooldown clears, so the steady state only alternates
/// ranks 1..=20 with ranks 21..=40 and rank 41 never gets a dispatch. The fallback
/// channel of [`GcScheduler::select_gc_candidates`] appends the most overdue regions
/// after the score-selected top-N, which is what closes the gap; the per-round checks
/// below still pin the score channel to the exact top-N of every tick, so the fix
/// cannot have been bought by weakening the scoring order.
#[tokio::test]
async fn test_gc_candidate_selection_never_starves_ranked_regions() {
    init_default_ut_logging();

    const TABLE_ID: TableId = 1;
    /// Regions that are legitimate candidates for the fairness contract.
    const RANKED: u32 = 41;
    /// `regions_per_table_threshold`, i.e. slots per tick.
    const CAPACITY: usize = 20;

    let peer = Peer::new(1, "127.0.0.1:3001");

    // Ranks 1..=41 share size and sst count; only `file_removed_cnt` decreases with
    // the rank, so scores are strictly decreasing and pairwise distinct
    // (rank 1 -> 46.0, ..., rank 41 -> 6.0) and the top 20 is stable across ticks.
    let mut region_stats = Vec::with_capacity(RANKED as usize + 1);
    let mut region_routes = Vec::with_capacity(RANKED as usize + 1);
    let mut gc_reports = HashMap::new();
    for rank in 1..=RANKED {
        let region_id = RegionId::new(TABLE_ID, rank);
        let mut stat = mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10);
        if let RegionManifestInfo::Mito {
            file_removed_cnt, ..
        } = &mut stat.region_manifest
        {
            *file_removed_cnt = u64::from(RANKED + 1 - rank);
        }
        region_stats.push(stat);
        region_routes.push((region_id, peer.clone()));
        gc_reports.insert(region_id, new_empty_report_with([region_id]));
    }

    // Control region below the ranked tail: leader and over the size threshold, but
    // it only holds orphans (no SSTs, nothing ever removed), so its own stats give it
    // a score of exactly 0.0 and the `score > 0.0` gate drops it. Its stat is built
    // from scratch, the 41 ranked stats above are untouched. It is deliberately
    // *outside* the contract asserted below: even if a future fix makes every
    // positive-score region rotate, a zero-score region must still never be
    // dispatched, so it can neither mask nor satisfy the boundedness assertion.
    let control_region = RegionId::new(TABLE_ID, RANKED + 1);
    region_stats.push(mock_region_stat(
        control_region,
        RegionRole::Leader,
        TEST_REGION_SIZE_200MB,
        0,
    ));
    region_routes.push((control_region, peer.clone()));
    gc_reports.insert(control_region, new_empty_report_with([control_region]));

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(HashMap::from([(
                TABLE_ID,
                region_stats,
            )])))),
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(TABLE_ID, (TABLE_ID, region_routes))])),
    );

    let config = GcSchedulerOptions {
        regions_per_table_threshold: CAPACITY,
        ..Default::default()
    };
    // The starvation modelled here only exists while the cooldown equals the ticker
    // interval. Assert the premise so a change to either constant is reported here
    // instead of silently turning this model into a no-op.
    assert_eq!(
        config.gc_cooldown_period, TICKER_INTERVAL,
        "the modelled race requires gc_cooldown_period == TICKER_INTERVAL"
    );

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Phase A re-selects the top 20 (plus the most overdue regions), phase B shifts to
    // the ranks the cooldown left eligible; the alternation is exactly the steady state
    // the equal cooldown/ticker period produces.
    let phases = [
        TickPhase::FirstTick,
        TickPhase::PreviousRoundFinishedLate,
        TickPhase::PreviousGroupPastCooldown,
        TickPhase::PreviousRoundFinishedLate,
        TickPhase::PreviousGroupPastCooldown,
        TickPhase::PreviousRoundFinishedLate,
    ];

    let mut selections: Vec<(TickPhase, HashSet<RegionId>)> = Vec::with_capacity(phases.len());
    let mut previous: HashSet<RegionId> = HashSet::new();

    for (index, phase) in phases.into_iter().enumerate() {
        let round = index + 1;

        match phase {
            TickPhase::FirstTick => {}
            TickPhase::PreviousGroupPastCooldown => {
                model_tick_clock(&scheduler, &HashSet::new()).await
            }
            TickPhase::PreviousRoundFinishedLate => model_tick_clock(&scheduler, &previous).await,
        }

        ctx.begin_dispatch_round(round);
        let job_report = scheduler
            .handle_tick()
            .await
            .unwrap_or_else(|e| panic!("round {round} ({phase:?}): gc tick failed: {e:?}"));

        let records = ctx.dispatches_in_round(round);
        let dispatched: HashSet<RegionId> = records
            .iter()
            .flat_map(|record| record.region_ids.iter().copied())
            .collect();

        // Per-round check 1: a tick never hands out more slots than the score channel
        // (`CAPACITY`) plus the listing-staleness fallback channel (`CAPACITY` again),
        // and never dispatches the same region twice within one tick.
        assert!(
            dispatched.len() <= 2 * CAPACITY,
            "round {round} ({phase:?}): dispatched {} regions with capacity {CAPACITY} plus a \
             fallback slice of {CAPACITY}",
            dispatched.len()
        );
        assert_eq!(
            dispatched.len(),
            records.iter().map(|r| r.region_ids.len()).sum::<usize>(),
            "round {round} ({phase:?}): a region was dispatched twice in one tick: {records:?}"
        );
        // Steady state stays on fast listing; a full listing must not be triggered
        // by this model (that would instead be the `full_file_listing_interval` path).
        assert!(
            records.iter().all(|record| !record.full),
            "round {round} ({phase:?}): unexpected full-listing dispatch: {records:?}"
        );

        // Per-round check 2: every dispatched region resolves through the table route
        // to the single reachable peer, i.e. no candidate is dropped as unroutable.
        for region_id in &dispatched {
            assert_eq!(
                route_leader(&ctx, TABLE_ID, *region_id),
                Some(peer.clone()),
                "round {round} ({phase:?}): region {region_id} did not resolve to {peer}"
            );
        }

        // Per-round check 3: the datanode receipts are attributed back correctly.
        let (per_datanode_reports, failed_datanodes) = match job_report {
            GcJobReport::PerDatanode {
                per_datanode_reports,
                failed_datanodes,
            } => (per_datanode_reports, failed_datanodes),
            GcJobReport::Combined { .. } => {
                panic!("round {round} ({phase:?}): a scheduled tick reports per datanode")
            }
        };
        assert!(
            failed_datanodes.is_empty(),
            "round {round} ({phase:?}): datanode dispatch failed: {failed_datanodes:?}"
        );
        assert_eq!(
            per_datanode_reports.len(),
            1,
            "round {round} ({phase:?}): every candidate lives on the one reachable peer"
        );
        let mut report = GcReport::default();
        for datanode_report in per_datanode_reports.into_values() {
            report.merge(datanode_report);
        }
        assert!(
            report.need_retry_regions.is_empty(),
            "round {round} ({phase:?}): no region should need a retry, got {:?}",
            report.need_retry_regions
        );
        assert_eq!(
            report.processed_regions, dispatched,
            "round {round} ({phase:?}): receipts must cover exactly the dispatched regions"
        );

        // Per-round check 4: the phase's own expectation, which pins down the model.
        // Eligibility follows the modelled clock: phase A has nothing cooling down, while
        // phase B still has the previous round in cooldown.
        let mut eligible: Vec<RegionId> = (1..=RANKED)
            .map(|rank| RegionId::new(TABLE_ID, rank))
            .filter(|region_id| match phase {
                TickPhase::FirstTick | TickPhase::PreviousGroupPastCooldown => true,
                TickPhase::PreviousRoundFinishedLate => !previous.contains(region_id),
            })
            .collect();
        // Ranks are ordered by score (rank 1 is the highest), so the score channel of this
        // tick is exactly the `CAPACITY` best-scoring eligible ranks.
        eligible.sort_by_key(|region_id| region_id.region_number());
        let score_selected: HashSet<RegionId> = eligible.iter().copied().take(CAPACITY).collect();
        assert!(
            dispatched.is_superset(&score_selected),
            "round {round} ({phase:?}): the top-{CAPACITY} eligible ranks {score_selected:?} must \
             always win their slots — the listing-staleness fallback only appends after them and \
             never displaces a higher score — but this tick dispatched {dispatched:?}"
        );
        let eligible: HashSet<RegionId> = eligible.into_iter().collect();
        assert!(
            eligible.is_superset(&dispatched),
            "round {round} ({phase:?}): every dispatched region must be eligible (leader, over the \
             size threshold, positive score, not cooling down); got {:?} outside {eligible:?}",
            dispatched.difference(&eligible).collect::<Vec<_>>()
        );

        previous = dispatched.clone();
        selections.push((phase, dispatched));
    }

    // The zero-score control region passes the role and size gates but fails the score
    // gate, so it must never be dispatched: it is out of the fairness contract.
    assert!(
        !ctx.dispatched_region_ids().contains(&control_region),
        "the orphan-only control region {control_region} has a score of 0.0 and must never be dispatched"
    );

    // Core assertion: over the whole window every legitimate candidate got at least
    // one dispatch. Fail loudly with the diagnosis (starved ranks plus each round's
    // selection) instead of just "sets differ".
    let covered = ctx.dispatched_region_ids();
    let starved_ranks: Vec<u32> = (1..=RANKED)
        .filter(|rank| !covered.contains(&RegionId::new(TABLE_ID, *rank)))
        .collect();
    let per_round: Vec<String> = selections
        .iter()
        .map(|(phase, dispatch)| {
            let mut ranks: Vec<u32> = dispatch
                .iter()
                .map(|region_id| region_id.region_number())
                .collect();
            ranks.sort_unstable();
            format!("round -> {phase:?}: ranks {ranks:?}")
        })
        .collect();

    assert!(
        starved_ranks.is_empty(),
        "GC candidate selection starved {} of {RANKED} ranked regions over {} ticks \
         with capacity {CAPACITY} per tick.\n\
         never-dispatched ranks (1 = highest score): {starved_ranks:?}\n\
         per-round dispatch:\n{}\n\
         every ranked region is a leader, over min_region_size_threshold and has a \
         positive score, so all of them are legitimate GC candidates.",
        starved_ranks.len(),
        selections.len(),
        per_round.join("\n"),
    );

    // The covered set is exactly the 41 ranked ids: no legitimate candidate is left
    // out (the assertion above) and nothing outside the contract leaks in (the
    // zero-score control region is checked separately just above).
    let expected_covered: HashSet<RegionId> = (1..=RANKED)
        .map(|rank| RegionId::new(TABLE_ID, rank))
        .collect();
    assert_eq!(
        covered,
        expected_covered,
        "the union of {} ticks must cover exactly the {RANKED} ranked regions; per-round \
         dispatch:\n{}",
        selections.len(),
        per_round.join("\n"),
    );
}

// ---------------------------------------------------------------------------
// GC candidate selection: the listing-staleness fallback channel
// ---------------------------------------------------------------------------

/// The fallback channel must never displace a higher score.
///
/// Scenario: one very high scoring region that was *fully listed just now*, so a purely
/// staleness-driven selector would put it last, plus 12 much lower scoring regions that
/// have been overdue for an hour. `regions_per_table_threshold == 2` gives the score
/// channel two slots and the fallback two more, so the fallback pool is
/// over-subscribed. The high score region keeps its slot on every tick: the fallback
/// only appends after the score channel and can never take a slot away from it.
#[tokio::test]
async fn test_gc_fallback_channel_never_preempts_top_n_score() {
    init_default_ut_logging();

    const TABLE_ID: TableId = 1;
    /// `regions_per_table_threshold`, i.e. slots of the score channel.
    const CAPACITY: usize = 2;
    const OVERDUE_REGIONS: u32 = 12;

    let hot_region = RegionId::new(TABLE_ID, 1);
    let mut hot_stat = mock_region_stat(hot_region, RegionRole::Leader, TEST_REGION_SIZE_200MB, 10);
    if let RegionManifestInfo::Mito {
        file_removed_cnt, ..
    } = &mut hot_stat.region_manifest
    {
        // score = 10 * 0.5 + 100 * 1.0 = 105.0
        *file_removed_cnt = 100;
    }

    let mut region_stats = vec![hot_stat];
    let mut overdue = Vec::with_capacity(OVERDUE_REGIONS as usize);
    for rank in 2..=OVERDUE_REGIONS + 1 {
        let region_id = RegionId::new(TABLE_ID, rank);
        // Same sst count, no removals: an identical, much lower score of 5.0 each.
        region_stats.push(mock_region_stat(
            region_id,
            RegionRole::Leader,
            TEST_REGION_SIZE_200MB,
            10,
        ));
        overdue.push(region_id);
    }

    let table_stats = HashMap::from([(TABLE_ID, region_stats)]);

    let ctx = Arc::new(MockSchedulerCtx {
        table_to_region_stats: Arc::new(Mutex::new(Some(table_stats.clone()))),
        ..Default::default()
    });

    let config = GcSchedulerOptions {
        regions_per_table_threshold: CAPACITY,
        ..Default::default()
    };

    // Seeding: the high score region was fully listed *now*, every other region an hour
    // ago. Staleness alone would therefore pick the overdue regions first.
    let now = Instant::now();
    let long_ago = now
        .checked_sub(Duration::from_secs(60 * 60))
        .expect("the test needs monotonic uptime longer than one hour");
    let mut tracker = HashMap::new();
    tracker.insert(
        hot_region,
        crate::gc::tracker::RegionGcInfo {
            last_gc_time: long_ago,
            last_full_listing_time: Some(now),
        },
    );
    for region_id in &overdue {
        tracker.insert(
            *region_id,
            crate::gc::tracker::RegionGcInfo {
                last_gc_time: long_ago,
                last_full_listing_time: Some(long_ago),
            },
        );
    }

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(tracker)),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    // Three consecutive ticks over the same stats and tracker: a region that wins a slot
    // by score keeps winning it.
    let mut first_round: Option<Vec<RegionId>> = None;
    for round in 1..=3 {
        let candidates = scheduler
            .select_gc_candidates(&table_stats)
            .await
            .unwrap_or_else(|e| panic!("round {round}: selection failed: {e:?}"));
        let table_candidates = candidates.get(&TABLE_ID).unwrap_or_else(|| {
            panic!("round {round}: expected table {TABLE_ID} to have candidates")
        });

        // Score channel + fallback slice, both full: the fallback pool is not exhausted.
        assert_eq!(
            table_candidates.len(),
            2 * CAPACITY,
            "round {round}: expected the {CAPACITY} score slots plus {CAPACITY} fallback \
             slots, got {table_candidates:?}"
        );
        // The high score region wins the first slot even though it is the *least* overdue.
        assert_eq!(
            table_candidates[0].region_id, hot_region,
            "round {round}: the highest score must keep the first slot, got {:?}",
            table_candidates[0].region_id
        );
        assert_eq!(
            table_candidates
                .iter()
                .filter(|candidate| candidate.region_id == hot_region)
                .count(),
            1,
            "round {round}: the fallback channel must not append a region the score channel \
             already selected: {table_candidates:?}"
        );
        // Every appended fallback candidate is an overdue low score region.
        for candidate in table_candidates.iter().skip(CAPACITY) {
            assert!(
                overdue.contains(&candidate.region_id),
                "round {round}: fallback slot went to {}, expected one of the overdue regions",
                candidate.region_id
            );
            assert!(
                candidate.score < table_candidates[0].score,
                "round {round}: a fallback candidate must not outrank the score channel"
            );
        }

        let ids: Vec<RegionId> = table_candidates
            .iter()
            .map(|candidate| candidate.region_id)
            .collect();
        match &first_round {
            None => first_round = Some(ids),
            Some(expected) => assert_eq!(
                &ids, expected,
                "round {round}: selection is deterministic, a tick must not drop the high \
                 score region"
            ),
        }
    }
}

/// The fallback channel is subject to the same cooldown as the score channel: a region
/// picked because its listing is overdue must not be dispatched again on the next tick
/// while it is still cooling down.
#[tokio::test]
async fn test_gc_fallback_candidates_respect_cooldown() {
    init_default_ut_logging();

    const TABLE_ID: TableId = 1;
    /// `regions_per_table_threshold`, i.e. slots of the score channel.
    const CAPACITY: usize = 2;
    const REGIONS: u32 = 5;
    const ROUNDS: usize = 3;

    let peer = Peer::new(1, "127.0.0.1:3001");

    // Every region shares the same positive score, so the candidates beyond the score
    // channel can only come from the staleness fallback channel.
    let mut region_stats = Vec::with_capacity(REGIONS as usize);
    let mut region_routes = Vec::with_capacity(REGIONS as usize);
    let mut gc_reports = HashMap::new();
    for rank in 1..=REGIONS {
        let region_id = RegionId::new(TABLE_ID, rank);
        region_stats.push(mock_region_stat(
            region_id,
            RegionRole::Leader,
            TEST_REGION_SIZE_200MB,
            10,
        ));
        region_routes.push((region_id, peer.clone()));
        gc_reports.insert(region_id, new_empty_report_with([region_id]));
    }

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(HashMap::from([(
                TABLE_ID,
                region_stats,
            )])))),
            gc_reports: Arc::new(Mutex::new(gc_reports)),
            ..Default::default()
        }
        .with_table_routes(HashMap::from([(TABLE_ID, (TABLE_ID, region_routes))])),
    );

    let config = GcSchedulerOptions {
        regions_per_table_threshold: CAPACITY,
        ..Default::default()
    };

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let all: HashSet<RegionId> = (1..=REGIONS)
        .map(|rank| RegionId::new(TABLE_ID, rank))
        .collect();
    let mut previous: HashSet<RegionId> = HashSet::new();
    let mut dispatched_per_round: Vec<HashSet<RegionId>> = Vec::with_capacity(ROUNDS);

    for round in 1..=ROUNDS {
        // Nothing is tracked on the first tick; after it, the previous round is exactly
        // the group that is still cooling down at the next tick boundary.
        if round > 1 {
            model_tick_clock(&scheduler, &previous).await;
        }

        ctx.begin_dispatch_round(round);
        scheduler
            .handle_tick()
            .await
            .unwrap_or_else(|e| panic!("round {round}: gc tick failed: {e:?}"));

        let records = ctx.dispatches_in_round(round);
        let dispatched: HashSet<RegionId> = records
            .iter()
            .flat_map(|record| record.region_ids.iter().copied())
            .collect();

        if round == 1 {
            // Nothing is cooling down, so both channels fill: `CAPACITY` by score plus
            // `CAPACITY` by staleness.
            assert_eq!(
                dispatched.len(),
                2 * CAPACITY,
                "round 1: expected the {CAPACITY} score slots plus {CAPACITY} fallback \
                 slots, got {dispatched:?}"
            );
        }
        // The cooldown holds for every dispatched region, including the fallback ones: no
        // region is dispatched twice in a row.
        assert!(
            dispatched.is_disjoint(&previous),
            "round {round}: regions dispatched on the previous tick are still cooling down, but \
             {:?} were dispatched again",
            dispatched.intersection(&previous).collect::<Vec<_>>()
        );
        assert!(
            !dispatched.is_empty(),
            "round {round}: at least one region must be eligible"
        );
        assert!(
            dispatched.len() <= 2 * CAPACITY,
            "round {round}: dispatched {} regions with {CAPACITY} score slots plus \
             {CAPACITY} fallback slots",
            dispatched.len()
        );
        assert!(
            all.is_superset(&dispatched),
            "round {round}: only the table's own regions may be dispatched, got {:?}",
            dispatched.difference(&all).collect::<Vec<_>>()
        );

        previous = dispatched.clone();
        dispatched_per_round.push(dispatched);
    }

    // The cooldown must rotate instead of blocking: within the window every region is
    // dispatched once.
    let covered: HashSet<RegionId> = dispatched_per_round.iter().flatten().copied().collect();
    assert_eq!(
        covered, all,
        "every region must be dispatched within {ROUNDS} ticks when the fallback channel \
         obeys the cooldown; per-round dispatch: {dispatched_per_round:?}"
    );
}
