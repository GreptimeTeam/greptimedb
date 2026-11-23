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
        // Should only have 3 candidates due to regions_per_table_threshold
        assert_eq!(
            table_candidates.len(),
            3,
            "Expected 3 candidates for table {} due to regions_per_table_threshold, got {}",
            table_id,
            table_candidates.len()
        );

        // Verify that the top 3 scoring regions are selected
        // Regions 8, 9, 10 should have the highest scores (file_removed_cnt: 16, 18, 20)
        // They should be returned in descending order by score
        let expected_regions = vec![10, 9, 8];
        let actual_regions: Vec<u32> = table_candidates
            .iter()
            .map(|c| c.region_id.region_number())
            .collect();

        assert_eq!(
            actual_regions, expected_regions,
            "Expected regions {:?} to be selected, got {:?}",
            expected_regions, actual_regions
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
/// Currently red: the top 20 are re-selected whenever their cooldown clears, so the
/// steady state only alternates ranks 1..=20 with ranks 21..=40 and the last ranked
/// region never gets a dispatch, i.e. it is starved forever. The assertion below
/// reports the never-dispatched ranks together with every round's selection.
#[tokio::test]
#[ignore = "known starvation: top-N selection without rotation leaves ranks >= 41 unselected forever"]
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

    // Phase A re-selects the top 20, phase B shifts to ranks 21..=40; the alternation
    // is exactly the steady state the equal cooldown/ticker period produces.
    let phases = [
        TickPhase::FirstTick,
        TickPhase::PreviousRoundFinishedLate,
        TickPhase::PreviousGroupPastCooldown,
        TickPhase::PreviousRoundFinishedLate,
        TickPhase::PreviousGroupPastCooldown,
        TickPhase::PreviousRoundFinishedLate,
    ];
    let capacity = CAPACITY as u32;

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

        // Per-round check 1: a tick never hands out more slots than the capacity and
        // never dispatches the same region twice within one tick.
        assert!(
            dispatched.len() <= CAPACITY,
            "round {round} ({phase:?}): dispatched {} regions with capacity {CAPACITY}",
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
        let expected: HashSet<RegionId> = match phase {
            TickPhase::FirstTick | TickPhase::PreviousGroupPastCooldown => (1..=capacity)
                .map(|rank| RegionId::new(TABLE_ID, rank))
                .collect(),
            TickPhase::PreviousRoundFinishedLate => (capacity + 1..=2 * capacity)
                .map(|rank| RegionId::new(TABLE_ID, rank))
                .collect(),
        };
        assert_eq!(
            dispatched, expected,
            "round {round} ({phase:?}): ranks are ordered by score (rank 1 = highest, \
             all 41 ranked regions are leaders over the size threshold with a positive \
             score), so this tick should have dispatched exactly {expected:?}"
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
}
