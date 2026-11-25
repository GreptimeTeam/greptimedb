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
        // `regions_per_table_threshold` is an upper bound, so a table of 10 qualified
        // regions yields at most 3 candidates: 2 score seats plus the single staleness
        // seat, all inside the budget.
        assert_eq!(
            table_candidates.len(),
            3,
            "Expected at most regions_per_table_threshold candidates for table {}, got {}",
            table_id,
            table_candidates.len()
        );

        // Verify that the best scoring regions take the score seats: with 3 seats and one
        // of them reserved for the staleness seat, regions 10 and 9 (file_removed_cnt: 20
        // and 18) are returned first, in descending order by score.
        let expected_score_regions = vec![10, 9];
        let actual_score_regions: Vec<u32> = table_candidates
            .iter()
            .take(2)
            .map(|c| c.region_id.region_number())
            .collect();

        assert_eq!(
            actual_score_regions, expected_score_regions,
            "Expected regions {:?} to be selected by score, got {:?}",
            expected_score_regions, actual_score_regions
        );

        // The last seat is the staleness one: a region outside the score seats, never a
        // duplicate of one. None of the regions was ever tracked as GC'd, so all of them
        // are equally stale and the tie-break is score: region 8 is the best-scoring of
        // the remaining 8.
        let stale_regions: Vec<u32> = table_candidates
            .iter()
            .skip(2)
            .map(|c| c.region_id.region_number())
            .collect();
        assert_eq!(
            stale_regions,
            vec![8],
            "Expected the staleness seat to be the best-scoring region outside the score \
             seats, got {:?}",
            stale_regions
        );

        let mut unique_regions: Vec<u32> = actual_score_regions;
        unique_regions.extend(stale_regions);
        unique_regions.sort_unstable();
        unique_regions.dedup();
        assert_eq!(
            unique_regions.len(),
            3,
            "a region must not be selected by both the score seats and the staleness seat"
        );

        // The score seats are ordered by score descending, and the staleness seat never
        // outranks them.
        for i in 0..table_candidates.len().min(2) - 1 {
            assert!(
                table_candidates[i].score >= table_candidates[i + 1].score,
                "Candidates should be sorted by score descending: {} >= {}",
                table_candidates[i].score,
                table_candidates[i + 1].score
            );
        }
        assert!(
            table_candidates[1].score >= table_candidates[2].score,
            "the staleness seat must not outrank the score seats: {} >= {}",
            table_candidates[1].score,
            table_candidates[2].score
        );
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
    /// so it has cleared its cooldown and the score seats keep their ranks.
    PreviousGroupPastCooldown,
    /// Phase B: the previous round finished slightly after the tick boundary, so
    /// its group is still cooling down and the next ranks take the seats.
    PreviousRoundFinishedLate,
}

/// How far back one tick of age moves a region's timestamp in [`LogicalClock`].
const AGE_STRIDE: Duration = Duration::from_secs(1);

/// Logical clock for the gap between two ticks that keeps every region's *relative*
/// age.
///
/// A unit test cannot spend [`TICKER_INTERVAL`] of wall time per round, and the
/// scheduler reads the real [`Instant`]. So instead of waiting, this rewrites the
/// in-memory tracker to what the scheduler would observe at the next tick boundary,
/// counting each region's age in ticks since it was last dispatched:
///
/// * age `0` — dispatched on the previous tick, so still inside
///   `gc_cooldown_period`: the timestamp is "just now".
/// * age `k >= 1` — dispatched `k` ticks ago: the timestamp sits
///   `gc_cooldown_period + 1s + (k - 1) * AGE_STRIDE` in the past, i.e. an age-1
///   region has just cleared its cooldown and every further tick of age moves the
///   timestamp one stride further back.
///
/// Only the *scale* of a tick differs from production — the real clock advances by
/// `TICKER_INTERVAL`, this one by [`AGE_STRIDE`]. Older is still earlier for every
/// pair of regions, which is what the staleness seat sorts on, so "least recently
/// dispatched" is modelled faithfully instead of being flattened onto one timestamp
/// (flat timestamps would leave the age ordering unverifiable).
struct LogicalClock {
    /// Ticks since each tracked region was last dispatched, as of the previous tick
    /// boundary. Untracked regions are not in here: they were never GC'd.
    ages: HashMap<RegionId, usize>,
}

impl LogicalClock {
    fn new() -> Self {
        Self {
            ages: HashMap::new(),
        }
    }

    /// Advance to the next tick boundary and publish it into the scheduler.
    ///
    /// `previous` is the group dispatched on the last tick. When
    /// `previous_still_cooling` the group finished just after the boundary and stays
    /// inside its cooldown (age 0), otherwise it cleared it exactly (age 1). Every
    /// other tracked region ages by one tick, so the relative order of all ages is
    /// preserved.
    async fn tick(
        &mut self,
        scheduler: &GcScheduler,
        previous: &HashSet<RegionId>,
        previous_still_cooling: bool,
    ) {
        let now = Instant::now();
        let cleared = now
            .checked_sub(scheduler.config.gc_cooldown_period + Duration::from_secs(1))
            .expect("the model needs monotonic uptime longer than gc_cooldown_period");

        let mut tracker = scheduler.region_gc_tracker.lock().await;
        for (region_id, info) in tracker.iter_mut() {
            let age = if previous.contains(region_id) {
                usize::from(!previous_still_cooling)
            } else {
                self.ages.get(region_id).copied().unwrap_or(0) + 1
            };
            self.ages.insert(*region_id, age);
            info.last_gc_time = if age == 0 {
                // Inside the cooldown window.
                now
            } else {
                cleared - AGE_STRIDE * ((age - 1) as u32)
            };
            // `last_full_listing_time` is kept fresh so every region stays on fast
            // listing: a real tick would not reach `full_file_listing_interval` (24h)
            // either, and fast listing keeps one dispatch per tick instead of a
            // fast/full split.
            info.last_full_listing_time = Some(now);
        }
    }
}

/// The `seats` best scoring regions of `eligible`.
///
/// Ranks in these tests are ordered by score (rank 1 has the highest score), so this
/// is the score seats of a tick: the `seats` eligible regions with the smallest rank
/// numbers.
fn score_seats(eligible: &HashSet<RegionId>, table_id: TableId, seats: usize) -> HashSet<RegionId> {
    let mut ranks: Vec<u32> = eligible
        .iter()
        .map(|region_id| region_id.region_number())
        .collect();
    ranks.sort_unstable();
    ranks
        .into_iter()
        .take(seats)
        .map(|rank| RegionId::new(table_id, rank))
        .collect()
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

/// Boundedness contract for GC candidate selection, on one *specific* trajectory.
///
/// Scenario: one table with 41 regions that are all equally reachable leaders over
/// `min_region_size_threshold`, with stable, pairwise distinct, positive scores, and
/// `regions_per_table_threshold == 20`, so each tick hands out at most 20 seats from
/// inside that budget: 19 by score and one by staleness. Because
/// `gc_cooldown_period` equals `TICKER_INTERVAL` (300s), the elapsed time between
/// ticks lands on either side of exactly one cooldown period; this test walks a fixed
/// six-tick alternation of those two phases. The contract it asserts is that every
/// legitimate candidate is dispatched at least once, i.e. a fixed top-N slice must not
/// monopolise the seats forever.
///
/// This trajectory is one instance, not the worst case: the alternating phases keep
/// flipping whole groups out of cooldown, which covers the 41 ranks quickly. The
/// worst case — every region out of cooldown on every tick, scores unchanged — and the
/// resulting `1 + max(0, R - N)` dispatch bound are asserted separately, by
/// [`test_gc_candidate_selection_dispatch_bound_sliding_window`].
///
/// The driver is the real `handle_tick` path (candidate selection, route resolution,
/// `process_datanode_gc`, receipt handling); only the stats, routes, per-region
/// reports and the inter-tick elapsed time are injected.
///
/// Without the staleness seat this scenario is red: the best scoring regions are
/// re-selected whenever their cooldown clears, so the steady state only alternates the
/// top ranks and the tail never gets a dispatch. The staleness seat of
/// [`GcScheduler::select_gc_candidates`] is what closes the gap; the per-round checks
/// below still pin the score seats to the exact best `CAPACITY - 1` eligible ranks of
/// every tick, so the fix cannot have been bought by weakening the scoring order.
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

    // Phase A keeps the score seats on the best ranks and lets the staleness seat pick
    // the oldest eligible tail region, phase B shifts to the ranks the cooldown left
    // eligible; the alternation is exactly the steady state the equal cooldown/ticker
    // period produces.
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
    let mut clock = LogicalClock::new();

    for (index, phase) in phases.into_iter().enumerate() {
        let round = index + 1;

        match phase {
            TickPhase::FirstTick => {}
            TickPhase::PreviousGroupPastCooldown => clock.tick(&scheduler, &previous, false).await,
            TickPhase::PreviousRoundFinishedLate => clock.tick(&scheduler, &previous, true).await,
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

        // Per-round check 1: a tick never hands out more than `regions_per_table_threshold`
        // seats — 19 by score plus the one staleness seat, all inside the `CAPACITY`
        // budget — and never dispatches the same region twice within one tick.
        assert!(
            dispatched.len() <= CAPACITY,
            "round {round} ({phase:?}): dispatched {} regions, more than the {CAPACITY} seats \
             a table may contribute",
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
        let eligible: HashSet<RegionId> = (1..=RANKED)
            .map(|rank| RegionId::new(TABLE_ID, rank))
            .filter(|region_id| match phase {
                TickPhase::FirstTick | TickPhase::PreviousGroupPastCooldown => true,
                TickPhase::PreviousRoundFinishedLate => !previous.contains(region_id),
            })
            .collect();
        // Ranks are ordered by score (rank 1 is the highest), so the score seats of this
        // tick are exactly the best `CAPACITY - 1` eligible ranks; the one remaining seat
        // goes to the oldest eligible region, so it can never displace them.
        let score_selected = score_seats(&eligible, TABLE_ID, CAPACITY - 1);
        assert!(
            dispatched.is_superset(&score_selected),
            "round {round} ({phase:?}): the top-{} eligible ranks {score_selected:?} must \
             always win their seats — the staleness seat is allocated inside the same \
             {CAPACITY} seats and can never displace a higher score — but this tick \
             dispatched {dispatched:?}",
            CAPACITY - 1
        );
        assert_eq!(
            dispatched.len(),
            CAPACITY.min(eligible.len()),
            "round {round} ({phase:?}): a tick fills its seat budget, {CAPACITY} seats out of \
             {} eligible regions, but dispatched {dispatched:?}",
            eligible.len()
        );
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

/// Worst-case dispatch bound of the seat rotation: `1 + max(0, R - N)` ticks.
///
/// Scenario: the same single table of `RANKED` score-ranked regions, but this time
/// every region is out of its cooldown on every tick and the scores never change —
/// the sliding window in which a pure top-N slice starves its tail the fastest.
/// With `N` seats split into `N - 1` score seats and one staleness seat, the score
/// seats keep ranks `1..=N-1` on every tick and the staleness seat hands out exactly
/// one new tail rank per tick in least-recently-dispatched order (a dispatched region
/// becomes the newest, so it cannot be seated again before the whole tail has moved
/// through). Rank `R` is therefore reached on tick `R - (N - 1)`, i.e. within
/// `1 + max(0, R - N)` ticks of first becoming eligible; with 41 regions and `N = 20`
/// that is 22 ticks.
///
/// The assertions are on the real `handle_tick` path, with only the stats, routes,
/// reports and the modelled per-tick age injected. Nothing here is left to luck: the
/// dispatched set is pinned to exactly "the best `N - 1` ranks plus the next tail
/// rank" on every tick, so the bound comes from the seat allocation and not from the
/// specifics of one phase sequence.
#[tokio::test]
async fn test_gc_candidate_selection_dispatch_bound_sliding_window() {
    init_default_ut_logging();

    const TABLE_ID: TableId = 1;
    /// Regions that are legitimate candidates, ranked by score (rank 1 is highest).
    const RANKED: u32 = 41;
    /// `regions_per_table_threshold`: the seat budget of one table per tick.
    const CAPACITY: usize = 20;
    /// The bound under test, with nothing to spare: `1 + max(0, R - N)`.
    const BOUND: usize = 1 + (RANKED as usize - CAPACITY);

    let peer = Peer::new(1, "127.0.0.1:3001");

    // Pairwise distinct scores that never change: rank 1 -> 46.0, ..., rank 41 -> 6.0.
    let mut region_stats = Vec::with_capacity(RANKED as usize);
    let mut region_routes = Vec::with_capacity(RANKED as usize);
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
    assert_eq!(
        config.gc_cooldown_period, TICKER_INTERVAL,
        "the modelled sliding window requires gc_cooldown_period == TICKER_INTERVAL"
    );

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let mut clock = LogicalClock::new();
    let mut previous: HashSet<RegionId> = HashSet::new();
    // First tick each region was dispatched on.
    let mut dispatched_at: HashMap<RegionId, usize> = HashMap::new();

    for round in 1..=BOUND {
        // Phase A of every tick: the previous round cleared its cooldown exactly, so
        // every region is eligible and only the scores and the ages decide.
        if round > 1 {
            clock.tick(&scheduler, &previous, false).await;
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

        // Every rank is eligible on every tick, so the seat budget is always full and
        // still never exceeds `regions_per_table_threshold`.
        assert_eq!(
            dispatched.len(),
            CAPACITY,
            "round {round}: all {RANKED} regions are eligible, so the tick must fill exactly \
             {CAPACITY} seats; dispatched {records:?}"
        );
        // Exactly the score seats plus the next tail rank: the `CAPACITY - 1` best ranks
        // keep their seats and the staleness seat walks the tail one rank per tick.
        let all: HashSet<RegionId> = (1..=RANKED)
            .map(|rank| RegionId::new(TABLE_ID, rank))
            .collect();
        let mut expected = score_seats(&all, TABLE_ID, CAPACITY - 1);
        let expected_stale_rank = (CAPACITY as u32 - 1) + round as u32;
        expected.insert(RegionId::new(TABLE_ID, expected_stale_rank));
        assert_eq!(
            dispatched,
            expected,
            "round {round}: expected the {} score seats plus the next stale rank \
             {expected_stale_rank}, i.e. {expected:?}; the records were {records:?}",
            CAPACITY - 1
        );

        for region_id in &dispatched {
            dispatched_at.entry(*region_id).or_insert(round);
        }
        previous = dispatched;
    }

    // Every legitimate candidate was dispatched inside the window ...
    let starved: Vec<u32> = (1..=RANKED)
        .filter(|rank| !dispatched_at.contains_key(&RegionId::new(TABLE_ID, *rank)))
        .collect();
    assert!(
        starved.is_empty(),
        "GC candidate selection starved {starved:?} of {RANKED} ranked regions inside the \
         {BOUND}-tick window (N = {CAPACITY}); dispatches per region: {dispatched_at:?}"
    );
    // ... and the last ranked region made it on the very last tick of the bound, so the
    // bound is tight rather than merely satisfied.
    let last_rank = RegionId::new(TABLE_ID, RANKED);
    let last_rank_round = dispatched_at.get(&last_rank).copied();
    assert!(
        last_rank_round.is_some_and(|round| round <= BOUND),
        "the lowest ranked region {last_rank} must be dispatched within 1 + max(0, R - N) = \
         {BOUND} ticks, but it was dispatched in {last_rank_round:?}: {dispatched_at:?}"
    );
    assert_eq!(
        last_rank_round,
        Some(BOUND),
        "with a sliding window of ages the staleness seat reaches rank {RANKED} exactly on \
         tick 1 + (R - N) = {BOUND}: {dispatched_at:?}"
    );
}

// ---------------------------------------------------------------------------
// GC candidate selection: the staleness seat
// ---------------------------------------------------------------------------

/// The staleness seat must never displace a score seat.
///
/// Scenario: one very high scoring region that was GC'd just now, so the staleness
/// ordering (least recently GC'd first) would put it last, plus 12 much lower scoring
/// regions whose last GC is an hour old. `regions_per_table_threshold == 2` gives the
/// table two seats: one score seat and one staleness seat, and the staleness pool is
/// over-subscribed. The high score region keeps the score seat on every tick — the
/// seats are allocated inside the same budget, the score seat first — and the
/// staleness seat goes to an overdue low score region instead.
#[tokio::test]
async fn test_gc_staleness_seat_never_preempts_score_seat() {
    init_default_ut_logging();

    const TABLE_ID: TableId = 1;
    /// `regions_per_table_threshold`, i.e. one score seat plus one staleness seat.
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

    // Seeding: the high score region cleared its cooldown a second ago, so it is the
    // most recently GC'd region of the table, while every other region is an hour
    // overdue. Ordering by that key alone would therefore pick the overdue regions
    // first.
    let now = Instant::now();
    let recently = now
        .checked_sub(config.gc_cooldown_period + Duration::from_secs(1))
        .expect("the test needs monotonic uptime longer than gc_cooldown_period");
    let long_ago = now
        .checked_sub(Duration::from_secs(60 * 60))
        .expect("the test needs monotonic uptime longer than one hour");
    let mut tracker = HashMap::new();
    tracker.insert(
        hot_region,
        crate::gc::tracker::RegionGcInfo {
            last_gc_time: recently,
            last_full_listing_time: Some(recently),
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

    // Three consecutive ticks over the same stats and tracker: a region that wins a seat
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

        // Both seats are filled and the total stays inside the budget; the staleness pool
        // below the score seat is not exhausted.
        assert_eq!(
            table_candidates.len(),
            CAPACITY,
            "round {round}: expected the {CAPACITY} seats (one by score, one by staleness), \
             got {table_candidates:?}"
        );
        // The high score region wins the score seat even though it is the *least* overdue.
        assert_eq!(
            table_candidates[0].region_id, hot_region,
            "round {round}: the highest score must keep the first seat, got {:?}",
            table_candidates[0].region_id
        );
        assert_eq!(
            table_candidates
                .iter()
                .filter(|candidate| candidate.region_id == hot_region)
                .count(),
            1,
            "round {round}: the staleness seat must not repeat a region the score seat \
             already selected: {table_candidates:?}"
        );
        // The staleness seat is an overdue low score region.
        for candidate in table_candidates.iter().skip(CAPACITY - 1) {
            assert!(
                overdue.contains(&candidate.region_id),
                "round {round}: the staleness seat went to {}, expected one of the overdue \
                 regions",
                candidate.region_id
            );
            assert!(
                candidate.score < table_candidates[0].score,
                "round {round}: a staleness-seated candidate must not outrank the score seat"
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

/// The staleness seat is subject to the same cooldown as the score seats: a region
/// seated because its last GC is the oldest must not be dispatched again on the next
/// tick while it is still cooling down.
#[tokio::test]
async fn test_gc_staleness_seat_respects_cooldown() {
    init_default_ut_logging();

    const TABLE_ID: TableId = 1;
    /// `regions_per_table_threshold`, i.e. one score seat plus one staleness seat.
    const CAPACITY: usize = 2;
    const REGIONS: u32 = 5;
    const ROUNDS: usize = 3;

    let peer = Peer::new(1, "127.0.0.1:3001");

    // Every region shares the same positive score, so the staleness seat is the only
    // thing that can rotate the candidates past the score seat.
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
    let mut clock = LogicalClock::new();

    for round in 1..=ROUNDS {
        // Nothing is tracked on the first tick; after it, the previous round is exactly
        // the group that is still cooling down at the next tick boundary.
        if round > 1 {
            clock.tick(&scheduler, &previous, true).await;
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
            // Nothing is cooling down, so both seats fill: one by score, one by
            // staleness, and the total is exactly `regions_per_table_threshold`.
            assert_eq!(
                dispatched.len(),
                CAPACITY,
                "round 1: expected the {CAPACITY} seats (one by score, one by staleness), \
                 got {dispatched:?}"
            );
        }
        // The cooldown holds for every dispatched region, including the staleness-seated
        // ones: no region is dispatched twice in a row.
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
            dispatched.len() <= CAPACITY,
            "round {round}: dispatched {} regions, more than the {CAPACITY} seats a table \
             may contribute",
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
        "every region must be dispatched within {ROUNDS} ticks when the staleness seat \
         obeys the cooldown; per-round dispatch: {dispatched_per_round:?}"
    );
}

// ---------------------------------------------------------------------------
// GC candidate selection: a candidate must be dispatchable
// ---------------------------------------------------------------------------

/// A region the table route cannot dispatch to must not hold a candidate seat.
///
/// Counterexample (from review): `regions_per_table_threshold == 1` with
/// `gc_cooldown_period == TICKER_INTERVAL`. One seat means no score seat at all — the
/// single seat is the staleness one — so the tick goes to the least recently
/// dispatched eligible region. A (score 100) and D (score 90) take turns that way
/// because they are the tracked regions with the oldest timestamps. B (score 10) is
/// reported as a leader by the datanode but its route entry carries no leader peer,
/// and C (score 1) has never been GC'd; neither is in the tracker, so B — never GC'd
/// and higher scoring — always wins the single staleness seat.
///
/// `aggregate_candidates_by_datanode` then drops B as unroutable, so
/// `process_datanode_gc` never runs for it and `update_full_listing_time` never moves
/// its `last_gc_time`. The rotation key that is supposed to walk the tail therefore
/// stays pinned on B forever and C is never dispatched, even though every other region
/// rotates.
///
/// Selection now drops regions the route cannot dispatch to, i.e. the candidate pool is
/// the dispatchable set, so the seat goes to C first and A and D take their turns
/// afterwards. B is never dispatched.
#[tokio::test]
async fn test_gc_candidate_selection_skips_region_without_leader_peer() {
    init_default_ut_logging();

    const TABLE_ID: TableId = 1;
    /// `regions_per_table_threshold`: a single seat, which `N == 1` devotes entirely to
    /// staleness.
    const CAPACITY: usize = 1;
    /// Ticks the scenario runs for. The counterexample never dispatches the low score
    /// region, so any finite window is enough to observe the starvation.
    const ROUNDS: usize = 6;
    /// Bound on when the low score region must show up. It is the least recently
    /// dispatched region of the table, so the fix dispatches it on the very first tick;
    /// the bound is loose enough to stay valid for any rotation-based fix instead of
    /// pinning the exact round.
    const MAX_TICKS_TO_STARVED_REGION: usize = 3;

    let peer = Peer::new(1, "127.0.0.1:3001");
    let high_region = RegionId::new(TABLE_ID, 1); // A, score 100
    let alternate_region = RegionId::new(TABLE_ID, 2); // D, score 90
    let unroutable_region = RegionId::new(TABLE_ID, 3); // B, score 10, no leader peer
    let starved_region = RegionId::new(TABLE_ID, 4); // C, score 1

    // `sst_num` stays 0, so a region's score is exactly its `file_removed_cnt`.
    let mut region_stats = Vec::new();
    for (region_id, score) in [
        (high_region, 100),
        (alternate_region, 90),
        (unroutable_region, 10),
        (starved_region, 1),
    ] {
        let mut stat = mock_region_stat(region_id, RegionRole::Leader, TEST_REGION_SIZE_200MB, 0);
        if let RegionManifestInfo::Mito {
            file_removed_cnt, ..
        } = &mut stat.region_manifest
        {
            *file_removed_cnt = score;
        }
        region_stats.push(stat);
    }

    let ctx = Arc::new(
        MockSchedulerCtx {
            table_to_region_stats: Arc::new(Mutex::new(Some(HashMap::from([(
                TABLE_ID,
                region_stats,
            )])))),
            gc_reports: Arc::new(Mutex::new(HashMap::from([
                (high_region, new_empty_report_with([high_region])),
                (alternate_region, new_empty_report_with([alternate_region])),
                (
                    unroutable_region,
                    new_empty_report_with([unroutable_region]),
                ),
                (starved_region, new_empty_report_with([starved_region])),
            ]))),
            ..Default::default()
        }
        .with_region_leaders(HashMap::from([(
            TABLE_ID,
            (
                TABLE_ID,
                vec![
                    (high_region, Some(peer.clone())),
                    (alternate_region, Some(peer.clone())),
                    // The counterexample: a leader region without a leader peer, so the
                    // aggregation step can only drop it.
                    (unroutable_region, None),
                    (starved_region, Some(peer.clone())),
                ],
            ),
        )])),
    );

    let config = GcSchedulerOptions {
        regions_per_table_threshold: CAPACITY,
        ..Default::default()
    };
    // The counterexample needs the cooldown to equal the ticker interval, otherwise the
    // score channel's own group stays eligible and the alternation below does not
    // happen.
    assert_eq!(
        config.gc_cooldown_period, TICKER_INTERVAL,
        "the modelled starvation requires gc_cooldown_period == TICKER_INTERVAL"
    );

    // Seed the two high score regions so they take the single seat in turn: A was
    // dispatched inside the current tick (still cooling down), D finished one cooldown
    // ago (eligible again, and the oldest tracked region). B and C stay untracked, i.e.
    // never GC'd, which outranks every tracked region on the staleness key.
    let now = Instant::now();
    let cooling_since = now;
    let cooled_down_at = now
        .checked_sub(config.gc_cooldown_period + Duration::from_secs(1))
        .expect("the test needs monotonic uptime longer than gc_cooldown_period");
    let tracker = HashMap::from([
        (
            high_region,
            crate::gc::tracker::RegionGcInfo {
                last_gc_time: cooling_since,
                last_full_listing_time: Some(cooling_since),
            },
        ),
        (
            alternate_region,
            crate::gc::tracker::RegionGcInfo {
                last_gc_time: cooled_down_at,
                last_full_listing_time: Some(cooled_down_at),
            },
        ),
    ]);

    let scheduler = GcScheduler {
        ctx: ctx.clone(),
        runtime_switch_manager: crate::gc::scheduler::new_test_runtime_switch_manager(),
        receiver: GcScheduler::channel().1,
        config,
        region_gc_tracker: Arc::new(tokio::sync::Mutex::new(tracker)),
        last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
    };

    let mut previous: HashSet<RegionId> = HashSet::new();
    let mut clock = LogicalClock::new();
    // First tick each region was dispatched on.
    let mut dispatched_at: HashMap<RegionId, usize> = HashMap::new();

    for round in 1..=ROUNDS {
        // From the second tick on, the group dispatched by the previous tick is exactly
        // the group still in cooldown at this tick boundary; everything else is past it
        // and keeps its relative age.
        if round > 1 {
            clock.tick(&scheduler, &previous, true).await;
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

        // A single seat, and never the same region twice.
        assert!(
            dispatched.len() <= CAPACITY,
            "round {round}: dispatched {} regions, more than the {CAPACITY} seat a table may \
             contribute: {records:?}",
            dispatched.len()
        );
        assert_eq!(
            dispatched.len(),
            records.iter().map(|r| r.region_ids.len()).sum::<usize>(),
            "round {round}: a region was dispatched twice in one tick: {records:?}"
        );
        // Everything that reaches a datanode resolves to the one reachable peer, and the
        // route-less region never does.
        for region_id in &dispatched {
            assert_eq!(
                route_leader(&ctx, TABLE_ID, *region_id),
                Some(peer.clone()),
                "round {round}: region {region_id} was dispatched without a leader peer"
            );
        }
        assert!(
            !dispatched.contains(&unroutable_region),
            "round {round}: {unroutable_region} has no leader peer in its route and cannot be \
             dispatched: {records:?}"
        );

        for region_id in &dispatched {
            dispatched_at.entry(*region_id).or_insert(round);
        }
        previous = dispatched;
    }

    // The high score regions are not dropped by the filter or by the rotation: the
    // single seat still comes back around to them, so the starvation below cannot have
    // been fixed by dropping them.
    for region_id in [high_region, alternate_region] {
        assert!(
            dispatched_at.contains_key(&region_id),
            "the seat rotation must keep reaching the high score regions, but {region_id} was \
             never dispatched over {ROUNDS} ticks: {dispatched_at:?}"
        );
    }

    // Core assertion: the low score region gets a dispatch within a bounded number of
    // ticks. Keyed on the counterexample, it is seated by the staleness rule on every
    // tick and then dropped as unroutable, so its rotation key never advances.
    let starved_round = dispatched_at.get(&starved_region).copied();
    assert!(
        starved_round.is_some_and(|round| round <= MAX_TICKS_TO_STARVED_REGION),
        "region {starved_region} (score 1) was not dispatched within \
         {MAX_TICKS_TO_STARVED_REGION} of {ROUNDS} ticks; it was dispatched in {starved_round:?}. \
         The seat always went to the route-less region {unroutable_region}, which \
         aggregation drops, so its last GC never advances and it keeps holding the seat. \
         Dispatches per round: {dispatched_at:?}"
    );
}
