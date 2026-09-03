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

//! [`LedgerMemoryPool`]: adapts a memory-ledger [`Account`] to DataFusion's
//! [`MemoryPool`] trait, so DataFusion operator memory draws from the same
//! budget as every other handle on the account (e.g. the scan tracker).
//!
//! # Mapping
//!
//! The pool holds one zero-sized [`AccountGuard`] acquired at construction and
//! grows/shrinks it as reservations change:
//!
//! - [`MemoryPool::try_grow`] maps to [`AccountGuard::try_grow`]; on failure it
//!   returns a DataFusion resources-exhausted error naming the consumer and the
//!   account's current usage and limit.
//! - [`MemoryPool::shrink`] maps to [`AccountGuard::shrink`], called only with
//!   whole-permit amounts once that many bytes have actually been freed
//!   ([`AccountGuard::shrink`] rounds down, so passing raw byte counts through
//!   would over-release).
//! - [`MemoryPool::reserved`] reports the permit-rounded bytes charged to the
//!   account.
//!
//! Account permits are coarse-grained (KB/MB), so the adapter tracks the exact
//! reserved bytes itself and keeps the guard at exactly
//! `ceil(reserved / granularity)` permits: sub-permit requests aggregate into
//! shared permits instead of charging one permit per call, and an aggregated
//! shrink releases exactly the permits the preceding grows acquired.
//!
//! [`MemoryPool::grow`] is contractually infallible, but the backing account
//! can be exhausted. The portion it cannot back is charged to the guard as
//! account-visible debt, so later fallible admissions see it.

use std::fmt;
use std::sync::Mutex;

use common_memory_manager::PermitGranularity;
use common_memory_manager::ledger::{Account, AccountGuard};
use datafusion::execution::memory_pool::{
    MemoryLimit, MemoryPool, MemoryReservation, human_readable_size,
};
use datafusion_common::{DataFusionError, resources_datafusion_err};

/// A [`MemoryPool`] backed by a memory-ledger [`Account`].
pub struct LedgerMemoryPool {
    account: Account,
    /// Permit granularity of `account`. Must match [`Account::granularity`].
    granularity: PermitGranularity,
    state: Mutex<PoolState>,
}

struct PoolState {
    /// The pool's grant on the account, always holding exactly
    /// `ceil(reserved_bytes / granularity)` bytes, backed or unbacked.
    guard: AccountGuard,
    /// Exact bytes reserved by DataFusion reservations, before permit rounding.
    reserved_bytes: u64,
    /// The exact reservation total exceeded `u64` or could not be rounded.
    reserved_overflowed: bool,
}

enum CoverError {
    Missing(u64),
    Unrepresentable,
}

impl LedgerMemoryPool {
    /// Creates a pool drawing from `account`. `granularity` must be the permit
    /// granularity the account was created with.
    pub fn new(account: Account, granularity: PermitGranularity) -> Self {
        let guard = account
            .try_acquire(0)
            .expect("zero-byte acquisition cannot fail");
        Self {
            account,
            granularity,
            state: Mutex::new(PoolState {
                guard,
                reserved_bytes: 0,
                reserved_overflowed: false,
            }),
        }
    }

    /// Rounds `bytes` up to whole permits of the account's granularity.
    fn round_up_to_permits(&self, bytes: u64) -> Option<u64> {
        let granularity = self.granularity.bytes();
        bytes
            .checked_add(granularity - 1)
            .map(|sum| sum / granularity * granularity)
    }

    /// Grows the guard so its charge covers `new_reserved` bytes
    /// rounded up to whole permits. On failure returns the missing bytes and
    /// leaves the state untouched.
    fn try_cover(&self, state: &mut PoolState, new_reserved: u64) -> Result<(), CoverError> {
        if state.guard.unbacked_overflowed() {
            return Err(CoverError::Unrepresentable);
        }
        let needed = self
            .round_up_to_permits(new_reserved)
            .ok_or(CoverError::Unrepresentable)?;
        let covered = state.guard.charged_bytes();
        if needed <= covered || state.guard.try_grow(needed - covered) {
            Ok(())
        } else {
            Err(CoverError::Missing(needed - covered))
        }
    }

    fn insufficient_capacity_err(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> DataFusionError {
        resources_datafusion_err!(
            "Failed to allocate additional {} for {} with {} already allocated for this reservation - memory account {} has {} used of {} limit",
            human_readable_size(additional),
            reservation.consumer().name(),
            human_readable_size(reservation.size()),
            self.account.name(),
            human_readable_size(self.account.used_bytes() as usize),
            human_readable_size(self.account.target_limit_bytes() as usize)
        )
    }
}

impl MemoryPool for LedgerMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        let mut state = self.state.lock().unwrap();
        let Some(new_reserved) = state.reserved_bytes.checked_add(additional as u64) else {
            state.guard.poison_unbacked();
            state.reserved_bytes = u64::MAX;
            state.reserved_overflowed = true;
            return;
        };
        match self.try_cover(&mut state, new_reserved) {
            Ok(()) => {}
            Err(CoverError::Missing(missing)) => state.guard.charge_unbacked(missing),
            Err(CoverError::Unrepresentable) => {
                state.guard.poison_unbacked();
                state.reserved_overflowed = true;
            }
        }
        state.reserved_bytes = new_reserved;
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        let mut state = self.state.lock().unwrap();
        if state.reserved_overflowed {
            return;
        }
        state.reserved_bytes = state.reserved_bytes.saturating_sub(shrink as u64);
        let Some(needed) = self.round_up_to_permits(state.reserved_bytes) else {
            state.guard.poison_unbacked();
            state.reserved_overflowed = true;
            return;
        };
        let release = state.guard.charged_bytes().saturating_sub(needed);
        if release > 0 {
            let released = state.guard.shrink(release);
            // `release` is permit-aligned and within the charge, so
            // `AccountGuard::shrink` releases exactly it.
            // Every operation rereads `granted_bytes` as ground truth, so a
            // deviation would be corrected on the next shrink anyway.
            debug_assert_eq!(released, release);
        }
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        let mut state = self.state.lock().unwrap();
        let Some(new_reserved) = state.reserved_bytes.checked_add(additional as u64) else {
            return Err(self.insufficient_capacity_err(reservation, additional));
        };
        if state.reserved_overflowed || state.guard.unbacked_overflowed() {
            return Err(self.insufficient_capacity_err(reservation, additional));
        }
        if self.try_cover(&mut state, new_reserved).is_err() {
            return Err(self.insufficient_capacity_err(reservation, additional));
        }
        state.reserved_bytes = new_reserved;
        Ok(())
    }

    fn reserved(&self) -> usize {
        let state = self.state.lock().unwrap();
        if state.reserved_overflowed {
            usize::MAX
        } else {
            state.guard.charged_bytes().min(usize::MAX as u64) as usize
        }
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.account.target_limit_bytes() as usize)
    }
}

impl fmt::Debug for LedgerMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LedgerMemoryPool")
            .field("account", &self.account.name())
            .field("reserved", &self.reserved())
            .field(
                "unbacked",
                &self.state.lock().unwrap().guard.unbacked_bytes(),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_memory_manager::ledger::Category;
    use datafusion::execution::memory_pool::MemoryConsumer;

    use super::*;

    const KB: u64 = 1024;

    fn pool_with_limit(limit_kb: u64) -> (Account, Arc<dyn MemoryPool>) {
        let account = Account::new(
            "query/engine",
            Category::Query,
            limit_kb * KB,
            PermitGranularity::Kilobyte,
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(LedgerMemoryPool::new(
            account.clone(),
            PermitGranularity::Kilobyte,
        ));
        (account, pool)
    }

    #[test]
    fn grow_shrink_round_trip_returns_reserved_to_zero() {
        let (account, pool) = pool_with_limit(16);
        let r1 = MemoryConsumer::new("op-a").register(&pool);
        let r2 = MemoryConsumer::new("op-b").register(&pool);

        // Sub-permit requests aggregate instead of charging one permit each.
        r1.try_grow(512).unwrap();
        r1.try_grow(512).unwrap();
        assert_eq!(pool.reserved() as u64, KB);

        // 1024 + 1500 bytes reserved round up to three 1 KB permits.
        r2.try_grow(1500).unwrap();
        assert_eq!(pool.reserved() as u64, 3 * KB);
        assert_eq!(account.used_bytes(), 3 * KB);

        // Aggregated frees release exactly what the grows acquired.
        r1.free();
        assert_eq!(pool.reserved() as u64, 2 * KB);
        r2.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn repeated_sub_permit_shrinks_keep_books_consistent() {
        let (account, pool) = pool_with_limit(8);
        let r = MemoryConsumer::new("dribble").register(&pool);
        r.try_grow(2 * KB as usize).unwrap();

        // 20 x 100 B shrinks at KB granularity: the guard releases a permit
        // only once a whole permit's worth of bytes has actually been freed,
        // and pool/account books stay in lockstep the whole way.
        for _ in 0..20 {
            r.shrink(100);
            let expected = r.size().div_ceil(KB as usize) * KB as usize;
            assert_eq!(pool.reserved(), expected);
            assert_eq!(account.used_bytes() as usize, expected);
        }
        // 2048 - 2000 = 48 B still reserved: the guard is not emptied while
        // the reservation holds a balance.
        assert_eq!(r.size(), 48);
        assert_eq!(pool.reserved() as u64, KB);
        r.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn try_grow_beyond_limit_reports_resources_exhausted() {
        let (_account, pool) = pool_with_limit(4);
        let r = MemoryConsumer::new("hungry-operator").register(&pool);

        let err = r.try_grow(8 * KB as usize).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "unexpected error: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("hungry-operator"), "missing consumer: {msg}");
        assert!(msg.contains("query/engine"), "missing account: {msg}");

        // A failed grow leaves no charge behind; the limit itself still fits.
        assert_eq!(pool.reserved(), 0);
        r.try_grow(4 * KB as usize).unwrap();
    }

    #[test]
    fn concurrent_reservations_never_exceed_account_limit() {
        let (account, pool) = pool_with_limit(8);

        std::thread::scope(|s| {
            for t in 0..4u64 {
                let pool = pool.clone();
                let account = account.clone();
                s.spawn(move || {
                    let r = MemoryConsumer::new(format!("op-{t}")).register(&pool);
                    for i in 0..200u64 {
                        let bytes = ((t + i) % 3 + 1) * KB;
                        if r.try_grow(bytes as usize).is_ok() {
                            assert!(account.used_bytes() <= account.effective_limit_bytes());
                            assert!(pool.reserved() as u64 <= 8 * KB);
                            r.shrink(bytes as usize);
                        }
                    }
                });
            }
        });

        assert_eq!(pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn pool_shares_budget_with_external_account_handle() {
        let (account, pool) = pool_with_limit(10);
        let r = MemoryConsumer::new("pool-face").register(&pool);

        // An external handle on the same account takes most of the budget.
        let external = account.try_acquire(8 * KB).unwrap();
        // 2 KB left: a 4 KB pool-side grow must fail...
        let err = r.try_grow(4 * KB as usize).unwrap_err();
        assert!(matches!(err, DataFusionError::ResourcesExhausted(_)));
        // ...while the exact remainder still fits.
        r.try_grow(2 * KB as usize).unwrap();
        assert_eq!(account.used_bytes(), 10 * KB);

        // Capacity released by the external face becomes visible to the pool.
        drop(external);
        r.try_grow(8 * KB as usize).unwrap();
        assert_eq!(account.used_bytes(), 10 * KB);
        r.free();
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn infallible_grow_charges_account_visible_debt() {
        let account = Account::new(
            "query/engine",
            Category::Query,
            4 * KB,
            PermitGranularity::Kilobyte,
        );
        let pool = Arc::new(LedgerMemoryPool::new(
            account.clone(),
            PermitGranularity::Kilobyte,
        ));
        let dyn_pool: Arc<dyn MemoryPool> = pool.clone();
        let r = MemoryConsumer::new("infallible-grow").register(&dyn_pool);

        r.try_grow(4 * KB as usize).unwrap(); // account full
        r.grow(2 * KB as usize); // must not panic
        assert_eq!(account.backed_bytes(), 4 * KB);
        assert_eq!(account.unbacked_bytes(), 2 * KB);
        assert_eq!(dyn_pool.reserved() as u64, 6 * KB);
        assert_eq!(account.used_bytes(), 6 * KB);

        r.shrink(2 * KB as usize);
        assert_eq!(account.backed_bytes(), 4 * KB);
        assert_eq!(account.unbacked_bytes(), 0);
        assert_eq!(dyn_pool.reserved() as u64, 4 * KB);
        r.free();
        assert_eq!(dyn_pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);

        // With capacity available again, grow takes real permits.
        r.grow(KB as usize);
        assert_eq!(account.unbacked_bytes(), 0);
        assert_eq!(account.used_bytes(), KB);
        r.free();
        assert_eq!(dyn_pool.reserved(), 0);
    }

    #[test]
    fn accounted_debt_blocks_fallible_growth_after_external_release() {
        let (account, pool) = pool_with_limit(4);
        let external = account.try_acquire(4 * KB).unwrap();
        let reservation = MemoryConsumer::new("pool-face").register(&pool);

        reservation.grow(2 * KB as usize);
        assert_eq!(account.backed_bytes(), 4 * KB);
        assert_eq!(account.unbacked_bytes(), 2 * KB);
        assert_eq!(account.used_bytes(), 6 * KB);
        assert_eq!(pool.reserved() as u64, 2 * KB);

        drop(external);
        let err = reservation.try_grow(3 * KB as usize).unwrap_err();
        assert!(matches!(err, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(reservation.size() as u64, 2 * KB);
        assert_eq!(pool.reserved() as u64, 2 * KB);
        assert_eq!(account.backed_bytes(), 0);
        assert_eq!(account.unbacked_bytes(), 2 * KB);
        assert_eq!(account.used_bytes(), 2 * KB);

        reservation.try_grow(2 * KB as usize).unwrap();
        assert_eq!(reservation.size() as u64, 4 * KB);
        assert_eq!(pool.reserved() as u64, 4 * KB);
        assert_eq!(account.backed_bytes(), 2 * KB);
        assert_eq!(account.unbacked_bytes(), 2 * KB);
        assert_eq!(account.used_bytes(), 4 * KB);
        reservation.free();
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn infallible_grow_accounts_beyond_permit_conversion_limit() {
        let (account, pool) = pool_with_limit(4);
        let reservation = MemoryConsumer::new("large-infallible").register(&pool);
        let max_permit_bytes = PermitGranularity::Kilobyte.permits_to_bytes(u32::MAX);
        let requested = max_permit_bytes + KB;

        reservation.grow(requested as usize);
        assert_eq!(reservation.size() as u64, requested);
        assert_eq!(pool.reserved() as u64, requested);
        assert_eq!(account.backed_bytes(), 0);
        assert_eq!(account.unbacked_bytes(), requested);
        assert_eq!(account.used_bytes(), requested);

        reservation.free();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    #[test]
    fn unrepresentable_infallible_grow_poison_blocks_fallible_growth() {
        let account = Account::new(
            "query/engine",
            Category::Query,
            4 * KB,
            PermitGranularity::Kilobyte,
        );
        let pool = Arc::new(LedgerMemoryPool::new(
            account.clone(),
            PermitGranularity::Kilobyte,
        ));
        let dyn_pool: Arc<dyn MemoryPool> = pool.clone();
        let reservation = MemoryConsumer::new("overflow").register(&dyn_pool);

        pool.grow(&reservation, usize::MAX);
        assert_eq!(pool.reserved(), usize::MAX);
        assert_eq!(account.unbacked_bytes(), u64::MAX);
        assert_eq!(account.used_bytes(), u64::MAX);

        let err = pool.try_grow(&reservation, 1).unwrap_err();
        assert!(matches!(err, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(pool.reserved(), usize::MAX);
        assert_eq!(account.used_bytes(), u64::MAX);

        pool.shrink(&reservation, usize::MAX);
        assert_eq!(pool.reserved(), usize::MAX);
        assert_eq!(account.used_bytes(), u64::MAX);
    }
}

/// End-to-end tests: real DataFusion `ORDER BY` queries executing on a
/// [`LedgerMemoryPool`] (wrapped in `TrackConsumersPool`, with an explicit
/// `DiskManager`), exercising the spill path, the graceful-error path, the
/// one-account-two-faces contract, and runtime limit shrink.
#[cfg(test)]
mod e2e_tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use common_memory_manager::ledger::Category;
    use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::TrackConsumersPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use tempfile::TempDir;

    use super::*;

    const MB: u64 = 1024 * 1024;
    /// Payload width making a row cost ~262 B in Arrow memory (i64 key +
    /// 250 B string data + 4 B offset), so row counts translate to sizes.
    const PAYLOAD_LEN: usize = 250;
    const ROWS_PER_BATCH: usize = 512;

    fn query_account(limit_bytes: u64) -> Account {
        Account::new(
            "query/engine",
            Category::Query,
            limit_bytes,
            PermitGranularity::Kilobyte,
        )
    }

    /// A `SessionContext` whose operator memory comes from `account` through
    /// `TrackConsumersPool<LedgerMemoryPool>`, with an explicit disk manager.
    fn ledger_session(account: &Account, disk: DiskManagerMode) -> SessionContext {
        let pool = TrackConsumersPool::new(
            LedgerMemoryPool::new(account.clone(), account.granularity()),
            NonZeroUsize::new(2).unwrap(),
        );
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(pool))
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(disk))
            .build_arc()
            .unwrap();
        let config = SessionConfig::new()
            // One partition, one sort consumer: deterministic accounting.
            .with_target_partitions(1)
            .with_batch_size(ROWS_PER_BATCH)
            // The 10 MiB default merge reservation dwarfs the test budgets
            // and would fail the sort before it could ever spill.
            .with_sort_spill_reservation_bytes(256 * 1024)
            .with_sort_in_place_threshold_bytes(0);
        SessionContext::new_with_config_rt(config, runtime)
    }

    /// A temp spill root (kept alive by the returned guard) and the disk
    /// manager mode pointing at it.
    fn spill_dir() -> (TempDir, DiskManagerMode) {
        let dir = tempfile::Builder::new()
            .prefix("ledger-pool-spill-")
            .tempdir()
            .unwrap();
        let mode = DiskManagerMode::Directories(vec![dir.path().to_path_buf()]);
        (dir, mode)
    }

    /// Registers table `t(k BIGINT, payload STRING)` holding `rows` rows in
    /// descending key order, chunked into batches of `rows_per_batch`.
    fn register_wide_table(ctx: &SessionContext, rows: usize, rows_per_batch: usize) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let mut batches = Vec::new();
        let mut next = rows as i64;
        while next > 0 {
            let n = rows_per_batch.min(next as usize) as i64;
            let keys: Vec<i64> = (0..n).map(|i| next - i).collect();
            let payloads: Vec<String> = keys
                .iter()
                .map(|k| format!("{k:0>width$}", width = PAYLOAD_LEN))
                .collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(payloads)),
            ];
            batches.push(RecordBatch::try_new(schema.clone(), columns).unwrap());
            next -= n;
        }
        let table = MemTable::try_new(schema, vec![batches]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
    }

    /// Sums `spill_count`/`spilled_bytes` over the whole physical plan.
    fn spill_metrics(plan: &dyn ExecutionPlan) -> (usize, usize) {
        let (mut count, mut bytes) = (0, 0);
        if let Some(metrics) = plan.metrics() {
            count += metrics.spill_count().unwrap_or(0);
            bytes += metrics.spilled_bytes().unwrap_or(0);
        }
        for child in plan.children() {
            let (c, b) = spill_metrics(child.as_ref());
            count += c;
            bytes += b;
        }
        (count, bytes)
    }

    /// Runs the ordering query, asserts the result is complete and fully
    /// sorted, and returns `(spill_count, spilled_bytes)` of the executed
    /// plan.
    async fn run_sorted_query(ctx: &SessionContext, rows: usize) -> (usize, usize) {
        let df = ctx
            .sql("SELECT k, payload FROM t ORDER BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let batches = collect(plan.clone(), ctx.task_ctx()).await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, rows);
        let mut expected = 1i64;
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for &k in keys.values().iter() {
                assert_eq!(k, expected);
                expected += 1;
            }
        }
        spill_metrics(plan.as_ref())
    }

    /// A sort ~4x larger than the budget completes by spilling through the
    /// explicitly configured disk manager, with the spill visible in the
    /// plan metrics and the account fully drained afterwards.
    #[tokio::test]
    async fn sort_beyond_budget_spills_and_completes() {
        const ROWS: usize = 64_000; // ~16 MiB against a 4 MiB budget

        let account = query_account(4 * MB);
        let (_dir, disk) = spill_dir();
        let ctx = ledger_session(&account, disk);
        register_wide_table(&ctx, ROWS, ROWS_PER_BATCH);

        let (spill_count, spilled_bytes) = run_sorted_query(&ctx, ROWS).await;
        assert!(spill_count > 0, "expected the sort to spill");
        assert!(spilled_bytes > 0);

        // Temp files are cleaned up and every reservation returned to the
        // ledger: the account is immediately reusable.
        assert_eq!(ctx.runtime_env().disk_manager.used_disk_space(), 0);
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    /// With the disk manager disabled, a sort that cannot fit its input in
    /// the budget fails with a resources-exhausted error naming the consumer
    /// (via `TrackConsumersPool`) and the backing account — never a panic —
    /// and leaves no reservation behind.
    #[tokio::test]
    async fn sort_beyond_budget_without_disk_fails_gracefully() {
        const ROWS: usize = 40_000; // ~10 MiB in one batch against 4 MiB

        let account = query_account(4 * MB);
        let ctx = ledger_session(&account, DiskManagerMode::Disabled);
        // A single batch larger than the whole budget: the sorter's very
        // first reservation fails, surfacing the pool's error undiluted by
        // a later failed-spill error.
        register_wide_table(&ctx, ROWS, ROWS);

        let df = ctx
            .sql("SELECT k, payload FROM t ORDER BY k")
            .await
            .unwrap();
        let err = df.collect().await.unwrap_err();

        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
            "unexpected error: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("ExternalSorter"), "missing consumer: {msg}");
        assert!(msg.contains("query/engine"), "missing account: {msg}");

        // The failed query leaves no residue in the pool or the account.
        assert_eq!(ctx.runtime_env().memory_pool.reserved(), 0);
        assert_eq!(account.used_bytes(), 0);
    }

    /// One account, two faces, under a real query: an async-face guard (the
    /// scan-tracker face) squeezes the pool face until the same query has to
    /// spill; releasing the guard restores in-memory execution.
    #[tokio::test]
    async fn external_guard_squeezes_query_into_spill_until_released() {
        const ROWS: usize = 12_000; // ~3 MiB dataset

        let account = query_account(16 * MB);
        let (_dir, disk) = spill_dir();
        let ctx = ledger_session(&account, disk);
        register_wide_table(&ctx, ROWS, ROWS_PER_BATCH);

        // Baseline: the query runs entirely in memory.
        let (spill_count, _) = run_sorted_query(&ctx, ROWS).await;
        assert_eq!(spill_count, 0, "baseline must not spill");

        // The async face takes 12 of the 16 MiB.
        let squeeze = account.acquire(12 * MB).await.unwrap();
        assert_eq!(account.used_bytes(), 12 * MB);

        // The pool face sees only the remainder: the same query now spills,
        // yet still completes correctly.
        let (spill_count, _) = run_sorted_query(&ctx, ROWS).await;
        assert!(spill_count > 0, "expected the squeezed query to spill");
        // The query returned its reservations; only the guard remains.
        assert_eq!(account.used_bytes(), 12 * MB);

        // Releasing the guard restores pure in-memory execution.
        drop(squeeze);
        let (spill_count, _) = run_sorted_query(&ctx, ROWS).await;
        assert_eq!(spill_count, 0, "released budget must stop the spilling");
        assert_eq!(account.used_bytes(), 0);
    }

    /// Shrinking the account limit at runtime changes how the next DataFusion
    /// query executes: what ran in memory under the old limit spills under
    /// the new one, once `collect_shrink` converges the deficit.
    #[tokio::test]
    async fn runtime_shrink_pushes_query_into_spill() {
        const ROWS: usize = 32_000; // ~8 MiB dataset

        let account = query_account(32 * MB);
        let (_dir, disk) = spill_dir();
        let ctx = ledger_session(&account, disk);
        register_wide_table(&ctx, ROWS, ROWS_PER_BATCH);

        // Under the initial limit the query runs entirely in memory.
        let (spill_count, _) = run_sorted_query(&ctx, ROWS).await;
        assert_eq!(spill_count, 0, "baseline must not spill");

        // Shrink to 4 MiB while 28 MiB are granted to an async-face guard:
        // the 4 idle MiB harvest instantly, the remaining 24 MiB deficit
        // converges via the collector once the guard releases.
        let held = account.acquire(28 * MB).await.unwrap();
        assert_eq!(account.set_limit_bytes(4 * MB), 24 * MB);
        assert_eq!(account.effective_limit_bytes(), 28 * MB);
        drop(held);
        account.collect_shrink().await;
        assert_eq!(account.effective_limit_bytes(), 4 * MB);

        // The pool reports the new limit, and the same query now spills.
        assert!(matches!(
            ctx.runtime_env().memory_pool.memory_limit(),
            MemoryLimit::Finite(n) if n as u64 == 4 * MB
        ));
        let (spill_count, _) = run_sorted_query(&ctx, ROWS).await;
        assert!(spill_count > 0, "expected the shrunk budget to force spill");
        assert_eq!(account.used_bytes(), 0);
    }
}
