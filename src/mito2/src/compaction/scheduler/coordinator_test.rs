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

use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc};

use super::*;
use crate::schedule::scheduler::{LocalScheduler, Scheduler};

/// Controls the executor boundary, not the policy or region state under test.
#[derive(Default)]
struct ManualExecutor {
    jobs: Mutex<VecDeque<Job>>,
    reject: AtomicBool,
}

impl ManualExecutor {
    async fn run_next(&self) {
        let job = self.jobs.lock().unwrap().pop_front().unwrap();
        job.await;
    }

    fn len(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl Scheduler for ManualExecutor {
    fn schedule(&self, job: Job) -> Result<()> {
        if self.reject.load(Ordering::Relaxed) {
            return InvalidSchedulerStateSnafu.fail();
        }
        self.jobs.lock().unwrap().push_back(job);
        Ok(())
    }

    async fn stop(&self, _: bool) -> Result<()> {
        assert_eq!(0, self.len());
        Ok(())
    }
}

fn unit(bytes: u64, id: u64, started: &mpsc::UnboundedSender<u64>) -> ReadyUnit {
    let started = started.clone();
    ReadyUnit::new(
        CancellableTaskState::new(),
        Arc::new(CompactionCost::new(bytes)),
        Box::pin(async move {
            started.send(id).unwrap();
        }),
        Box::pin(async {}),
    )
}

async fn recv<T>(rx: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn test_single_region_borrows_all_slots_without_exceeding_global_capacity() {
    let coordinator = Arc::new(CompactionCoordinator::new(
        Arc::new(LocalScheduler::new(4)),
        4,
    ));
    let running = Arc::new(AtomicUsize::new(0));
    let permits = Arc::new(Semaphore::new(0));
    let (started, mut rx) = mpsc::unbounded_channel();
    for id in 0..12 {
        let running = running.clone();
        let permits = permits.clone();
        let started = started.clone();
        coordinator
            .schedule(
                RegionId::from_u64(1),
                ReadyUnit::new(
                    CancellableTaskState::new(),
                    Arc::new(CompactionCost::new(100)),
                    Box::pin(async move {
                        assert!(running.fetch_add(1, Ordering::SeqCst) < 4);
                        started.send(id).unwrap();
                        permits.acquire().await.unwrap().forget();
                        running.fetch_sub(1, Ordering::SeqCst);
                    }),
                    Box::pin(async {}),
                ),
            )
            .unwrap();
    }
    for _ in 0..4 {
        recv(&mut rx).await;
    }
    assert_eq!(4, running.load(Ordering::SeqCst));
    assert!(rx.try_recv().is_err());
    permits.add_permits(12);
    coordinator.stop().await.unwrap();
    assert_eq!(0, running.load(Ordering::SeqCst));
    assert_eq!(8, rx.len());
}

#[tokio::test]
async fn test_late_region_is_not_hidden_behind_an_existing_fifo_backlog() {
    let executor = Arc::new(ManualExecutor::default());
    let coordinator = Arc::new(CompactionCoordinator::new(executor.clone(), 4));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let a = RegionId::from_u64(1);
    let b = RegionId::from_u64(2);
    for id in 0..8 {
        coordinator.schedule(a, unit(10, id, &tx)).unwrap();
    }
    assert_eq!(4, executor.len());
    coordinator.schedule(b, unit(10, 100, &tx)).unwrap();
    for _ in 0..5 {
        executor.run_next().await;
    }
    assert_eq!(
        (0..4).chain([100]).collect::<Vec<_>>(),
        (0..5).map(|_| rx.try_recv().unwrap()).collect::<Vec<_>>()
    );
    // Once B drains, A resumes using every slot it has work for.
    assert_eq!(4, executor.len());
    for _ in 0..4 {
        executor.run_next().await;
    }
    coordinator.stop().await.unwrap();
}

#[tokio::test]
async fn test_fairness_accounts_for_bytes_instead_of_task_counts() {
    let executor = Arc::new(ManualExecutor::default());
    let coordinator = Arc::new(CompactionCoordinator::new(executor.clone(), 1));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let a = RegionId::from_u64(1);
    let b = RegionId::from_u64(2);
    coordinator.schedule(a, unit(100, 1, &tx)).unwrap();
    coordinator.schedule(a, unit(100, 2, &tx)).unwrap();
    for id in 10..30 {
        coordinator.schedule(b, unit(1, id, &tx)).unwrap();
    }
    executor.run_next().await;
    assert_eq!(1, recv(&mut rx).await);
    for expected in 10..30 {
        executor.run_next().await;
        assert_eq!(expected, recv(&mut rx).await);
    }
    executor.run_next().await;
    assert_eq!(2, recv(&mut rx).await);
    coordinator.stop().await.unwrap();
}

#[tokio::test]
async fn test_actual_cost_reconciliation_changes_subsequent_admission() {
    let executor = Arc::new(ManualExecutor::default());
    let coordinator = Arc::new(CompactionCoordinator::new(executor.clone(), 1));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let a = RegionId::from_u64(1);
    let b = RegionId::from_u64(2);
    let first = unit(1000, 1, &tx);
    let cost = first.cost.clone();
    coordinator.schedule(a, first).unwrap();
    coordinator.schedule(a, unit(10, 2, &tx)).unwrap();
    coordinator.schedule(b, unit(10, 10, &tx)).unwrap();
    coordinator.schedule(b, unit(10, 11, &tx)).unwrap();
    cost.record_actual(1);
    for expected in [1, 10, 2, 11] {
        executor.run_next().await;
        assert_eq!(expected, recv(&mut rx).await);
    }
    coordinator.stop().await.unwrap();
}

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[rstest::rstest]
#[case::queued(false, false)]
#[case::authorized_not_polled(true, false)]
#[case::cancelled_before_submission(false, true)]
#[tokio::test]
async fn test_cancelled_unit_releases_resources_before_notification_without_running(
    #[case] admitted: bool,
    #[case] initially_cancelled: bool,
) {
    let executor = Arc::new(ManualExecutor::default());
    let coordinator = Arc::new(CompactionCoordinator::new(executor.clone(), 1));
    let a = RegionId::from_u64(1);
    let b = RegionId::from_u64(2);
    let (started, mut starts) = mpsc::unbounded_channel();
    if !admitted {
        coordinator.schedule(a, unit(1, 1, &started)).unwrap();
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = DropCount(dropped.clone());
    let state = CancellableTaskState::new();
    if initially_cancelled {
        state.request_cancel();
    }
    let (completed, mut completions) = mpsc::unbounded_channel();
    coordinator
        .schedule(
            b,
            ReadyUnit::new(
                state.clone(),
                Arc::new(CompactionCost::new(100)),
                Box::pin(async move {
                    let _guard = guard;
                    started.send(2).unwrap();
                }),
                Box::pin(async move {
                    completed.send(dropped.load(Ordering::SeqCst)).unwrap();
                }),
            ),
        )
        .unwrap();
    state.request_cancel();
    coordinator.cancel_pending(b);
    if admitted {
        executor.run_next().await;
    }
    assert_eq!(1, recv(&mut completions).await);
    assert!(starts.try_recv().is_err());
    if !admitted {
        executor.run_next().await;
    }
    coordinator.stop().await.unwrap();
}

#[tokio::test]
async fn test_executor_rejection_drains_queued_work_without_recursion_or_credit_leaks() {
    let executor = Arc::new(ManualExecutor::default());
    let coordinator = Arc::new(CompactionCoordinator::new(executor.clone(), 1));
    let (tx, _rx) = mpsc::unbounded_channel();
    let region = RegionId::from_u64(1);
    coordinator.schedule(region, unit(1, 0, &tx)).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let (completed, mut completions) = mpsc::unbounded_channel();
    for id in 0..128 {
        let guard = DropCount(dropped.clone());
        let completed = completed.clone();
        coordinator
            .schedule(
                region,
                ReadyUnit::new(
                    CancellableTaskState::new(),
                    Arc::new(CompactionCost::new(1)),
                    Box::pin(async move {
                        let _guard = guard;
                        panic!("rejected job ran");
                    }),
                    Box::pin(async move {
                        completed.send(id).unwrap();
                    }),
                ),
            )
            .unwrap();
    }
    executor.reject.store(true, Ordering::Relaxed);
    executor.run_next().await;
    for _ in 0..128 {
        recv(&mut completions).await;
    }
    assert_eq!(128, dropped.load(Ordering::SeqCst));
    assert_eq!(0, executor.len());
    assert_eq!(0, coordinator.lock().running);
    coordinator.stop().await.unwrap();
}

#[tokio::test]
async fn test_job_panic_does_not_lose_completion_or_the_executor_slot() {
    let coordinator = Arc::new(CompactionCoordinator::new(
        Arc::new(LocalScheduler::new(1)),
        1,
    ));
    let (completed, mut rx) = mpsc::unbounded_channel();
    let done = completed.clone();
    coordinator
        .schedule(
            RegionId::from_u64(1),
            ReadyUnit::new(
                CancellableTaskState::new(),
                Arc::new(CompactionCost::new(1)),
                Box::pin(async {
                    panic!("executor panic fixture");
                }),
                Box::pin(async move {
                    done.send(1).unwrap();
                }),
            ),
        )
        .unwrap();
    coordinator
        .schedule(RegionId::from_u64(2), unit(1, 2, &completed))
        .unwrap();
    let mut results = [recv(&mut rx).await, recv(&mut rx).await];
    results.sort_unstable();
    assert_eq!([1, 2], results);
    coordinator.stop().await.unwrap();
    assert!(
        coordinator
            .schedule(RegionId::from_u64(1), unit(1, 3, &completed))
            .is_err()
    );
}

#[tokio::test]
async fn test_service_debt_survives_a_region_batch_boundary() {
    let executor = Arc::new(ManualExecutor::default());
    let coordinator = Arc::new(CompactionCoordinator::new(executor.clone(), 1));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let a = RegionId::from_u64(1);
    let b = RegionId::from_u64(2);
    coordinator.schedule(a, unit(100, 1, &tx)).unwrap();
    for id in 10..30 {
        coordinator.schedule(b, unit(1, id, &tx)).unwrap();
    }
    executor.run_next().await;
    assert_eq!(1, recv(&mut rx).await);
    // A has no active/ready unit between its batches, but must not erase its debt.
    coordinator.schedule(a, unit(100, 2, &tx)).unwrap();
    for expected in (10..30).chain([2]) {
        executor.run_next().await;
        assert_eq!(expected, recv(&mut rx).await);
    }
    coordinator.forget_region(a);
    coordinator.forget_region(b);
    assert!(coordinator.lock().regions.is_empty());
    coordinator.stop().await.unwrap();
}

#[tokio::test]
async fn test_old_cancellation_does_not_withdraw_a_reopened_regions_unit() {
    let executor = Arc::new(ManualExecutor::default());
    let coordinator = Arc::new(CompactionCoordinator::new(executor.clone(), 1));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let region = RegionId::from_u64(1);
    coordinator
        .schedule(RegionId::from_u64(2), unit(1, 0, &tx))
        .unwrap();
    let old = unit(1, 1, &tx);
    let old_state = old.state.clone();
    coordinator.schedule(region, old).unwrap();
    coordinator.forget_region(region);
    coordinator.schedule(region, unit(1, 2, &tx)).unwrap();
    old_state.request_cancel();
    coordinator.cancel_pending(region);
    for expected in [0, 2] {
        executor.run_next().await;
        assert_eq!(expected, recv(&mut rx).await);
    }
    assert_eq!(0, executor.len());
    coordinator.stop().await.unwrap();
}
