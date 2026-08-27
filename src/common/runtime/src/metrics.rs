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

//! Runtime metrics
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use catio::Scheduler;
use lazy_static::lazy_static;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::*;

use crate::global::{QUERY_TASK_CLASS, WRITE_TASK_CLASS};

pub const THREAD_NAME_LABEL: &str = "thread_name";

lazy_static! {
    pub static ref METRIC_RUNTIME_THREADS_ALIVE: IntGaugeVec = register_int_gauge_vec!(
        "greptime_runtime_threads_alive",
        "runtime threads alive",
        &[THREAD_NAME_LABEL]
    )
    .unwrap();
    pub static ref METRIC_RUNTIME_THREADS_IDLE: IntGaugeVec = register_int_gauge_vec!(
        "greptime_runtime_threads_idle",
        "runtime threads idle",
        &[THREAD_NAME_LABEL]
    )
    .unwrap();
}

#[derive(Clone, Default)]
struct ClassSnapshot {
    tasks: u64,
    wakes: u64,
    polls: u64,
    completed: u64,
    cancelled: u64,
    total_admission_wait: Duration,
    total_exec_time: Duration,
}

struct WorkloadSchedulerCollector {
    scheduler: Scheduler,
    enabled: IntGauge,
    active: IntGauge,
    weight: IntGaugeVec,
    queued: IntGaugeVec,
    tasks: IntCounterVec,
    wakes: IntCounterVec,
    polls: IntCounterVec,
    completed: IntCounterVec,
    cancelled: IntCounterVec,
    total_admission_wait: CounterVec,
    total_exec_time: CounterVec,
    snapshots: Mutex<BTreeMap<&'static str, ClassSnapshot>>,
}

impl WorkloadSchedulerCollector {
    fn new(scheduler: Scheduler) -> Self {
        let workload_label = &["workload"];
        Self {
            scheduler,
            enabled: IntGauge::new(
                "greptime_workload_scheduler_enabled",
                "Whether the workload scheduler is enabled",
            )
            .unwrap(),
            active: IntGauge::new(
                "greptime_workload_scheduler_active_polls",
                "Task polls admitted to Tokio but not yet completed",
            )
            .unwrap(),
            weight: IntGaugeVec::new(
                Opts::new(
                    "greptime_workload_scheduler_weight",
                    "Configured workload scheduler weight",
                ),
                workload_label,
            )
            .unwrap(),
            queued: IntGaugeVec::new(
                Opts::new(
                    "greptime_workload_scheduler_queued_tasks",
                    "Tasks queued in the workload scheduler",
                ),
                workload_label,
            )
            .unwrap(),
            tasks: IntCounterVec::new(
                Opts::new(
                    "greptime_workload_scheduler_tasks_total",
                    "Futures wrapped by the workload scheduler",
                ),
                workload_label,
            )
            .unwrap(),
            wakes: IntCounterVec::new(
                Opts::new(
                    "greptime_workload_scheduler_wakes_total",
                    "Proxy-waker calls received by the workload scheduler",
                ),
                workload_label,
            )
            .unwrap(),
            polls: IntCounterVec::new(
                Opts::new(
                    "greptime_workload_scheduler_polls_total",
                    "Cumulative task polls admitted by the workload scheduler",
                ),
                workload_label,
            )
            .unwrap(),
            completed: IntCounterVec::new(
                Opts::new(
                    "greptime_workload_scheduler_completed_total",
                    "Futures completed by the workload scheduler",
                ),
                workload_label,
            )
            .unwrap(),
            cancelled: IntCounterVec::new(
                Opts::new(
                    "greptime_workload_scheduler_cancelled_total",
                    "Futures cancelled by the workload scheduler",
                ),
                workload_label,
            )
            .unwrap(),
            total_admission_wait: CounterVec::new(
                Opts::new(
                    "greptime_workload_scheduler_admission_wait_seconds_total",
                    "Cumulative workload scheduler admission wait time in seconds",
                ),
                workload_label,
            )
            .unwrap(),
            total_exec_time: CounterVec::new(
                Opts::new(
                    "greptime_workload_scheduler_exec_duration_seconds_total",
                    "Cumulative workload scheduler execution time in seconds",
                ),
                workload_label,
            )
            .unwrap(),
            snapshots: Mutex::new(BTreeMap::new()),
        }
    }

    fn update_locked(&self, snapshots: &mut BTreeMap<&'static str, ClassSnapshot>) {
        let stats = self.scheduler.stats();
        self.enabled.set(i64::from(self.scheduler.is_enabled()));
        self.active.set(
            stats
                .active_polls
                .min(i64::MAX as usize)
                .try_into()
                .unwrap_or(i64::MAX),
        );

        for (class, workload) in [(QUERY_TASK_CLASS, "query"), (WRITE_TASK_CLASS, "write")] {
            let class_stats = stats.classes.get(&class).cloned().unwrap_or_default();
            let labels = &[workload];
            self.weight
                .with_label_values(labels)
                .set(i64::from(class_stats.weight));
            self.queued.with_label_values(labels).set(
                class_stats
                    .queued
                    .min(i64::MAX as usize)
                    .try_into()
                    .unwrap_or(i64::MAX),
            );
            let previous = snapshots.entry(workload).or_default();
            self.tasks
                .with_label_values(labels)
                .inc_by(class_stats.tasks.saturating_sub(previous.tasks));
            self.wakes
                .with_label_values(labels)
                .inc_by(class_stats.wakes.saturating_sub(previous.wakes));
            self.polls
                .with_label_values(labels)
                .inc_by(class_stats.polls.saturating_sub(previous.polls));
            self.completed
                .with_label_values(labels)
                .inc_by(class_stats.completed.saturating_sub(previous.completed));
            self.cancelled
                .with_label_values(labels)
                .inc_by(class_stats.cancelled.saturating_sub(previous.cancelled));
            self.total_admission_wait.with_label_values(labels).inc_by(
                class_stats
                    .total_admission_wait
                    .saturating_sub(previous.total_admission_wait)
                    .as_secs_f64(),
            );
            self.total_exec_time.with_label_values(labels).inc_by(
                class_stats
                    .total_exec_time
                    .saturating_sub(previous.total_exec_time)
                    .as_secs_f64(),
            );
            *previous = ClassSnapshot {
                tasks: class_stats.tasks,
                wakes: class_stats.wakes,
                polls: class_stats.polls,
                completed: class_stats.completed,
                cancelled: class_stats.cancelled,
                total_admission_wait: class_stats.total_admission_wait,
                total_exec_time: class_stats.total_exec_time,
            };
        }
    }
}

impl Collector for WorkloadSchedulerCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut desc = self.enabled.desc();
        desc.extend(self.active.desc());
        desc.extend(self.weight.desc());
        desc.extend(self.queued.desc());
        desc.extend(self.tasks.desc());
        desc.extend(self.wakes.desc());
        desc.extend(self.polls.desc());
        desc.extend(self.completed.desc());
        desc.extend(self.cancelled.desc());
        desc.extend(self.total_admission_wait.desc());
        desc.extend(self.total_exec_time.desc());
        desc
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut snapshots = self.snapshots.lock().unwrap();
        self.update_locked(&mut snapshots);
        let mut families = self.enabled.collect();
        families.extend(self.active.collect());
        families.extend(self.weight.collect());
        families.extend(self.queued.collect());
        families.extend(self.tasks.collect());
        families.extend(self.wakes.collect());
        families.extend(self.polls.collect());
        families.extend(self.completed.collect());
        families.extend(self.cancelled.collect());
        families.extend(self.total_admission_wait.collect());
        families.extend(self.total_exec_time.collect());
        families
    }
}

pub(crate) fn register_workload_scheduler_metrics(scheduler: Scheduler) {
    register(Box::new(WorkloadSchedulerCollector::new(scheduler)))
        .expect("workload scheduler metrics collector registration must succeed");
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use prometheus::proto::{MetricFamily, MetricType};

    use super::*;

    fn family<'a>(families: &'a [MetricFamily], name: &str) -> &'a MetricFamily {
        families
            .iter()
            .find(|family| family.name() == name)
            .unwrap_or_else(|| panic!("missing metric family {name}"))
    }

    fn counter_values(families: &[MetricFamily]) -> BTreeMap<String, BTreeMap<String, f64>> {
        [
            "greptime_workload_scheduler_tasks_total",
            "greptime_workload_scheduler_wakes_total",
            "greptime_workload_scheduler_polls_total",
            "greptime_workload_scheduler_completed_total",
            "greptime_workload_scheduler_cancelled_total",
            "greptime_workload_scheduler_admission_wait_seconds_total",
            "greptime_workload_scheduler_exec_duration_seconds_total",
        ]
        .into_iter()
        .map(|name| {
            let values = family(families, name)
                .get_metric()
                .iter()
                .map(|metric| {
                    (
                        metric.get_label()[0].value().to_string(),
                        metric.get_counter().value(),
                    )
                })
                .collect();
            (name.to_string(), values)
        })
        .collect()
    }

    #[test]
    fn workload_scheduler_collector_reports_class_metrics_and_deltas() {
        let scheduler = Scheduler::builder()
            .max_concurrent_polls(1)
            .weight(QUERY_TASK_CLASS, 2)
            .weight(WRITE_TASK_CLASS, 3)
            .build();
        scheduler.set_enabled(true);
        let collector = WorkloadSchedulerCollector::new(scheduler.clone());

        let first = collector.collect();
        let expected = [
            (
                "greptime_workload_scheduler_enabled",
                MetricType::GAUGE,
                false,
            ),
            (
                "greptime_workload_scheduler_active_polls",
                MetricType::GAUGE,
                false,
            ),
            (
                "greptime_workload_scheduler_weight",
                MetricType::GAUGE,
                true,
            ),
            (
                "greptime_workload_scheduler_queued_tasks",
                MetricType::GAUGE,
                true,
            ),
            (
                "greptime_workload_scheduler_tasks_total",
                MetricType::COUNTER,
                true,
            ),
            (
                "greptime_workload_scheduler_wakes_total",
                MetricType::COUNTER,
                true,
            ),
            (
                "greptime_workload_scheduler_polls_total",
                MetricType::COUNTER,
                true,
            ),
            (
                "greptime_workload_scheduler_completed_total",
                MetricType::COUNTER,
                true,
            ),
            (
                "greptime_workload_scheduler_cancelled_total",
                MetricType::COUNTER,
                true,
            ),
            (
                "greptime_workload_scheduler_admission_wait_seconds_total",
                MetricType::COUNTER,
                true,
            ),
            (
                "greptime_workload_scheduler_exec_duration_seconds_total",
                MetricType::COUNTER,
                true,
            ),
        ];
        let expected_names: BTreeSet<_> = expected.iter().map(|(name, _, _)| *name).collect();
        let actual_names: BTreeSet<_> = first.iter().map(MetricFamily::name).collect();
        assert_eq!(expected_names, actual_names);
        for (name, metric_type, has_workload_label) in expected {
            let metric_family = family(&first, name);
            assert_eq!(metric_type, metric_family.get_field_type(), "{name}");
            let workloads: BTreeSet<_> = metric_family
                .get_metric()
                .iter()
                .flat_map(|metric| {
                    assert_eq!(has_workload_label, !metric.get_label().is_empty());
                    metric
                        .get_label()
                        .iter()
                        .map(|label| {
                            assert_eq!("workload", label.name());
                            label.value()
                        })
                        .collect::<Vec<_>>()
                })
                .collect();
            if has_workload_label {
                assert_eq!(BTreeSet::from(["query", "write"]), workloads);
            } else {
                assert!(workloads.is_empty());
            }
        }

        let second = collector.collect();
        assert_eq!(counter_values(&first), counter_values(&second));

        scheduler.set_enabled(true);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let query = scheduler.spawn_in(QUERY_TASK_CLASS, async {
                tokio::time::sleep(Duration::from_millis(1)).await;
            });
            let write = scheduler.spawn_in(WRITE_TASK_CLASS, async {
                tokio::time::sleep(Duration::from_millis(1)).await;
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                query.await.unwrap();
                write.await.unwrap();
            })
            .await
            .expect("scheduled test tasks did not complete");
        });

        let third = collector.collect();
        let before = counter_values(&second);
        let after = counter_values(&third);
        for workload in ["query", "write"] {
            for metric in [
                "greptime_workload_scheduler_tasks_total",
                "greptime_workload_scheduler_polls_total",
                "greptime_workload_scheduler_exec_duration_seconds_total",
            ] {
                assert!(
                    after[metric][workload] > before[metric][workload],
                    "{metric} did not increase for {workload}"
                );
            }
        }
    }
}
