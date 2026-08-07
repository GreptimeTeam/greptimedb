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

#[derive(Clone)]
struct WorkloadSchedulerCollector {
    scheduler: Scheduler,
    polls: IntGaugeVec,
    queued: IntGaugeVec,
    active: IntGauge,
}

impl WorkloadSchedulerCollector {
    fn new(scheduler: Scheduler) -> Self {
        Self {
            scheduler,
            polls: IntGaugeVec::new(
                Opts::new(
                    "greptime_workload_scheduler_polls",
                    "Cumulative task polls admitted by the workload scheduler",
                ),
                &["workload"],
            )
            .unwrap(),
            queued: IntGaugeVec::new(
                Opts::new(
                    "greptime_workload_scheduler_queued_tasks",
                    "Tasks queued in the workload scheduler",
                ),
                &["workload"],
            )
            .unwrap(),
            active: IntGauge::new(
                "greptime_workload_scheduler_active_polls",
                "Task polls admitted to Tokio but not yet completed",
            )
            .unwrap(),
        }
    }

    fn update(&self) {
        let stats = self.scheduler.stats();
        for (class, workload) in [(QUERY_TASK_CLASS, "query"), (WRITE_TASK_CLASS, "write")] {
            let class_stats = stats.classes.get(&class).cloned().unwrap_or_default();
            self.polls
                .with_label_values(&[workload])
                .set(class_stats.polls.min(i64::MAX as u64) as i64);
            self.queued
                .with_label_values(&[workload])
                .set(class_stats.queued.min(i64::MAX as usize) as i64);
        }
        self.active
            .set(stats.active_polls.min(i64::MAX as usize) as i64);
    }
}

impl Collector for WorkloadSchedulerCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut desc = self.polls.desc();
        desc.extend(self.queued.desc());
        desc.extend(self.active.desc());
        desc
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.update();
        let mut families = self.polls.collect();
        families.extend(self.queued.collect());
        families.extend(self.active.collect());
        families
    }
}

pub(crate) fn register_workload_scheduler_metrics(scheduler: Scheduler) {
    let _ = register(Box::new(WorkloadSchedulerCollector::new(scheduler)));
}
