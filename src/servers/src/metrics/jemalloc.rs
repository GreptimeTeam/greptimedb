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

mod error;

use common_telemetry::error;
use error::UpdateJemallocMetricsSnafu;
use metrics::gauge;
use once_cell::sync::Lazy;
use snafu::ResultExt;
use tikv_jemalloc_ctl::stats::{allocated_mib, resident_mib};
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats};

pub(crate) const METRIC_JEMALLOC_RESIDENT: &str = "sys.jemalloc.resident";
pub(crate) const METRIC_JEMALLOC_ALLOCATED: &str = "sys.jemalloc.allocated";

pub(crate) static JEMALLOC_COLLECTOR: Lazy<Option<JemallocCollector>> = Lazy::new(|| {
    let collector = JemallocCollector::try_new()
        .map_err(|e| {
            error!(e; "Failed to retrieve jemalloc metrics");
            e
        })
        .ok();
    collector.map(|c| {
        if let Err(e) = c.update() {
            error!(e; "Failed to update jemalloc metrics");
        };
        c
    })
});

pub(crate) struct JemallocCollector {
    epoch: epoch_mib,
    allocated: allocated_mib,
    resident: resident_mib,
}

impl JemallocCollector {
    pub(crate) fn try_new() -> crate::error::Result<Self> {
        let e = epoch::mib().context(UpdateJemallocMetricsSnafu)?;
        let allocated = stats::allocated::mib().context(UpdateJemallocMetricsSnafu)?;
        let resident = stats::resident::mib().context(UpdateJemallocMetricsSnafu)?;
        Ok(Self {
            epoch: e,
            allocated,
            resident,
        })
    }

    pub(crate) fn update(&self) -> crate::error::Result<()> {
        let _ = self.epoch.advance().context(UpdateJemallocMetricsSnafu)?;
        let allocated = self.allocated.read().context(UpdateJemallocMetricsSnafu)?;
        let resident = self.resident.read().context(UpdateJemallocMetricsSnafu)?;
        gauge!(METRIC_JEMALLOC_ALLOCATED, allocated as f64);
        gauge!(METRIC_JEMALLOC_RESIDENT, resident as f64);
        Ok(())
    }
}
