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

//! Cache lookup counters shared by frontend, pipeline, and protocol handlers.
//!
//! The `type` label identifies `otlp_trace_aux`, `otlp_metrics_legacy`,
//! `pipeline_table`, `pipeline`, `pipeline_original`, `pipeline_failover`, or
//! `mysql_prepared_stmt`. Counters aggregate across catalogs and connections.
//! Trace lookups count distinct service/operation keys once per request, excluding
//! rechecks; legacy metric lookups count each requested metric name. Pipeline
//! loaders count successful coalesced waiters as hits and failed loads as misses.
//! Failover counts one outcome after resolving all candidate schemas; an ambiguous
//! result is a miss. Table and prepared-statement caches count each lookup.

use lazy_static::lazy_static;
use prometheus::{IntCounterVec, register_int_counter_vec};

lazy_static! {
    /// Frontend cache hits by cache type.
    pub static ref CACHE_HIT: IntCounterVec = register_int_counter_vec!(
        "greptime_frontend_cache_hit",
        "frontend cache hit",
        &["type"]
    )
    .unwrap();
    /// Frontend cache misses by cache type.
    pub static ref CACHE_MISS: IntCounterVec = register_int_counter_vec!(
        "greptime_frontend_cache_miss",
        "frontend cache miss",
        &["type"]
    )
    .unwrap();
}

/// Records one completed cache lookup.
pub fn record_cache_lookup(cache_type: &str, hit: bool) {
    if hit {
        CACHE_HIT.with_label_values(&[cache_type]).inc();
    } else {
        CACHE_MISS.with_label_values(&[cache_type]).inc();
    }
}

#[cfg(test)]
mod tests {
    use prometheus::core::Collector;

    use super::*;

    #[test]
    fn test_cache_lookup_counters() {
        record_cache_lookup("test", true);
        record_cache_lookup("test", false);
        record_cache_lookup("test", false);
        for (counter, name, expected) in [
            (&*CACHE_HIT, "greptime_frontend_cache_hit", 1),
            (&*CACHE_MISS, "greptime_frontend_cache_miss", 2),
        ] {
            assert_eq!(counter.with_label_values(&["test"]).get(), expected);
            let families = counter.collect();
            assert_eq!(families[0].name(), name);
            assert_eq!(families[0].get_metric()[0].get_label()[0].name(), "type");
        }
    }
}
