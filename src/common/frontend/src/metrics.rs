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
//! Both outcomes for all known caches are exported from the first metric update.

use lazy_static::lazy_static;
use prometheus::{IntCounterVec, register_int_counter_vec};

const CACHE_TYPES: [&str; 7] = [
    "otlp_trace_aux",
    "otlp_metrics_legacy",
    "pipeline_table",
    "pipeline",
    "pipeline_original",
    "pipeline_failover",
    "mysql_prepared_stmt",
];

lazy_static! {
    static ref CACHE_COUNTERS: (IntCounterVec, IntCounterVec) = {
        let hits = register_int_counter_vec!(
            "greptime_frontend_cache_hit",
            "frontend cache hit",
            &["type"]
        )
        .unwrap();
        let misses = register_int_counter_vec!(
            "greptime_frontend_cache_miss",
            "frontend cache miss",
            &["type"]
        )
        .unwrap();
        // Register both outcomes together so an all-hit or all-miss cache has both series.
        for cache_type in CACHE_TYPES {
            hits.with_label_values(&[cache_type]);
            misses.with_label_values(&[cache_type]);
        }
        (hits, misses)
    };

    /// Frontend cache hits by cache type.
    pub static ref CACHE_HIT: IntCounterVec = CACHE_COUNTERS.0.clone();
    /// Frontend cache misses by cache type.
    pub static ref CACHE_MISS: IntCounterVec = CACHE_COUNTERS.1.clone();
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
    use super::*;

    #[test]
    fn test_cache_lookup_counters() {
        record_cache_lookup("pipeline", false);
        // Scrape before accessing CACHE_HIT, which would initialize it and hide a missing series.
        let families = prometheus::gather();
        for (name, expected) in [
            ("greptime_frontend_cache_hit", 0.0),
            ("greptime_frontend_cache_miss", 1.0),
        ] {
            let family = families
                .iter()
                .find(|family| family.name() == name)
                .unwrap();
            assert_eq!(family.get_metric().len(), CACHE_TYPES.len());
            for cache_type in CACHE_TYPES {
                let metric = family
                    .get_metric()
                    .iter()
                    .find(|metric| metric.get_label()[0].value() == cache_type)
                    .unwrap();
                assert_eq!(metric.get_label()[0].name(), "type");
                assert_eq!(
                    metric.get_counter().value(),
                    if cache_type == "pipeline" {
                        expected
                    } else {
                        0.0
                    }
                );
            }
        }
        record_cache_lookup("pipeline", true);
        record_cache_lookup("pipeline", false);
        assert_eq!(CACHE_HIT.with_label_values(&["pipeline"]).get(), 1);
        assert_eq!(CACHE_MISS.with_label_values(&["pipeline"]).get(), 2);
    }
}
