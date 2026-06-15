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

//! Observe-only PromQL HTTP query frontend instrumentation.
//!
//! This module intentionally does not cache, coalesce, normalize, or replay
//! query responses. It only observes identical in-flight `query_range` work so
//! a future query frontend can be built without changing current behavior.

use std::collections::HashMap;

use lazy_static::lazy_static;
use parking_lot::Mutex;
use query::parser::PromQuery;

const OUTCOME_FIRST_IN_FLIGHT: &str = "first_in_flight";
const OUTCOME_DUPLICATE_IN_FLIGHT: &str = "duplicate_in_flight";
const OUTCOME_COMPLETED: &str = "completed";
const OUTCOME_ERRORED: &str = "errored";
const OUTCOME_CANCELLED: &str = "cancelled";

lazy_static! {
    static ref IN_FLIGHT_REGISTRY: Mutex<InFlightRegistry> =
        Mutex::new(InFlightRegistry::default());
}

/// Exact observe-only key for PromQL range queries.
///
/// The full query is kept only in memory for equality/collision checks. Metrics
/// and logs must not include it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct QueryFrontendKey {
    db: String,
    read_preference: String,
    query: String,
    start: String,
    end: String,
    step: String,
    lookback: String,
}

impl QueryFrontendKey {
    pub(crate) fn new(db: String, read_preference: String, prom_query: &PromQuery) -> Self {
        Self {
            db,
            read_preference,
            query: prom_query.query.clone(),
            start: prom_query.start.clone(),
            end: prom_query.end.clone(),
            step: prom_query.step.clone(),
            lookback: prom_query.lookback.clone(),
        }
    }

    #[cfg(test)]
    fn for_test(
        db: &str,
        read_preference: &str,
        query: &str,
        start: &str,
        end: &str,
        step: &str,
        lookback: &str,
    ) -> Self {
        Self {
            db: db.to_string(),
            read_preference: read_preference.to_string(),
            query: query.to_string(),
            start: start.to_string(),
            end: end.to_string(),
            step: step.to_string(),
            lookback: lookback.to_string(),
        }
    }

    fn db(&self) -> &str {
        &self.db
    }
}

#[derive(Default)]
struct InFlightRegistry {
    in_flight: HashMap<QueryFrontendKey, usize>,
}

impl InFlightRegistry {
    fn enter(&mut self, key: QueryFrontendKey) -> bool {
        if let Some(count) = self.in_flight.get_mut(&key) {
            *count += 1;
            true
        } else {
            self.in_flight.insert(key, 1);
            false
        }
    }

    fn exit(&mut self, key: &QueryFrontendKey) {
        if let Some(count) = self.in_flight.get_mut(key) {
            if *count > 1 {
                *count -= 1;
            } else {
                self.in_flight.remove(key);
            }
        }
    }

    #[cfg(test)]
    fn active_count(&self, key: &QueryFrontendKey) -> usize {
        self.in_flight.get(key).copied().unwrap_or(0)
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }
}

/// RAII guard for an observed in-flight range query.
///
/// Dropping the guard removes its entry even if later response conversion exits
/// early. The guard does not affect execution of the query.
pub(crate) struct InFlightGuard {
    key: QueryFrontendKey,
    completed: bool,
    errored: bool,
}

impl InFlightGuard {
    pub(crate) fn mark_completed(&mut self) {
        self.completed = true;
    }

    pub(crate) fn mark_errored(&mut self) {
        self.errored = true;
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        IN_FLIGHT_REGISTRY.lock().exit(&self.key);
        let outcome = if self.errored {
            OUTCOME_ERRORED
        } else if self.completed {
            OUTCOME_COMPLETED
        } else {
            OUTCOME_CANCELLED
        };
        observe(self.key.db(), outcome);
    }
}

pub(crate) fn observe_range_query(key: QueryFrontendKey) -> InFlightGuard {
    let duplicate = IN_FLIGHT_REGISTRY.lock().enter(key.clone());
    let outcome = if duplicate {
        OUTCOME_DUPLICATE_IN_FLIGHT
    } else {
        OUTCOME_FIRST_IN_FLIGHT
    };
    observe(key.db(), outcome);
    InFlightGuard {
        key,
        completed: false,
        errored: false,
    }
}

fn observe(db: &str, outcome: &str) {
    crate::metrics::METRIC_HTTP_PROMETHEUS_QUERY_FRONTEND_OBSERVATIONS
        .with_label_values(&[db, outcome])
        .inc();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_equality_and_difference_are_exact() {
        let base = QueryFrontendKey::for_test("db", "LEADER", "up", "1", "2", "5s", "5m");
        assert_eq!(
            base,
            QueryFrontendKey::for_test("db", "LEADER", "up", "1", "2", "5s", "5m")
        );
        assert_ne!(
            base,
            QueryFrontendKey::for_test("other", "LEADER", "up", "1", "2", "5s", "5m")
        );
        assert_ne!(
            base,
            QueryFrontendKey::for_test("db", "FOLLOWER", "up", "1", "2", "5s", "5m")
        );
        assert_ne!(
            base,
            QueryFrontendKey::for_test("db", "LEADER", "rate(up[5m])", "1", "2", "5s", "5m")
        );
        assert_ne!(
            base,
            QueryFrontendKey::for_test("db", "LEADER", "up", "0", "2", "5s", "5m")
        );
        assert_ne!(
            base,
            QueryFrontendKey::for_test("db", "LEADER", "up", "1", "3", "5s", "5m")
        );
        assert_ne!(
            base,
            QueryFrontendKey::for_test("db", "LEADER", "up", "1", "2", "10s", "5m")
        );
        assert_ne!(
            base,
            QueryFrontendKey::for_test("db", "LEADER", "up", "1", "2", "5s", "1m")
        );
    }

    #[test]
    fn registry_tracks_duplicate_enter_and_exit() {
        let key = QueryFrontendKey::for_test("db", "LEADER", "up", "1", "2", "5s", "5m");
        let mut registry = InFlightRegistry::default();

        assert!(!registry.enter(key.clone()));
        assert_eq!(1, registry.active_count(&key));

        assert!(registry.enter(key.clone()));
        assert_eq!(2, registry.active_count(&key));

        registry.exit(&key);
        assert_eq!(1, registry.active_count(&key));

        registry.exit(&key);
        assert_eq!(0, registry.active_count(&key));
        assert!(registry.is_empty());
    }

    #[test]
    fn guard_removes_entry_on_drop() {
        let key = QueryFrontendKey::for_test("guard_db", "LEADER", "up", "1", "2", "5s", "5m");
        {
            let _guard = observe_range_query(key.clone());
            assert_eq!(1, IN_FLIGHT_REGISTRY.lock().active_count(&key));
        }
        assert_eq!(0, IN_FLIGHT_REGISTRY.lock().active_count(&key));
    }

    #[test]
    fn guard_removes_entry_after_mark_errored() {
        let key =
            QueryFrontendKey::for_test("guard_error_db", "LEADER", "up", "1", "2", "5s", "5m");
        {
            let mut guard = observe_range_query(key.clone());
            guard.mark_errored();
            assert_eq!(1, IN_FLIGHT_REGISTRY.lock().active_count(&key));
        }
        assert_eq!(0, IN_FLIGHT_REGISTRY.lock().active_count(&key));
    }

    #[test]
    fn duplicate_guards_exit_independently() {
        let key =
            QueryFrontendKey::for_test("duplicate_guard_db", "LEADER", "up", "1", "2", "5s", "5m");
        let first_guard = observe_range_query(key.clone());
        let second_guard = observe_range_query(key.clone());
        assert_eq!(2, IN_FLIGHT_REGISTRY.lock().active_count(&key));

        drop(first_guard);
        assert_eq!(1, IN_FLIGHT_REGISTRY.lock().active_count(&key));

        drop(second_guard);
        assert_eq!(0, IN_FLIGHT_REGISTRY.lock().active_count(&key));
    }
}
