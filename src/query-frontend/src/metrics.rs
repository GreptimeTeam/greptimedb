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

//! Stable metric and label names for query frontend observability.
//!
//! This module only declares the naming surface; it does not register or own
//! any metric instruments. Keeping the names here lets the eventual collector
//! and any observe-only call sites agree on identifiers. Per the key contract,
//! only the database label is safe to emit — never the query text.

/// Metric name for query frontend observations.
pub const METRIC_QUERY_FRONTEND_OBSERVATIONS: &str = "greptime_query_frontend_observations_total";

/// Label carrying the target database name.
pub const LABEL_DB: &str = "db";

/// Label carrying the per-request outcome (see the `OUTCOME_*` constants).
pub const LABEL_OUTCOME: &str = "outcome";

/// Outcome label value: request bypassed the frontend.
pub const OUTCOME_BYPASS: &str = "bypass";

/// Outcome label value: request was handled by the frontend.
pub const OUTCOME_ENGAGE: &str = "engage";
