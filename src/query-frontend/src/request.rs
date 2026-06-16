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

//! Protocol-neutral request DTO consumed by the query frontend.
//!
//! Fields are intentionally kept as opaque strings so this crate does not
//! depend on `servers`, `frontend`, `query`, or `session` types. Callers are
//! responsible for translating their protocol-specific representations (for
//! example, a PromQL range query plus its session context) into these strings
//! verbatim. The frontend does not normalize them.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::key::QueryKey;

/// A single query handed to the frontend.
///
/// The fields mirror the inputs that uniquely identify a PromQL range query
/// today, but the types are protocol-neutral strings so the same DTO can carry
/// other query shapes later.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryFrontendRequest {
    /// Target database/schema name.
    pub db: String,
    /// Read preference (e.g. `LEADER`, `FOLLOWER`), as an opaque string.
    pub read_preference: String,
    /// The raw query expression.
    pub query: String,
    /// Range start, verbatim from the caller.
    pub start: String,
    /// Range end, verbatim from the caller.
    pub end: String,
    /// Range step, verbatim from the caller.
    pub step: String,
    /// Lookback delta, verbatim from the caller.
    pub lookback: String,
}

impl QueryFrontendRequest {
    /// Returns the exact identity [`QueryKey`] for this request.
    pub fn key(&self) -> QueryKey {
        QueryKey::from(self)
    }

    /// Converts this request into its exact identity [`QueryKey`].
    pub fn into_key(self) -> QueryKey {
        QueryKey::from(self)
    }
}

impl fmt::Debug for QueryFrontendRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryFrontendRequest")
            .field("db", &self.db)
            .field("read_preference", &self.read_preference)
            .field("query", &"[REDACTED]")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("step", &self.step)
            .field("lookback", &self.lookback)
            .finish()
    }
}

#[cfg(test)]
pub(crate) fn test_request() -> QueryFrontendRequest {
    QueryFrontendRequest {
        db: "db".to_string(),
        read_preference: "LEADER".to_string(),
        query: "up".to_string(),
        start: "1".to_string(),
        end: "2".to_string(),
        step: "5s".to_string(),
        lookback: "5m".to_string(),
    }
}
