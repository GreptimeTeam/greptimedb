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

//! Exact identity key for query frontend requests.
//!
//! The key compares two requests for *exact* equality across every field. No
//! normalization (timestamps, query text, steps) is performed: two requests are
//! the same only if all of `db`, `read_preference`, `query`, `start`, `end`,
//! `step`, and `lookback` are byte-for-byte identical.
//!
//! The query text is retained only in memory for equality and hashing. It must
//! never be emitted in metrics or logs.

use std::fmt;

use crate::request::QueryFrontendRequest;

/// Exact identity key derived from a [`QueryFrontendRequest`].
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryKey {
    db: String,
    read_preference: String,
    query: String,
    start: String,
    end: String,
    step: String,
    lookback: String,
}

impl QueryKey {
    /// Returns the target database, the only field safe to attach to telemetry.
    pub fn db(&self) -> &str {
        &self.db
    }
}

impl fmt::Debug for QueryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryKey")
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

impl From<&QueryFrontendRequest> for QueryKey {
    fn from(request: &QueryFrontendRequest) -> Self {
        Self {
            db: request.db.clone(),
            read_preference: request.read_preference.clone(),
            query: request.query.clone(),
            start: request.start.clone(),
            end: request.end.clone(),
            step: request.step.clone(),
            lookback: request.lookback.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::test_request;

    fn base() -> QueryKey {
        test_request().key()
    }

    #[test]
    fn equal_keys_match() {
        assert_eq!(base(), test_request().key());
    }

    #[test]
    fn each_field_distinguishes_the_key() {
        let request = test_request();

        assert_ne!(
            base(),
            QueryFrontendRequest {
                db: "other".to_string(),
                ..request.clone()
            }
            .key(),
            "db must be part of the identity"
        );
        assert_ne!(
            base(),
            QueryFrontendRequest {
                read_preference: "FOLLOWER".to_string(),
                ..request.clone()
            }
            .key(),
            "read_preference must be part of the identity"
        );
        assert_ne!(
            base(),
            QueryFrontendRequest {
                query: "rate(up[5m])".to_string(),
                ..request.clone()
            }
            .key(),
            "query must be part of the identity"
        );
        assert_ne!(
            base(),
            QueryFrontendRequest {
                start: "0".to_string(),
                ..request.clone()
            }
            .key(),
            "start must be part of the identity"
        );
        assert_ne!(
            base(),
            QueryFrontendRequest {
                end: "3".to_string(),
                ..request.clone()
            }
            .key(),
            "end must be part of the identity"
        );
        assert_ne!(
            base(),
            QueryFrontendRequest {
                step: "10s".to_string(),
                ..request.clone()
            }
            .key(),
            "step must be part of the identity"
        );
        assert_ne!(
            base(),
            QueryFrontendRequest {
                lookback: "1m".to_string(),
                ..request
            }
            .key(),
            "lookback must be part of the identity"
        );
    }

    #[test]
    fn key_from_request_keeps_every_field() {
        let request = test_request();
        assert_eq!(base(), request.key());
        assert_eq!("db", request.key().db());
    }

    #[test]
    fn debug_redacts_query_text() {
        let request = QueryFrontendRequest {
            query: "secret_query".to_string(),
            ..test_request()
        };
        let request_debug = format!("{request:?}");
        let key_debug = format!("{:?}", request.key());

        assert!(!request_debug.contains("secret_query"));
        assert!(request_debug.contains("[REDACTED]"));
        assert!(!key_debug.contains("secret_query"));
        assert!(key_debug.contains("[REDACTED]"));
    }
}
