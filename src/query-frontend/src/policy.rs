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

//! Decides whether the query frontend engages for a given request.
//!
//! A policy is the gate in front of any future optimization. The default
//! [`BypassPolicy`] always bypasses, so the frontend is a pass-through until a
//! real policy is introduced.

use crate::request::QueryFrontendRequest;

/// What the frontend should do with a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    /// Hand the request straight to the underlying executor; the frontend adds
    /// no behavior.
    Bypass,
    /// Let the frontend handle the request (coalescing, caching, etc. in a
    /// later iteration).
    Engage,
}

impl PolicyDecision {
    /// Returns `true` when the frontend should engage.
    pub fn is_engage(&self) -> bool {
        matches!(self, PolicyDecision::Engage)
    }
}

/// Decides, per request, whether the frontend engages.
pub trait QueryFrontendPolicy: Send + Sync {
    /// Returns the [`PolicyDecision`] for `request`.
    fn decide(&self, request: &QueryFrontendRequest) -> PolicyDecision;
}

/// Default policy that always bypasses, keeping runtime behavior unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct BypassPolicy;

impl QueryFrontendPolicy for BypassPolicy {
    fn decide(&self, _request: &QueryFrontendRequest) -> PolicyDecision {
        PolicyDecision::Bypass
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_always_bypasses() {
        let policy = BypassPolicy;
        let request = crate::request::test_request();
        assert_eq!(PolicyDecision::Bypass, policy.decide(&request));
        assert!(!policy.decide(&request).is_engage());
    }
}
