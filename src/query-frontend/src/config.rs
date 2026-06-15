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

//! Query frontend configuration.
//!
//! The default is fully disabled: constructing [`QueryFrontendConfig::default`]
//! yields a no-op frontend so that merely wiring this crate in does not change
//! runtime behavior. Enabling features is opt-in for later iterations.

use serde::{Deserialize, Serialize};

/// Configuration for the query frontend.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryFrontendConfig {
    /// Master switch for the query frontend. Disabled by default; when `false`
    /// the frontend must behave as a pass-through.
    pub enable: bool,
}

impl QueryFrontendConfig {
    /// Returns whether the query frontend is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_disabled() {
        let config = QueryFrontendConfig::default();
        assert!(!config.enable);
        assert!(!config.is_enabled());
    }

    #[test]
    fn empty_config_deserializes_to_disabled_default() {
        // An empty object exercises the `#[serde(default)]` path and must yield
        // the disabled default.
        let config: QueryFrontendConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(QueryFrontendConfig::default(), config);
        assert!(!config.is_enabled());
    }

    #[test]
    fn enable_round_trips() {
        let config = QueryFrontendConfig { enable: true };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: QueryFrontendConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
        assert!(parsed.is_enabled());
    }
}
