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

use serde::{Deserialize, Serialize};

/// Options for Jaeger query APIs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JaegerOptions {
    /// Whether to enable Jaeger query APIs.
    pub enable: bool,
}

impl Default for JaegerOptions {
    fn default() -> Self {
        Self { enable: true }
    }
}

#[cfg(test)]
mod tests {
    use super::JaegerOptions;

    #[test]
    fn test_jaeger_options() {
        let default = JaegerOptions::default();
        assert!(default.enable);
    }
}
