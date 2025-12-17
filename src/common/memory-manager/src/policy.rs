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

use std::time::Duration;

use humantime::{format_duration, parse_duration};
use serde::{Deserialize, Serialize};

/// Default wait timeout for memory acquisition.
pub const DEFAULT_MEMORY_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Defines how to react when memory cannot be acquired immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnExhaustedPolicy {
    /// Wait until enough memory is released, bounded by timeout.
    Wait { timeout: Duration },

    /// Fail immediately if memory is not available.
    Fail,
}

impl Default for OnExhaustedPolicy {
    fn default() -> Self {
        OnExhaustedPolicy::Wait {
            timeout: DEFAULT_MEMORY_WAIT_TIMEOUT,
        }
    }
}

impl Serialize for OnExhaustedPolicy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let text = match self {
            OnExhaustedPolicy::Fail => "fail".to_string(),
            OnExhaustedPolicy::Wait { timeout } if *timeout == DEFAULT_MEMORY_WAIT_TIMEOUT => {
                "wait".to_string()
            }
            OnExhaustedPolicy::Wait { timeout } => format!("wait({})", format_duration(*timeout)),
        };
        serializer.serialize_str(&text)
    }
}

impl<'de> Deserialize<'de> for OnExhaustedPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let lower = raw.to_ascii_lowercase();

        // Accept both "skip" (legacy) and "fail".
        if lower == "skip" || lower == "fail" {
            return Ok(OnExhaustedPolicy::Fail);
        }
        if lower == "wait" {
            return Ok(OnExhaustedPolicy::default());
        }
        if lower.starts_with("wait(") && lower.ends_with(')') {
            let inner = &raw[5..raw.len() - 1];
            let timeout = parse_duration(inner).map_err(serde::de::Error::custom)?;
            return Ok(OnExhaustedPolicy::Wait { timeout });
        }

        Err(serde::de::Error::custom(format!(
            "invalid memory policy: {}, expected wait, wait(<duration>), fail",
            raw
        )))
    }
}
