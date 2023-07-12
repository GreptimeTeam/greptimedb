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

//! Common traits and structures for the procedure framework.

use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ProcedureConfig {
    /// Max retry times of procedure.
    pub max_retry_times: usize,
    /// Initial retry delay of procedures, increases exponentially.
    #[serde(with = "humantime_serde")]
    pub retry_delay: Duration,
}

impl Default for ProcedureConfig {
    fn default() -> ProcedureConfig {
        ProcedureConfig {
            max_retry_times: 3,
            retry_delay: Duration::from_millis(500),
        }
    }
}
