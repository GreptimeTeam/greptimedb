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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct MemoryOptions {
    /// Whether to enable heap profiling activation.
    /// When true, heap profiling will be activated during startup.
    /// Default is true.
    pub enable_heap_profiling: bool,
}

impl Default for MemoryOptions {
    fn default() -> Self {
        Self {
            enable_heap_profiling: true,
        }
    }
}
