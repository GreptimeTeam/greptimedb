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

//! storage engine config

use std::time::Duration;

use common_base::readable_size::ReadableSize;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub manifest_checkpoint_on_startup: bool,
    pub manifest_checkpoint_margin: Option<u16>,
    pub manifest_gc_duration: Option<Duration>,
    pub max_files_in_l0: usize,
    pub max_purge_tasks: usize,
    pub sst_write_buffer_size: ReadableSize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            manifest_checkpoint_on_startup: false,
            manifest_checkpoint_margin: Some(10),
            manifest_gc_duration: Some(Duration::from_secs(30)),
            max_files_in_l0: 8,
            max_purge_tasks: 32,
            sst_write_buffer_size: ReadableSize::mb(8),
        }
    }
}
