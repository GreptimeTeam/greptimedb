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

//! Configurations.

const DEFAULT_NUM_WORKERS: usize = 1;
const MAX_NUM_WORKERS: usize = 512;

/// Configuration for [MitoEngine](crate::engine::MitoEngine).
#[derive(Debug)]
pub struct MitoConfig {
    /// Number of region workers.
    pub num_workers: usize,
}

impl Default for MitoConfig {
    fn default() -> Self {
        MitoConfig {
            num_workers: DEFAULT_NUM_WORKERS,
        }
    }
}

impl MitoConfig {
    /// Sanitize incorrect configurations.
    pub(crate) fn sanitize(&mut self) {
        if self.num_workers == 0 {
            self.num_workers = DEFAULT_NUM_WORKERS;
        } else if self.num_workers > MAX_NUM_WORKERS {
            self.num_workers = MAX_NUM_WORKERS;
        }
        self.num_workers = self.num_workers.next_power_of_two();
    }
}
