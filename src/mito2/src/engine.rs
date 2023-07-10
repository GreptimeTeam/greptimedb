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

use std::sync::Arc;

use crate::config::MitoConfig;
use crate::worker::WorkerGroup;

/// Region engine implementation for timeseries data.
#[derive(Clone)]
pub struct MitoEngine {
    inner: Arc<EngineInner>,
}

impl MitoEngine {
    /// Returns a new [MitoEngine] with specific `config`.
    pub fn new(config: MitoConfig) -> MitoEngine {
        MitoEngine {
            inner: Arc::new(EngineInner::new(config)),
        }
    }
}

/// Inner struct of [MitoEngine].
struct EngineInner {
    /// Region workers group.
    workers: WorkerGroup,
}

impl EngineInner {
    /// Returns a new [EngineInner] with specific `config`.
    fn new(_config: MitoConfig) -> EngineInner {
        EngineInner {
            workers: WorkerGroup::default(),
        }
    }
}
