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

use flow::FlownodeServer;
use tracing_appender::non_blocking::WorkerGuard;

use crate::options::GreptimeOptions;

pub const APP_NAME: &str = "greptime-flownode";

type FlownodeOptions = GreptimeOptions<flow::FlownodeOptions>;

pub struct Instance {
    flownode: FlownodeServer,

    // Keep the logging guard to prevent the worker from being dropped.
    _guard: Vec<WorkerGuard>,
}

impl Instance {
    pub fn new(flownode: FlownodeServer, guard: Vec<WorkerGuard>) -> Self {
        Self {
            flownode,
            _guard: guard,
        }
    }

    pub fn flownode_mut(&mut self) -> &mut FlownodeServer {
        &mut self.flownode
    }

    pub fn flownode(&self) -> &FlownodeServer {
        &self.flownode
    }
}
