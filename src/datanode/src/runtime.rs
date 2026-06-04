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

use common_runtime::{Runtime, create_runtime};

use crate::config::DatanodeRuntimeOptions;

#[derive(Clone, Debug)]
pub struct DatanodeRuntimes {
    query_runtime: Runtime,
    ingest_runtime: Runtime,
}

impl DatanodeRuntimes {
    pub fn new(options: &DatanodeRuntimeOptions) -> Self {
        Self {
            query_runtime: create_runtime(
                "datanode-query",
                "datanode-query-worker",
                options.query_rt_size,
            ),
            ingest_runtime: create_runtime(
                "datanode-ingest",
                "datanode-ingest-worker",
                options.ingest_rt_size,
            ),
        }
    }

    pub fn query_runtime(&self) -> Runtime {
        self.query_runtime.clone()
    }

    pub fn ingest_runtime(&self) -> Runtime {
        self.ingest_runtime.clone()
    }
}

#[cfg(test)]
mod tests {
    use common_runtime::runtime::RuntimeTrait;

    use super::*;
    use crate::config::DatanodeRuntimeOptions;

    #[test]
    fn test_query_runtime_spawn_uses_query_thread_name() {
        let runtimes = DatanodeRuntimes::new(&DatanodeRuntimeOptions {
            query_rt_size: 1,
            ingest_rt_size: 1,
        });
        let runtime = runtimes.query_runtime();

        let runtime_to_spawn = runtime.clone();
        let thread_name = runtime.block_on(async move {
            runtime_to_spawn
                .spawn(async move {
                    std::thread::current()
                        .name()
                        .unwrap_or_default()
                        .to_string()
                })
                .await
                .unwrap()
        });

        assert!(
            thread_name.starts_with("datanode-query-worker"),
            "unexpected query runtime thread name: {thread_name}"
        );
    }

    #[test]
    fn test_datanode_runtimes_use_named_runtimes() {
        let runtimes = DatanodeRuntimes::new(&DatanodeRuntimeOptions {
            query_rt_size: 1,
            ingest_rt_size: 1,
        });

        assert_eq!("datanode-query", runtimes.query_runtime().name());
        assert_eq!("datanode-ingest", runtimes.ingest_runtime().name());
    }
}
