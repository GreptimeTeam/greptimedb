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

use testcontainers::clients::Cli as DockerCli;
use testcontainers::Container;

use crate::wal_util::kafka::image::Image;

/// A runtime running a cluster consisting of a single Kafka node and a single ZooKeeper node.
#[derive(Default)]
pub struct Runtime {
    docker: DockerCli,
}

impl Runtime {
    /// Starts the runtime. The runtime terminates when the returned container is dropped.
    pub async fn start(&self) -> Container<Image> {
        self.docker.run(Image::default())
    }
}

#[macro_export]
macro_rules! start_kafka {
    () => {
        let _ = Runtime::default().start();
    };
}
