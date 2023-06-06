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

use crate::cases::TestCase;
use crate::chaos::bare::client::{process, Client as ChaosClient};
use crate::chaos::bare::nemesis::process::AttackProcess;
use crate::chaos::nemesis::Nemesis;
use crate::cluster::bare::debug::DebugCluster;
use crate::cluster::bare::node::NodeInfo;
use crate::cluster::Cluster;
pub struct ExampleTest;

#[async_trait::async_trait]
impl TestCase for ExampleTest {
    fn name(&self) -> &'static str {
        "example"
    }

    async fn run(&self) {
        // Default debug cluster starts 3 datanode nodes.
        let cluster = DebugCluster;
        let chaos_client = Arc::new(ChaosClient::default());

        let mut attack = AttackProcess::new(chaos_client.clone(), process::Signal::Term);

        // Lists all nodes.
        let nodes = cluster.nodes().await.unwrap();
        let nodes_num = nodes.len();

        // Choose a lucky one.
        let node = nodes
            .iter()
            .find(|node| matches!(node.info, NodeInfo::Datanode { .. }))
            .unwrap();

        // Attacks a datanode.
        attack.invoke(node).await.unwrap();

        // Lists all nodes again.
        let nodes = cluster.nodes().await.unwrap();

        assert_eq!(nodes_num - 1, nodes.len());
    }
}
