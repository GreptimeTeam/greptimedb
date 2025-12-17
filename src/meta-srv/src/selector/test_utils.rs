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

use common_meta::distributed_time_constants::default_distributed_time_constants;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use rand::prelude::SliceRandom;

use crate::cluster::MetaPeerClientBuilder;
use crate::error::Result;
use crate::metasrv::SelectorContext;
use crate::selector::{Selector, SelectorOptions};

/// Returns [SelectorContext] for test purpose.
pub fn new_test_selector_context() -> SelectorContext {
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let meta_peer_client = MetaPeerClientBuilder::default()
        .election(None)
        .in_memory(kv_backend.clone())
        .build()
        .map(Arc::new)
        .unwrap();

    SelectorContext {
        server_addr: "127.0.0.1:3002".to_string(),
        datanode_lease_secs: default_distributed_time_constants().region_lease.as_secs(),
        flownode_lease_secs: default_distributed_time_constants()
            .flownode_lease
            .as_secs(),
        kv_backend,
        meta_peer_client,
        table_id: None,
    }
}

/// It always returns shuffled `nodes`.
pub struct RandomNodeSelector {
    nodes: Vec<Peer>,
}

impl RandomNodeSelector {
    pub fn new(nodes: Vec<Peer>) -> Self {
        Self { nodes }
    }
}

#[async_trait::async_trait]
impl Selector for RandomNodeSelector {
    type Context = SelectorContext;
    type Output = Vec<Peer>;

    async fn select(&self, _ctx: &Self::Context, _opts: SelectorOptions) -> Result<Self::Output> {
        let mut rng = rand::rng();
        let mut nodes = self.nodes.clone();
        nodes.shuffle(&mut rng);
        Ok(nodes)
    }
}
