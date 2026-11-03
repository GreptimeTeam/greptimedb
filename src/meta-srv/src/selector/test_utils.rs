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

use common_meta::affinity::NoopAffinity;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::peer::Peer;
use rand::prelude::SliceRandom;

use crate::cluster::MetaPeerClientBuilder;
use crate::error::Result;
use crate::metasrv::SelectorContext;
use crate::selector::{Selector, SelectorOptions};
use crate::test_util::new_node_scheduling_discovery;

/// Returns [SelectorContext] for test purpose.
///
/// Its `peer_discovery` is decorated with
/// [`SchedulingAwarePeerDiscovery`](crate::discovery::node_scheduling::SchedulingAwarePeerDiscovery)
/// backed by an empty scheduling state, so every datanode is schedulable until a test marks it
/// otherwise in the same in-memory backend.
pub fn new_test_selector_context() -> SelectorContext {
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let meta_peer_client = MetaPeerClientBuilder::default()
        .election(None)
        .in_memory(kv_backend)
        .build()
        .map(Arc::new)
        .unwrap();

    SelectorContext {
        affinity_fallback: Default::default(),
        affinity: Arc::new(NoopAffinity),
        peer_discovery: Arc::new(new_node_scheduling_discovery(&meta_peer_client).1),
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

    async fn select(
        &self,
        _ctx: &Self::Context,
        _opts: SelectorOptions<'_>,
    ) -> Result<Self::Output> {
        let mut rng = rand::rng();
        let mut nodes = self.nodes.clone();
        nodes.shuffle(&mut rng);
        Ok(nodes)
    }
}
