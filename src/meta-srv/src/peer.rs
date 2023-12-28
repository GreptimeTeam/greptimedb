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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use common_meta::peer::Peer;
use common_meta::ClusterId;
use common_telemetry::warn;

/// Used to look up specific peer by `peer_id`.
pub trait PeerLookup: Send + Sync {
    /// Returns `None` stands for target [Peer] is unavailable temporally
    fn peer(&self, cluster_id: ClusterId, peer_id: u64) -> Option<Peer>;
}

pub type PeerIdentifier = (ClusterId, u64);

#[derive(Debug, Default, Clone)]
pub struct NaivePeerRegistry(Arc<RwLock<HashMap<PeerIdentifier, Peer>>>);

impl NaivePeerRegistry {
    /// Registers a [Peer].
    pub fn register(&self, cluster_id: ClusterId, peer: Peer) {
        let mut inner = self.0.write().unwrap();
        if let Some(previous) = inner.insert((cluster_id, peer.id), peer) {
            warn!(
                "Registered a peer, it overwritten the previous peer: {:?}",
                previous
            )
        }
    }

    /// Deregisters a [Peer].
    pub fn deregister(&self, cluster_id: ClusterId, peer_id: u64) {
        let mut inner = self.0.write().unwrap();
        if inner.remove(&(cluster_id, peer_id)).is_none() {
            warn!(
                "Trying to deregister a non-exist peer, peer_id: {}",
                peer_id
            );
        }
    }
}

impl PeerLookup for NaivePeerRegistry {
    fn peer(&self, cluster_id: ClusterId, peer_id: u64) -> Option<Peer> {
        let inner = self.0.read().unwrap();
        inner.get(&(cluster_id, peer_id)).cloned()
    }
}

#[cfg(test)]
mod tests {
    use common_meta::peer::Peer;

    use super::{NaivePeerRegistry, PeerLookup};

    #[test]
    fn test_naive_peer_registry() {
        let lookup = NaivePeerRegistry::default();
        lookup.register(0, Peer::empty(1024));

        assert!(lookup.peer(0, 1024).is_some());
        assert!(lookup.peer(0, 1025).is_none());

        lookup.deregister(0, 1024);
        assert!(lookup.peer(0, 1024).is_none());
    }
}
