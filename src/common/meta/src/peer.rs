// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Display, Formatter};

use api::v1::meta::Peer as PbPeer;
use serde::{Deserialize, Serialize};

/// The State of the [Peer].
#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub enum PeerState {
    /// The following cases in which the [Peer] will be downgraded.
    ///
    /// - The [Peer] or [Region][crate::rpc::router::Region] on the [Peer] is unavailable(e.g., Crashed, Network disconnected).
    /// - The [Region][crate::rpc::router::Region] on the [Peer] was planned to migrate to another [Peer].
    Downgraded,
}

#[derive(Debug, Default, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct Peer {
    /// Node identifier. Unique in a cluster.
    pub id: u64,
    pub addr: String,
    /// `None` by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<PeerState>,
}

impl Peer {
    /// Returns true if the [Peer] is downgraded.
    ///
    /// The following cases in which the [Peer] will be downgraded.
    ///
    /// - The [Peer] or [Region][crate::rpc::router::Region] on the [Peer] is unavailable(e.g., Crashed, Network disconnected).
    /// - The [Region][crate::rpc::router::Region] on the [Peer] was planned to migrate to another [Peer].
    ///
    pub fn is_downgraded(&self) -> bool {
        matches!(self.state, Some(PeerState::Downgraded))
    }

    /// Marks the [Peer] as downgraded.
    ///
    /// We should downgrade a [Peer] before deactivating a [Region][crate::rpc::router::Region] on it:
    ///
    /// - During the [Region][crate::rpc::router::Region] Failover Procedure.
    /// - Migrating a [Region][crate::rpc::router::Region].
    ///
    /// **Notes:** Meta Server will stop renewing the lease for a [Region][crate::rpc::router::Region] on the downgraded [Peer].
    ///
    pub fn downgrade(&mut self) {
        self.state = Some(PeerState::Downgraded)
    }
}

impl From<PbPeer> for Peer {
    fn from(p: PbPeer) -> Self {
        Self {
            id: p.id,
            addr: p.addr,
            state: None,
        }
    }
}

impl From<Peer> for PbPeer {
    fn from(p: Peer) -> Self {
        Self {
            id: p.id,
            addr: p.addr,
        }
    }
}

impl Peer {
    pub fn new(id: u64, addr: impl Into<String>) -> Self {
        Self {
            id,
            addr: addr.into(),
            state: None,
        }
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer-{}({})", self.id, self.addr)
    }
}

#[cfg(test)]
mod tests {

    use super::Peer;

    #[test]
    fn test_peer_serde() {
        let peer = Peer::new(2, "test");

        let serialized = serde_json::to_string(&peer).unwrap();

        let expected = r#"{"id":2,"addr":"test"}"#;
        assert_eq!(serialized, expected);

        let decoded: Peer = serde_json::from_str(&serialized).unwrap();

        assert_eq!(peer, decoded);
    }

    #[test]
    fn test_peer_is_downgraded() {
        let mut peer = Peer::new(2, "test");

        assert!(!peer.is_downgraded());

        peer.downgrade();

        assert!(peer.is_downgraded());
    }
}
