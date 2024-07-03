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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use api::v1::meta::Peer as PbPeer;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::{ClusterId, DatanodeId, FlownodeId};

#[derive(Debug, Default, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct Peer {
    /// Node identifier. Unique in a cluster.
    pub id: u64,
    pub addr: String,
}

impl From<PbPeer> for Peer {
    fn from(p: PbPeer) -> Self {
        Self {
            id: p.id,
            addr: p.addr,
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
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn empty(id: u64) -> Self {
        Self {
            id,
            addr: String::new(),
        }
    }
}

impl Display for Peer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer-{}({})", self.id, self.addr)
    }
}

/// can query peer given a node id
#[async_trait::async_trait]
pub trait PeerLookupService {
    async fn datanode(&self, cluster_id: ClusterId, id: DatanodeId) -> Result<Option<Peer>, Error>;
    async fn flownode(&self, cluster_id: ClusterId, id: FlownodeId) -> Result<Option<Peer>, Error>;
}

pub type PeerLookupServiceRef = Arc<dyn PeerLookupService + Send + Sync>;
