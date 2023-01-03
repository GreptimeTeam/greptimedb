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

tonic::include_proto!("greptime.v1.meta");

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

pub const PROTOCOL_VERSION: u64 = 1;

#[derive(Default)]
pub struct PeerDict {
    peers: HashMap<Peer, usize>,
    index: usize,
}

impl PeerDict {
    pub fn get_or_insert(&mut self, peer: Peer) -> usize {
        let index = self.peers.entry(peer).or_insert_with(|| {
            let v = self.index;
            self.index += 1;
            v
        });

        *index
    }

    pub fn into_peers(self) -> Vec<Peer> {
        let mut array = vec![Peer::default(); self.index];
        for (p, i) in self.peers {
            array[i] = p;
        }
        array
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Peer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.addr.hash(state);
    }
}

impl Eq for Peer {}

impl RequestHeader {
    #[inline]
    pub fn new((cluster_id, member_id): (u64, u64)) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            member_id,
        }
    }
}

impl ResponseHeader {
    #[inline]
    pub fn success(cluster_id: u64) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            ..Default::default()
        }
    }

    #[inline]
    pub fn failed(cluster_id: u64, error: Error) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            cluster_id,
            error: Some(error),
        }
    }

    #[inline]
    pub fn is_not_leader(&self) -> bool {
        if let Some(error) = &self.error {
            if error.code == ErrorCode::NotLeader as i32 {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    NoActiveDatanodes = 1,
    NotLeader = 2,
}

impl Error {
    #[inline]
    pub fn no_active_datanodes() -> Self {
        Self {
            code: ErrorCode::NoActiveDatanodes as i32,
            err_msg: "No active datanodes".to_string(),
        }
    }

    #[inline]
    pub fn is_not_leader() -> Self {
        Self {
            code: ErrorCode::NotLeader as i32,
            err_msg: "Current server is not leader".to_string(),
        }
    }
}

impl HeartbeatResponse {
    #[inline]
    pub fn is_not_leader(&self) -> bool {
        if let Some(header) = &self.header {
            return header.is_not_leader();
        }
        false
    }
}

macro_rules! gen_set_header {
    ($req: ty) => {
        impl $req {
            #[inline]
            pub fn set_header(&mut self, (cluster_id, member_id): (u64, u64)) {
                self.header = Some(RequestHeader::new((cluster_id, member_id)));
            }
        }
    };
}

gen_set_header!(HeartbeatRequest);
gen_set_header!(RouteRequest);
gen_set_header!(CreateRequest);
gen_set_header!(RangeRequest);
gen_set_header!(DeleteRequest);
gen_set_header!(PutRequest);
gen_set_header!(BatchPutRequest);
gen_set_header!(CompareAndPutRequest);
gen_set_header!(DeleteRangeRequest);
gen_set_header!(MoveValueRequest);

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn test_peer_dict() {
        let mut dict = PeerDict::default();

        dict.get_or_insert(Peer {
            id: 1,
            addr: "111".to_string(),
        });
        dict.get_or_insert(Peer {
            id: 2,
            addr: "222".to_string(),
        });
        dict.get_or_insert(Peer {
            id: 1,
            addr: "111".to_string(),
        });
        dict.get_or_insert(Peer {
            id: 1,
            addr: "111".to_string(),
        });
        dict.get_or_insert(Peer {
            id: 1,
            addr: "111".to_string(),
        });
        dict.get_or_insert(Peer {
            id: 1,
            addr: "111".to_string(),
        });
        dict.get_or_insert(Peer {
            id: 2,
            addr: "222".to_string(),
        });

        assert_eq!(2, dict.index);
        assert_eq!(
            vec![
                Peer {
                    id: 1,
                    addr: "111".to_string(),
                },
                Peer {
                    id: 2,
                    addr: "222".to_string(),
                }
            ],
            dict.into_peers()
        );
    }
}
