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

pub mod etcd;
#[cfg(feature = "pg_kvbackend")]
pub mod postgres;

use std::fmt::{self, Debug};
use std::sync::Arc;

use tokio::sync::broadcast::Receiver;

use crate::error::Result;
use crate::metasrv::MetasrvNodeInfo;

pub const ELECTION_KEY: &str = "__metasrv_election";
pub const CANDIDATES_ROOT: &str = "__metasrv_election_candidates/";

pub(crate) const CANDIDATE_LEASE_SECS: u64 = 600;
const KEEP_ALIVE_INTERVAL_SECS: u64 = CANDIDATE_LEASE_SECS / 2;

/// Messages sent when the leader changes.
#[derive(Debug, Clone)]
pub enum LeaderChangeMessage {
    Elected(Arc<dyn LeaderKey>),
    StepDown(Arc<dyn LeaderKey>),
}

/// LeaderKey is a key that represents the leader of metasrv.
/// The structure is correponding to [etcd_client::LeaderKey].
pub trait LeaderKey: Send + Sync + Debug {
    /// The name in byte. name is the election identifier that corresponds to the leadership key.
    fn name(&self) -> &[u8];

    /// The key in byte. key is an opaque key representing the ownership of the election. If the key
    /// is deleted, then leadership is lost.
    fn key(&self) -> &[u8];

    /// The creation revision of the key.
    fn rev(&self) -> i64;

    /// The lease ID of the election leader.
    fn lease(&self) -> i64;
}

impl fmt::Display for LeaderChangeMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leader_key = match self {
            LeaderChangeMessage::Elected(leader_key) => {
                write!(f, "Elected(")?;
                leader_key
            }
            LeaderChangeMessage::StepDown(leader_key) => {
                write!(f, "StepDown(")?;
                leader_key
            }
        };
        write!(f, "LeaderKey {{ ")?;
        write!(f, "name: {}", String::from_utf8_lossy(leader_key.name()))?;
        write!(f, ", key: {}", String::from_utf8_lossy(leader_key.key()))?;
        write!(f, ", rev: {}", leader_key.rev())?;
        write!(f, ", lease: {}", leader_key.lease())?;
        write!(f, " }})")
    }
}

#[async_trait::async_trait]
pub trait Election: Send + Sync {
    type Leader;

    /// Returns `true` if current node is the leader.
    fn is_leader(&self) -> bool;

    /// When a new leader is born, it may need some initialization
    /// operations (asynchronous), this method tells us when these
    /// initialization operations can be performed.
    ///
    /// note: a new leader will only return true on the first call.
    fn in_infancy(&self) -> bool;

    /// Registers a candidate for the election.
    async fn register_candidate(&self, node_info: &MetasrvNodeInfo) -> Result<()>;

    /// Gets all candidates in the election.
    async fn all_candidates(&self) -> Result<Vec<MetasrvNodeInfo>>;

    /// Campaign waits to acquire leadership in an election.
    ///
    /// Multiple sessions can participate in the election,
    /// but only one can be the leader at a time.
    async fn campaign(&self) -> Result<()>;

    /// Returns the leader value for the current election.
    async fn leader(&self) -> Result<Self::Leader>;

    /// Releases election leadership so other campaigners may
    /// acquire leadership on the election.
    async fn resign(&self) -> Result<()>;

    fn subscribe_leader_change(&self) -> Receiver<LeaderChangeMessage>;
}
