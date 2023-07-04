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

use std::sync::Arc;

use etcd_client::LeaderKey;
use tokio::sync::broadcast::Receiver;

use crate::error::Result;

pub const LEASE_SECS: i64 = 5;
// In a lease, there are two opportunities for renewal.
pub const KEEP_ALIVE_PERIOD_SECS: u64 = LEASE_SECS as u64 / 2;
pub const ELECTION_KEY: &str = "__meta_srv_election";

#[derive(Debug, Clone)]
pub enum LeaderChangeMessage {
    Elected(Arc<LeaderKey>),
    StepDown(Arc<LeaderKey>),
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
