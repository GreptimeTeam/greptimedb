pub(crate) mod etcd;

use crate::error::Result;

#[async_trait::async_trait]
pub trait Election: Send + Sync {
    type Leader;

    /// Returns `true` if current node is the leader.
    fn is_leader(&self) -> bool;

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
}
