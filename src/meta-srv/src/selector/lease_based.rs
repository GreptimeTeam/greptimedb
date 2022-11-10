use api::v1::meta::Peer;
use common_time::util as time_util;

use super::Namespace;
use super::Selector;
use crate::error::Result;
use crate::keys::LeaseKey;
use crate::keys::LeaseValue;
use crate::lease;
use crate::metasrv::Context;

pub struct LeaseBasedSelector;

#[async_trait::async_trait]
impl Selector for LeaseBasedSelector {
    type Context = Context;
    type Output = Vec<Peer>;

    async fn select(&self, ns: Namespace, ctx: &Self::Context) -> Result<Self::Output> {
        // filter out the nodes out lease
        let lease_filter = |_: &LeaseKey, v: &LeaseValue| {
            time_util::current_time_millis() - v.timestamp_millis < ctx.datanode_lease_secs
        };
        let mut lease_kvs = lease::alive_datanodes(ns, &ctx.kv_store, lease_filter).await?;
        // TODO(jiachun): At the moment we are just pushing the latest to the forefront,
        // and it is better to use load-based strategies in the future.
        lease_kvs.sort_by(|a, b| b.1.timestamp_millis.cmp(&a.1.timestamp_millis));

        let peers = lease_kvs
            .into_iter()
            .map(|(k, v)| Peer {
                id: k.node_id,
                addr: v.node_addr,
            })
            .collect::<Vec<_>>();

        Ok(peers)
    }
}
