use api::v1::meta::Peer;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;

use crate::error::Result;
use crate::keys::LeaseKey;
use crate::keys::LeaseValue;
use crate::keys::DN_LEASE_PREFIX;
use crate::service::store::kv::KvStoreRef;
use crate::util;

pub async fn find_datanodes<P>(
    cluster_id: u64,
    kv_store: KvStoreRef,
    predicate: P,
) -> Result<Vec<Peer>>
where
    P: Fn(&LeaseKey, &LeaseValue) -> bool,
{
    let key = get_lease_prefix(cluster_id);
    let range_end = util::get_prefix_end_key(&key);
    let req = RangeRequest {
        key,
        range_end,
        ..Default::default()
    };

    let res = kv_store.range(req).await?;

    let RangeResponse { kvs, .. } = res;
    let mut peers = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let dn_key: LeaseKey = kv.key.try_into()?;
        let dn_value: LeaseValue = kv.value.try_into()?;
        if !predicate(&dn_key, &dn_value) {
            continue;
        }
        peers.push(Peer {
            id: dn_key.node_id,
            addr: dn_value.node_addr,
        })
    }

    Ok(peers)
}

#[inline]
pub fn get_lease_prefix(cluster_id: u64) -> Vec<u8> {
    format!("{}-{}", DN_LEASE_PREFIX, cluster_id).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_prefix() {
        let dn_key = get_lease_prefix(1);
        assert_eq!(b"__meta_dnlease-1".to_vec(), dn_key);
        let range_end = util::get_prefix_end_key(&dn_key);
        assert_eq!(b"__meta_dnlease-2".to_vec(), range_end);
    }
}
