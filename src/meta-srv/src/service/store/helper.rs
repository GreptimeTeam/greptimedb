use api::v1::meta::Peer;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;

use super::kv::KvStoreRef;
use crate::error::Result;
use crate::keys::DatanodeKey;
use crate::keys::DatanodeValue;
use crate::keys::KvBytes;
use crate::keys::DATANODE_REGISTER_PREFIX;

pub async fn find_all_datanodes(cluster_id: u64, kv_store: KvStoreRef) -> Result<Vec<Peer>> {
    let key = get_datanode_prefix(cluster_id);
    let range_end = get_prefix(&key);
    let req = RangeRequest {
        key,
        range_end,
        ..Default::default()
    };

    let res = kv_store.range(req).await?;

    let RangeResponse { kvs, .. } = res;
    let mut peers = Vec::with_capacity(kvs.len());
    for kv in kvs {
        let dn_key = DatanodeKey::from_bytes(kv.key)?;
        let dn_value = DatanodeValue::from_bytes(kv.value)?;
        peers.push(Peer {
            id: dn_key.node_id,
            addr: dn_value.node_addr,
        })
    }

    Ok(peers)
}

#[inline]
pub fn get_datanode_prefix(cluster_id: u64) -> Vec<u8> {
    format!("{}-{}", DATANODE_REGISTER_PREFIX, cluster_id).into_bytes()
}

/// Get prefix end key of `key`.
#[inline]
pub fn get_prefix(key: &[u8]) -> Vec<u8> {
    for (i, v) in key.iter().enumerate().rev() {
        if *v < 0xFF {
            let mut end = Vec::from(&key[..=i]);
            end[i] = *v + 1;
            return end;
        }
    }

    // next prefix does not exist (e.g., 0xffff);
    vec![0]
}
