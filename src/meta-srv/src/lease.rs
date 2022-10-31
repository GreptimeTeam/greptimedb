use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;

use crate::error::Result;
use crate::keys::LeaseKey;
use crate::keys::LeaseValue;
use crate::keys::DN_LEASE_PREFIX;
use crate::service::store::kv::KvStoreRef;
use crate::util;

pub async fn alive_datanodes<P>(
    cluster_id: u64,
    kv_store: KvStoreRef,
    predicate: P,
) -> Result<Vec<(LeaseKey, LeaseValue)>>
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
    let mut lease_kvs = vec![];
    for kv in kvs {
        let lease_key: LeaseKey = kv.key.try_into()?;
        let lease_value: LeaseValue = kv.value.try_into()?;
        if !predicate(&lease_key, &lease_value) {
            continue;
        }
        lease_kvs.push((lease_key, lease_value));
    }

    Ok(lease_kvs)
}

#[inline]
pub fn get_lease_prefix(cluster_id: u64) -> Vec<u8> {
    format!("{}-{}", DN_LEASE_PREFIX, cluster_id).into_bytes()
}
