use crate::error::Result;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, PutRequest};

pub(crate) async fn put_conditionally(
    kv_backend: &KvBackendRef,
    key: Vec<u8>,
    value: Vec<u8>,
    if_not_exists: bool,
) -> Result<bool> {
    let success = if if_not_exists {
        let req = CompareAndPutRequest::new()
            .with_key(key)
            .with_expect(vec![])
            .with_value(value);
        let res = kv_backend.compare_and_put(req).await?;
        res.success
    } else {
        let req = PutRequest::new().with_key(key).with_value(value);
        kv_backend.put(req).await?;
        true
    };

    Ok(success)
}
