use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::Result;
use crate::key::{DeserializedValueWithBytes, TableMetaValue};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse};

pub(crate) fn build_txn_response_decoder_fn<T>(
    raw_key: Vec<u8>,
) -> impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<T>>>
where
    T: Serialize + DeserializeOwned + TableMetaValue,
{
    move |txn_res: &Vec<TxnOpResponse>| {
        txn_res
            .iter()
            .filter_map(|resp| {
                if let TxnOpResponse::ResponseGet(r) = resp {
                    Some(r)
                } else {
                    None
                }
            })
            .flat_map(|r| &r.kvs)
            .find(|kv| kv.key == raw_key)
            .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
            .transpose()
    }
}

pub(crate) fn build_put_if_absent_txn(key: Vec<u8>, value: Vec<u8>) -> Txn {
    Txn::new()
        .when(vec![Compare::with_not_exist_value(
            key.clone(),
            CompareOp::Equal,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), value)])
        .or_else(vec![TxnOp::Get(key)])
}

pub(crate) fn build_compare_and_put_txn(key: Vec<u8>, old_value: Vec<u8>, value: Vec<u8>) -> Txn {
    Txn::new()
        .when(vec![Compare::with_value(
            key.clone(),
            CompareOp::Equal,
            old_value,
        )])
        .and_then(vec![TxnOp::Put(key.clone(), value)])
        .or_else(vec![TxnOp::Get(key)])
}
