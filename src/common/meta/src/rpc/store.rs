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

use std::fmt::{Display, Formatter};
use std::ops::Bound;

use api::v1::meta::{
    BatchDeleteRequest as PbBatchDeleteRequest, BatchDeleteResponse as PbBatchDeleteResponse,
    BatchGetRequest as PbBatchGetRequest, BatchGetResponse as PbBatchGetResponse,
    BatchPutRequest as PbBatchPutRequest, BatchPutResponse as PbBatchPutResponse,
    CompareAndPutRequest as PbCompareAndPutRequest,
    CompareAndPutResponse as PbCompareAndPutResponse, DeleteRangeRequest as PbDeleteRangeRequest,
    DeleteRangeResponse as PbDeleteRangeResponse, PutRequest as PbPutRequest,
    PutResponse as PbPutResponse, RangeRequest as PbRangeRequest, RangeResponse as PbRangeResponse,
    ResponseHeader as PbResponseHeader,
};

use crate::error;
use crate::error::Result;
use crate::rpc::{util, KeyValue};

pub fn to_range(key: Vec<u8>, range_end: Vec<u8>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    match (&key[..], &range_end[..]) {
        (_, []) => (Bound::Included(key.clone()), Bound::Included(key)),
        // If both key and range_end are ‘\0’, then range represents all keys.
        ([0], [0]) => (Bound::Unbounded, Bound::Unbounded),
        // If range_end is ‘\0’, the range is all keys greater than or equal to the key argument.
        (_, [0]) => (Bound::Included(key), Bound::Unbounded),
        (_, _) => (Bound::Included(key), Bound::Excluded(range_end)),
    }
}

#[derive(Debug, Clone, Default)]
pub struct RangeRequest {
    /// key is the first key for the range, If range_end is not given, the
    /// request only looks up key.
    pub key: Vec<u8>,
    /// range_end is the upper bound on the requested range [key, range_end).
    /// If range_end is '\0', the range is all keys >= key.
    /// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
    /// then the range request gets all keys prefixed with key.
    /// If both key and range_end are '\0', then the range request returns all
    /// keys.
    pub range_end: Vec<u8>,
    /// limit is a limit on the number of keys returned for the request. When
    /// limit is set to 0, it is treated as no limit.
    pub limit: i64,
    /// keys_only when set returns only the keys and not the values.
    pub keys_only: bool,
}

impl From<RangeRequest> for PbRangeRequest {
    fn from(req: RangeRequest) -> Self {
        Self {
            header: None,
            key: req.key,
            range_end: req.range_end,
            limit: req.limit,
            keys_only: req.keys_only,
        }
    }
}

impl From<PbRangeRequest> for RangeRequest {
    fn from(value: PbRangeRequest) -> Self {
        Self {
            key: value.key,
            range_end: value.range_end,
            limit: value.limit,
            keys_only: value.keys_only,
        }
    }
}

impl Display for RangeRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RangeRequest{{key: '{}', range_end: '{}', limit: {}, keys_only: {}}}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.range_end),
            self.limit,
            self.keys_only
        )
    }
}

impl RangeRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            key: vec![],
            range_end: vec![],
            limit: 0,
            keys_only: false,
        }
    }

    /// Returns the `RangeBounds`.
    pub fn range(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        to_range(self.key.clone(), self.range_end.clone())
    }

    /// key is the first key for the range, If range_end is not given, the
    /// request only looks up key.
    #[inline]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self
    }

    /// key is the first key for the range, If range_end is not given, the
    /// request only looks up key.
    ///
    /// range_end is the upper bound on the requested range [key, range_end).
    /// If range_end is '\0', the range is all keys >= key.
    /// If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
    /// then the range request gets all keys prefixed with key.
    /// If both key and range_end are '\0', then the range request returns all
    /// keys.
    #[inline]
    pub fn with_range(mut self, key: impl Into<Vec<u8>>, range_end: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self.range_end = range_end.into();
        self
    }

    /// Gets all keys prefixed with key.
    /// range_end is the key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
    #[inline]
    pub fn with_prefix(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self.range_end = util::get_prefix_end_key(&self.key);
        self
    }

    /// limit is a limit on the number of keys returned for the request. When
    /// limit is set to 0, it is treated as no limit.
    #[inline]
    pub fn with_limit(mut self, limit: i64) -> Self {
        self.limit = limit;
        self
    }

    /// keys_only when set returns only the keys and not the values.
    #[inline]
    pub fn with_keys_only(mut self) -> Self {
        self.keys_only = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeResponse {
    pub kvs: Vec<KeyValue>,
    pub more: bool,
}

impl Display for RangeResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RangeResponse{{kvs: [{}], more: {}}}",
            self.kvs
                .iter()
                .map(|kv| kv.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            self.more
        )
    }
}

impl TryFrom<PbRangeResponse> for RangeResponse {
    type Error = error::Error;

    fn try_from(pb: PbRangeResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self {
            kvs: pb.kvs.into_iter().map(KeyValue::new).collect(),
            more: pb.more,
        })
    }
}

impl RangeResponse {
    pub fn to_proto_resp(self, header: PbResponseHeader) -> PbRangeResponse {
        PbRangeResponse {
            header: Some(header),
            kvs: self.kvs.into_iter().map(Into::into).collect(),
            more: self.more,
        }
    }

    #[inline]
    pub fn take_kvs(&mut self) -> Vec<KeyValue> {
        self.kvs.drain(..).collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PutRequest {
    /// key is the key, in bytes, to put into the key-value store.
    pub key: Vec<u8>,
    /// value is the value, in bytes, to associate with the key in the
    /// key-value store.
    pub value: Vec<u8>,
    /// If prev_kv is set, gets the previous key-value pair before changing it.
    /// The previous key-value pair will be returned in the put response.
    pub prev_kv: bool,
}

impl From<PutRequest> for PbPutRequest {
    fn from(req: PutRequest) -> Self {
        Self {
            header: None,
            key: req.key,
            value: req.value,
            prev_kv: req.prev_kv,
        }
    }
}

impl From<PbPutRequest> for PutRequest {
    fn from(value: PbPutRequest) -> Self {
        Self {
            key: value.key,
            value: value.value,
            prev_kv: value.prev_kv,
        }
    }
}

impl PutRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            key: vec![],
            value: vec![],
            prev_kv: false,
        }
    }

    /// key is the key, in bytes, to put into the key-value store.
    #[inline]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self
    }

    /// value is the value, in bytes, to associate with the key in the
    /// key-value store.
    #[inline]
    pub fn with_value(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.value = value.into();
        self
    }

    /// If prev_kv is set, gets the previous key-value pair before changing it.
    /// The previous key-value pair will be returned in the put response.
    #[inline]
    pub fn with_prev_kv(mut self) -> Self {
        self.prev_kv = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PutResponse {
    pub prev_kv: Option<KeyValue>,
}

impl TryFrom<PbPutResponse> for PutResponse {
    type Error = error::Error;

    fn try_from(pb: PbPutResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self {
            prev_kv: pb.prev_kv.map(KeyValue::new),
        })
    }
}

impl PutResponse {
    pub fn to_proto_resp(self, header: PbResponseHeader) -> PbPutResponse {
        PbPutResponse {
            header: Some(header),
            prev_kv: self.prev_kv.map(Into::into),
        }
    }
}

#[derive(Clone)]
pub struct BatchGetRequest {
    pub keys: Vec<Vec<u8>>,
}

impl From<BatchGetRequest> for PbBatchGetRequest {
    fn from(req: BatchGetRequest) -> Self {
        Self {
            header: None,
            keys: req.keys,
        }
    }
}

impl From<PbBatchGetRequest> for BatchGetRequest {
    fn from(value: PbBatchGetRequest) -> Self {
        Self { keys: value.keys }
    }
}

impl Default for BatchGetRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchGetRequest {
    #[inline]
    pub fn new() -> Self {
        Self { keys: vec![] }
    }

    #[inline]
    pub fn add_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.keys.push(key.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct BatchGetResponse {
    pub kvs: Vec<KeyValue>,
}

impl Display for BatchGetResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}]",
            self.kvs
                .iter()
                .map(|kv| kv.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

impl TryFrom<PbBatchGetResponse> for BatchGetResponse {
    type Error = error::Error;

    fn try_from(pb: PbBatchGetResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self {
            kvs: pb.kvs.into_iter().map(KeyValue::new).collect(),
        })
    }
}

impl BatchGetResponse {
    pub fn to_proto_resp(self, header: PbResponseHeader) -> PbBatchGetResponse {
        PbBatchGetResponse {
            header: Some(header),
            kvs: self.kvs.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BatchPutRequest {
    pub kvs: Vec<KeyValue>,
    /// If prev_kv is set, gets the previous key-value pairs before changing it.
    /// The previous key-value pairs will be returned in the batch put response.
    pub prev_kv: bool,
}

impl From<BatchPutRequest> for PbBatchPutRequest {
    fn from(req: BatchPutRequest) -> Self {
        Self {
            header: None,
            kvs: req.kvs.into_iter().map(Into::into).collect(),
            prev_kv: req.prev_kv,
        }
    }
}

impl From<PbBatchPutRequest> for BatchPutRequest {
    fn from(value: PbBatchPutRequest) -> Self {
        Self {
            kvs: value.kvs.into_iter().map(KeyValue::new).collect(),
            prev_kv: value.prev_kv,
        }
    }
}

impl BatchPutRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            kvs: vec![],
            prev_kv: false,
        }
    }

    #[inline]
    pub fn add_kv(mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.kvs.push(KeyValue {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    /// If prev_kv is set, gets the previous key-value pair before changing it.
    /// The previous key-value pair will be returned in the put response.
    #[inline]
    pub fn with_prev_kv(mut self) -> Self {
        self.prev_kv = true;
        self
    }
}

#[derive(Debug, Clone)]
pub struct BatchPutResponse {
    pub prev_kvs: Vec<KeyValue>,
}

impl TryFrom<PbBatchPutResponse> for BatchPutResponse {
    type Error = error::Error;

    fn try_from(pb: PbBatchPutResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self {
            prev_kvs: pb.prev_kvs.into_iter().map(KeyValue::new).collect(),
        })
    }
}

impl BatchPutResponse {
    pub fn to_proto_resp(self, header: PbResponseHeader) -> PbBatchPutResponse {
        PbBatchPutResponse {
            header: Some(header),
            prev_kvs: self.prev_kvs.into_iter().map(Into::into).collect(),
        }
    }

    #[inline]
    pub fn take_prev_kvs(&mut self) -> Vec<KeyValue> {
        self.prev_kvs.drain(..).collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct BatchDeleteRequest {
    pub keys: Vec<Vec<u8>>,
    /// If prev_kv is set, gets the previous key-value pairs before deleting it.
    /// The previous key-value pairs will be returned in the batch delete response.
    pub prev_kv: bool,
}

impl From<BatchDeleteRequest> for PbBatchDeleteRequest {
    fn from(req: BatchDeleteRequest) -> Self {
        Self {
            header: None,
            keys: req.keys,
            prev_kv: req.prev_kv,
        }
    }
}

impl From<PbBatchDeleteRequest> for BatchDeleteRequest {
    fn from(value: PbBatchDeleteRequest) -> Self {
        Self {
            keys: value.keys,
            prev_kv: value.prev_kv,
        }
    }
}

impl BatchDeleteRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            keys: vec![],
            prev_kv: false,
        }
    }

    #[inline]
    pub fn add_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// If prev_kv is set, gets the previous key-value pair before deleting it.
    /// The previous key-value pair will be returned in the batch delete response.
    #[inline]
    pub fn with_prev_kv(mut self) -> Self {
        self.prev_kv = true;
        self
    }
}

#[derive(Debug, Clone)]
pub struct BatchDeleteResponse {
    pub prev_kvs: Vec<KeyValue>,
}

impl TryFrom<PbBatchDeleteResponse> for BatchDeleteResponse {
    type Error = error::Error;

    fn try_from(pb: PbBatchDeleteResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self {
            prev_kvs: pb.prev_kvs.into_iter().map(KeyValue::new).collect(),
        })
    }
}

impl BatchDeleteResponse {
    pub fn to_proto_resp(self, header: PbResponseHeader) -> PbBatchDeleteResponse {
        PbBatchDeleteResponse {
            header: Some(header),
            prev_kvs: self.prev_kvs.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompareAndPutRequest {
    /// key is the key, in bytes, to put into the key-value store.
    pub key: Vec<u8>,
    pub expect: Vec<u8>,
    /// value is the value, in bytes, to associate with the key in the
    /// key-value store.
    pub value: Vec<u8>,
}

impl From<CompareAndPutRequest> for PbCompareAndPutRequest {
    fn from(req: CompareAndPutRequest) -> Self {
        Self {
            header: None,
            key: req.key,
            expect: req.expect,
            value: req.value,
        }
    }
}

impl From<PbCompareAndPutRequest> for CompareAndPutRequest {
    fn from(value: PbCompareAndPutRequest) -> Self {
        Self {
            key: value.key,
            expect: value.expect,
            value: value.value,
        }
    }
}

impl CompareAndPutRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            key: vec![],
            expect: vec![],
            value: vec![],
        }
    }

    /// key is the key, in bytes, to put into the key-value store.
    #[inline]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self
    }

    /// expect is the previous value, in bytes
    #[inline]
    pub fn with_expect(mut self, expect: impl Into<Vec<u8>>) -> Self {
        self.expect = expect.into();
        self
    }

    /// value is the value, in bytes, to associate with the key in the
    /// key-value store.
    #[inline]
    pub fn with_value(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.value = value.into();
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct CompareAndPutResponse {
    pub success: bool,
    pub prev_kv: Option<KeyValue>,
}

impl TryFrom<PbCompareAndPutResponse> for CompareAndPutResponse {
    type Error = error::Error;

    fn try_from(pb: PbCompareAndPutResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self {
            success: pb.success,
            prev_kv: pb.prev_kv.map(KeyValue::new),
        })
    }
}

impl CompareAndPutResponse {
    pub fn handle<R, E, F>(self, f: F) -> std::result::Result<R, E>
    where
        F: FnOnce(Self) -> std::result::Result<R, E>,
    {
        f(self)
    }

    pub fn to_proto_resp(self, header: PbResponseHeader) -> PbCompareAndPutResponse {
        PbCompareAndPutResponse {
            header: Some(header),
            success: self.success,
            prev_kv: self.prev_kv.map(Into::into),
        }
    }

    #[inline]
    pub fn is_success(&self) -> bool {
        self.success
    }

    #[inline]
    pub fn take_prev_kv(&mut self) -> Option<KeyValue> {
        self.prev_kv.take()
    }
}

#[derive(Debug, Clone, Default)]
pub struct DeleteRangeRequest {
    /// key is the first key to delete in the range.
    pub key: Vec<u8>,
    /// range_end is the key following the last key to delete for the range
    /// [key, range_end).
    /// If range_end is not given, the range is defined to contain only the key
    /// argument.
    /// If range_end is one bit larger than the given key, then the range is all
    /// the keys with the prefix (the given key).
    /// If range_end is '\0', the range is all keys greater than or equal to the
    /// key argument.
    pub range_end: Vec<u8>,
    /// If prev_kv is set, gets the previous key-value pairs before deleting it.
    /// The previous key-value pairs will be returned in the delete response.
    pub prev_kv: bool,
    // TODO(jiachun):
    // Add a "limit" in delete request?
    // To avoid a huge delete block everything.
}

impl From<DeleteRangeRequest> for PbDeleteRangeRequest {
    fn from(req: DeleteRangeRequest) -> Self {
        Self {
            header: None,
            key: req.key,
            range_end: req.range_end,
            prev_kv: req.prev_kv,
        }
    }
}

impl From<PbDeleteRangeRequest> for DeleteRangeRequest {
    fn from(value: PbDeleteRangeRequest) -> Self {
        Self {
            key: value.key,
            range_end: value.range_end,
            prev_kv: value.prev_kv,
        }
    }
}

impl DeleteRangeRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            key: vec![],
            range_end: vec![],
            prev_kv: false,
        }
    }

    /// Returns the `RangeBounds`.
    pub fn range(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        to_range(self.key.clone(), self.range_end.clone())
    }

    /// key is the first key to delete in the range. If range_end is not given,
    /// the range is defined to contain only the key argument.
    #[inline]
    pub fn with_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self
    }

    /// key is the first key to delete in the range.
    ///
    /// range_end is the key following the last key to delete for the range
    /// [key, range_end).
    /// If range_end is not given, the range is defined to contain only the key
    /// argument.
    /// If range_end is one bit larger than the given key, then the range is all
    /// the keys with the prefix (the given key).
    /// If range_end is '\0', the range is all keys greater than or equal to the
    /// key argument.
    #[inline]
    pub fn with_range(mut self, key: impl Into<Vec<u8>>, range_end: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self.range_end = range_end.into();
        self
    }

    /// Deletes all keys prefixed with key.
    /// range_end is one bit larger than the given key.
    #[inline]
    pub fn with_prefix(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self.range_end = util::get_prefix_end_key(&self.key);
        self
    }

    /// If prev_kv is set, gets the previous key-value pairs before deleting it.
    /// The previous key-value pairs will be returned in the delete response.
    #[inline]
    pub fn with_prev_kv(mut self) -> Self {
        self.prev_kv = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteRangeResponse {
    pub deleted: i64,
    pub prev_kvs: Vec<KeyValue>,
}

impl TryFrom<PbDeleteRangeResponse> for DeleteRangeResponse {
    type Error = error::Error;

    fn try_from(pb: PbDeleteRangeResponse) -> Result<Self> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self {
            deleted: pb.deleted,
            prev_kvs: pb.prev_kvs.into_iter().map(KeyValue::new).collect(),
        })
    }
}

impl DeleteRangeResponse {
    pub fn to_proto_resp(self, header: PbResponseHeader) -> PbDeleteRangeResponse {
        PbDeleteRangeResponse {
            header: Some(header),
            deleted: self.deleted,
            prev_kvs: self.prev_kvs.into_iter().map(Into::into).collect(),
        }
    }

    #[inline]
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    #[inline]
    pub fn take_prev_kvs(&mut self) -> Vec<KeyValue> {
        self.prev_kvs.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::{
        BatchPutRequest as PbBatchPutRequest, BatchPutResponse as PbBatchPutResponse,
        CompareAndPutRequest as PbCompareAndPutRequest,
        CompareAndPutResponse as PbCompareAndPutResponse,
        DeleteRangeRequest as PbDeleteRangeRequest, DeleteRangeResponse as PbDeleteRangeResponse,
        KeyValue as PbKeyValue, PutRequest as PbPutRequest, PutResponse as PbPutResponse,
        RangeRequest as PbRangeRequest, RangeResponse as PbRangeResponse,
    };

    use super::*;

    #[test]
    fn test_range_request_trans() {
        let (key, range_end, limit) = (b"test_key1".to_vec(), b"test_range_end1".to_vec(), 1);

        let req = RangeRequest::new()
            .with_range(key.clone(), range_end.clone())
            .with_limit(limit)
            .with_keys_only();

        let into_req: PbRangeRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(key, into_req.key);
        assert_eq!(range_end, into_req.range_end);
        assert_eq!(limit, into_req.limit);
        assert!(into_req.keys_only);
    }

    #[test]
    fn test_prefix_request_trans() {
        let (key, limit) = (b"test_key1".to_vec(), 1);

        let req = RangeRequest::new()
            .with_prefix(key.clone())
            .with_limit(limit)
            .with_keys_only();

        let into_req: PbRangeRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(key, into_req.key);
        assert_eq!(b"test_key2".to_vec(), into_req.range_end);
        assert_eq!(limit, into_req.limit);
        assert!(into_req.keys_only);
    }

    #[test]
    fn test_range_response_trans() {
        let pb_res = PbRangeResponse {
            header: None,
            kvs: vec![
                PbKeyValue {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
                PbKeyValue {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                },
            ],
            more: true,
        };

        let mut res: RangeResponse = pb_res.try_into().unwrap();
        assert!(res.more);
        let mut kvs = res.take_kvs();
        let kv0 = kvs.get_mut(0).unwrap();
        assert_eq!(b"k1".to_vec(), kv0.key().to_vec());
        assert_eq!(b"k1".to_vec(), kv0.take_key());
        assert_eq!(b"v1".to_vec(), kv0.value().to_vec());
        assert_eq!(b"v1".to_vec(), kv0.take_value());

        let kv1 = kvs.get_mut(1).unwrap();
        assert_eq!(b"k2".to_vec(), kv1.key().to_vec());
        assert_eq!(b"k2".to_vec(), kv1.take_key());
        assert_eq!(b"v2".to_vec(), kv1.value().to_vec());
        assert_eq!(b"v2".to_vec(), kv1.take_value());
    }

    #[test]
    fn test_put_request_trans() {
        let (key, value) = (b"test_key1".to_vec(), b"test_value1".to_vec());

        let req = PutRequest::new()
            .with_key(key.clone())
            .with_value(value.clone())
            .with_prev_kv();

        let into_req: PbPutRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(key, into_req.key);
        assert_eq!(value, into_req.value);
    }

    #[test]
    fn test_put_response_trans() {
        let pb_res = PbPutResponse {
            header: None,
            prev_kv: Some(PbKeyValue {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }),
        };

        let res: PutResponse = pb_res.try_into().unwrap();
        let mut kv = res.prev_kv.unwrap();
        assert_eq!(b"k1".to_vec(), kv.key().to_vec());
        assert_eq!(b"k1".to_vec(), kv.take_key());
        assert_eq!(b"v1".to_vec(), kv.value().to_vec());
        assert_eq!(b"v1".to_vec(), kv.take_value());
    }

    #[test]
    fn test_batch_get_request_trans() {
        let req = BatchGetRequest::default()
            .add_key(b"test_key1".to_vec())
            .add_key(b"test_key2".to_vec())
            .add_key(b"test_key3".to_vec());

        let into_req: PbBatchGetRequest = req.into();

        assert!(into_req.header.is_none());
        assert_eq!(b"test_key1".as_slice(), into_req.keys.first().unwrap());
        assert_eq!(b"test_key2".as_slice(), into_req.keys.get(1).unwrap());
        assert_eq!(b"test_key3".as_slice(), into_req.keys.get(2).unwrap());
    }

    #[test]
    fn test_batch_get_response_trans() {
        let pb_res = PbBatchGetResponse {
            header: None,
            kvs: vec![PbKeyValue {
                key: b"test_key1".to_vec(),
                value: b"test_value1".to_vec(),
            }],
        };
        let res: BatchGetResponse = pb_res.try_into().unwrap();
        let kvs = res.kvs;
        assert_eq!(b"test_key1".as_slice(), kvs[0].key());
        assert_eq!(b"test_value1".as_slice(), kvs[0].value());
    }

    #[test]
    fn test_batch_put_request_trans() {
        let req = BatchPutRequest::new()
            .add_kv(b"test_key1".to_vec(), b"test_value1".to_vec())
            .add_kv(b"test_key2".to_vec(), b"test_value2".to_vec())
            .add_kv(b"test_key3".to_vec(), b"test_value3".to_vec())
            .with_prev_kv();

        let into_req: PbBatchPutRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(b"test_key1".to_vec(), into_req.kvs.first().unwrap().key);
        assert_eq!(b"test_key2".to_vec(), into_req.kvs.get(1).unwrap().key);
        assert_eq!(b"test_key3".to_vec(), into_req.kvs.get(2).unwrap().key);
        assert_eq!(b"test_value1".to_vec(), into_req.kvs.first().unwrap().value);
        assert_eq!(b"test_value2".to_vec(), into_req.kvs.get(1).unwrap().value);
        assert_eq!(b"test_value3".to_vec(), into_req.kvs.get(2).unwrap().value);
        assert!(into_req.prev_kv);
    }

    #[test]
    fn test_batch_put_response_trans() {
        let pb_res = PbBatchPutResponse {
            header: None,
            prev_kvs: vec![PbKeyValue {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }],
        };

        let mut res: BatchPutResponse = pb_res.try_into().unwrap();
        let kvs = res.take_prev_kvs();
        assert_eq!(b"k1".to_vec(), kvs[0].key().to_vec());
        assert_eq!(b"v1".to_vec(), kvs[0].value().to_vec());
    }

    #[test]
    fn test_batch_delete_request_trans() {
        let req = BatchDeleteRequest::new()
            .add_key(b"test_key1".to_vec())
            .add_key(b"test_key2".to_vec())
            .add_key(b"test_key3".to_vec())
            .with_prev_kv();

        let into_req: PbBatchDeleteRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(&b"test_key1".to_vec(), into_req.keys.first().unwrap());
        assert_eq!(&b"test_key2".to_vec(), into_req.keys.get(1).unwrap());
        assert_eq!(&b"test_key3".to_vec(), into_req.keys.get(2).unwrap());
        assert!(into_req.prev_kv);
    }

    #[test]
    fn test_batch_delete_response_trans() {
        let pb_res = PbBatchDeleteResponse {
            header: None,
            prev_kvs: vec![PbKeyValue {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }],
        };

        let res: BatchDeleteResponse = pb_res.try_into().unwrap();
        let kvs = res.prev_kvs;
        assert_eq!(b"k1".to_vec(), kvs[0].key().to_vec());
        assert_eq!(b"v1".to_vec(), kvs[0].value().to_vec());
    }

    #[test]
    fn test_compare_and_put_request_trans() {
        let (key, expect, value) = (
            b"test_key1".to_vec(),
            b"test_expect1".to_vec(),
            b"test_value1".to_vec(),
        );

        let req = CompareAndPutRequest::new()
            .with_key(key.clone())
            .with_expect(expect.clone())
            .with_value(value.clone());

        let into_req: PbCompareAndPutRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(key, into_req.key);
        assert_eq!(expect, into_req.expect);
        assert_eq!(value, into_req.value);
    }

    #[test]
    fn test_compare_and_put_response_trans() {
        let pb_res = PbCompareAndPutResponse {
            header: None,
            success: true,
            prev_kv: Some(PbKeyValue {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            }),
        };

        let mut res: CompareAndPutResponse = pb_res.try_into().unwrap();
        let mut kv = res.take_prev_kv().unwrap();
        assert_eq!(b"k1".to_vec(), kv.key().to_vec());
        assert_eq!(b"k1".to_vec(), kv.take_key());
        assert_eq!(b"v1".to_vec(), kv.value().to_vec());
        assert_eq!(b"v1".to_vec(), kv.take_value());
    }

    #[test]
    fn test_delete_range_request_trans() {
        let (key, range_end) = (b"test_key1".to_vec(), b"test_range_end1".to_vec());

        let req = DeleteRangeRequest::new()
            .with_range(key.clone(), range_end.clone())
            .with_prev_kv();

        let into_req: PbDeleteRangeRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(key, into_req.key);
        assert_eq!(range_end, into_req.range_end);
        assert!(into_req.prev_kv);
    }

    #[test]
    fn test_delete_prefix_request_trans() {
        let key = b"test_key1".to_vec();

        let req = DeleteRangeRequest::new()
            .with_prefix(key.clone())
            .with_prev_kv();

        let into_req: PbDeleteRangeRequest = req.into();
        assert!(into_req.header.is_none());
        assert_eq!(key, into_req.key);
        assert_eq!(b"test_key2".to_vec(), into_req.range_end);
        assert!(into_req.prev_kv);
    }

    #[test]
    fn test_delete_range_response_trans() {
        let pb_res = PbDeleteRangeResponse {
            header: None,
            deleted: 2,
            prev_kvs: vec![
                PbKeyValue {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
                PbKeyValue {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                },
            ],
        };

        let mut res: DeleteRangeResponse = pb_res.try_into().unwrap();
        assert_eq!(2, res.deleted());
        let mut kvs = res.take_prev_kvs();
        let kv0 = kvs.get_mut(0).unwrap();
        assert_eq!(b"k1".to_vec(), kv0.key().to_vec());
        assert_eq!(b"k1".to_vec(), kv0.take_key());
        assert_eq!(b"v1".to_vec(), kv0.value().to_vec());
        assert_eq!(b"v1".to_vec(), kv0.take_value());

        let kv1 = kvs.get_mut(1).unwrap();
        assert_eq!(b"k2".to_vec(), kv1.key().to_vec());
        assert_eq!(b"k2".to_vec(), kv1.take_key());
        assert_eq!(b"v2".to_vec(), kv1.value().to_vec());
        assert_eq!(b"v2".to_vec(), kv1.take_value());
    }
}
