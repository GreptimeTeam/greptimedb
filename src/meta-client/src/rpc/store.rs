use api::v1::meta::DeleteRangeRequest as PbDeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse as PbDeleteRangeResponse;
use api::v1::meta::PutRequest as PbPutRequest;
use api::v1::meta::PutResponse as PbPutResponse;
use api::v1::meta::RangeRequest as PbRangeRequest;
use api::v1::meta::RangeResponse as PbRangeResponse;

use super::util;
use super::KeyValue;
use super::ResponseHeader;
use crate::error;

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
    pub fn with_key_range(
        mut self,
        key: impl Into<Vec<u8>>,
        range_end: impl Into<Vec<u8>>,
    ) -> Self {
        self.key = key.into();
        self.range_end = range_end.into();
        self
    }

    /// Gets all keys prefixed with key.
    /// range_end is the key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
    #[inline]
    pub fn with_prefix(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self.range_end = util::get_prefix(&self.key);
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

#[derive(Debug, Clone)]
pub struct RangeResponse(PbRangeResponse);

impl TryFrom<PbRangeResponse> for RangeResponse {
    type Error = error::Error;

    fn try_from(pb: PbRangeResponse) -> Result<Self, Self::Error> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self::new(pb))
    }
}

impl RangeResponse {
    #[inline]
    pub fn new(res: PbRangeResponse) -> Self {
        Self(res)
    }

    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    #[inline]
    pub fn take_kvs(&mut self) -> Vec<KeyValue> {
        self.0.kvs.drain(..).map(KeyValue::new).collect()
    }

    #[inline]
    pub fn more(&self) -> bool {
        self.0.more
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

#[derive(Debug, Clone)]
pub struct PutResponse(PbPutResponse);

impl TryFrom<PbPutResponse> for PutResponse {
    type Error = error::Error;

    fn try_from(pb: PbPutResponse) -> Result<Self, Self::Error> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self::new(pb))
    }
}

impl PutResponse {
    #[inline]
    pub fn new(res: PbPutResponse) -> Self {
        Self(res)
    }

    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    #[inline]
    pub fn take_prev_kv(&mut self) -> Option<KeyValue> {
        self.0.prev_kv.take().map(KeyValue::new)
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

impl DeleteRangeRequest {
    #[inline]
    pub fn new() -> Self {
        Self {
            key: vec![],
            range_end: vec![],
            prev_kv: false,
        }
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
    pub fn with_key_range(
        mut self,
        key: impl Into<Vec<u8>>,
        range_end: impl Into<Vec<u8>>,
    ) -> Self {
        self.key = key.into();
        self.range_end = range_end.into();
        self
    }

    /// Deletes all keys prefixed with key.
    /// range_end is one bit larger than the given key.
    #[inline]
    pub fn with_prefix(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = key.into();
        self.range_end = util::get_prefix(&self.key);
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

#[derive(Debug, Clone)]
pub struct DeleteRangeResponse(PbDeleteRangeResponse);

impl TryFrom<PbDeleteRangeResponse> for DeleteRangeResponse {
    type Error = error::Error;

    fn try_from(pb: PbDeleteRangeResponse) -> Result<Self, Self::Error> {
        util::check_response_header(pb.header.as_ref())?;

        Ok(Self::new(pb))
    }
}

impl DeleteRangeResponse {
    #[inline]
    pub fn new(res: PbDeleteRangeResponse) -> Self {
        Self(res)
    }

    #[inline]
    pub fn take_header(&mut self) -> Option<ResponseHeader> {
        self.0.header.take().map(ResponseHeader::new)
    }

    pub fn deleted(&self) -> i64 {
        self.0.deleted
    }

    #[inline]
    pub fn take_prev_kvs(&mut self) -> Vec<KeyValue> {
        self.0.prev_kvs.drain(..).map(KeyValue::new).collect()
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::DeleteRangeRequest as PbDeleteRangeRequest;
    use api::v1::meta::DeleteRangeResponse as PbDeleteRangeResponse;
    use api::v1::meta::KeyValue as PbKeyValue;
    use api::v1::meta::PutRequest as PbPutRequest;
    use api::v1::meta::PutResponse as PbPutResponse;
    use api::v1::meta::RangeRequest as PbRangeRequest;
    use api::v1::meta::RangeResponse as PbRangeResponse;

    use super::*;

    #[test]
    fn test_range_request_trans() {
        let (key, range_end, limit) = (b"test_key1".to_vec(), b"test_range_end1".to_vec(), 1);

        let req = RangeRequest::new()
            .with_key_range(key.clone(), range_end.clone())
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

        let mut res = RangeResponse::new(pb_res);
        assert!(res.take_header().is_none());
        assert!(res.more());
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

        let mut res = PutResponse::new(pb_res);
        assert!(res.take_header().is_none());
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
            .with_key_range(key.clone(), range_end.clone())
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
        assert!(res.take_header().is_none());
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
