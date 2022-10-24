mod store;

use api::v1::meta::KeyValue as PbKeyValue;
use api::v1::meta::ResponseHeader as PbResponseHeader;
pub use store::DeleteRangeRequest;
pub use store::DeleteRangeResponse;
pub use store::PutRequest;
pub use store::PutResponse;
pub use store::RangeRequest;
pub use store::RangeResponse;

#[derive(Debug, Clone)]
pub struct ResponseHeader(PbResponseHeader);

impl ResponseHeader {
    #[inline]
    pub(crate) const fn new(header: PbResponseHeader) -> Self {
        Self(header)
    }

    #[inline]
    pub const fn protocol_version(&self) -> u64 {
        self.0.protocol_version
    }

    #[inline]
    pub const fn cluster_id(&self) -> u64 {
        self.0.cluster_id
    }

    #[inline]
    pub const fn error_code(&self) -> i32 {
        match self.0.error.as_ref() {
            Some(err) => err.code,
            None => 0,
        }
    }

    #[inline]
    pub fn error_msg(&self) -> String {
        match self.0.error.as_ref() {
            Some(err) => err.err_msg.clone(),
            None => "None".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyValue(PbKeyValue);

impl KeyValue {
    #[inline]
    pub(crate) const fn new(kv: PbKeyValue) -> Self {
        Self(kv)
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.0.key
    }

    #[inline]
    pub fn take_key(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.0.key)
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        &self.0.value
    }

    #[inline]
    pub fn take_value(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.0.value)
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::{Error, ResponseHeader as PbResponseHeader};

    use super::*;

    #[test]
    fn test_response_header_trans() {
        let pb_header = PbResponseHeader {
            protocol_version: 101,
            cluster_id: 1,
            error: Some(Error {
                code: 100,
                err_msg: "test".to_string(),
            }),
        };

        let header = ResponseHeader::new(pb_header);
        assert_eq!(101, header.protocol_version());
        assert_eq!(1, header.cluster_id());
        assert_eq!(100, header.error_code());
        assert_eq!("test".to_string(), header.error_msg());
    }
}
