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

pub mod ddl;
pub mod lock;
pub mod router;
pub mod store;
pub mod util;

use std::fmt::{Display, Formatter};

use api::v1::meta::{KeyValue as PbKeyValue, ResponseHeader as PbResponseHeader};

#[derive(Debug, Clone)]
pub struct ResponseHeader(PbResponseHeader);

impl ResponseHeader {
    #[inline]
    pub fn protocol_version(&self) -> u64 {
        self.0.protocol_version
    }

    #[inline]
    pub fn cluster_id(&self) -> u64 {
        self.0.cluster_id
    }

    #[inline]
    pub fn error_code(&self) -> i32 {
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

#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl From<KeyValue> for PbKeyValue {
    fn from(kv: KeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
        }
    }
}

impl From<etcd_client::KeyValue> for KeyValue {
    fn from(kv: etcd_client::KeyValue) -> Self {
        Self {
            key: kv.key().to_vec(),
            value: kv.value().to_vec(),
        }
    }
}

impl From<KeyValue> for (Vec<u8>, Vec<u8>) {
    fn from(kv: KeyValue) -> Self {
        (kv.key, kv.value)
    }
}

impl From<(Vec<u8>, Vec<u8>)> for KeyValue {
    fn from(kv: (Vec<u8>, Vec<u8>)) -> Self {
        Self {
            key: kv.0,
            value: kv.1,
        }
    }
}

impl Display for KeyValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({}, {})",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value)
        )
    }
}

impl KeyValue {
    #[inline]
    pub fn new(kv: PbKeyValue) -> Self {
        Self {
            key: kv.key,
            value: kv.value,
        }
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    #[inline]
    pub fn take_key(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.key)
    }

    #[inline]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    #[inline]
    pub fn take_value(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.value)
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

        let header = ResponseHeader(pb_header);
        assert_eq!(101, header.protocol_version());
        assert_eq!(1, header.cluster_id());
        assert_eq!(100, header.error_code());
        assert_eq!("test".to_string(), header.error_msg());
    }
}
