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

pub mod lock;
pub mod router;
mod store;
pub mod util;

use std::fmt::{Display, Formatter};

use api::v1::meta::{
    KeyValue as PbKeyValue, Peer as PbPeer, ResponseHeader as PbResponseHeader,
    TableName as PbTableName,
};
pub use router::{
    CreateRequest, Partition, Region, RouteRequest, RouteResponse, Table, TableRoute,
};
use serde::{Deserialize, Serialize};
pub use store::{
    BatchGetRequest, BatchGetResponse, BatchPutRequest, BatchPutResponse, CompareAndPutRequest,
    CompareAndPutResponse, DeleteRangeRequest, DeleteRangeResponse, MoveValueRequest,
    MoveValueResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};

#[derive(Debug, Clone)]
pub struct ResponseHeader(PbResponseHeader);

impl ResponseHeader {
    #[inline]
    pub(crate) fn new(header: PbResponseHeader) -> Self {
        Self(header)
    }

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

#[derive(Debug, Clone)]
pub struct KeyValue(PbKeyValue);

impl KeyValue {
    #[inline]
    pub(crate) fn new(kv: PbKeyValue) -> Self {
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

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct TableName {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
}

impl Display for TableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.catalog_name, self.schema_name, self.table_name
        )
    }
}

impl TableName {
    pub fn new(
        catalog_name: impl Into<String>,
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            schema_name: schema_name.into(),
            table_name: table_name.into(),
        }
    }
}

impl From<TableName> for PbTableName {
    fn from(tb: TableName) -> Self {
        Self {
            catalog_name: tb.catalog_name,
            schema_name: tb.schema_name,
            table_name: tb.table_name,
        }
    }
}

impl From<PbTableName> for TableName {
    fn from(tb: PbTableName) -> Self {
        Self {
            catalog_name: tb.catalog_name,
            schema_name: tb.schema_name,
            table_name: tb.table_name,
        }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Deserialize, Serialize)]
pub struct Peer {
    pub id: u64,
    pub addr: String,
}

impl From<PbPeer> for Peer {
    fn from(p: PbPeer) -> Self {
        Self {
            id: p.id,
            addr: p.addr,
        }
    }
}

impl Peer {
    pub fn new(id: u64, addr: impl Into<String>) -> Self {
        Self {
            id,
            addr: addr.into(),
        }
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
