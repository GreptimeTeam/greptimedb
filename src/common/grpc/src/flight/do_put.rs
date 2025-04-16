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

use arrow_flight::PutResult;
use common_base::AffectedRows;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{Error, SerdeJsonSnafu};

/// The metadata for "DoPut" requests and responses.
///
/// Currently, there's only a "request_id", for coordinating requests and responses in the streams.
/// Client can set a unique request id in this metadata, and the server will return the same id in
/// the corresponding response. In doing so, a client can know how to do with its pending requests.
#[derive(Serialize, Deserialize)]
pub struct DoPutMetadata {
    request_id: i64,
}

impl DoPutMetadata {
    pub fn new(request_id: i64) -> Self {
        Self { request_id }
    }

    pub fn request_id(&self) -> i64 {
        self.request_id
    }
}

/// The response in the "DoPut" returned stream.
#[derive(Serialize, Deserialize)]
pub struct DoPutResponse {
    /// The same "request_id" in the request; see the [DoPutMetadata].
    request_id: i64,
    /// The successfully ingested rows number.
    affected_rows: AffectedRows,
}

impl DoPutResponse {
    pub fn new(request_id: i64, affected_rows: AffectedRows) -> Self {
        Self {
            request_id,
            affected_rows,
        }
    }

    pub fn request_id(&self) -> i64 {
        self.request_id
    }

    pub fn affected_rows(&self) -> AffectedRows {
        self.affected_rows
    }
}

impl TryFrom<PutResult> for DoPutResponse {
    type Error = Error;

    fn try_from(value: PutResult) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value.app_metadata).context(SerdeJsonSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_do_put_metadata() {
        let serialized = r#"{"request_id":42}"#;
        let metadata = serde_json::from_str::<DoPutMetadata>(serialized).unwrap();
        assert_eq!(metadata.request_id(), 42);
    }

    #[test]
    fn test_serde_do_put_response() {
        let x = DoPutResponse::new(42, 88);
        let serialized = serde_json::to_string(&x).unwrap();
        assert_eq!(serialized, r#"{"request_id":42,"affected_rows":88}"#);
    }
}
