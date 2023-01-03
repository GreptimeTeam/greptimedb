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

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use datatypes::schema::SchemaRef;

use crate::storage::chunk::ChunkReader;
use crate::storage::consts;
use crate::storage::requests::{GetRequest, ScanRequest};
use crate::storage::responses::{GetResponse, ScanResponse};

/// A consistent read-only view of region.
#[async_trait]
pub trait Snapshot: Send + Sync {
    type Error: ErrorExt + Send + Sync;
    type Reader: ChunkReader;

    fn schema(&self) -> &SchemaRef;

    async fn scan(
        &self,
        ctx: &ReadContext,
        request: ScanRequest,
    ) -> Result<ScanResponse<Self::Reader>, Self::Error>;

    async fn get(&self, ctx: &ReadContext, request: GetRequest)
        -> Result<GetResponse, Self::Error>;
}

/// Context for read.
#[derive(Debug, Clone)]
pub struct ReadContext {
    /// Suggested batch size of chunk.
    pub batch_size: usize,
}

impl Default for ReadContext {
    fn default() -> ReadContext {
        ReadContext {
            batch_size: consts::READ_BATCH_SIZE,
        }
    }
}
