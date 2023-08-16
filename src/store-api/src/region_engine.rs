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

//! Region Engine's definition

use std::sync::Arc;

use api::v1::QueryRequest;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;

use crate::region_request::RegionRequest;
use crate::storage::RegionId;

#[async_trait]
pub trait RegionEngine {
    /// Name of this engine
    fn name(&self) -> &str;

    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<Output, BoxedError>;

    async fn handle_query(
        &self,
        region_id: RegionId,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream, BoxedError>;

    async fn get_metadata(&self, region_id: RegionId) -> Result<SchemaRef, BoxedError>;
}

pub type RegionEngineRef = Arc<dyn RegionEngine>;
