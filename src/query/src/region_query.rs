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

use std::sync::Arc;

use api::v1::region::QueryRequest;
use async_trait::async_trait;
use common_recordbatch::SendableRecordBatchStream;

use crate::error::Result;

#[async_trait]
pub trait RegionQueryHandler: Send + Sync {
    async fn do_get(&self, request: QueryRequest) -> Result<SendableRecordBatchStream>;
}

pub type RegionQueryHandlerRef = Arc<dyn RegionQueryHandler>;
