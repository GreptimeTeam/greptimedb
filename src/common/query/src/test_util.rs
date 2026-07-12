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

use datafusion::catalog::CatalogProviderList;
use datafusion::logical_expr::LogicalPlan;

use crate::error::Result;
use crate::logical_plan::SubstraitPlanDecoder;

/// Dummy [`SubstraitPlanDecoder`] for test.
pub struct DummyDecoder;

impl DummyDecoder {
    pub fn arc() -> Arc<Self> {
        Arc::new(DummyDecoder)
    }
}

#[async_trait::async_trait]
impl SubstraitPlanDecoder for DummyDecoder {
    async fn decode(
        &self,
        _message: bytes::Bytes,
        _catalog_list: Arc<dyn CatalogProviderList>,
        _optimize: bool,
    ) -> Result<LogicalPlan> {
        unreachable!()
    }
}
