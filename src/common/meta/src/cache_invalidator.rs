// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use table::metadata::TableId;

use crate::error::Result;
use crate::table_name::TableName;

/// Places context of invalidating cache. e.g., span id, trace id etc.
#[derive(Default)]
pub struct Context {
    pub subject: Option<String>,
}

#[async_trait::async_trait]
pub trait CacheInvalidator: Send + Sync {
    // Invalidates table cache
    async fn invalidate_table_id(&self, ctx: &Context, table_id: TableId) -> Result<()>;

    async fn invalidate_table_name(&self, ctx: &Context, table_name: TableName) -> Result<()>;
}

pub type CacheInvalidatorRef = Arc<dyn CacheInvalidator>;

pub struct DummyCacheInvalidator;

#[async_trait::async_trait]
impl CacheInvalidator for DummyCacheInvalidator {
    async fn invalidate_table_id(&self, _ctx: &Context, _table_id: TableId) -> Result<()> {
        Ok(())
    }

    async fn invalidate_table_name(&self, _ctx: &Context, _table_name: TableName) -> Result<()> {
        Ok(())
    }
}
