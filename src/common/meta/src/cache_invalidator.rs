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

use common_telemetry::debug;
use table::metadata::TableId;

use crate::error::Result;
use crate::key::table_info::TableInfoKey;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteKey;
use crate::key::TableMetaKey;
use crate::table_name::TableName;

/// KvBackend cache invalidator
#[async_trait::async_trait]
pub trait KvCacheInvalidator: Send + Sync {
    async fn invalidate_key(&self, key: &[u8]);
}

pub type KvCacheInvalidatorRef = Arc<dyn KvCacheInvalidator>;

pub struct DummyKvCacheInvalidator;

#[async_trait::async_trait]
impl KvCacheInvalidator for DummyKvCacheInvalidator {
    async fn invalidate_key(&self, _key: &[u8]) {}
}

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

#[derive(Clone)]
pub struct TableMetadataCacheInvalidator(KvCacheInvalidatorRef);

impl TableMetadataCacheInvalidator {
    pub fn new(kv_cache_invalidator: KvCacheInvalidatorRef) -> Self {
        Self(kv_cache_invalidator)
    }
}

#[async_trait::async_trait]
impl CacheInvalidator for TableMetadataCacheInvalidator {
    async fn invalidate_table_name(&self, _ctx: &Context, table_name: TableName) -> Result<()> {
        let key: TableNameKey = (&table_name).into();

        self.0.invalidate_key(&key.as_raw_key()).await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        Ok(())
    }

    async fn invalidate_table_id(&self, _ctx: &Context, table_id: TableId) -> Result<()> {
        let key = TableInfoKey::new(table_id);
        self.0.invalidate_key(&key.as_raw_key()).await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        let key = &TableRouteKey { table_id };
        self.0.invalidate_key(&key.as_raw_key()).await;
        debug!(
            "invalidated cache key: {}",
            String::from_utf8_lossy(&key.as_raw_key())
        );

        Ok(())
    }
}
