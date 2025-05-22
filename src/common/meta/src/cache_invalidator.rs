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

use crate::error::Result;
use crate::flow_name::FlowName;
use crate::instruction::{CacheIdent, DropFlow};
use crate::key::flow::flow_info::FlowInfoKey;
use crate::key::flow::flow_name::FlowNameKey;
use crate::key::flow::flow_route::FlowRouteKey;
use crate::key::flow::flownode_flow::FlownodeFlowKey;
use crate::key::flow::table_flow::TableFlowKey;
use crate::key::schema_name::SchemaNameKey;
use crate::key::table_info::TableInfoKey;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteKey;
use crate::key::view_info::ViewInfoKey;
use crate::key::MetadataKey;

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
    async fn invalidate(&self, ctx: &Context, caches: &[CacheIdent]) -> Result<()>;
}

pub type CacheInvalidatorRef = Arc<dyn CacheInvalidator>;

pub struct DummyCacheInvalidator;

#[async_trait::async_trait]
impl CacheInvalidator for DummyCacheInvalidator {
    async fn invalidate(&self, _ctx: &Context, _caches: &[CacheIdent]) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> CacheInvalidator for T
where
    T: KvCacheInvalidator,
{
    async fn invalidate(&self, _ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        for cache in caches {
            match cache {
                CacheIdent::TableId(table_id) => {
                    let key = TableInfoKey::new(*table_id);
                    self.invalidate_key(&key.to_bytes()).await;

                    let key = TableRouteKey::new(*table_id);
                    self.invalidate_key(&key.to_bytes()).await;

                    let key = ViewInfoKey::new(*table_id);
                    self.invalidate_key(&key.to_bytes()).await;
                }
                CacheIdent::TableName(table_name) => {
                    let key: TableNameKey = table_name.into();
                    self.invalidate_key(&key.to_bytes()).await
                }
                CacheIdent::SchemaName(schema_name) => {
                    let key: SchemaNameKey = schema_name.into();
                    self.invalidate_key(&key.to_bytes()).await;
                }
                CacheIdent::CreateFlow(_) => {
                    // Do nothing
                }
                CacheIdent::DropFlow(DropFlow {
                    flow_id,
                    source_table_ids,
                    flow_part2node_id,
                }) => {
                    // invalidate flow route/flownode flow/table flow
                    let mut keys = Vec::with_capacity(
                        source_table_ids.len() * flow_part2node_id.len()
                            + flow_part2node_id.len() * 2,
                    );
                    for table_id in source_table_ids {
                        for (partition_id, node_id) in flow_part2node_id {
                            let key =
                                TableFlowKey::new(*table_id, *node_id, *flow_id, *partition_id)
                                    .to_bytes();
                            keys.push(key);
                        }
                    }

                    for (partition_id, node_id) in flow_part2node_id {
                        let key =
                            FlownodeFlowKey::new(*node_id, *flow_id, *partition_id).to_bytes();
                        keys.push(key);
                        let key = FlowRouteKey::new(*flow_id, *partition_id).to_bytes();
                        keys.push(key);
                    }

                    for key in keys {
                        self.invalidate_key(&key).await;
                    }
                }
                CacheIdent::FlowName(FlowName {
                    catalog_name,
                    flow_name,
                }) => {
                    let key = FlowNameKey::new(catalog_name, flow_name);
                    self.invalidate_key(&key.to_bytes()).await
                }
                CacheIdent::FlowId(flow_id) => {
                    let key = FlowInfoKey::new(*flow_id);
                    self.invalidate_key(&key.to_bytes()).await;
                }
            }
        }
        Ok(())
    }
}
