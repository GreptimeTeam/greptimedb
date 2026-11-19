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

mod table_flownode;
pub use table_flownode::{
    TableFlownodeSetCache, TableFlownodeSetCacheRef, new_table_flownode_set_cache,
};

use std::sync::Arc;

use moka::future::Cache;

use crate::cache::CacheContainer;
use crate::instruction::CacheIdent;
use crate::key::FlowId;
use crate::key::flow::flow_info::{FlowInfoManager, FlowInfoValue};
use crate::kv_backend::KvBackendRef;

pub type FlowInfoCache = CacheContainer<FlowId, Arc<FlowInfoValue>, CacheIdent>;
pub type FlowInfoCacheRef = Arc<FlowInfoCache>;

pub fn new_flow_info_cache(
    name: String,
    cache: Cache<(usize, FlowId), Arc<FlowInfoValue>>,
    backend: KvBackendRef,
) -> FlowInfoCache {
    let manager = Arc::new(FlowInfoManager::new(backend));
    let single = manager.clone();
    CacheContainer::new_coordinated(
        name,
        cache,
        Arc::new(move |id| {
            let manager = single.clone();
            Box::pin(async move { Ok(manager.get(*id).await?.map(Arc::new)) })
        }),
        |token| match token {
            CacheIdent::FlowId(id) => Some(id),
            _ => None,
        },
    )
    .with_batch_initializer(Arc::new(move |ids| {
        let manager = manager.clone();
        Box::pin(async move {
            Ok(manager
                .batch_get(&ids)
                .await?
                .into_iter()
                .map(|(id, info)| (id, Arc::new(info)))
                .collect())
        })
    }))
}
