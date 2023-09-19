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

pub use client::{CachedMetaKvBackend, MetaKvBackend};

mod client;
mod manager;

#[cfg(feature = "testing")]
pub mod mock;
pub use manager::KvBackendCatalogManager;

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
