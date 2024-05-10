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

use crate::cache_invalidator::{CacheInvalidator, Context};
use crate::error::Result;
use crate::instruction::CacheIdent;

pub type CacheRegistryRef = Arc<CacheRegistry>;

/// [CacheRegistryBuilder] provides ability of
/// - Register the `cache` which implements the [CacheInvalidator] trait into [CacheRegistry].
/// - Build a [CacheRegistry]
#[derive(Default)]
pub struct CacheRegistryBuilder {
    registry: CacheRegistry,
}

impl CacheRegistryBuilder {
    pub fn add_cache<T: CacheInvalidator + 'static>(mut self, cache: Arc<T>) -> Self {
        self.registry.register(cache);
        self
    }

    pub fn build(self) -> CacheRegistry {
        self.registry
    }
}

/// [CacheRegistry] provides ability of
/// - Get a specific `cache`.
#[derive(Default)]
pub struct CacheRegistry {
    indexes: Vec<Arc<dyn CacheInvalidator>>,
    registry: anymap2::SendSyncAnyMap,
}

#[async_trait::async_trait]
impl CacheInvalidator for Arc<CacheRegistry> {
    async fn invalidate(&self, ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        for index in &self.indexes {
            index.invalidate(ctx, caches).await?;
        }
        Ok(())
    }
}

impl CacheRegistry {
    /// Sets the value stored in the collection for the type `T`.
    /// Returns true if the collection already had a value of type `T`
    fn register<T: CacheInvalidator + 'static>(&mut self, cache: Arc<T>) -> bool {
        self.indexes.push(cache.clone());
        self.registry.insert(cache).is_some()
    }

    /// Returns __cloned__ the value stored in the collection for the type `T`, if it exists.
    pub fn get<T: Send + Sync + Clone + 'static>(&self) -> Option<T> {
        self.registry.get().cloned()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    use moka::future::{Cache, CacheBuilder};

    use crate::cache::registry::CacheRegistryBuilder;
    use crate::cache::*;
    use crate::instruction::CacheIdent;

    fn test_cache(name: &str) -> CacheContainer<String, String, CacheIdent> {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let filter: TokenFilter<CacheIdent> = Box::new(|_| true);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });
        let invalidator: Invalidator<String, String, CacheIdent> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));

        CacheContainer::new(name.to_string(), cache, invalidator, init, filter)
    }

    fn test_i32_cache(name: &str) -> CacheContainer<i32, String, CacheIdent> {
        let cache: Cache<i32, String> = CacheBuilder::new(128).build();
        let filter: TokenFilter<CacheIdent> = Box::new(|_| true);
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<i32, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("foo".to_string())) })
        });
        let invalidator: Invalidator<i32, String, CacheIdent> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));

        CacheContainer::new(name.to_string(), cache, invalidator, init, filter)
    }

    #[tokio::test]
    async fn test_register() {
        let builder = CacheRegistryBuilder::default();
        let i32_cache = Arc::new(test_i32_cache("i32_cache"));
        let cache = Arc::new(test_cache("string_cache"));
        let registry = builder.add_cache(i32_cache).add_cache(cache).build();

        let cache = registry
            .get::<Arc<CacheContainer<i32, String, CacheIdent>>>()
            .unwrap();
        assert_eq!(cache.name(), "i32_cache");

        let cache = registry
            .get::<Arc<CacheContainer<String, String, CacheIdent>>>()
            .unwrap();
        assert_eq!(cache.name(), "string_cache");
    }
}
