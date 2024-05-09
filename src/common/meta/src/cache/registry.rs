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

use std::sync::{Arc, RwLock};

use crate::cache_invalidator::{CacheInvalidator, Context};
use crate::error::Result;
use crate::instruction::CacheIdent;

pub type CacheRegistryRef = Arc<CacheRegistry>;

/// [CacheRegistry] provides ability of
/// - Get a specific [CacheContainer](crate::cache::CacheContainer).
/// - Register the [CacheContainer](crate::cache::CacheContainer) which implements the [CacheInvalidator] trait.
#[derive(Default)]
pub struct CacheRegistry {
    inner: RwLock<CacheRegistryInner>,
}

#[derive(Default)]
struct CacheRegistryInner {
    indexes: Vec<Arc<dyn CacheInvalidator>>,
    registry: anymap2::SendSyncAnyMap,
}

#[async_trait::async_trait]
impl CacheInvalidator for Arc<CacheRegistry> {
    async fn invalidate(&self, ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        let indexes = {
            let inner = self.inner.read().unwrap();
            inner.indexes.clone()
        };
        for index in indexes {
            index.invalidate(ctx, caches).await?;
        }
        Ok(())
    }
}

impl CacheRegistry {
    /// Sets the value stored in the collection for the type `T`.
    /// Returns true if the collection already had a value of type `T`
    pub fn register<T: CacheInvalidator + 'static>(&self, cache: Arc<T>) -> bool {
        let mut inner = self.inner.write().unwrap();
        inner.indexes.push(cache.clone());
        inner.registry.insert(cache).is_some()
    }

    /// Returns __cloned__ the value stored in the collection for the type `T`, if it exists.
    pub fn get<T: Send + Sync + Clone + 'static>(&self) -> Option<T> {
        let inner = self.inner.read().unwrap();
        inner.registry.get().cloned()
    }

    /// Sets the value stored in the collection for the type `T` if not exists.
    /// Returns stored value or existing value of type `T`.
    pub fn get_or_register<T: CacheInvalidator + 'static>(&self, cache: Arc<T>) -> Arc<T> {
        let mut inner = self.inner.write().unwrap();
        if let Some(cache) = inner.registry.get::<Arc<T>>() {
            cache.clone()
        } else {
            inner.indexes.push(cache.clone());
            inner.registry.insert(cache.clone());
            cache
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    use moka::future::{Cache, CacheBuilder};

    use crate::cache::registry::CacheRegistry;
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

    fn test_i32_cache() -> CacheContainer<i32, String, CacheIdent> {
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

        CacheContainer::new("test".to_string(), cache, invalidator, init, filter)
    }

    #[tokio::test]
    async fn test_register() {
        let registry = CacheRegistry::default();
        let cache = Arc::new(test_cache("test"));
        assert!(!registry.register(cache.clone()));
        assert!(registry.register(cache.clone()));
        let cache = Arc::new(test_cache("test"));
        assert!(registry.register(cache.clone()));
        let cache = Arc::new(test_i32_cache());
        assert!(!registry.register(cache.clone()));
        assert!(registry.register(cache.clone()));

        let cache = registry
            .get::<Arc<CacheContainer<i32, String, CacheIdent>>>()
            .unwrap();
        assert_eq!(cache.get(1024).await.unwrap().unwrap(), "foo");
    }

    #[tokio::test]
    async fn test_get_or_register() {
        let registry = CacheRegistry::default();
        let cache = Arc::new(test_cache("test"));
        registry.get_or_register(cache);
        let another_cache = Arc::new(test_cache("yet another"));
        let registered = registry.get_or_register(another_cache);
        assert_eq!(registered.name(), "test");
    }
}
