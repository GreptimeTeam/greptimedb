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

use anymap2::SendSyncAnyMap;
use futures::future::join_all;

use crate::cache_invalidator::{CacheInvalidator, Context};
use crate::error::Result;
use crate::instruction::CacheIdent;

pub type CacheRegistryRef = Arc<CacheRegistry>;
pub type LayeredCacheRegistryRef = Arc<LayeredCacheRegistry>;

/// [LayeredCacheRegistry] Builder.
#[derive(Default)]
pub struct LayeredCacheRegistryBuilder {
    registry: LayeredCacheRegistry,
}

impl LayeredCacheRegistryBuilder {
    /// Adds [CacheRegistry] into the next layer.
    ///
    /// During cache invalidation, [LayeredCacheRegistry] ensures sequential invalidation
    /// of each layer (after the previous layer).
    pub fn add_cache_registry(mut self, registry: CacheRegistry) -> Self {
        self.registry.layers.push(registry);

        self
    }

    /// Returns __cloned__ the value stored in the collection for the type `T`, if it exists.
    pub fn get<T: Send + Sync + Clone + 'static>(&self) -> Option<T> {
        self.registry.get()
    }

    /// Builds the [LayeredCacheRegistry]
    pub fn build(self) -> LayeredCacheRegistry {
        self.registry
    }
}

/// [LayeredCacheRegistry] invalidate caches sequentially from the first layer.
#[derive(Default)]
pub struct LayeredCacheRegistry {
    layers: Vec<CacheRegistry>,
}

#[async_trait::async_trait]
impl CacheInvalidator for LayeredCacheRegistry {
    async fn invalidate(&self, ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        let mut results = Vec::with_capacity(self.layers.len());
        for registry in &self.layers {
            results.push(registry.invalidate(ctx, caches).await);
        }
        results.into_iter().collect::<Result<Vec<_>>>().map(|_| ())
    }
}

impl LayeredCacheRegistry {
    /// Returns __cloned__ the value stored in the collection for the type `T`, if it exists.
    pub fn get<T: Send + Sync + Clone + 'static>(&self) -> Option<T> {
        for registry in &self.layers {
            if let Some(cache) = registry.get::<T>() {
                return Some(cache);
            }
        }

        None
    }
}

/// [CacheRegistryBuilder] provides ability of
/// - Register the `cache` which implements the [CacheInvalidator] trait into [CacheRegistry].
/// - Build a [CacheRegistry]
#[derive(Default)]
pub struct CacheRegistryBuilder {
    registry: CacheRegistry,
}

impl CacheRegistryBuilder {
    /// Adds the cache.
    pub fn add_cache<T: CacheInvalidator + 'static>(mut self, cache: Arc<T>) -> Self {
        self.registry.register(cache);
        self
    }

    /// Builds [CacheRegistry].
    pub fn build(self) -> CacheRegistry {
        self.registry
    }
}

/// [CacheRegistry] provides ability of
/// - Get a specific `cache`.
#[derive(Default)]
pub struct CacheRegistry {
    indexes: Vec<Arc<dyn CacheInvalidator>>,
    registry: SendSyncAnyMap,
}

#[async_trait::async_trait]
impl CacheInvalidator for CacheRegistry {
    async fn invalidate(&self, ctx: &Context, caches: &[CacheIdent]) -> Result<()> {
        let tasks = self
            .indexes
            .iter()
            .map(|invalidator| invalidator.invalidate(ctx, caches));
        join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
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
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::Arc;

    use moka::future::{Cache, CacheBuilder};

    use crate::cache::registry::CacheRegistryBuilder;
    use crate::cache::*;
    use crate::instruction::CacheIdent;

    fn always_true_filter(_: &CacheIdent) -> bool {
        true
    }

    fn test_cache(
        name: &str,
        invalidator: Invalidator<String, String, CacheIdent>,
    ) -> CacheContainer<String, String, CacheIdent> {
        let cache: Cache<String, String> = CacheBuilder::new(128).build();
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<String, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("hi".to_string())) })
        });

        CacheContainer::new(
            name.to_string(),
            cache,
            invalidator,
            init,
            always_true_filter,
        )
    }

    fn test_i32_cache(
        name: &str,
        invalidator: Invalidator<i32, String, CacheIdent>,
    ) -> CacheContainer<i32, String, CacheIdent> {
        let cache: Cache<i32, String> = CacheBuilder::new(128).build();
        let counter = Arc::new(AtomicI32::new(0));
        let moved_counter = counter.clone();
        let init: Initializer<i32, String> = Arc::new(move |_| {
            moved_counter.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(Some("foo".to_string())) })
        });

        CacheContainer::new(
            name.to_string(),
            cache,
            invalidator,
            init,
            always_true_filter,
        )
    }

    #[tokio::test]
    async fn test_register() {
        let builder = CacheRegistryBuilder::default();
        let invalidator: Invalidator<_, String, CacheIdent> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));
        let i32_cache = Arc::new(test_i32_cache("i32_cache", invalidator));
        let invalidator: Invalidator<_, String, CacheIdent> =
            Box::new(|_, _| Box::pin(async { Ok(()) }));
        let cache = Arc::new(test_cache("string_cache", invalidator));
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

    #[tokio::test]
    async fn test_layered_registry() {
        let builder = LayeredCacheRegistryBuilder::default();
        // 1st layer
        let counter = Arc::new(AtomicBool::new(false));
        let moved_counter = counter.clone();
        let invalidator: Invalidator<String, String, CacheIdent> = Box::new(move |_, _| {
            let counter = moved_counter.clone();
            Box::pin(async move {
                assert!(!counter.load(Ordering::Relaxed));
                counter.store(true, Ordering::Relaxed);
                Ok(())
            })
        });
        let cache = Arc::new(test_cache("string_cache", invalidator));
        let builder =
            builder.add_cache_registry(CacheRegistryBuilder::default().add_cache(cache).build());
        // 2nd layer
        let moved_counter = counter.clone();
        let invalidator: Invalidator<i32, String, CacheIdent> = Box::new(move |_, _| {
            let counter = moved_counter.clone();
            Box::pin(async move {
                assert!(counter.load(Ordering::Relaxed));
                Ok(())
            })
        });
        let i32_cache = Arc::new(test_i32_cache("i32_cache", invalidator));
        let builder = builder
            .add_cache_registry(CacheRegistryBuilder::default().add_cache(i32_cache).build());

        let registry = builder.build();
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
