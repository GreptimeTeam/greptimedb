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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use datatypes::timestamp::TimestampNanosecond;
use moka::future::Cache;

use crate::error::{CacheLoadSnafu, Result};
use crate::etl::Pipeline;
use crate::manager::PipelineVersion;
use crate::util::{generate_pipeline_cache_key, generate_pipeline_cache_key_suffix};

/// Pipeline table cache size.
const PIPELINES_CACHE_SIZE: u64 = 10000;

/// Pipeline cache is located on a separate file on purpose,
/// to encapsulate inner cache. Only public methods are exposed.
///
/// Entries are keyed by the *requested* schema rather than the schema the
/// pipeline is stored under, so that a lookup is a single key probe and can be
/// served by [`Cache::try_get_with`]. Resolving which stored schema a request
/// maps to happens in the loader, which is the authoritative path.
pub(crate) struct PipelineCache {
    pipelines: Cache<String, Arc<Pipeline>>,
    original_pipelines: Cache<String, PipelineContent>,
    /// If the pipeline table is invalid, we can use this cache to prevent failures when writing logs through the pipeline
    /// The failover cache never expires, but it will be updated when the pipelines cache is updated.
    failover_cache: Cache<String, PipelineContent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelineContent {
    pub name: String,
    pub content: String,
    pub version: TimestampNanosecond,
    pub schema: String,
}

impl PipelineCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            pipelines: Cache::builder()
                .max_capacity(PIPELINES_CACHE_SIZE)
                .time_to_live(ttl)
                .name("pipelines")
                .build(),
            original_pipelines: Cache::builder()
                .max_capacity(PIPELINES_CACHE_SIZE)
                .time_to_live(ttl)
                .name("original_pipelines")
                .build(),
            failover_cache: Cache::builder()
                .max_capacity(PIPELINES_CACHE_SIZE)
                .name("failover_cache")
                .build(),
        }
    }

    /// Get the compiled pipeline, running `init` only on a miss. Concurrent
    /// misses on the same key share one `init` call.
    pub(crate) async fn get_pipeline_with(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
        init: impl Future<Output = Result<Arc<Pipeline>>>,
    ) -> Result<Arc<Pipeline>> {
        let key = generate_pipeline_cache_key(schema, name, version);
        self.pipelines
            .try_get_with(key, init)
            .await
            .map_err(|error| CacheLoadSnafu { error }.build())
    }

    /// Get the pipeline definition, running `init` only on a miss. Concurrent
    /// misses on the same key share one `init` call.
    pub(crate) async fn get_pipeline_str_with(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
        init: impl Future<Output = Result<PipelineContent>>,
    ) -> Result<PipelineContent> {
        let key = generate_pipeline_cache_key(schema, name, version);
        self.original_pipelines
            .try_get_with(key, init)
            .await
            .map_err(|error| CacheLoadSnafu { error }.build())
    }

    pub(crate) async fn get_failover_cache(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Option<PipelineContent> {
        let key = generate_pipeline_cache_key(schema, name, version);
        self.failover_cache.get(&key).await
    }

    pub(crate) async fn insert_failover_cache(
        &self,
        schema: &str,
        content: PipelineContent,
        with_latest: bool,
    ) {
        for key in cache_keys(schema, &content, with_latest) {
            self.failover_cache.insert(key, content.clone()).await;
        }
    }

    /// Prime the caches with a pipeline that was just created, so the creating
    /// frontend does not have to read it back.
    pub(crate) async fn insert_pipeline(
        &self,
        schema: &str,
        content: PipelineContent,
        compiled: Arc<Pipeline>,
    ) {
        for key in cache_keys(schema, &content, true) {
            self.pipelines.insert(key.clone(), compiled.clone()).await;
            self.original_pipelines
                .insert(key.clone(), content.clone())
                .await;
            self.failover_cache.insert(key, content.clone()).await;
        }
    }

    // remove cache with version and latest in all schemas
    pub(crate) async fn remove_cache(&self, name: &str, version: PipelineVersion) {
        let version_suffix = generate_pipeline_cache_key_suffix(name, version);
        let latest_suffix = generate_pipeline_cache_key_suffix(name, None);

        let ks = self
            .pipelines
            .iter()
            .map(|(k, _)| k)
            .chain(self.original_pipelines.iter().map(|(k, _)| k))
            .chain(self.failover_cache.iter().map(|(k, _)| k))
            .filter(|k| k.ends_with(&version_suffix) || k.ends_with(&latest_suffix))
            .collect::<Vec<_>>();

        for k in ks {
            let k = k.as_str();
            self.pipelines.invalidate(k).await;
            self.original_pipelines.invalidate(k).await;
            self.failover_cache.invalidate(k).await;
        }
    }
}

/// The key under the pipeline's own version, plus the `latest` alias when the
/// pipeline is the newest one under that name.
fn cache_keys(schema: &str, content: &PipelineContent, with_latest: bool) -> Vec<String> {
    let mut keys = vec![generate_pipeline_cache_key(
        schema,
        &content.name,
        Some(content.version),
    )];
    if with_latest {
        keys.push(generate_pipeline_cache_key(schema, &content.name, None));
    }
    keys
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Barrier;

    use super::*;

    fn test_content() -> PipelineContent {
        PipelineContent {
            name: "p".to_string(),
            content: "processors:".to_string(),
            version: TimestampNanosecond::new(1),
            schema: String::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_misses_run_one_loader() {
        const CONCURRENCY: usize = 8;

        let cache = Arc::new(PipelineCache::new(Duration::from_secs(60)));
        let loads = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(CONCURRENCY));

        let handles = (0..CONCURRENCY)
            .map(|_| {
                let (cache, loads, barrier) = (cache.clone(), loads.clone(), barrier.clone());
                tokio::spawn(async move {
                    barrier.wait().await;
                    cache
                        .get_pipeline_str_with("db", "p", None, async {
                            loads.fetch_add(1, Ordering::SeqCst);
                            // Hold the loader open so every caller is waiting on it.
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            Ok(test_content())
                        })
                        .await
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            assert_eq!(handle.await.unwrap(), test_content());
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    /// A pipeline read through `get_pipeline_str` only populates
    /// `original_pipelines`, so deletion must sweep that cache too.
    #[tokio::test]
    async fn test_remove_cache_drops_definition_only_entries() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let content = test_content();

        let load = || async { Ok(content.clone()) };
        cache
            .get_pipeline_str_with("db", "p", None, load())
            .await
            .unwrap();

        cache.remove_cache("p", Some(content.version)).await;

        let loads = AtomicUsize::new(0);
        cache
            .get_pipeline_str_with("db", "p", None, async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(content.clone())
            })
            .await
            .unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }
}
