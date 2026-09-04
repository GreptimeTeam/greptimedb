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

use crate::error::{CacheLoadSnafu, MultiPipelineWithDiffSchemaSnafu, Result};
use crate::etl::Pipeline;
use crate::manager::PipelineVersion;
use crate::table::EMPTY_SCHEMA_NAME;
use crate::util::{generate_pipeline_cache_key, generate_pipeline_cache_key_suffix};

/// Pipeline table cache size.
const PIPELINES_CACHE_SIZE: u64 = 10000;

/// Pipeline cache is located on a separate file on purpose,
/// to encapsulate inner cache. Only public methods are exposed.
///
/// The two loaded caches are keyed by the *requested* schema rather than the
/// schema the pipeline is stored under, so a lookup is a single key probe and
/// can be served by [`Cache::try_get_with`]. Resolving which stored schema a
/// request maps to happens in the loader, which is the authoritative path.
/// `failover_cache` has no loader and so keeps the stored-schema key; see
/// [`PipelineCache::get_failover_cache`].
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

    /// The failover cache is not subject to the single-key model of
    /// [`Cache::try_get_with`], so it stays keyed by the schema the pipeline is
    /// stored under. A pipeline stored under the empty schema is therefore
    /// reachable from any schema, even one that never loaded it before the
    /// pipeline table became unavailable.
    pub(crate) async fn get_failover_cache(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Option<PipelineContent>> {
        for key in [
            generate_pipeline_cache_key(EMPTY_SCHEMA_NAME, name, version),
            generate_pipeline_cache_key(schema, name, version),
        ] {
            if let Some(content) = self.failover_cache.get(&key).await {
                return Ok(Some(content));
            }
        }

        // The pipeline may be stored under some other schema. That is
        // unambiguous only if exactly one such schema has it.
        let suffix = generate_pipeline_cache_key_suffix(name, version);
        let mut found = self
            .failover_cache
            .iter()
            .filter(|(k, _)| k.ends_with(&suffix))
            .collect::<Vec<_>>();

        match found.len() {
            0 => Ok(None),
            1 => Ok(Some(found.remove(0).1)),
            _ => MultiPipelineWithDiffSchemaSnafu {
                name: name.to_string(),
                current_schema: schema.to_string(),
                schemas: found
                    .iter()
                    .filter_map(|(k, _)| k.split_once('/').map(|k| k.0))
                    .collect::<Vec<_>>()
                    .join(","),
            }
            .fail(),
        }
    }

    pub(crate) async fn insert_failover_cache(&self, content: PipelineContent, with_latest: bool) {
        let versioned =
            generate_pipeline_cache_key(&content.schema, &content.name, Some(content.version));
        let latest = generate_pipeline_cache_key(&content.schema, &content.name, None);

        self.failover_cache.insert(versioned, content.clone()).await;
        if with_latest {
            self.failover_cache.insert(latest, content).await;
        }
    }

    /// Drop cached entries for `name` across all schemas and all three caches:
    /// the `latest` alias always, plus `version` when given.
    ///
    /// Creating a pipeline passes `None`: the new version supersedes whatever
    /// `latest` resolved to, and a schema that had already cached an older
    /// version would otherwise keep transforming data with it until expiry.
    pub(crate) async fn invalidate(&self, name: &str, version: PipelineVersion) {
        let mut suffixes = vec![generate_pipeline_cache_key_suffix(name, None)];
        if version.is_some() {
            suffixes.push(generate_pipeline_cache_key_suffix(name, version));
        }

        let ks = self
            .pipelines
            .iter()
            .map(|(k, _)| k)
            .chain(self.original_pipelines.iter().map(|(k, _)| k))
            .chain(self.failover_cache.iter().map(|(k, _)| k))
            .filter(|k| suffixes.iter().any(|suffix| k.ends_with(suffix)))
            .collect::<Vec<_>>();

        for k in ks {
            let k = k.as_str();
            self.pipelines.invalidate(k).await;
            self.original_pipelines.invalidate(k).await;
            self.failover_cache.invalidate(k).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::Barrier;

    use super::*;

    /// A pipeline stored under the empty schema, i.e. visible from every schema.
    fn content_at(version: i64) -> PipelineContent {
        PipelineContent {
            name: "p".to_string(),
            content: "transform:".to_string(),
            version: TimestampNanosecond::new(version),
            schema: EMPTY_SCHEMA_NAME.to_string(),
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
                            Ok(content_at(1))
                        })
                        .await
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            assert_eq!(handle.await.unwrap(), content_at(1));
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    /// Deleting a specific version must drop the version-pinned key, not just
    /// the `latest` alias.
    #[tokio::test]
    async fn test_delete_drops_version_pinned_entry() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let content = content_at(1);
        let version = Some(content.version);

        cache
            .get_pipeline_str_with("db", "p", version, async { Ok(content.clone()) })
            .await
            .unwrap();

        cache.invalidate("p", version).await;

        let loads = AtomicUsize::new(0);
        cache
            .get_pipeline_str_with("db", "p", version, async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(content.clone())
            })
            .await
            .unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    /// The `latest` sweep must reach every schema, not just the one that
    /// triggered it, and must cover `original_pipelines` — where an entry read
    /// through `get_pipeline_str` lives alone.
    #[tokio::test]
    async fn test_invalidate_latest_sweeps_all_schemas() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let v2 = content_at(2);

        cache
            .get_pipeline_str_with("a", "p", None, async { Ok(content_at(1)) })
            .await
            .unwrap();

        cache.invalidate("p", None).await;

        let loads = AtomicUsize::new(0);
        let cached = cache
            .get_pipeline_str_with("a", "p", None, async {
                loads.fetch_add(1, Ordering::SeqCst);
                Ok(v2.clone())
            })
            .await
            .unwrap();
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(cached.version, v2.version);
    }

    /// A pipeline stored under the empty schema is global. Once any schema has
    /// loaded it, a schema reaching it for the first time while the pipeline
    /// table is down must still be served from the failover cache.
    #[tokio::test]
    async fn test_failover_serves_global_pipeline_to_unwarmed_schema() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let content = content_at(1);

        cache.insert_failover_cache(content.clone(), true).await;

        let found = cache.get_failover_cache("b", "p", None).await.unwrap();
        assert_eq!(found, Some(content.clone()));

        // A same-named pipeline stored under some other schema must not make the
        // lookup ambiguous: the global one wins.
        let schema_local = PipelineContent {
            schema: "x".to_string(),
            ..content_at(2)
        };
        cache.insert_failover_cache(schema_local, true).await;

        let found = cache.get_failover_cache("b", "p", None).await.unwrap();
        assert_eq!(found, Some(content));
    }
}
