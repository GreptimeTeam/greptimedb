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

use common_frontend::metrics;
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
/// `pipelines` and `original_pipelines` are keyed by the *requested* schema so
/// a lookup is a single key probe through [`Cache::entry`];
/// resolving it to a stored schema is the loader's job. `failover_cache` has no
/// loader and keeps the stored-schema key.
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

    /// Concurrent misses share one `init` call; successful waiters count as hits.
    pub(crate) async fn get_pipeline_with(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
        init: impl Future<Output = Result<Arc<Pipeline>>>,
    ) -> Result<Arc<Pipeline>> {
        let key = generate_pipeline_cache_key(schema, name, version);
        let entry = self.pipelines.entry(key).or_try_insert_with(init).await;
        metrics::record_cache_lookup(
            "pipeline",
            entry.as_ref().is_ok_and(|entry| !entry.is_fresh()),
        );
        entry
            .map(|entry| entry.into_value())
            .map_err(|error| CacheLoadSnafu { error }.build())
    }

    /// Concurrent misses share one `init` call; successful waiters count as hits.
    pub(crate) async fn get_pipeline_str_with(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
        init: impl Future<Output = Result<PipelineContent>>,
    ) -> Result<PipelineContent> {
        let key = generate_pipeline_cache_key(schema, name, version);
        let entry = self
            .original_pipelines
            .entry(key)
            .or_try_insert_with(init)
            .await;
        metrics::record_cache_lookup(
            "pipeline_original",
            entry.as_ref().is_ok_and(|entry| !entry.is_fresh()),
        );
        entry
            .map(|entry| entry.into_value())
            .map_err(|error| CacheLoadSnafu { error }.build())
    }

    /// Resolves across schemas, unlike the loaded caches: a pipeline stored
    /// under the empty schema is reachable from any schema.
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
                metrics::record_cache_lookup("pipeline_failover", true);
                return Ok(Some(content));
            }
        }

        // Stored under some other schema; unambiguous only if exactly one has it.
        let suffix = generate_pipeline_cache_key_suffix(name, version);
        let mut found = self
            .failover_cache
            .iter()
            .filter(|(k, _)| k.ends_with(&suffix))
            .collect::<Vec<_>>();

        let result = match found.len() {
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
        };
        metrics::record_cache_lookup(
            "pipeline_failover",
            result.as_ref().is_ok_and(|content| content.is_some()),
        );
        result
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

    /// Dropping the stale `latest` aliases also clears the failover entries, so
    /// the new version is written back: an outage before the first read-back
    /// would otherwise have nothing to fall back on.
    pub(crate) async fn on_pipeline_created(&self, content: PipelineContent) {
        self.invalidate(&content.name, None).await;
        self.insert_failover_cache(content, true).await;
    }

    /// Sweeps every schema and all three caches: the `latest` alias always,
    /// plus `version` when given.
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

    use common_frontend::metrics::{CACHE_HIT, CACHE_MISS};
    use tokio::sync::Notify;

    use super::*;

    /// Stored under the empty schema, i.e. visible from every schema.
    fn content_at(version: i64) -> PipelineContent {
        PipelineContent {
            name: "p".to_string(),
            content: "transform:".to_string(),
            version: TimestampNanosecond::new(version),
            schema: EMPTY_SCHEMA_NAME.to_string(),
        }
    }

    #[tokio::test]
    async fn test_concurrent_misses_run_one_loader() {
        const CONCURRENCY: usize = 8;

        let hits = CACHE_HIT.with_label_values(&["pipeline_original"]);
        let misses = CACHE_MISS.with_label_values(&["pipeline_original"]);
        let before = (hits.get(), misses.get());
        let cache = PipelineCache::new(Duration::from_secs(60));
        let loads = AtomicUsize::new(0);
        let release = Notify::new();
        let mut requests = (0..CONCURRENCY)
            .map(|_| {
                Box::pin(cache.get_pipeline_str_with("db", "p", None, async {
                    loads.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(content_at(1))
                }))
            })
            .collect::<Vec<_>>();
        for request in &mut requests {
            assert!(futures::poll!(request.as_mut()).is_pending());
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        release.notify_waiters();
        for request in requests {
            assert_eq!(request.await.unwrap(), content_at(1));
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(
            (hits.get(), misses.get()),
            (before.0 + CONCURRENCY as u64 - 1, before.1 + 1)
        );
    }

    #[tokio::test]
    async fn test_pipeline_lookup_metrics() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        for compiled in [false, true] {
            let cache_type = if compiled {
                "pipeline"
            } else {
                "pipeline_original"
            };
            let hits = CACHE_HIT.with_label_values(&[cache_type]);
            let misses = CACHE_MISS.with_label_values(&[cache_type]);
            let before = (hits.get(), misses.get());
            let load = async |fail| {
                if compiled {
                    cache
                        .get_pipeline_with("db", "p", None, async {
                            if fail {
                                return Err(crate::error::PipelineNotFoundSnafu {
                                    name: "p",
                                    version: None,
                                }
                                .build());
                            }
                            Ok(Arc::new(
                                crate::manager::table::PipelineTable::compile_pipeline(
                                    "transform:\n  - fields: [message]\n    type: string",
                                )?,
                            ))
                        })
                        .await
                        .map(|_| ())
                } else {
                    cache
                        .get_pipeline_str_with("db", "p", None, async {
                            if fail {
                                return Err(crate::error::PipelineNotFoundSnafu {
                                    name: "p",
                                    version: None,
                                }
                                .build());
                            }
                            Ok(content_at(1))
                        })
                        .await
                        .map(|_| ())
                }
            };
            assert!(load(true).await.is_err());
            load(false).await.unwrap();
            // A warm lookup must not evaluate the failing initializer.
            load(true).await.unwrap();
            assert_eq!((hits.get(), misses.get()), (before.0 + 1, before.1 + 2));
        }
    }

    #[tokio::test]
    async fn test_failover_lookup_metrics() {
        let hits = CACHE_HIT.with_label_values(&["pipeline_failover"]);
        let misses = CACHE_MISS.with_label_values(&["pipeline_failover"]);
        let before = (hits.get(), misses.get());
        let cache = PipelineCache::new(Duration::from_secs(60));
        assert!(
            cache
                .get_failover_cache("db", "p", None)
                .await
                .unwrap()
                .is_none()
        );
        let local = PipelineContent {
            schema: "a".into(),
            ..content_at(1)
        };
        cache.insert_failover_cache(local.clone(), true).await;
        // Direct schema lookup and cross-schema fallback each count one hit.
        assert_eq!(
            cache.get_failover_cache("a", "p", None).await.unwrap(),
            Some(local.clone())
        );
        assert_eq!(
            cache.get_failover_cache("db", "p", None).await.unwrap(),
            Some(local)
        );
        cache
            .insert_failover_cache(
                PipelineContent {
                    schema: "b".into(),
                    ..content_at(2)
                },
                true,
            )
            .await;
        assert!(cache.get_failover_cache("db", "p", None).await.is_err());
        cache.insert_failover_cache(content_at(3), true).await;
        assert_eq!(
            cache.get_failover_cache("db", "p", None).await.unwrap(),
            Some(content_at(3))
        );
        assert_eq!((hits.get(), misses.get()), (before.0 + 3, before.1 + 2));
    }

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

    #[tokio::test]
    async fn test_create_drops_stale_latest_and_primes_failover() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let v2 = content_at(2);

        cache
            .get_pipeline_str_with("a", "p", None, async { Ok(content_at(1)) })
            .await
            .unwrap();
        cache.insert_failover_cache(content_at(1), true).await;

        cache.on_pipeline_created(v2.clone()).await;

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

        let failover = cache.get_failover_cache("b", "p", None).await.unwrap();
        assert_eq!(failover.map(|c| c.version), Some(v2.version));
    }

    #[tokio::test]
    async fn test_failover_serves_global_pipeline_to_unwarmed_schema() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let content = content_at(1);

        cache.insert_failover_cache(content.clone(), true).await;

        let found = cache.get_failover_cache("b", "p", None).await.unwrap();
        assert_eq!(found, Some(content.clone()));

        // A same-named pipeline under another schema must not shadow the global one.
        let schema_local = PipelineContent {
            schema: "x".to_string(),
            ..content_at(2)
        };
        cache.insert_failover_cache(schema_local, true).await;

        let found = cache.get_failover_cache("b", "p", None).await.unwrap();
        assert_eq!(found, Some(content));
    }
}
