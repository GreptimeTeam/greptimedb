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
use std::time::Duration;

use datatypes::timestamp::TimestampNanosecond;
use moka::sync::Cache;

use crate::error::{MultiPipelineWithDiffSchemaSnafu, Result};
use crate::etl::Pipeline;
use crate::manager::PipelineVersion;
use crate::table::EMPTY_SCHEMA_NAME;
use crate::util::{generate_pipeline_cache_key, generate_pipeline_cache_key_suffix};

/// Pipeline table cache size.
const PIPELINES_CACHE_SIZE: u64 = 10000;
/// Pipeline table cache time to live.
const PIPELINES_CACHE_TTL: Duration = Duration::from_secs(10);

/// Pipeline cache is located on a separate file on purpose,
/// to encapsulate inner cache. Only public methods are exposed.
pub(crate) struct PipelineCache {
    pipelines: Cache<String, Arc<Pipeline>>,
    original_pipelines: Cache<String, (String, TimestampNanosecond)>,
}

impl PipelineCache {
    pub(crate) fn new() -> Self {
        Self {
            pipelines: Cache::builder()
                .max_capacity(PIPELINES_CACHE_SIZE)
                .time_to_live(PIPELINES_CACHE_TTL)
                .build(),
            original_pipelines: Cache::builder()
                .max_capacity(PIPELINES_CACHE_SIZE)
                .time_to_live(PIPELINES_CACHE_TTL)
                .build(),
        }
    }

    pub(crate) fn insert_pipeline_cache(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
        pipeline: Arc<Pipeline>,
        with_latest: bool,
    ) {
        insert_cache_generic(
            &self.pipelines,
            schema,
            name,
            version,
            pipeline,
            with_latest,
        );
    }

    pub(crate) fn insert_pipeline_str_cache(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
        pipeline: (String, TimestampNanosecond),
        with_latest: bool,
    ) {
        insert_cache_generic(
            &self.original_pipelines,
            schema,
            name,
            version,
            pipeline,
            with_latest,
        );
    }

    pub(crate) fn get_pipeline_cache(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Option<Arc<Pipeline>>> {
        get_cache_generic(&self.pipelines, schema, name, version)
    }

    pub(crate) fn get_pipeline_str_cache(
        &self,
        schema: &str,
        name: &str,
        version: PipelineVersion,
    ) -> Result<Option<(String, TimestampNanosecond)>> {
        get_cache_generic(&self.original_pipelines, schema, name, version)
    }

    // remove cache with version and latest in all schemas
    pub(crate) fn remove_cache(&self, name: &str, version: PipelineVersion) {
        let version_suffix = generate_pipeline_cache_key_suffix(name, version);
        let latest_suffix = generate_pipeline_cache_key_suffix(name, None);

        let ks = self
            .pipelines
            .iter()
            .filter_map(|(k, _)| {
                if k.ends_with(&version_suffix) || k.ends_with(&latest_suffix) {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for k in ks {
            let k = k.as_str();
            self.pipelines.remove(k);
            self.original_pipelines.remove(k);
        }
    }
}

fn insert_cache_generic<T: Clone + Send + Sync + 'static>(
    cache: &Cache<String, T>,
    schema: &str,
    name: &str,
    version: PipelineVersion,
    value: T,
    with_latest: bool,
) {
    let k = generate_pipeline_cache_key(schema, name, version);
    cache.insert(k, value.clone());
    if with_latest {
        let k = generate_pipeline_cache_key(schema, name, None);
        cache.insert(k, value);
    }
}

fn get_cache_generic<T: Clone + Send + Sync + 'static>(
    cache: &Cache<String, T>,
    schema: &str,
    name: &str,
    version: PipelineVersion,
) -> Result<Option<T>> {
    // lets try empty schema first
    let k = generate_pipeline_cache_key(EMPTY_SCHEMA_NAME, name, version);
    if let Some(value) = cache.get(&k) {
        return Ok(Some(value));
    }
    // use input schema
    let k = generate_pipeline_cache_key(schema, name, version);
    if let Some(value) = cache.get(&k) {
        return Ok(Some(value));
    }

    // try all schemas
    let suffix_key = generate_pipeline_cache_key_suffix(name, version);
    let mut ks = cache
        .iter()
        .filter(|e| e.0.ends_with(&suffix_key))
        .collect::<Vec<_>>();

    match ks.len() {
        0 => Ok(None),
        1 => Ok(Some(ks.remove(0).1)),
        _ => MultiPipelineWithDiffSchemaSnafu {
            schemas: ks
                .iter()
                .filter_map(|(k, _)| k.split_once('/').map(|k| k.0))
                .collect::<Vec<_>>()
                .join(","),
        }
        .fail()?,
    }
}
