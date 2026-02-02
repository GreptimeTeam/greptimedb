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

pub mod error;

use std::sync::Arc;
use std::time::Duration;

use catalog::kvbackend::new_table_cache;
use common_meta::cache::{
    CacheRegistry, CacheRegistryBuilder, LayeredCacheRegistryBuilder, new_schema_cache,
    new_table_flownode_set_cache, new_table_info_cache, new_table_name_cache,
    new_table_route_cache, new_table_schema_cache, new_view_info_cache,
};
use common_meta::kv_backend::KvBackendRef;
use moka::future::CacheBuilder;
use partition::cache::new_partition_info_cache;
use snafu::OptionExt;

use crate::error::Result;

const DEFAULT_CACHE_MAX_CAPACITY: u64 = 65536;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_CACHE_TTI: Duration = Duration::from_secs(5 * 60);

pub const TABLE_INFO_CACHE_NAME: &str = "table_info_cache";
pub const VIEW_INFO_CACHE_NAME: &str = "view_info_cache";
pub const TABLE_NAME_CACHE_NAME: &str = "table_name_cache";
pub const TABLE_CACHE_NAME: &str = "table_cache";
pub const SCHEMA_CACHE_NAME: &str = "schema_cache";
pub const TABLE_SCHEMA_NAME_CACHE_NAME: &str = "table_schema_name_cache";
pub const TABLE_FLOWNODE_SET_CACHE_NAME: &str = "table_flownode_set_cache";
pub const TABLE_ROUTE_CACHE_NAME: &str = "table_route_cache";
pub const PARTITION_INFO_CACHE_NAME: &str = "partition_info_cache";

/// Builds cache registry for datanode, including:
/// - Schema cache.
/// - Table id to schema name cache.
pub fn build_datanode_cache_registry(kv_backend: KvBackendRef) -> CacheRegistry {
    // Builds table id schema name cache that never expires.
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY).build();
    let table_id_schema_cache = Arc::new(new_table_schema_cache(
        TABLE_SCHEMA_NAME_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds schema cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let schema_cache = Arc::new(new_schema_cache(
        SCHEMA_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    CacheRegistryBuilder::default()
        .add_cache(table_id_schema_cache)
        .add_cache(schema_cache)
        .build()
}

/// Builds cache registry for frontend and datanode, including:
/// - Table info cache
/// - Table name cache
/// - Table route cache
/// - Table flow node cache
/// - View cache
/// - Schema cache
pub fn build_fundamental_cache_registry(kv_backend: KvBackendRef) -> CacheRegistry {
    // Builds table info cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_info_cache = Arc::new(new_table_info_cache(
        TABLE_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table name cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_name_cache = Arc::new(new_table_name_cache(
        TABLE_NAME_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table route cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_route_cache = Arc::new(new_table_route_cache(
        TABLE_ROUTE_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table flownode set cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_flownode_set_cache = Arc::new(new_table_flownode_set_cache(
        TABLE_FLOWNODE_SET_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));
    // Builds the view info cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let view_info_cache = Arc::new(new_view_info_cache(
        VIEW_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds schema cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let schema_cache = Arc::new(new_schema_cache(
        SCHEMA_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    let table_id_schema_cache = Arc::new(new_table_schema_cache(
        TABLE_SCHEMA_NAME_CACHE_NAME.to_string(),
        CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY).build(),
        kv_backend,
    ));
    CacheRegistryBuilder::default()
        .add_cache(table_info_cache)
        .add_cache(table_name_cache)
        .add_cache(table_route_cache)
        .add_cache(view_info_cache)
        .add_cache(table_flownode_set_cache)
        .add_cache(schema_cache)
        .add_cache(table_id_schema_cache)
        .build()
}

// TODO(weny): Make the cache configurable.
pub fn with_default_composite_cache_registry(
    builder: LayeredCacheRegistryBuilder,
) -> Result<LayeredCacheRegistryBuilder> {
    let table_info_cache = builder.get().context(error::CacheRequiredSnafu {
        name: TABLE_INFO_CACHE_NAME,
    })?;
    let table_name_cache = builder.get().context(error::CacheRequiredSnafu {
        name: TABLE_NAME_CACHE_NAME,
    })?;
    let table_route_cache = builder.get().context(error::CacheRequiredSnafu {
        name: TABLE_ROUTE_CACHE_NAME,
    })?;

    // Builds table cache
    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let table_cache = Arc::new(new_table_cache(
        TABLE_CACHE_NAME.to_string(),
        cache,
        table_info_cache,
        table_name_cache,
    ));

    let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build();
    let partition_info_cache = Arc::new(new_partition_info_cache(
        PARTITION_INFO_CACHE_NAME.to_string(),
        cache,
        table_route_cache,
    ));

    let registry = CacheRegistryBuilder::default()
        .add_cache(table_cache)
        .add_cache(partition_info_cache)
        .build();

    Ok(builder.add_cache_registry(registry))
}
