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

use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use catalog::kvbackend::new_table_cache;
use common_meta::cache::{
    CacheRegistry, CacheRegistryBuilder, LayeredCacheRegistry, LayeredCacheRegistryBuilder,
    new_flow_info_cache, new_schema_cache, new_table_flownode_set_cache, new_table_info_cache,
    new_table_name_cache, new_table_route_cache, new_table_schema_cache, new_view_info_cache,
};
use common_meta::kv_backend::KvBackendRef;
use moka::future::{Cache, CacheBuilder};
use partition::cache::new_partition_info_cache;
use snafu::OptionExt;

use crate::error::Result;

const DEFAULT_CACHE_MAX_CAPACITY: u64 = 65536;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_CACHE_TTI: Duration = Duration::from_secs(5 * 60);

// Capacity is in entries, matching the existing unweighted Moka caches.
fn cache_builder<K: Send + Sync + Hash + Eq + 'static, V: Send + Sync + Clone + 'static>(
    name: &str,
) -> CacheBuilder<K, V, Cache<K, V>> {
    let env = format!(
        "GREPTIMEDB_EXPERIMENTAL_{}_MAX_ENTRIES",
        name.to_ascii_uppercase()
    );
    let value = std::env::var(&env).ok();
    let capacity = parse_cache_capacity(&env, value.as_deref());
    common_telemetry::info!(
        "Metadata cache {name} capacity: {capacity} entries, configured by {env}"
    );
    CacheBuilder::new(capacity)
}

fn parse_cache_capacity(env: &str, value: Option<&str>) -> u64 {
    match value {
        None => DEFAULT_CACHE_MAX_CAPACITY,
        Some(value) => value.parse().unwrap_or_else(|_| {
            common_telemetry::warn!("Invalid {env}: expected an unsigned entry count; using {DEFAULT_CACHE_MAX_CAPACITY}");
            DEFAULT_CACHE_MAX_CAPACITY
        }),
    }
}

fn default_cache<K: Send + Sync + Hash + Eq + 'static, V: Send + Sync + Clone + 'static>(
    name: &str,
) -> Cache<K, V> {
    cache_builder(name)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build()
}

pub const FLOW_INFO_CACHE_NAME: &str = "flow_info_cache";
pub const TABLE_INFO_CACHE_NAME: &str = "table_info_cache";
pub const VIEW_INFO_CACHE_NAME: &str = "view_info_cache";
pub const TABLE_NAME_CACHE_NAME: &str = "table_name_cache";
pub const TABLE_CACHE_NAME: &str = "table_cache";
pub const SCHEMA_CACHE_NAME: &str = "schema_cache";
pub const TABLE_SCHEMA_NAME_CACHE_NAME: &str = "table_schema_name_cache";
pub const TABLE_FLOWNODE_SET_CACHE_NAME: &str = "table_flownode_set_cache";
pub const TABLE_ROUTE_CACHE_NAME: &str = "table_route_cache";
pub const PARTITION_INFO_CACHE_NAME: &str = "partition_info_cache";

/// Builds the datanode cache registry with base metadata caches before derived caches.
///
/// Invalidating the base layer first prevents a concurrent query from refilling a derived cache
/// from stale metadata after that derived cache has been invalidated.
pub fn build_datanode_layered_cache_registry(kv_backend: KvBackendRef) -> LayeredCacheRegistry {
    let cache = cache_builder(TABLE_SCHEMA_NAME_CACHE_NAME).build();
    let table_id_schema_cache = Arc::new(new_table_schema_cache(
        TABLE_SCHEMA_NAME_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    let cache = default_cache(SCHEMA_CACHE_NAME);
    let schema_cache = Arc::new(new_schema_cache(
        SCHEMA_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    let cache = default_cache(TABLE_INFO_CACHE_NAME);
    let table_info_cache = Arc::new(new_table_info_cache(
        TABLE_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    let cache = default_cache(TABLE_NAME_CACHE_NAME);
    let table_name_cache = Arc::new(new_table_name_cache(
        TABLE_NAME_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    let cache = default_cache(TABLE_ROUTE_CACHE_NAME);
    let table_route_cache = Arc::new(new_table_route_cache(
        TABLE_ROUTE_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    let cache = default_cache(TABLE_CACHE_NAME);
    let table_cache = Arc::new(new_table_cache(
        TABLE_CACHE_NAME.to_string(),
        cache,
        table_info_cache.clone(),
        table_name_cache.clone(),
    ));

    let cache = default_cache(PARTITION_INFO_CACHE_NAME);
    let partition_info_cache = Arc::new(new_partition_info_cache(
        PARTITION_INFO_CACHE_NAME.to_string(),
        cache,
        table_route_cache.clone(),
    ));

    let base_registry = CacheRegistryBuilder::default()
        .add_cache(table_id_schema_cache)
        .add_cache(schema_cache)
        .add_cache(table_info_cache)
        .add_cache(table_name_cache)
        .add_cache(table_route_cache)
        .build();
    let derived_registry = CacheRegistryBuilder::default()
        .add_cache(table_cache)
        .add_cache(partition_info_cache)
        .build();

    LayeredCacheRegistryBuilder::default()
        .add_cache_registry(base_registry)
        .add_cache_registry(derived_registry)
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
    let cache = default_cache(TABLE_INFO_CACHE_NAME);
    let table_info_cache = Arc::new(new_table_info_cache(
        TABLE_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table name cache
    let cache = default_cache(TABLE_NAME_CACHE_NAME);
    let table_name_cache = Arc::new(new_table_name_cache(
        TABLE_NAME_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table route cache
    let cache = default_cache(TABLE_ROUTE_CACHE_NAME);
    let table_route_cache = Arc::new(new_table_route_cache(
        TABLE_ROUTE_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table flownode set cache
    let cache = default_cache(TABLE_FLOWNODE_SET_CACHE_NAME);
    let table_flownode_set_cache = Arc::new(new_table_flownode_set_cache(
        TABLE_FLOWNODE_SET_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));
    let flow_info_cache = Arc::new(new_flow_info_cache(
        FLOW_INFO_CACHE_NAME.to_string(),
        default_cache(FLOW_INFO_CACHE_NAME),
        kv_backend.clone(),
    ));
    // Builds the view info cache
    let cache = default_cache(VIEW_INFO_CACHE_NAME);
    let view_info_cache = Arc::new(new_view_info_cache(
        VIEW_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds schema cache
    let cache = default_cache(SCHEMA_CACHE_NAME);
    let schema_cache = Arc::new(new_schema_cache(
        SCHEMA_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    let table_id_schema_cache = Arc::new(new_table_schema_cache(
        TABLE_SCHEMA_NAME_CACHE_NAME.to_string(),
        cache_builder(TABLE_SCHEMA_NAME_CACHE_NAME).build(),
        kv_backend,
    ));
    CacheRegistryBuilder::default()
        .add_cache(table_info_cache)
        .add_cache(table_name_cache)
        .add_cache(table_route_cache)
        .add_cache(view_info_cache)
        .add_cache(flow_info_cache)
        .add_cache(table_flownode_set_cache)
        .add_cache(schema_cache)
        .add_cache(table_id_schema_cache)
        .build()
}

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
    let cache = default_cache(TABLE_CACHE_NAME);
    let table_cache = Arc::new(new_table_cache(
        TABLE_CACHE_NAME.to_string(),
        cache,
        table_info_cache,
        table_name_cache,
    ));

    let cache = default_cache(PARTITION_INFO_CACHE_NAME);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_cache_builder_capacity() {
        let name = "GREPTIMEDB_EXPERIMENTAL_TABLE_INFO_CACHE_MAX_ENTRIES";
        let value = std::env::var(name).ok();
        let expected = parse_cache_capacity(name, value.as_deref());
        let cache = default_cache::<u32, u32>(TABLE_INFO_CACHE_NAME);
        assert_eq!(cache.policy().max_capacity(), Some(expected));
        assert_eq!(cache.policy().time_to_live(), Some(DEFAULT_CACHE_TTL));
        assert_eq!(cache.policy().time_to_idle(), Some(DEFAULT_CACHE_TTI));
    }

    #[test]
    fn test_metadata_cache_capacity() {
        for (value, expected) in [
            (None, DEFAULT_CACHE_MAX_CAPACITY),
            (Some("0"), 0),
            (Some("262144"), 262144),
            (Some("-1"), DEFAULT_CACHE_MAX_CAPACITY),
            (Some(""), DEFAULT_CACHE_MAX_CAPACITY),
            (Some("18446744073709551616"), DEFAULT_CACHE_MAX_CAPACITY),
        ] {
            assert_eq!(parse_cache_capacity("test", value), expected);
        }
    }
}
