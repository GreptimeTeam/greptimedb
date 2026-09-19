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

use catalog::kvbackend::{TableCacheRef, new_table_cache};
use common_meta::cache::{
    CacheRegistry, CacheRegistryBuilder, LayeredCacheRegistry, LayeredCacheRegistryBuilder,
    SchemaCacheRef, TableInfoCacheRef, TableNameCacheRef, TableRouteCacheRef, TableSchemaCacheRef,
    new_schema_cache, new_table_flownode_set_cache, new_table_info_cache, new_table_name_cache,
    new_table_route_cache, new_table_schema_cache, new_view_info_cache,
};
use common_meta::kv_backend::KvBackendRef;
use moka::future::{Cache, CacheBuilder};
use partition::cache::{PartitionInfoCacheRef, new_partition_info_cache};
use snafu::OptionExt;

use crate::error::Result;

const DEFAULT_CACHE_MAX_CAPACITY: u64 = 65536;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_CACHE_TTI: Duration = Duration::from_secs(5 * 60);

fn default_cache<K: Send + Sync + Hash + Eq + 'static, V: Send + Sync + Clone + 'static>()
-> Cache<K, V> {
    CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
        .time_to_live(DEFAULT_CACHE_TTL)
        .time_to_idle(DEFAULT_CACHE_TTI)
        .build()
}

pub const TABLE_INFO_CACHE_NAME: &str = "table_info_cache";
pub const VIEW_INFO_CACHE_NAME: &str = "view_info_cache";
pub const TABLE_NAME_CACHE_NAME: &str = "table_name_cache";
pub const TABLE_CACHE_NAME: &str = "table_cache";
pub const SCHEMA_CACHE_NAME: &str = "schema_cache";
pub const TABLE_SCHEMA_NAME_CACHE_NAME: &str = "table_schema_name_cache";
pub const TABLE_FLOWNODE_SET_CACHE_NAME: &str = "table_flownode_set_cache";
pub const TABLE_ROUTE_CACHE_NAME: &str = "table_route_cache";
pub const PARTITION_INFO_CACHE_NAME: &str = "partition_info_cache";

/// The caches of a datanode.
///
/// The `table_cache` is derived from the `table_info_cache` and the `table_name_cache`, and the
/// `partition_info_cache` is derived from the `table_route_cache`.
struct DatanodeCaches {
    table_id_schema_cache: TableSchemaCacheRef,
    schema_cache: SchemaCacheRef,
    table_info_cache: TableInfoCacheRef,
    table_name_cache: TableNameCacheRef,
    table_route_cache: TableRouteCacheRef,
    table_cache: TableCacheRef,
    partition_info_cache: PartitionInfoCacheRef,
}

impl DatanodeCaches {
    /// Builds the caches of a datanode.
    ///
    /// The table/route caches are needed by the catalog manager and by the partition rule manager
    /// of the datanode, which are used to plan the `MergeScan` nodes of the plans received from the
    /// frontend (e.g. a nested merge scan that queries another datanode).
    fn new(kv_backend: KvBackendRef) -> Self {
        // Builds table id schema name cache that never expires.
        let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY).build();
        let table_id_schema_cache = Arc::new(new_table_schema_cache(
            TABLE_SCHEMA_NAME_CACHE_NAME.to_string(),
            cache,
            kv_backend.clone(),
        ));

        // Builds schema cache
        let cache = default_cache();
        let schema_cache = Arc::new(new_schema_cache(
            SCHEMA_CACHE_NAME.to_string(),
            cache,
            kv_backend.clone(),
        ));

        // Builds table info cache
        let cache = default_cache();
        let table_info_cache = Arc::new(new_table_info_cache(
            TABLE_INFO_CACHE_NAME.to_string(),
            cache,
            kv_backend.clone(),
        ));

        // Builds table name cache
        let cache = default_cache();
        let table_name_cache = Arc::new(new_table_name_cache(
            TABLE_NAME_CACHE_NAME.to_string(),
            cache,
            kv_backend.clone(),
        ));

        // Builds table cache
        let cache = default_cache();
        let table_cache = Arc::new(new_table_cache(
            TABLE_CACHE_NAME.to_string(),
            cache,
            table_info_cache.clone(),
            table_name_cache.clone(),
        ));

        // Builds table route cache
        let cache = default_cache();
        let table_route_cache = Arc::new(new_table_route_cache(
            TABLE_ROUTE_CACHE_NAME.to_string(),
            cache,
            kv_backend.clone(),
        ));

        // Builds partition info cache
        let cache = default_cache();
        let partition_info_cache = Arc::new(new_partition_info_cache(
            PARTITION_INFO_CACHE_NAME.to_string(),
            cache,
            table_route_cache.clone(),
        ));

        Self {
            table_id_schema_cache,
            schema_cache,
            table_info_cache,
            table_name_cache,
            table_route_cache,
            table_cache,
            partition_info_cache,
        }
    }

    /// Splits the caches into the layer of the caches that are read from the kv backend, and the
    /// layer of the caches derived from them.
    fn into_layers(self) -> (CacheRegistry, CacheRegistry) {
        let base_registry = CacheRegistryBuilder::default()
            .add_cache(self.table_id_schema_cache)
            .add_cache(self.schema_cache)
            .add_cache(self.table_info_cache)
            .add_cache(self.table_name_cache)
            .add_cache(self.table_route_cache)
            .build();
        let derived_registry = CacheRegistryBuilder::default()
            .add_cache(self.table_cache)
            .add_cache(self.partition_info_cache)
            .build();

        (base_registry, derived_registry)
    }
}

/// Builds the layered cache registry for datanode.
///
/// The registry holds every cache a datanode needs:
/// - Schema cache.
/// - Table id to schema name cache.
/// - Table info cache.
/// - Table name cache.
/// - Table cache.
/// - Table route cache.
/// - Partition info cache.
///
/// The first layer holds the caches the datanode reads from the kv backend: the schema cache, the
/// table id to schema name cache, the table info cache, the table name cache and the table route
/// cache. The second layer holds the caches derived from them: the table cache (from the table info
/// and the table name caches) and the partition info cache (from the table route cache).
///
/// [LayeredCacheRegistry] invalidates a layer only after the previous layer finished invalidating
/// (see [LayeredCacheRegistryBuilder::add_cache_registry]), so the derived caches are invalidated
/// after the caches they are derived from. A flat [CacheRegistry] invalidates its caches
/// concurrently: a derived cache could be invalidated first and then refilled by a concurrent query
/// from a base cache that still holds the old value, and the stale metadata would stay in the
/// derived cache until the entry expires. This is the only datanode cache registry: production code
/// and test infrastructure build the same one.
pub fn build_datanode_layered_cache_registry(kv_backend: KvBackendRef) -> LayeredCacheRegistry {
    let (base_registry, derived_registry) = DatanodeCaches::new(kv_backend).into_layers();

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
    let cache = default_cache();
    let table_info_cache = Arc::new(new_table_info_cache(
        TABLE_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table name cache
    let cache = default_cache();
    let table_name_cache = Arc::new(new_table_name_cache(
        TABLE_NAME_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table route cache
    let cache = default_cache();
    let table_route_cache = Arc::new(new_table_route_cache(
        TABLE_ROUTE_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds table flownode set cache
    let cache = default_cache();
    let table_flownode_set_cache = Arc::new(new_table_flownode_set_cache(
        TABLE_FLOWNODE_SET_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));
    // Builds the view info cache
    let cache = default_cache();
    let view_info_cache = Arc::new(new_view_info_cache(
        VIEW_INFO_CACHE_NAME.to_string(),
        cache,
        kv_backend.clone(),
    ));

    // Builds schema cache
    let cache = default_cache();
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
    let cache = default_cache();
    let table_cache = Arc::new(new_table_cache(
        TABLE_CACHE_NAME.to_string(),
        cache,
        table_info_cache,
        table_name_cache,
    ));

    let cache = default_cache();
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
    use std::sync::Arc;

    use catalog::kvbackend::TableCacheRef;
    use common_meta::cache::{
        SchemaCacheRef, TableInfoCacheRef, TableNameCacheRef, TableRouteCacheRef,
        TableSchemaCacheRef,
    };
    use common_meta::kv_backend::KvBackendRef;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use partition::cache::PartitionInfoCacheRef;

    use super::*;

    fn memory_backend() -> KvBackendRef {
        Arc::new(MemoryKvBackend::<common_meta::error::Error>::new())
    }

    /// Asserts that the registry holds every cache the catalog manager and the partition rule
    /// manager of a datanode look up.
    macro_rules! assert_datanode_caches {
        ($registry:expr) => {
            assert!($registry.get::<TableSchemaCacheRef>().is_some());
            assert!($registry.get::<SchemaCacheRef>().is_some());
            assert!($registry.get::<TableInfoCacheRef>().is_some());
            assert!($registry.get::<TableNameCacheRef>().is_some());
            assert!($registry.get::<TableCacheRef>().is_some());
            assert!($registry.get::<TableRouteCacheRef>().is_some());
            assert!($registry.get::<PartitionInfoCacheRef>().is_some());
        };
    }

    #[test]
    fn test_datanode_layered_cache_registry_holds_all_caches() {
        let registry = build_datanode_layered_cache_registry(memory_backend());

        assert_datanode_caches!(registry);
    }
}
