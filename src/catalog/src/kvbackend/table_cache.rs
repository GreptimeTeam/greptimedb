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

use common_meta::cache::{CacheContainer, Initializer, TableInfoCacheRef, TableNameCacheRef};
use common_meta::error::{Result as MetaResult, ValueNotExistSnafu};
use common_meta::instruction::CacheIdent;
use futures::future::BoxFuture;
use moka::future::Cache;
use snafu::OptionExt;
use table::dist_table::DistTable;
use table::table_name::TableName;
use table::TableRef;

pub type TableCacheRef = Arc<TableCache>;

/// [TableCache] caches the [TableName] to [TableRef] mapping.
pub type TableCache = CacheContainer<TableName, TableRef, CacheIdent>;

/// Constructs a [TableCache].
pub fn new_table_cache(
    name: String,
    cache: Cache<TableName, TableRef>,
    table_info_cache: TableInfoCacheRef,
    table_name_cache: TableNameCacheRef,
) -> TableCache {
    let init = init_factory(table_info_cache, table_name_cache);

    CacheContainer::new(name, cache, Box::new(invalidator), init, filter)
}

fn init_factory(
    table_info_cache: TableInfoCacheRef,
    table_name_cache: TableNameCacheRef,
) -> Initializer<TableName, TableRef> {
    Arc::new(move |table_name| {
        let table_info_cache = table_info_cache.clone();
        let table_name_cache = table_name_cache.clone();
        Box::pin(async move {
            let table_id = table_name_cache
                .get_by_ref(table_name)
                .await?
                .context(ValueNotExistSnafu)?;
            let table_info = table_info_cache
                .get_by_ref(&table_id)
                .await?
                .context(ValueNotExistSnafu)?;

            Ok(Some(DistTable::table(table_info)))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableName, TableRef>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, MetaResult<()>> {
    Box::pin(async move {
        if let CacheIdent::TableName(table_name) = ident {
            cache.invalidate(table_name).await
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::TableName(_))
}
