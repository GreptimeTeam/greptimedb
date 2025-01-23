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

//! Cache for table id to schema name mapping.

use std::sync::Arc;

use futures_util::future::BoxFuture;
use moka::future::Cache;
use snafu::OptionExt;
use store_api::storage::TableId;

use crate::cache::{CacheContainer, Initializer};
use crate::error;
use crate::instruction::CacheIdent;
use crate::key::schema_name::SchemaName;
use crate::key::table_info::TableInfoManager;
use crate::kv_backend::KvBackendRef;

pub type TableSchemaCache = CacheContainer<TableId, Arc<SchemaName>, CacheIdent>;
pub type TableSchemaCacheRef = Arc<TableSchemaCache>;

/// Constructs a [TableSchemaCache].
pub fn new_table_schema_cache(
    name: String,
    cache: Cache<TableId, Arc<SchemaName>>,
    kv_backend: KvBackendRef,
) -> TableSchemaCache {
    let table_info_manager = TableInfoManager::new(kv_backend);
    let init = init_factory(table_info_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, filter)
}

fn init_factory(table_info_manager: TableInfoManager) -> Initializer<TableId, Arc<SchemaName>> {
    Arc::new(move |table_id| {
        let table_info_manager = table_info_manager.clone();
        Box::pin(async move {
            let raw_table_info = table_info_manager
                .get(*table_id)
                .await?
                .context(error::ValueNotExistSnafu)?
                .into_inner()
                .table_info;

            Ok(Some(Arc::new(SchemaName {
                catalog_name: raw_table_info.catalog_name,
                schema_name: raw_table_info.schema_name,
            })))
        })
    })
}

/// Never invalidates table id schema cache.
fn invalidator<'a>(
    _cache: &'a Cache<TableId, Arc<SchemaName>>,
    _ident: &'a CacheIdent,
) -> BoxFuture<'a, error::Result<()>> {
    Box::pin(std::future::ready(Ok(())))
}

/// Never invalidates table id schema cache.
fn filter(_ident: &CacheIdent) -> bool {
    false
}
