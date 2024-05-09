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

use futures::future::BoxFuture;
use moka::future::Cache;
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;
use table::metadata::TableInfo;

use crate::cache::{CacheContainer, Initializer};
use crate::error;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::table_info::{TableInfoManager, TableInfoManagerRef};
use crate::kv_backend::KvBackendRef;

/// [TableInfoCache] caches the [TableId] to [TableInfo] mapping.
pub type TableInfoCache = CacheContainer<TableId, Arc<TableInfo>, CacheIdent>;

pub type TableInfoCacheRef = Arc<TableInfoCache>;

/// Constructs a [TableInfoCache].
pub fn new_table_info_cache(
    name: String,
    cache: Cache<TableId, Arc<TableInfo>>,
    kv_backend: KvBackendRef,
) -> TableInfoCache {
    let table_info_manager = Arc::new(TableInfoManager::new(kv_backend));
    let init = init_factory(table_info_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(table_info_manager: TableInfoManagerRef) -> Initializer<TableId, Arc<TableInfo>> {
    Arc::new(move |table_id| {
        let table_info_manager = table_info_manager.clone();
        Box::pin(async move {
            let raw_table_info = table_info_manager
                .get(*table_id)
                .await?
                .context(error::ValueNotExistSnafu {})?
                .into_inner()
                .table_info;
            Ok(Some(Arc::new(
                TableInfo::try_from(raw_table_info).context(error::ConvertRawTableInfoSnafu)?,
            )))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableId, Arc<TableInfo>>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if let CacheIdent::TableId(table_id) = ident {
            cache.invalidate(table_id).await
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::TableId(_))
}
