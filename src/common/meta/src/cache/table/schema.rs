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

use futures_util::future::BoxFuture;
use moka::future::Cache;
use snafu::OptionExt;

use crate::cache::{CacheContainer, Initializer};
use crate::error::ValueNotExistSnafu;
use crate::instruction::CacheIdent;
use crate::key::schema_name::{SchemaManager, SchemaName, SchemaNameKey, SchemaNameValue};
use crate::kv_backend::KvBackendRef;

pub type SchemaCache = CacheContainer<SchemaName, Arc<SchemaNameValue>, CacheIdent>;
pub type SchemaCacheRef = Arc<SchemaCache>;

/// Constructs a [SchemaCache].
pub fn new_schema_cache(
    name: String,
    cache: Cache<SchemaName, Arc<SchemaNameValue>>,
    kv_backend: KvBackendRef,
) -> SchemaCache {
    let schema_manager = SchemaManager::new(kv_backend.clone());
    let init = init_factory(schema_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(schema_manager: SchemaManager) -> Initializer<SchemaName, Arc<SchemaNameValue>> {
    Arc::new(move |schema_name| {
        let manager = schema_manager.clone();
        Box::pin(async move {
            let schema_value = manager
                .get(SchemaNameKey {
                    catalog: &schema_name.catalog_name,
                    schema: &schema_name.schema_name,
                })
                .await?
                .context(ValueNotExistSnafu)?
                .into_inner();
            Ok(Some(Arc::new(schema_value)))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<SchemaName, Arc<SchemaNameValue>>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, crate::error::Result<()>> {
    Box::pin(async move {
        if let CacheIdent::SchemaName(schema_name) = ident {
            cache.invalidate(schema_name).await
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::SchemaName(_))
}
