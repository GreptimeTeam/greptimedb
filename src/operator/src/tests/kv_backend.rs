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

use common_meta::key::catalog_name::{CatalogManager, CatalogNameKey};
use common_meta::key::schema_name::{SchemaManager, SchemaNameKey};
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::KvBackendRef;

pub async fn prepare_mocked_backend() -> KvBackendRef {
    let backend = Arc::new(MemoryKvBackend::default());

    let catalog_manager = CatalogManager::new(backend.clone());
    let schema_manager = SchemaManager::new(backend.clone());

    catalog_manager
        .create(CatalogNameKey::default(), false)
        .await
        .unwrap();
    schema_manager
        .create(SchemaNameKey::default(), None, false)
        .await
        .unwrap();

    backend
}
