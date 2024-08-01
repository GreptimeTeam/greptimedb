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

mod pg_get_userbyid;
mod table_is_visible;

use std::sync::Arc;

use pg_get_userbyid::PGGetUserByIdFunction;
use table_is_visible::PGTableIsVisibleFunction;

use crate::function_registry::FunctionRegistry;

#[macro_export]
macro_rules! pg_catalog_func_fullname {
    ($name:literal) => {
        concat!("pg_catalog.", $name)
    };
}

pub(super) struct PGCatalogFunction;

impl PGCatalogFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register(Arc::new(PGTableIsVisibleFunction));
        registry.register(Arc::new(PGGetUserByIdFunction));
    }
}
