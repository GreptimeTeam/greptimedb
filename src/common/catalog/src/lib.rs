// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod consts;
pub mod error;
mod helper;

pub use helper::{
    build_catalog_prefix, build_schema_prefix, build_table_global_prefix,
    build_table_regional_prefix, CatalogKey, CatalogValue, SchemaKey, SchemaValue, TableGlobalKey,
    TableGlobalValue, TableRegionalKey, TableRegionalValue,
};
