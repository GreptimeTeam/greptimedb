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

use common_catalog::build_db_string;

pub(crate) const METRIC_DB_LABEL: &str = "db";

pub(crate) const METRIC_CATALOG_MANAGER_CATALOG_COUNT: &str = "catalog.catalog_count";
pub(crate) const METRIC_CATALOG_MANAGER_SCHEMA_COUNT: &str = "catalog.schema_count";
pub(crate) const METRIC_CATALOG_MANAGER_TABLE_COUNT: &str = "catalog.table_count";

#[inline]
pub(crate) fn db_label(catalog: &str, schema: &str) -> (&'static str, String) {
    (METRIC_DB_LABEL, build_db_string(catalog, schema))
}
