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

use sql::dialect::{Dialect, MySqlDialect, PostgreSqlDialect};

/// [`CatalogProtocol`] will affect the behaviour of CatalogManager
/// Currently, it mainly affects the table names resolving
/// `pg_catalog` will be only visible under PostgreSQL protocol, and user
/// can access any `pg_catalog` tables with bare table name.
///
/// The `information_schema` will be visible for Mysql and any other protocol.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CatalogProtocol {
    MySQL,
    PostgreSQL,
    Other,
}

impl CatalogProtocol {
    pub fn from_query_dialect(query_dialect: &dyn Dialect) -> Self {
        if query_dialect.is::<MySqlDialect>() {
            CatalogProtocol::MySQL
        } else if query_dialect.is::<PostgreSqlDialect>() {
            CatalogProtocol::PostgreSQL
        } else {
            CatalogProtocol::Other
        }
    }
}
