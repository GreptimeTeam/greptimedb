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

use snafu::ensure;
use sql::ast::ObjectName;
use sql::error::{PermissionDeniedSnafu, Result};
use sql::parser::ParserContext;

use crate::QueryContextRef;

/// Parse table name into `(catalog, schema, table)` with query context and validates
/// if catalog matches current catalog in query context.
pub fn table_name_to_full_name(
    name: &str,
    query_ctx: &QueryContextRef,
) -> Result<(String, String, String)> {
    let obj_name = ParserContext::parse_table_name(name, query_ctx.sql_dialect())?;

    let (catalog, schema, table) = table_idents_to_full_name(&obj_name, query_ctx)?;
    // todo(hl): also check if schema matches when rbac is ready. https://github.com/GreptimeTeam/greptimedb/pull/3988/files#r1608687652
    ensure!(
        catalog == query_ctx.current_catalog(),
        PermissionDeniedSnafu {
            target: catalog,
            current: query_ctx.current_catalog(),
        }
    );
    Ok((catalog, schema, table))
}

// Re-export table_idents_to_full_name from sql::util
pub fn table_idents_to_full_name(
    obj_name: &ObjectName,
    query_ctx: &QueryContextRef,
) -> Result<(String, String, String)> {
    sql::util::table_idents_to_full_name(
        obj_name,
        query_ctx.current_catalog(),
        query_ctx.current_schema(),
    )
}
