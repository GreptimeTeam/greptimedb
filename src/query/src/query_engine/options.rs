// Copyright 2023 Greptime Team
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

use common_catalog::consts::DEFAULT_CATALOG_NAME;
use datafusion_common::TableReference;
use session::context::QueryContextRef;
use snafu::ensure;

use crate::error::{QueryAccessDeniedSnafu, Result};

#[derive(Default)]
pub struct QueryOptions {
    pub validate_table_references: bool,
}

pub fn validate_catalog_and_schema(
    catalog: Option<&str>,
    schema: &str,
    query_ctx: &QueryContextRef,
) -> Result<()> {
    ensure!(
        (catalog.is_none() || catalog.unwrap() == query_ctx.current_catalog())
            && schema == query_ctx.current_schema(),
        QueryAccessDeniedSnafu {
            catalog: catalog.unwrap_or(DEFAULT_CATALOG_NAME),
            schema: schema.to_string(),
        }
    );

    Ok(())
}

pub fn validate_table_references(name: TableReference, query_ctx: &QueryContextRef) -> Result<()> {
    match name {
        TableReference::Bare { .. } => Ok(()),
        TableReference::Partial { schema, .. } => {
            ensure!(
                schema == query_ctx.current_schema(),
                QueryAccessDeniedSnafu {
                    catalog: query_ctx.current_catalog(),
                    schema: schema.to_string(),
                }
            );
            Ok(())
        }
        TableReference::Full {
            catalog, schema, ..
        } => {
            ensure!(
                catalog == query_ctx.current_catalog() && schema == query_ctx.current_schema(),
                QueryAccessDeniedSnafu {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                }
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use session::context::QueryContext;

    use super::*;

    #[test]
    fn test_validate_table_ref() {
        let context = Arc::new(QueryContext::with("greptime", "public"));

        let table_ref = TableReference::Bare {
            table: "table_name",
        };
        let re = validate_table_references(table_ref, &context);
        assert!(re.is_ok());

        let table_ref = TableReference::Partial {
            schema: "public",
            table: "table_name",
        };
        let re = validate_table_references(table_ref, &context);
        assert!(re.is_ok());

        let table_ref = TableReference::Partial {
            schema: "wrong_schema",
            table: "table_name",
        };
        let re = validate_table_references(table_ref, &context);
        assert!(re.is_err());

        let table_ref = TableReference::Full {
            catalog: "greptime",
            schema: "public",
            table: "table_name",
        };
        let re = validate_table_references(table_ref, &context);
        assert!(re.is_ok());

        let table_ref = TableReference::Full {
            catalog: "wrong_catalog",
            schema: "public",
            table: "table_name",
        };
        let re = validate_table_references(table_ref, &context);
        assert!(re.is_err());
    }

    #[test]
    fn test_validate_catalog_and_schema() {
        let context = Arc::new(QueryContext::with("greptime", "public"));

        let re = validate_catalog_and_schema(None, "public", &context);
        assert!(re.is_ok());
        let re = validate_catalog_and_schema(None, "wrong_schema", &context);
        assert!(re.is_err());

        let re = validate_catalog_and_schema(Some("greptime"), "public", &context);
        assert!(re.is_ok());
        let re = validate_catalog_and_schema(Some("greptime"), "wrong_schema", &context);
        assert!(re.is_err());
        let re = validate_catalog_and_schema(Some("wrong_catalog"), "public", &context);
        assert!(re.is_err());
        let re = validate_catalog_and_schema(Some("wrong_catalog"), "wrong_schema", &context);
        assert!(re.is_err());
    }
}
