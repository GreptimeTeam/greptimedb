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

use datafusion_common::TableReference;
use session::context::QueryContextRef;
use snafu::ensure;

use crate::error::{QueryAccessDeniedSnafu, Result};

#[derive(Default)]
pub struct QueryEngineOptions {
    pub validate_table_references: bool,
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
