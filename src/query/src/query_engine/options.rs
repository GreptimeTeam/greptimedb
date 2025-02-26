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

use session::context::QueryContextRef;
use snafu::ensure;

use crate::error::{QueryAccessDeniedSnafu, Result};

#[derive(Default, Clone)]
pub struct QueryOptions {
    pub disallow_cross_catalog_query: bool,
}

// TODO(shuiyisong): remove one method after #559 is done
pub fn validate_catalog_and_schema(
    catalog: &str,
    schema: &str,
    query_ctx: &QueryContextRef,
) -> Result<()> {
    ensure!(
        catalog == query_ctx.current_catalog(),
        QueryAccessDeniedSnafu {
            catalog: catalog.to_string(),
            schema: schema.to_string(),
        }
    );

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use session::context::QueryContext;

    use super::*;

    #[test]
    fn test_validate_catalog_and_schema() {
        let context = Arc::new(QueryContext::with("greptime", "public"));

        validate_catalog_and_schema("greptime", "public", &context).unwrap();
        let re = validate_catalog_and_schema("greptime", "private_schema", &context);
        assert!(re.is_ok());
        let re = validate_catalog_and_schema("wrong_catalog", "public", &context);
        assert!(re.is_err());
        let re = validate_catalog_and_schema("wrong_catalog", "wrong_schema", &context);
        assert!(re.is_err());

        validate_catalog_and_schema("greptime", "information_schema", &context).unwrap();
    }
}
