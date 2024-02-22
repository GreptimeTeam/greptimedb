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

/// Ensure current function is invokded under `greptime` catalog.
#[macro_export]
macro_rules! ensure_greptime {
    ($func_ctx: expr) => {{
        use common_catalog::consts::DEFAULT_CATALOG_NAME;
        snafu::ensure!(
            $func_ctx.query_ctx.current_catalog() == DEFAULT_CATALOG_NAME,
            common_query::error::PermissionDeniedSnafu {
                err_msg: format!("current catalog is not {DEFAULT_CATALOG_NAME}")
            }
        );
    }};
}
