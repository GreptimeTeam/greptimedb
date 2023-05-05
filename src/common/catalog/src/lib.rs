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

use consts::DEFAULT_CATALOG_NAME;

pub mod consts;
pub mod error;

/// Formats table fully-qualified name
#[inline]
pub fn format_full_table_name(catalog: &str, schema: &str, table: &str) -> String {
    format!("{catalog}.{schema}.{table}")
}

/// Build db name from catalog and schema string
pub fn build_db_string(catalog: &str, schema: &str) -> String {
    if catalog == DEFAULT_CATALOG_NAME {
        schema.to_string()
    } else {
        format!("{catalog}-{schema}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_string() {
        assert_eq!("test", build_db_string(DEFAULT_CATALOG_NAME, "test"));
        assert_eq!("a0b1c2d3-test", build_db_string("a0b1c2d3", "test"));
    }
}
