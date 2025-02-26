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

use std::fmt::{self, Display};

use datafusion_common::TableReference as DfTableReference;

/// Represents a resolved path to a table of the form “catalog.schema.table”
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableReference<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
}

pub type OwnedTableReference = TableReference<'static>;

impl<'a> TableReference<'a> {
    pub fn bare(table: &'a str) -> Self {
        TableReference {
            catalog: common_catalog::consts::DEFAULT_CATALOG_NAME,
            schema: common_catalog::consts::DEFAULT_SCHEMA_NAME,
            table,
        }
    }

    pub fn full(catalog: &'a str, schema: &'a str, table: &'a str) -> Self {
        TableReference {
            catalog,
            schema,
            table,
        }
    }
}

impl Display for TableReference<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

impl<'a> From<TableReference<'a>> for DfTableReference {
    fn from(val: TableReference<'a>) -> Self {
        DfTableReference::full(val.catalog, val.schema, val.table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_reference() {
        let table_ref = TableReference {
            catalog: "greptime",
            schema: "public",
            table: "test",
        };

        assert_eq!("greptime.public.test", table_ref.to_string());
    }
}
