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

/// DROP TABLE statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTable {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
}

impl DropTable {
    /// Creates a statement for `DROP TABLE`
    pub fn new(catalog_name: String, schema_name: String, table_name: String) -> Self {
        DropTable {
            catalog_name,
            schema_name,
            table_name,
        }
    }
}
