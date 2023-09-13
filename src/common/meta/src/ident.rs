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

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use table::engine::TableReference;

#[derive(Eq, Hash, PartialEq, Clone, Debug, Default, Serialize, Deserialize)]
pub struct TableIdent {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub table_id: u32,
    pub engine: String,
}

impl TableIdent {
    pub fn table_ref(&self) -> TableReference {
        TableReference::full(&self.catalog, &self.schema, &self.table)
    }
}

impl Display for TableIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Table(id={}, name='{}.{}.{}', engine='{}')",
            self.table_id, self.catalog, self.schema, self.table, self.engine,
        )
    }
}
