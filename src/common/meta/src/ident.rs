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

use api::v1::meta::{TableIdent as RawTableIdent, TableName};
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{Error, InvalidProtoMsgSnafu};

#[derive(Eq, Hash, PartialEq, Clone, Debug, Default, Serialize, Deserialize)]
pub struct TableIdent {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub table_id: u32,
    pub engine: String,
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

impl TryFrom<RawTableIdent> for TableIdent {
    type Error = Error;

    fn try_from(value: RawTableIdent) -> Result<Self, Self::Error> {
        let table_name = value.table_name.context(InvalidProtoMsgSnafu {
            err_msg: "'table_name' is missing in TableIdent",
        })?;
        Ok(Self {
            catalog: table_name.catalog_name,
            schema: table_name.schema_name,
            table: table_name.table_name,
            table_id: value.table_id,
            engine: value.engine,
        })
    }
}

impl From<TableIdent> for RawTableIdent {
    fn from(table_ident: TableIdent) -> Self {
        Self {
            table_id: table_ident.table_id,
            engine: table_ident.engine,
            table_name: Some(TableName {
                catalog_name: table_ident.catalog,
                schema_name: table_ident.schema,
                table_name: table_ident.table,
            }),
        }
    }
}
