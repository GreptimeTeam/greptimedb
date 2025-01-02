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

use api::v1::alter_table_expr::Kind;
use api::v1::{AddColumn, AddColumns, AlterTableExpr, ColumnDef, RenameTable};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use derive_builder::Builder;

#[derive(Default, Builder)]
#[builder(default)]
pub struct TestAlterTableExpr {
    #[builder(setter(into), default = "DEFAULT_CATALOG_NAME.to_string()")]
    catalog_name: String,
    #[builder(setter(into), default = "DEFAULT_SCHEMA_NAME.to_string()")]
    schema_name: String,
    #[builder(setter(into))]
    table_name: String,
    #[builder(setter(into))]
    add_columns: Vec<ColumnDef>,
    #[builder(setter(into, strip_option))]
    new_table_name: Option<String>,
}

impl From<TestAlterTableExpr> for AlterTableExpr {
    fn from(value: TestAlterTableExpr) -> Self {
        if let Some(new_table_name) = value.new_table_name {
            Self {
                catalog_name: value.catalog_name,
                schema_name: value.schema_name,
                table_name: value.table_name,
                kind: Some(Kind::RenameTable(RenameTable { new_table_name })),
            }
        } else {
            Self {
                catalog_name: value.catalog_name,
                schema_name: value.schema_name,
                table_name: value.table_name,
                kind: Some(Kind::AddColumns(AddColumns {
                    add_columns: value
                        .add_columns
                        .into_iter()
                        .map(|col| AddColumn {
                            column_def: Some(col),
                            location: None,
                            add_if_not_exists: false,
                        })
                        .collect(),
                })),
            }
        }
    }
}
