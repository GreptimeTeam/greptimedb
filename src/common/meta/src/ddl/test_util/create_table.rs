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

use std::collections::HashMap;

use api::v1::column_def::try_as_column_schema;
use api::v1::{ColumnDef, CreateTableExpr, SemanticType};
use chrono::DateTime;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO2_ENGINE};
use datatypes::schema::RawSchema;
use derive_builder::Builder;
use store_api::storage::TableId;
use table::metadata::{RawTableInfo, RawTableMeta, TableIdent, TableType};
use table::requests::TableOptions;

#[derive(Default, Builder)]
#[builder(default)]
pub struct TestCreateTableExpr {
    #[builder(setter(into), default = "DEFAULT_CATALOG_NAME.to_string()")]
    catalog_name: String,
    #[builder(setter(into), default = "DEFAULT_SCHEMA_NAME.to_string()")]
    schema_name: String,
    #[builder(setter(into))]
    table_name: String,
    #[builder(setter(into))]
    desc: String,
    #[builder(setter(into))]
    column_defs: Vec<ColumnDef>,
    #[builder(setter(into))]
    time_index: String,
    #[builder(setter(into))]
    primary_keys: Vec<String>,
    create_if_not_exists: bool,
    table_options: HashMap<String, String>,
    table_id: Option<TableId>,
    #[builder(setter(into), default = "MITO2_ENGINE.to_string()")]
    engine: String,
}

impl From<TestCreateTableExpr> for CreateTableExpr {
    fn from(
        TestCreateTableExpr {
            catalog_name,
            schema_name,
            table_name,
            desc,
            column_defs,
            time_index,
            primary_keys,
            create_if_not_exists,
            table_options,
            table_id,
            engine,
        }: TestCreateTableExpr,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            table_name,
            desc,
            column_defs,
            time_index,
            primary_keys,
            create_if_not_exists,
            table_options,
            table_id: table_id.map(|id| api::v1::TableId { id }),
            engine,
        }
    }
}

/// Builds [RawTableInfo] from [CreateTableExpr].
pub fn build_raw_table_info_from_expr(expr: &CreateTableExpr) -> RawTableInfo {
    RawTableInfo {
        ident: TableIdent {
            table_id: expr
                .table_id
                .as_ref()
                .map(|table_id| table_id.id)
                .unwrap_or(0),
            version: 1,
        },
        name: expr.table_name.to_string(),
        desc: Some(expr.desc.to_string()),
        catalog_name: expr.catalog_name.to_string(),
        schema_name: expr.schema_name.to_string(),
        meta: RawTableMeta {
            schema: RawSchema {
                column_schemas: expr
                    .column_defs
                    .iter()
                    .map(|column| try_as_column_schema(column).unwrap())
                    .collect(),
                timestamp_index: expr
                    .column_defs
                    .iter()
                    .position(|column| column.semantic_type() == SemanticType::Timestamp),
                version: 0,
            },
            primary_key_indices: expr
                .primary_keys
                .iter()
                .map(|key| {
                    expr.column_defs
                        .iter()
                        .position(|column| &column.name == key)
                        .unwrap()
                })
                .collect(),
            value_indices: vec![],
            engine: expr.engine.to_string(),
            next_column_id: expr.column_defs.len() as u32,
            region_numbers: vec![],
            options: TableOptions::default(),
            created_on: DateTime::default(),
            partition_key_indices: vec![],
        },
        table_type: TableType::Base,
    }
}
