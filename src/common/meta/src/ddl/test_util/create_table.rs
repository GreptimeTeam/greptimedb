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

use api::v1::{ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
use common_catalog::consts::MITO2_ENGINE;
use derive_builder::Builder;
use store_api::storage::TableId;

#[derive(Default, Builder)]
pub struct TestColumnDef {
    #[builder(setter(into), default)]
    name: String,
    data_type: ColumnDataType,
    #[builder(default)]
    is_nullable: bool,
    semantic_type: SemanticType,
    #[builder(setter(into), default)]
    comment: String,
}

impl From<TestColumnDef> for ColumnDef {
    fn from(
        TestColumnDef {
            name,
            data_type,
            is_nullable,
            semantic_type,
            comment,
        }: TestColumnDef,
    ) -> Self {
        Self {
            name,
            data_type: data_type as i32,
            is_nullable,
            default_constraint: vec![],
            semantic_type: semantic_type as i32,
            comment,
            datatype_extension: None,
        }
    }
}

#[derive(Default, Builder)]
#[builder(default)]
pub struct TestCreateTableExpr {
    #[builder(setter(into))]
    catalog_name: String,
    #[builder(setter(into))]
    schema_name: String,
    #[builder(setter(into))]
    table_name: String,
    #[builder(setter(into))]
    desc: String,
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
