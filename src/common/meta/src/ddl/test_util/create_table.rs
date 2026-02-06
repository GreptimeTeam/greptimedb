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
use std::sync::Arc;

use api::v1::column_def::try_as_column_schema;
use api::v1::meta::Partition;
use api::v1::{ColumnDataType, ColumnDef, CreateTableExpr, SemanticType};
use chrono::DateTime;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, MITO_ENGINE, MITO2_ENGINE,
};
use datatypes::schema::Schema;
use derive_builder::Builder;
use store_api::storage::TableId;
use table::metadata::{TableIdent, TableInfo, TableMeta, TableType};
use table::requests::TableOptions;

use crate::ddl::test_util::columns::TestColumnDefBuilder;
use crate::rpc::ddl::CreateTableTask;

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
    #[builder(setter(into, strip_option))]
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

/// Builds [TableInfo] from [CreateTableExpr].
pub fn build_raw_table_info_from_expr(expr: &CreateTableExpr) -> TableInfo {
    TableInfo {
        ident: TableIdent {
            table_id: expr
                .table_id
                .as_ref()
                .map(|table_id| table_id.id)
                .unwrap_or(0),
            version: 1,
        },
        name: expr.table_name.clone(),
        desc: Some(expr.desc.clone()),
        catalog_name: expr.catalog_name.clone(),
        schema_name: expr.schema_name.clone(),
        meta: TableMeta {
            schema: Arc::new(Schema::new(
                expr.column_defs
                    .iter()
                    .map(|column| try_as_column_schema(column).unwrap())
                    .collect(),
            )),
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
            engine: expr.engine.clone(),
            next_column_id: expr.column_defs.len() as u32,
            options: TableOptions::try_from_iter(&expr.table_options).unwrap(),
            created_on: DateTime::default(),
            updated_on: DateTime::default(),
            partition_key_indices: vec![],
            column_ids: vec![],
        },
        table_type: TableType::Base,
    }
}

pub fn test_create_table_task(name: &str, table_id: TableId) -> CreateTableTask {
    let create_table = TestCreateTableExprBuilder::default()
        .column_defs([
            TestColumnDefBuilder::default()
                .name("ts")
                .data_type(ColumnDataType::TimestampMillisecond)
                .semantic_type(SemanticType::Timestamp)
                .build()
                .unwrap()
                .into(),
            TestColumnDefBuilder::default()
                .name("host")
                .data_type(ColumnDataType::String)
                .semantic_type(SemanticType::Tag)
                .build()
                .unwrap()
                .into(),
            TestColumnDefBuilder::default()
                .name("cpu")
                .data_type(ColumnDataType::Float64)
                .semantic_type(SemanticType::Field)
                .build()
                .unwrap()
                .into(),
        ])
        .table_id(table_id)
        .time_index("ts")
        .primary_keys(["host".into()])
        .table_name(name)
        .engine(MITO_ENGINE)
        .build()
        .unwrap()
        .into();
    let table_info = build_raw_table_info_from_expr(&create_table);
    CreateTableTask {
        create_table,
        // Single region
        partitions: vec![Partition::default()],
        table_info,
    }
}
