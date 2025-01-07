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

//! Util functions to help with fill impure default values columns in request

use ahash::{HashMap, HashMapExt, HashSet};
use datatypes::schema::ColumnSchema;
use snafu::ResultExt;
use table::metadata::{TableInfo, TableInfoRef};

use crate::error::{ConvertColumnDefaultConstraintSnafu, Result};
use crate::expr_factory::column_schemas_to_defs;

/// Find all columns that have impure default values
pub fn find_all_impure_columns(table_info: &TableInfo) -> Vec<ColumnSchema> {
    let columns = table_info.meta.schema.column_schemas();
    columns
        .iter()
        .filter(|column| column.is_default_impure())
        .cloned()
        .collect()
}

/// Fill impure default values in the request
pub struct ImpureDefaultFiller {
    table_info: TableInfoRef,
    impure_columns: HashMap<String, (api::v1::ColumnSchema, Option<api::v1::Value>)>,
}

impl ImpureDefaultFiller {
    pub fn new(table_info: TableInfoRef) -> Result<Self> {
        let impure_column_list = find_all_impure_columns(&table_info);
        let pks = table_info.meta.primary_key_indices.clone();
        let pk_names = pks
            .iter()
            .map(|&i| table_info.meta.schema.column_name_by_index(i).to_string())
            .collect::<Vec<_>>();
        let mut impure_columns = HashMap::new();
        for column in impure_column_list {
            let default_value = column
                .create_impure_default()
                .context(ConvertColumnDefaultConstraintSnafu {
                    column_name: column.name.clone(),
                })?
                .expect("impure default value");
            let grpc_default_value = api::helper::to_proto_value(default_value.clone());
            let def = column_schemas_to_defs(vec![column], &pk_names)?
                .into_iter()
                .next()
                .expect("column def have one element");
            let grpc_column_schema = api::v1::ColumnSchema {
                column_name: def.name,
                datatype: def.data_type,
                semantic_type: def.semantic_type,
                datatype_extension: def.datatype_extension,
                options: def.options,
            };
            impure_columns.insert(
                grpc_column_schema.column_name.clone(),
                (grpc_column_schema, grpc_default_value),
            );
        }
        Ok(Self {
            table_info,
            impure_columns,
        })
    }

    /// Fill impure default values in the request
    pub fn fill_rows(&self, rows: &mut api::v1::Rows) {
        let impure_columns_in_reqs: HashSet<_> = rows
            .schema
            .iter()
            .filter_map(|schema| {
                if self.impure_columns.contains_key(&schema.column_name) {
                    Some(&schema.column_name)
                } else {
                    None
                }
            })
            .collect();

        let (schema_append, row_append): (Vec<_>, Vec<_>) = self
            .impure_columns
            .iter()
            .filter_map(|(name, (schema, val))| {
                if !impure_columns_in_reqs.contains(name) {
                    Some((schema.clone(), val.clone().unwrap_or_default()))
                } else {
                    None
                }
            })
            .unzip();

        rows.schema.extend(schema_append);
        for row in rows.rows.iter_mut() {
            row.values.extend(row_append.clone());
        }
    }
}
