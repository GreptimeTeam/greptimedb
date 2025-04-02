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

use std::sync::Arc;

use ahash::{HashMap, HashMapExt, HashSet};
use datatypes::schema::ColumnSchema;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use table::metadata::{TableInfo, TableInfoRef};

use crate::error::{ConvertColumnDefaultConstraintSnafu, Result, UnexpectedSnafu};
use crate::expr_helper::column_schemas_to_defs;
use crate::insert::InstantAndNormalInsertRequests;

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
    impure_columns: HashMap<String, (api::v1::ColumnSchema, Option<api::v1::Value>)>,
}

impl ImpureDefaultFiller {
    pub fn new(table_info: TableInfoRef) -> Result<Self> {
        let impure_column_list = find_all_impure_columns(&table_info);
        let pks = &table_info.meta.primary_key_indices;
        let pk_names = pks
            .iter()
            .map(|&i| table_info.meta.schema.column_name_by_index(i).to_string())
            .collect::<Vec<_>>();
        let mut impure_columns = HashMap::new();
        for column in impure_column_list {
            let default_value = column
                .create_impure_default()
                .with_context(|_| ConvertColumnDefaultConstraintSnafu {
                    column_name: column.name.clone(),
                })?
                .with_context(|| UnexpectedSnafu {
                    violated: format!(
                        "Expect default value to be impure, found {:?}",
                        column.default_constraint()
                    ),
                })?;
            let grpc_default_value = api::helper::to_proto_value(default_value);
            let def = column_schemas_to_defs(vec![column], &pk_names)?.swap_remove(0);
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
        Ok(Self { impure_columns })
    }

    /// Fill impure default values in the request
    pub fn fill_rows(&self, rows: &mut api::v1::Rows) {
        let impure_columns_in_reqs: HashSet<_> = rows
            .schema
            .iter()
            .filter_map(|schema| {
                self.impure_columns
                    .contains_key(&schema.column_name)
                    .then_some(&schema.column_name)
            })
            .collect();

        if self.impure_columns.len() == impure_columns_in_reqs.len() {
            return;
        }

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
            row.values.extend_from_slice(row_append.as_slice());
        }
    }
}

/// Fill impure default values in the request(only for normal insert requests, since instant insert can be filled in flownode directly as a single source of truth)
pub fn fill_reqs_with_impure_default(
    table_infos: &HashMap<TableId, Arc<TableInfo>>,
    mut inserts: InstantAndNormalInsertRequests,
) -> Result<InstantAndNormalInsertRequests> {
    let fillers = table_infos
        .iter()
        .map(|(table_id, table_info)| {
            let table_id = *table_id;
            ImpureDefaultFiller::new(table_info.clone()).map(|filler| (table_id, filler))
        })
        .collect::<Result<HashMap<TableId, ImpureDefaultFiller>>>()?;

    let normal_inserts = &mut inserts.normal_requests;
    for request in normal_inserts.requests.iter_mut() {
        let region_id = RegionId::from(request.region_id);
        let table_id = region_id.table_id();
        let filler = fillers.get(&table_id).with_context(|| UnexpectedSnafu {
            violated: format!("impure default filler for table_id: {} not found", table_id),
        })?;

        if let Some(rows) = &mut request.rows {
            filler.fill_rows(rows);
        }
    }
    Ok(inserts)
}

#[cfg(test)]
mod tests {
    use api::v1::value::ValueData;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};
    use datatypes::value::Value;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use super::*;

    /// Create a test schema with 3 columns: `[col1 int32, ts timestampmills DEFAULT now(), col2 int32]`.
    fn new_test_schema() -> Schema {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true)
            .with_default_constraint(Some(datatypes::schema::ColumnDefaultConstraint::Function(
                "now()".to_string(),
            )))
            .unwrap(),
            ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true)
                .with_default_constraint(Some(datatypes::schema::ColumnDefaultConstraint::Value(
                    Value::from(1i32),
                )))
                .unwrap(),
        ];
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .version(123)
            .build()
            .unwrap()
    }

    pub fn new_table_info() -> TableInfo {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        TableInfoBuilder::default()
            .table_id(10)
            .table_version(5)
            .name("mytable")
            .meta(meta)
            .build()
            .unwrap()
    }

    fn column_schema_to_proto(
        column_schema: &[ColumnSchema],
        pk_names: &[String],
    ) -> Vec<api::v1::ColumnSchema> {
        column_schemas_to_defs(column_schema.to_vec(), pk_names)
            .unwrap()
            .into_iter()
            .map(|def| api::v1::ColumnSchema {
                column_name: def.name,
                datatype: def.data_type,
                semantic_type: def.semantic_type,
                datatype_extension: def.datatype_extension,
                options: def.options,
            })
            .collect()
    }

    #[test]
    fn test_impure_append() {
        let row = api::v1::Row {
            values: vec![api::v1::Value {
                value_data: Some(ValueData::I32Value(42)),
            }],
        };
        let schema = new_test_schema().column_schemas()[0].clone();
        let col_schemas = column_schema_to_proto(&[schema], &["col1".to_string()]);

        let mut rows = api::v1::Rows {
            schema: col_schemas,
            rows: vec![row],
        };

        let info = new_table_info();
        let filler = ImpureDefaultFiller::new(Arc::new(info)).unwrap();
        filler.fill_rows(&mut rows);

        assert_eq!(rows.schema[1].column_name, "ts");
        assert!(rows.schema.len() == 2 && rows.rows[0].values.len() == 2);
    }
}
