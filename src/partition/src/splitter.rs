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

use datatypes::data_type::DataType;
use datatypes::prelude::MutableVector;
use datatypes::schema::Schema;
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionNumber;
use table::requests::{DeleteRequest, InsertRequest};

use crate::error::{
    CreateDefaultToReadSnafu, FindPartitionColumnSnafu, FindRegionSnafu, InvalidDeleteRequestSnafu,
    InvalidInsertRequestSnafu, MissingDefaultValueSnafu, Result,
};
use crate::PartitionRuleRef;

pub type InsertRequestSplit = HashMap<RegionNumber, InsertRequest>;
pub type DeleteRequestSplit = HashMap<RegionNumber, DeleteRequest>;

pub struct WriteSplitter {
    partition_rule: PartitionRuleRef,
}

impl WriteSplitter {
    pub fn with_partition_rule(rule: PartitionRuleRef) -> Self {
        Self {
            partition_rule: rule,
        }
    }

    pub fn split_insert(
        &self,
        insert: InsertRequest,
        schema: &Schema,
    ) -> Result<InsertRequestSplit> {
        let row_nums = check_req(&insert)?;
        let mut insert = insert;
        let partition_columns = self.partition_rule.partition_columns();
        let missing_columns = schema
            .column_schemas()
            .iter()
            .filter(|schema| {
                partition_columns.contains(&schema.name)
                    && !insert.columns_values.contains_key(&schema.name)
            })
            .collect::<Vec<_>>();
        for column_schema in missing_columns {
            let default_values = column_schema
                .create_default_vector(row_nums)
                .context(CreateDefaultToReadSnafu {
                    column: &column_schema.name,
                })?
                .context(MissingDefaultValueSnafu {
                    column: &column_schema.name,
                })?;
            let _ = insert
                .columns_values
                .insert(column_schema.name.clone(), default_values);
        }
        let partition_columns =
            find_partitioning_values(&insert.columns_values, &partition_columns)?;
        let region_map = self.split_partitioning_values(&partition_columns)?;

        Ok(split_insert_request(&insert, region_map))
    }

    pub fn split_delete(
        &self,
        request: DeleteRequest,
        key_column_names: Vec<&String>,
    ) -> Result<DeleteRequestSplit> {
        Self::validate_delete_request(&request)?;

        let partition_columns = self.partition_rule.partition_columns();
        let values = find_partitioning_values(&request.key_column_values, &partition_columns)?;
        let regional_value_indexes = self.split_partitioning_values(&values)?;

        let requests = regional_value_indexes
            .into_iter()
            .map(|(region_id, value_indexes)| {
                let key_column_values = request
                    .key_column_values
                    .iter()
                    .filter(|(column_name, _)| key_column_names.contains(column_name))
                    .map(|(column_name, vector)| {
                        let mut builder = vector
                            .data_type()
                            .create_mutable_vector(value_indexes.len());

                        value_indexes.iter().for_each(|&index| {
                            builder.push_value_ref(vector.get(index).as_value_ref());
                        });

                        (column_name.to_string(), builder.to_vector())
                    })
                    .collect();
                (region_id, DeleteRequest { key_column_values })
            })
            .collect();
        Ok(requests)
    }

    fn validate_delete_request(request: &DeleteRequest) -> Result<()> {
        let rows = request
            .key_column_values
            .values()
            .next()
            .map(|x| x.len())
            .context(InvalidDeleteRequestSnafu {
                reason: "no key column values",
            })?;
        ensure!(
            rows > 0,
            InvalidDeleteRequestSnafu {
                reason: "no rows in delete request"
            }
        );
        ensure!(
            request
                .key_column_values
                .values()
                .map(|x| x.len())
                .all(|x| x == rows),
            InvalidDeleteRequestSnafu {
                reason: "the lengths of key column values are not the same"
            }
        );
        Ok(())
    }

    fn split_partitioning_values(
        &self,
        values: &[VectorRef],
    ) -> Result<HashMap<RegionNumber, Vec<usize>>> {
        if values.is_empty() {
            return Ok(HashMap::default());
        }
        let mut region_map: HashMap<RegionNumber, Vec<usize>> = HashMap::new();
        let row_count = values[0].len();
        for idx in 0..row_count {
            let region_id = match self
                .partition_rule
                .find_region(&partition_values(values, idx))
            {
                Ok(region_id) => region_id,
                Err(e) => {
                    let reason = format!("{e:?}");
                    return FindRegionSnafu { reason }.fail();
                }
            };
            region_map
                .entry(region_id)
                .or_insert_with(Vec::default)
                .push(idx);
        }
        Ok(region_map)
    }
}

fn check_req(insert: &InsertRequest) -> Result<usize> {
    let mut len: Option<usize> = None;
    for vector in insert.columns_values.values() {
        match len {
            Some(len) => ensure!(
                len == vector.len(),
                InvalidInsertRequestSnafu {
                    reason: "the lengths of vectors are not the same"
                }
            ),
            None => len = Some(vector.len()),
        }
    }
    let len = len.context(InvalidInsertRequestSnafu {
        reason: "The columns in the insert statement are empty.",
    })?;
    Ok(len)
}

fn find_partitioning_values(
    values: &HashMap<String, VectorRef>,
    partition_columns: &[String],
) -> Result<Vec<VectorRef>> {
    partition_columns
        .iter()
        .map(|column_name| {
            values
                .get(column_name)
                .cloned()
                .context(FindPartitionColumnSnafu { column_name })
        })
        .collect()
}

fn partition_values(partition_columns: &[VectorRef], idx: usize) -> Vec<Value> {
    partition_columns
        .iter()
        .map(|column| column.get(idx))
        .collect()
}

fn split_insert_request(
    insert: &InsertRequest,
    region_map: HashMap<RegionNumber, Vec<usize>>,
) -> InsertRequestSplit {
    let mut dist_insert: HashMap<RegionNumber, HashMap<&str, Box<dyn MutableVector>>> =
        HashMap::with_capacity(region_map.len());

    let row_num = insert
        .columns_values
        .values()
        .next()
        .map(|v| v.len())
        .unwrap_or(0);

    let column_count = insert.columns_values.len();
    for (column_name, vector) in &insert.columns_values {
        for (region_id, val_idxs) in &region_map {
            let region_insert = dist_insert
                .entry(*region_id)
                .or_insert_with(|| HashMap::with_capacity(column_count));
            let builder = region_insert
                .entry(column_name)
                .or_insert_with(|| vector.data_type().create_mutable_vector(row_num));
            val_idxs.iter().for_each(|idx| {
                // Safety: MutableVector is built according to column data type.
                builder.push_value_ref(vector.get(*idx).as_value_ref());
            });
        }
    }

    let catalog_name = &insert.catalog_name;
    let schema_name = &insert.schema_name;
    let table_name = &insert.table_name;
    dist_insert
        .into_iter()
        .map(|(region_number, vector_map)| {
            let columns_values = vector_map
                .into_iter()
                .map(|(column_name, mut builder)| (column_name.to_string(), builder.to_vector()))
                .collect();
            (
                region_number,
                InsertRequest {
                    catalog_name: catalog_name.to_string(),
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    columns_values,
                    region_number,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::result::Result;
    use std::sync::Arc;

    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::ScalarVectorBuilder;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema as DataTypesSchema};
    use datatypes::types::{BooleanType, Int16Type, StringType};
    use datatypes::value::Value;
    use datatypes::vectors::{
        BooleanVectorBuilder, Int16VectorBuilder, MutableVector, StringVectorBuilder, Vector,
    };
    use serde::{Deserialize, Serialize};
    use store_api::storage::RegionNumber;
    use table::requests::InsertRequest;

    use super::*;
    use crate::error::Error;
    use crate::partition::{PartitionExpr, PartitionRule};
    use crate::PartitionRuleRef;

    #[test]
    fn test_insert_req_check() {
        let right = mock_insert_request();
        let ret = check_req(&right);
        assert_eq!(ret.unwrap(), 3);

        let wrong = mock_wrong_insert_request();
        let ret = check_req(&wrong);
        assert!(ret.is_err());
    }

    fn assert_columns(columns: &HashMap<String, Arc<dyn Vector>>, expected: &[(&str, &[Value])]) {
        for (col_name, values) in expected {
            for (idx, value) in values.iter().enumerate() {
                assert_eq!(*value, columns.get(*col_name).unwrap().get(idx));
            }
        }
    }

    #[test]
    fn test_writer_spliter() {
        let insert = mock_insert_request();
        let rule = Arc::new(MockPartitionRule) as PartitionRuleRef;
        let spliter = WriteSplitter::with_partition_rule(rule);
        let mock_schema = DataTypesSchema::new(vec![
            ColumnSchema::new(
                "enable_reboot",
                ConcreteDataType::Boolean(BooleanType),
                false,
            ),
            ColumnSchema::new("id", ConcreteDataType::Int16(Int16Type {}), false),
            ColumnSchema::new("host", ConcreteDataType::String(StringType), true),
        ]);
        let ret = spliter.split_insert(insert, &mock_schema).unwrap();

        assert_eq!(2, ret.len());

        let r1_insert = ret.get(&0).unwrap();
        let r2_insert = ret.get(&1).unwrap();

        assert_eq!("demo", r1_insert.table_name);
        assert_eq!("demo", r2_insert.table_name);

        let r1_columns = &r1_insert.columns_values;
        assert_eq!(3, r1_columns.len());
        assert_columns(
            r1_columns,
            &[
                ("id", &[Value::from(1_i16)]),
                ("host", &[Value::from("host1")]),
                ("enable_reboot", &[Value::from(true)]),
            ],
        );

        let r2_columns = &r2_insert.columns_values;
        assert_eq!(3, r2_columns.len());

        assert_columns(
            r2_columns,
            &[
                ("id", &[Value::from(2_i16), Value::from(3_i16)]),
                ("host", &[Value::Null, Value::from("host3")]),
                ("enable_reboot", &[Value::from(false), Value::from(true)]),
            ],
        );
    }

    #[test]
    fn test_writer_spliter_without_partition_columns() {
        let (mock_schema, insert) = mock_schema_and_insert_request_without_partition_columns();
        let rule = Arc::new(MockPartitionRule) as PartitionRuleRef;
        let spliter = WriteSplitter::with_partition_rule(rule);
        let ret = spliter.split_insert(insert, &mock_schema).unwrap();

        assert_eq!(1, ret.len());

        let r1_insert = ret.get(&0).unwrap();

        assert_eq!("demo", r1_insert.table_name);

        let r1_columns = &r1_insert.columns_values;
        assert_eq!(3, r1_columns.len());
        assert_columns(
            r1_columns,
            &[
                (
                    "id",
                    &[Value::from(1_i16), Value::from(1_i16), Value::from(1_i16)],
                ),
                (
                    "host",
                    &[Value::from("host1"), Value::Null, Value::from("host3")],
                ),
                (
                    "enable_reboot",
                    &[Value::from(true), Value::from(false), Value::from(true)],
                ),
            ],
        );
    }

    #[test]
    fn test_partition_insert_request() {
        let insert = mock_insert_request();
        let region_map = HashMap::from([(1, vec![2, 0]), (2, vec![1])]);
        let dist_insert = split_insert_request(&insert, region_map);

        let r1_insert = dist_insert.get(&1_u32).unwrap();
        assert_eq!("demo", r1_insert.table_name);
        let expected: Value = 3_i16.into();
        assert_eq!(expected, r1_insert.columns_values.get("id").unwrap().get(0));
        let expected: Value = 1_i16.into();
        assert_eq!(expected, r1_insert.columns_values.get("id").unwrap().get(1));
        let expected: Value = "host3".into();
        assert_eq!(
            expected,
            r1_insert.columns_values.get("host").unwrap().get(0)
        );
        let expected: Value = "host1".into();
        assert_eq!(
            expected,
            r1_insert.columns_values.get("host").unwrap().get(1)
        );
        let expected: Value = true.into();
        assert_eq!(
            expected,
            r1_insert
                .columns_values
                .get("enable_reboot")
                .unwrap()
                .get(0)
        );
        let expected: Value = true.into();
        assert_eq!(
            expected,
            r1_insert
                .columns_values
                .get("enable_reboot")
                .unwrap()
                .get(1)
        );

        let r2_insert = dist_insert.get(&2_u32).unwrap();
        assert_eq!("demo", r2_insert.table_name);
        let expected: Value = 2_i16.into();
        assert_eq!(expected, r2_insert.columns_values.get("id").unwrap().get(0));
        assert_eq!(
            Value::Null,
            r2_insert.columns_values.get("host").unwrap().get(0)
        );
        let expected: Value = false.into();
        assert_eq!(
            expected,
            r2_insert
                .columns_values
                .get("enable_reboot")
                .unwrap()
                .get(0)
        );
    }

    #[test]
    fn test_partition_columns() {
        let insert = mock_insert_request();

        let partition_column_names = vec!["host".to_string(), "id".to_string()];
        let columns =
            find_partitioning_values(&insert.columns_values, &partition_column_names).unwrap();

        let host_column = columns[0].clone();
        assert_eq!(
            ConcreteDataType::String(StringType),
            host_column.data_type()
        );
        assert_eq!(1, host_column.null_count());

        let id_column = columns[1].clone();
        assert_eq!(ConcreteDataType::int16_datatype(), id_column.data_type());
        assert_eq!(0, id_column.null_count());
    }

    #[test]
    fn test_partition_values() {
        let mut builder = BooleanVectorBuilder::with_capacity(3);
        builder.push(Some(true));
        builder.push(Some(false));
        builder.push(Some(true));
        let v1 = builder.to_vector();

        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("host1"));
        builder.push(None);
        builder.push(Some("host3"));
        let v2 = builder.to_vector();

        let vectors = vec![v1, v2];

        let row_0_vals = partition_values(&vectors, 0);
        let expected: Vec<Value> = vec![true.into(), "host1".into()];
        assert_eq!(expected, row_0_vals);

        let row_1_vals = partition_values(&vectors, 1);
        let expected: Vec<Value> = vec![false.into(), Value::Null];
        assert_eq!(expected, row_1_vals);

        let row_2_vals = partition_values(&vectors, 2);
        let expected: Vec<Value> = vec![true.into(), "host3".into()];
        assert_eq!(expected, row_2_vals);
    }

    fn mock_insert_request() -> InsertRequest {
        let mut columns_values = HashMap::with_capacity(4);
        let mut builder = BooleanVectorBuilder::with_capacity(3);
        builder.push(Some(true));
        builder.push(Some(false));
        builder.push(Some(true));
        let _ = columns_values.insert("enable_reboot".to_string(), builder.to_vector());

        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("host1"));
        builder.push(None);
        builder.push(Some("host3"));
        let _ = columns_values.insert("host".to_string(), builder.to_vector());

        let mut builder = Int16VectorBuilder::with_capacity(3);
        builder.push(Some(1_i16));
        builder.push(Some(2_i16));
        builder.push(Some(3_i16));
        let _ = columns_values.insert("id".to_string(), builder.to_vector());

        InsertRequest {
            catalog_name: common_catalog::consts::DEFAULT_CATALOG_NAME.to_string(),
            schema_name: common_catalog::consts::DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "demo".to_string(),
            columns_values,
            region_number: 0,
        }
    }

    fn mock_schema_and_insert_request_without_partition_columns() -> (Schema, InsertRequest) {
        let mut columns_values = HashMap::with_capacity(4);
        let mut builder = BooleanVectorBuilder::with_capacity(3);
        builder.push(Some(true));
        builder.push(Some(false));
        builder.push(Some(true));
        let _ = columns_values.insert("enable_reboot".to_string(), builder.to_vector());

        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("host1"));
        builder.push(None);
        builder.push(Some("host3"));
        let _ = columns_values.insert("host".to_string(), builder.to_vector());

        let insert_request = InsertRequest {
            catalog_name: common_catalog::consts::DEFAULT_CATALOG_NAME.to_string(),
            schema_name: common_catalog::consts::DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "demo".to_string(),
            columns_values,
            region_number: 0,
        };

        let id_column = ColumnSchema::new("id", ConcreteDataType::Int16(Int16Type {}), false);
        let id_column = id_column
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(1_i16))))
            .unwrap();
        let mock_schema = DataTypesSchema::new(vec![
            ColumnSchema::new(
                "enable_reboot",
                ConcreteDataType::Boolean(BooleanType),
                false,
            ),
            id_column,
            ColumnSchema::new("host", ConcreteDataType::String(StringType), true),
        ]);

        (mock_schema, insert_request)
    }

    fn mock_wrong_insert_request() -> InsertRequest {
        let mut columns_values = HashMap::with_capacity(4);
        let mut builder = BooleanVectorBuilder::with_capacity(3);
        builder.push(Some(true));
        builder.push(Some(false));
        builder.push(Some(true));
        let _ = columns_values.insert("enable_reboot".to_string(), builder.to_vector());

        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("host1"));
        builder.push(None);
        builder.push(Some("host3"));
        let _ = columns_values.insert("host".to_string(), builder.to_vector());

        let mut builder = Int16VectorBuilder::with_capacity(1);
        builder.push(Some(1_i16));
        // two values are missing
        let _ = columns_values.insert("id".to_string(), builder.to_vector());

        InsertRequest {
            catalog_name: common_catalog::consts::DEFAULT_CATALOG_NAME.to_string(),
            schema_name: common_catalog::consts::DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "demo".to_string(),
            columns_values,
            region_number: 0,
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct MockPartitionRule;

    // PARTITION BY LIST COLUMNS(id) (
    //     PARTITION r0 VALUES IN(1),
    //     PARTITION r1 VALUES IN(2, 3),
    // );
    impl PartitionRule for MockPartitionRule {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn partition_columns(&self) -> Vec<String> {
            vec!["id".to_string()]
        }

        fn find_region(&self, values: &[Value]) -> Result<RegionNumber, Error> {
            let val = values.get(0).unwrap().clone();
            let id_1: Value = 1_i16.into();
            let id_2: Value = 2_i16.into();
            let id_3: Value = 3_i16.into();
            if val == id_1 {
                return Ok(0);
            }
            if val == id_2 || val == id_3 {
                return Ok(1);
            }
            unreachable!()
        }

        fn find_regions_by_exprs(&self, _: &[PartitionExpr]) -> Result<Vec<RegionNumber>, Error> {
            unimplemented!()
        }
    }
}
