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

use api::helper::{push_vals, ColumnDataTypeWrapper};
use api::v1::column::{SemanticType, Values};
use api::v1::{Column, InsertRequest as GrpcInsertRequest};
use datatypes::prelude::*;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionNumber;
use table::metadata::TableMeta;
use table::requests::InsertRequest;

use crate::error::{self, ColumnDataTypeSnafu, NotSupportedSnafu, Result, VectorToGrpcColumnSnafu};

pub(crate) fn to_grpc_columns(
    table_meta: &TableMeta,
    columns_values: &HashMap<String, VectorRef>,
) -> Result<(Vec<Column>, u32)> {
    let mut row_count = None;

    let columns = columns_values
        .iter()
        .map(|(column_name, vector)| {
            match row_count {
                Some(rows) => ensure!(
                    rows == vector.len(),
                    error::InvalidInsertRequestSnafu {
                        reason: "The row count of columns is not the same."
                    }
                ),

                None => row_count = Some(vector.len()),
            }

            let column = vector_to_grpc_column(table_meta, column_name, vector.clone())?;
            Ok(column)
        })
        .collect::<Result<Vec<_>>>()?;

    let row_count = row_count.unwrap_or(0) as u32;

    Ok((columns, row_count))
}

pub(crate) fn to_grpc_insert_request(
    table_meta: &TableMeta,
    region_number: RegionNumber,
    insert: InsertRequest,
) -> Result<GrpcInsertRequest> {
    let table_name = insert.table_name.clone();
    let (columns, row_count) = to_grpc_columns(table_meta, &insert.columns_values)?;
    Ok(GrpcInsertRequest {
        table_name,
        region_number,
        columns,
        row_count,
    })
}

fn vector_to_grpc_column(
    table_meta: &TableMeta,
    column_name: &str,
    vector: VectorRef,
) -> Result<Column> {
    let time_index_column = &table_meta
        .schema
        .timestamp_column()
        .context(NotSupportedSnafu {
            feat: "Table without time index.",
        })?
        .name;
    let semantic_type = if column_name == time_index_column {
        SemanticType::Timestamp
    } else {
        let column_index = table_meta
            .schema
            .column_index_by_name(column_name)
            .context(VectorToGrpcColumnSnafu {
                reason: format!("unable to find column {column_name} in table schema"),
            })?;
        if table_meta.primary_key_indices.contains(&column_index) {
            SemanticType::Tag
        } else {
            SemanticType::Field
        }
    };

    let datatype: ColumnDataTypeWrapper =
        vector.data_type().try_into().context(ColumnDataTypeSnafu)?;

    let mut column = Column {
        column_name: column_name.to_string(),
        semantic_type: semantic_type as i32,
        null_mask: vec![],
        datatype: datatype.datatype() as i32,
        values: Some(Values::default()), // vector values will be pushed into it below
    };
    push_vals(&mut column, 0, vector);
    Ok(column)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::ColumnDataType;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ScalarVectorBuilder;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{
        Int16VectorBuilder, Int32Vector, Int64Vector, MutableVector, StringVector,
        StringVectorBuilder,
    };
    use table::metadata::TableMetaBuilder;
    use table::requests::InsertRequest;

    use super::*;

    #[test]
    fn test_vector_to_grpc_column() {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
                .with_time_index(true),
            ColumnSchema::new("k", ConcreteDataType::int32_datatype(), false),
            ColumnSchema::new("v", ConcreteDataType::string_datatype(), true),
        ]));

        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![1])
            .next_column_id(3)
            .build()
            .unwrap();

        let column = vector_to_grpc_column(
            &table_meta,
            "ts",
            Arc::new(Int64Vector::from_slice([1, 2, 3])),
        )
        .unwrap();
        assert_eq!(column.column_name, "ts");
        assert_eq!(column.semantic_type, SemanticType::Timestamp as i32);
        assert_eq!(column.values.unwrap().i64_values, vec![1, 2, 3]);
        assert_eq!(column.null_mask, vec![0]);
        assert_eq!(column.datatype, ColumnDataType::Int64 as i32);

        let column = vector_to_grpc_column(
            &table_meta,
            "k",
            Arc::new(Int32Vector::from_slice([3, 2, 1])),
        )
        .unwrap();
        assert_eq!(column.column_name, "k");
        assert_eq!(column.semantic_type, SemanticType::Tag as i32);
        assert_eq!(column.values.unwrap().i32_values, vec![3, 2, 1]);
        assert_eq!(column.null_mask, vec![0]);
        assert_eq!(column.datatype, ColumnDataType::Int32 as i32);

        let column = vector_to_grpc_column(
            &table_meta,
            "v",
            Arc::new(StringVector::from(vec![
                Some("hello"),
                None,
                Some("greptime"),
            ])),
        )
        .unwrap();
        assert_eq!(column.column_name, "v");
        assert_eq!(column.semantic_type, SemanticType::Field as i32);
        assert_eq!(
            column.values.unwrap().string_values,
            vec!["hello", "greptime"]
        );
        assert_eq!(column.null_mask, vec![2]);
        assert_eq!(column.datatype, ColumnDataType::String as i32);
    }

    #[test]
    fn test_to_grpc_insert_request() {
        let schema = Schema::new(vec![
            ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
                .with_time_index(true),
            ColumnSchema::new("id", ConcreteDataType::int16_datatype(), false),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ]);

        let table_meta = TableMetaBuilder::default()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![])
            .next_column_id(3)
            .build()
            .unwrap();

        let insert_request = mock_insert_request();
        let request = to_grpc_insert_request(&table_meta, 12, insert_request).unwrap();

        verify_grpc_insert_request(request);
    }

    fn mock_insert_request() -> InsertRequest {
        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("host1"));
        builder.push(None);
        builder.push(Some("host3"));
        let host = builder.to_vector();

        let mut builder = Int16VectorBuilder::with_capacity(3);
        builder.push(Some(1_i16));
        builder.push(Some(2_i16));
        builder.push(Some(3_i16));
        let id = builder.to_vector();

        let columns_values = HashMap::from([("host".to_string(), host), ("id".to_string(), id)]);

        InsertRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "demo".to_string(),
            columns_values,
            region_number: 0,
        }
    }

    fn verify_grpc_insert_request(request: GrpcInsertRequest) {
        let table_name = request.table_name;
        assert_eq!("demo", table_name);

        for column in request.columns {
            let name = column.column_name;
            if name == "id" {
                assert_eq!(0, column.null_mask[0]);
                assert_eq!(ColumnDataType::Int16 as i32, column.datatype);
                assert_eq!(vec![1, 2, 3], column.values.as_ref().unwrap().i16_values);
            }
            if name == "host" {
                assert_eq!(2, column.null_mask[0]);
                assert_eq!(ColumnDataType::String as i32, column.datatype);
                assert_eq!(
                    vec!["host1", "host3"],
                    column.values.as_ref().unwrap().string_values
                );
            }
        }

        let region_number = request.region_number;
        assert_eq!(12, region_number);
    }
}
