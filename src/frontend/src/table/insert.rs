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
use api::v1::column::SemanticType;
use api::v1::{Column, InsertRequest as GrpcInsertRequest};
use common_query::Output;
use datatypes::prelude::{ConcreteDataType, VectorRef};
use futures::future;
use snafu::{ensure, ResultExt};
use store_api::storage::RegionNumber;
use table::requests::InsertRequest;

use super::DistTable;
use crate::error;
use crate::error::{JoinTaskSnafu, RequestDatanodeSnafu, Result};

impl DistTable {
    pub async fn dist_insert(&self, inserts: Vec<GrpcInsertRequest>) -> Result<Output> {
        let regions = inserts.iter().map(|x| x.region_number).collect::<Vec<_>>();
        let instances = self.find_datanode_instances(&regions).await?;

        let results = future::try_join_all(instances.into_iter().zip(inserts.into_iter()).map(
            |(instance, request)| {
                common_runtime::spawn_write(async move {
                    instance
                        .grpc_insert(request)
                        .await
                        .context(RequestDatanodeSnafu)
                })
            },
        ))
        .await
        .context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u32>>()?;
        Ok(Output::AffectedRows(affected_rows as _))
    }
}

pub fn insert_request_to_insert_batch(insert: &InsertRequest) -> Result<(Vec<Column>, u32)> {
    to_grpc_columns(&insert.columns_values)
}

pub(crate) fn to_grpc_columns(
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

            let datatype: ColumnDataTypeWrapper = vector
                .data_type()
                .try_into()
                .context(error::ColumnDataTypeSnafu)?;

            // TODO(hl): need refactor
            let semantic_type =
                if vector.data_type() == ConcreteDataType::timestamp_millisecond_datatype() {
                    SemanticType::Timestamp
                } else {
                    SemanticType::Field
                };

            let mut column = Column {
                column_name: column_name.clone(),
                semantic_type: semantic_type.into(),
                datatype: datatype.datatype() as i32,
                ..Default::default()
            };

            push_vals(&mut column, 0, vector.clone());
            Ok(column)
        })
        .collect::<Result<Vec<_>>>()?;

    let row_count = row_count.unwrap_or(0) as u32;

    Ok((columns, row_count))
}

pub(crate) fn to_grpc_insert_request(
    region_number: RegionNumber,
    insert: InsertRequest,
) -> Result<GrpcInsertRequest> {
    let table_name = insert.table_name.clone();
    let (columns, row_count) = insert_request_to_insert_batch(&insert)?;
    Ok(GrpcInsertRequest {
        table_name,
        region_number,
        columns,
        row_count,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use api::v1::ColumnDataType;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ScalarVectorBuilder;
    use datatypes::vectors::{Int16VectorBuilder, MutableVector, StringVectorBuilder};
    use table::requests::InsertRequest;

    use super::*;

    #[test]
    fn test_to_grpc_insert_request() {
        let insert_request = mock_insert_request();

        let request = to_grpc_insert_request(12, insert_request).unwrap();

        verify_grpc_insert_request(request);
    }

    fn mock_insert_request() -> InsertRequest {
        let mut columns_values = HashMap::with_capacity(4);

        let mut builder = StringVectorBuilder::with_capacity(3);
        builder.push(Some("host1"));
        builder.push(None);
        builder.push(Some("host3"));
        columns_values.insert("host".to_string(), builder.to_vector());

        let mut builder = Int16VectorBuilder::with_capacity(3);
        builder.push(Some(1_i16));
        builder.push(Some(2_i16));
        builder.push(Some(3_i16));
        columns_values.insert("id".to_string(), builder.to_vector());

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
