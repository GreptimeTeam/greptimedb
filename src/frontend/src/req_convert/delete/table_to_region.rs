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

use api::v1::region::{
    DeleteRequest as RegionDeleteRequest, DeleteRequests as RegionDeleteRequests,
};
use api::v1::Rows;
use store_api::storage::RegionId;
use table::metadata::TableInfo;
use table::requests::DeleteRequest as TableDeleteRequest;

use crate::error::Result;
use crate::req_convert::common::{column_schema, row_count};

pub struct TableToRegion<'a> {
    table_info: &'a TableInfo,
}

impl<'a> TableToRegion<'a> {
    pub fn new(table_info: &'a TableInfo) -> Self {
        Self { table_info }
    }

    pub fn convert(&self, request: TableDeleteRequest) -> Result<RegionDeleteRequests> {
        let region_id = RegionId::new(self.table_info.table_id(), 0).into();
        let row_count = row_count(&request.key_column_values)?;
        let schema = column_schema(self.table_info, &request.key_column_values)?;
        let rows = api::helper::vectors_to_rows(request.key_column_values.values(), row_count);
        Ok(RegionDeleteRequests {
            requests: vec![RegionDeleteRequest {
                region_id,
                rows: Some(Rows { schema, rows }),
            }],
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::value::ValueData;
    use api::v1::{ColumnDataType, SemanticType};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::scalars::ScalarVectorBuilder;
    use datatypes::schema::{ColumnSchema as DtColumnSchema, Schema};
    use datatypes::vectors::{Int16VectorBuilder, MutableVector, StringVectorBuilder};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};

    use super::*;

    #[test]
    fn test_delete_request_table_to_region() {
        let schema = Schema::new(vec![
            DtColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
                .with_time_index(true),
            DtColumnSchema::new("id", ConcreteDataType::int16_datatype(), false),
            DtColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
        ]);

        let table_meta = TableMetaBuilder::default()
            .schema(Arc::new(schema))
            .primary_key_indices(vec![1, 2])
            .next_column_id(3)
            .build()
            .unwrap();

        let table_info = Arc::new(
            TableInfoBuilder::default()
                .name("demo")
                .meta(table_meta)
                .table_id(1)
                .build()
                .unwrap(),
        );

        let delete_request = mock_delete_request();
        let mut request = TableToRegion::new(&table_info)
            .convert(delete_request)
            .unwrap();

        assert_eq!(request.requests.len(), 1);
        verify_region_insert_request(request.requests.pop().unwrap());
    }

    fn mock_delete_request() -> TableDeleteRequest {
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

        let key_column_values = HashMap::from([("host".to_string(), host), ("id".to_string(), id)]);

        TableDeleteRequest {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "demo".to_string(),
            key_column_values,
        }
    }

    fn verify_region_insert_request(request: RegionDeleteRequest) {
        assert_eq!(request.region_id, RegionId::new(1, 0).as_u64());

        let rows = request.rows.unwrap();
        for (i, column) in rows.schema.iter().enumerate() {
            let name = &column.column_name;
            if name == "id" {
                assert_eq!(ColumnDataType::Int16 as i32, column.datatype);
                assert_eq!(SemanticType::Tag as i32, column.semantic_type);
                let values = rows
                    .rows
                    .iter()
                    .map(|row| row.values[i].value_data.clone())
                    .collect::<Vec<_>>();
                assert_eq!(
                    vec![
                        Some(ValueData::I16Value(1)),
                        Some(ValueData::I16Value(2)),
                        Some(ValueData::I16Value(3))
                    ],
                    values
                );
            }
            if name == "host" {
                assert_eq!(ColumnDataType::String as i32, column.datatype);
                assert_eq!(SemanticType::Tag as i32, column.semantic_type);
                let values = rows
                    .rows
                    .iter()
                    .map(|row| row.values[i].value_data.clone())
                    .collect::<Vec<_>>();
                assert_eq!(
                    vec![
                        Some(ValueData::StringValue("host1".to_string())),
                        None,
                        Some(ValueData::StringValue("host3".to_string()))
                    ],
                    values
                );
            }
        }
    }
}
