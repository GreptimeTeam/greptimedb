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

use api::v1::{Rows, SemanticType};
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AddColumn, AddColumnLocation, AlterKind, RegionAlterRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::{build_rows, put_rows, rows_schema, CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_alter_add_column() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let request = RegionAlterRequest {
        schema_version: 0,
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "tag_1",
                        ConcreteDataType::string_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Tag,
                    column_id: 3,
                },
                location: Some(AddColumnLocation::First),
            }],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(request))
        .await
        .unwrap();

    let request = ScanRequest::default();
    let scanner = engine.scan(region_id, request).unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+-------+---------+---------------------+
| tag_1 | tag_0 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 0     | 0.0     | 1970-01-01T00:00:00 |
|       | 1     | 1.0     | 1970-01-01T00:00:01 |
|       | 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}
