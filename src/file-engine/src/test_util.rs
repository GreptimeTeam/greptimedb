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

use api::v1::SemanticType;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use common_time::timestamp::TimeUnit;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::metadata::ColumnMetadata;

pub fn new_test_object_store(prefix: &str) -> (TempDir, ObjectStore) {
    let dir = create_temp_dir(prefix);
    let store_dir = dir.path().to_string_lossy();
    let builder = Fs::default().root(&store_dir);
    (dir, ObjectStore::new(builder).unwrap().finish())
}

pub fn new_test_column_metadata() -> Vec<ColumnMetadata> {
    vec![
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_datatype(TimeUnit::Millisecond),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 0,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new("str", ConcreteDataType::string_datatype(), false),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new("num", ConcreteDataType::int64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 2,
        },
    ]
}

pub fn new_test_options() -> HashMap<String, String> {
    HashMap::from([
        ("format".to_string(), "csv".to_string()),
        ("location".to_string(), "test".to_string()),
        (
            "__private.file_table_meta".to_string(),
            "{\"files\":[\"1.csv\"],\"file_column_schemas\":[]}".to_string(),
        ),
    ])
}
