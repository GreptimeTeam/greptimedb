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

use std::sync::Arc;

use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use servers::mysql::writer::create_mysql_column_def;

use crate::mysql::{all_datatype_testing_data, TestingData};

#[test]
fn test_create_mysql_column_def() {
    let TestingData {
        column_schemas,
        mysql_columns_def,
        ..
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let columns_def = create_mysql_column_def(&schema).unwrap();
    assert_eq!(column_schemas.len(), columns_def.len());

    for (i, column_def) in columns_def.iter().enumerate() {
        let column_schema = &column_schemas[i];
        assert_eq!(column_schema.name, column_def.column);
        let expected_coltype = mysql_columns_def[i];
        assert_eq!(column_def.coltype, expected_coltype);
    }

    let column_schemas = vec![ColumnSchema::new(
        "lists",
        ConcreteDataType::list_datatype(ConcreteDataType::string_datatype()),
        true,
    )];
    let schema = Arc::new(Schema::new(column_schemas));
    assert!(create_mysql_column_def(&schema).is_err());
}
