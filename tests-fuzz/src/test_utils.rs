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

use datatypes::data_type::ConcreteDataType;

use crate::context::TableContext;
use crate::ir::create_expr::ColumnOption;
use crate::ir::Column;

pub fn new_test_ctx() -> TableContext {
    TableContext {
        name: "test".into(),
        columns: vec![
            Column {
                name: "host".into(),
                column_type: ConcreteDataType::string_datatype(),
                options: vec![ColumnOption::PrimaryKey],
            },
            Column {
                name: "idc".into(),
                column_type: ConcreteDataType::string_datatype(),
                options: vec![ColumnOption::PrimaryKey],
            },
            Column {
                name: "cpu_util".into(),
                column_type: ConcreteDataType::float64_datatype(),
                options: vec![],
            },
            Column {
                name: "memory_util".into(),
                column_type: ConcreteDataType::float64_datatype(),
                options: vec![],
            },
            Column {
                name: "disk_util".into(),
                column_type: ConcreteDataType::float64_datatype(),
                options: vec![],
            },
            Column {
                name: "ts".into(),
                column_type: ConcreteDataType::timestamp_millisecond_datatype(),
                options: vec![ColumnOption::TimeIndex],
            },
        ],
        partition: None,
        primary_keys: vec![],
        table_options: vec![],
    }
}
