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
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder, SchemaRef};

/// Column definition: (name, datatype, is_nullable)
pub type ColumnDef<'a> = (&'a str, LogicalTypeId, bool);

pub fn new_schema(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> Schema {
    let column_schemas: Vec<_> = column_defs
        .iter()
        .enumerate()
        .map(|(index, column_def)| {
            let datatype = column_def.1.data_type();
            if let Some(timestamp_index) = timestamp_index {
                ColumnSchema::new(column_def.0, datatype, column_def.2)
                    .with_time_index(index == timestamp_index)
            } else {
                ColumnSchema::new(column_def.0, datatype, column_def.2)
            }
        })
        .collect();

    SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .build()
        .unwrap()
}

pub fn new_schema_ref(column_defs: &[ColumnDef], timestamp_index: Option<usize>) -> SchemaRef {
    Arc::new(new_schema(column_defs, timestamp_index))
}
