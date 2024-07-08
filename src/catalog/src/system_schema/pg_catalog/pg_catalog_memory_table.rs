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

use datatypes::schema::{Schema, SchemaRef};
use datatypes::vectors::{BooleanVector, Int16Vector, StringVector, UInt32Vector, VectorRef};

use super::oid_column;
use super::table_names::PG_TYPE;
use crate::system_schema::memory_table::tables::{bool_column, i16_column, string_column};

pub(super) fn get_schema_columns(table_name: &str) -> (SchemaRef, Vec<VectorRef>) {
    //TODO(j0hn50n133): u32_column("typnamespace"), we don't have such thing as namespace id or database id.
    let (column_schemas, columns): (_, Vec<VectorRef>) = match table_name {
        PG_TYPE => (
            vec![
                oid_column(),
                string_column("typname"),
                i16_column("typlen"),
                bool_column("typbyval"),
                bool_column("typisdefined"),
            ],
            vec![
                Arc::new(UInt32Vector::from_vec(vec![16])), // oid
                Arc::new(StringVector::from(vec!["bool"])), // typlen
                Arc::new(Int16Vector::from_vec(vec![1])),   // typname
                Arc::new(BooleanVector::from(vec![true])),  // typbyval
                Arc::new(BooleanVector::from(vec![true])),  // typisdefined
            ],
        ),
        _ => unreachable!("Unknown table in pg_catalog: {}", table_name),
    };
    (Arc::new(Schema::new(column_schemas)), columns)
}
