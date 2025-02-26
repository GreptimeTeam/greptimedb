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

use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{Int16Vector, StringVector, UInt32Vector, VectorRef};

use super::oid_column;
use super::table_names::PG_TYPE;
use crate::memory_table_cols;
use crate::system_schema::utils::tables::{i16_column, string_column};

fn pg_type_schema_columns() -> (Vec<ColumnSchema>, Vec<VectorRef>) {
    // TODO(j0hn50n133): acquire this information from `DataType` instead of hardcoding it to avoid regression.
    memory_table_cols!(
        [oid, typname, typlen],
        [
            (1, "String", -1),
            (2, "Binary", -1),
            (3, "Int8", 1),
            (4, "Int16", 2),
            (5, "Int32", 4),
            (6, "Int64", 8),
            (7, "UInt8", 1),
            (8, "UInt16", 2),
            (9, "UInt32", 4),
            (10, "UInt64", 8),
            (11, "Float32", 4),
            (12, "Float64", 8),
            (13, "Decimal", 16),
            (14, "Date", 4),
            (15, "DateTime", 8),
            (16, "Timestamp", 8),
            (17, "Time", 8),
            (18, "Duration", 8),
            (19, "Interval", 16),
            (20, "List", -1),
        ]
    );
    (
        // not quiet identical with pg, we only follow the definition in pg
        vec![oid_column(), string_column("typname"), i16_column("typlen")],
        vec![
            Arc::new(UInt32Vector::from_vec(oid)), // oid
            Arc::new(StringVector::from(typname)),
            Arc::new(Int16Vector::from_vec(typlen)), // typlen in bytes
        ],
    )
}

pub(super) fn get_schema_columns(table_name: &str) -> (SchemaRef, Vec<VectorRef>) {
    let (column_schemas, columns): (_, Vec<VectorRef>) = match table_name {
        PG_TYPE => pg_type_schema_columns(),
        _ => unreachable!("Unknown table in pg_catalog: {}", table_name),
    };
    (Arc::new(Schema::new(column_schemas)), columns)
}
