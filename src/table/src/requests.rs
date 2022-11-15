// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Table and TableEngine requests
use std::collections::HashMap;

use datatypes::prelude::VectorRef;
use datatypes::schema::{ColumnSchema, SchemaRef};

use crate::metadata::TableId;

/// Insert request
#[derive(Debug)]
pub struct InsertRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub columns_values: HashMap<String, VectorRef>,
}

#[derive(Debug, Clone)]
pub struct CreateDatabaseRequest {
    pub db_name: String,
}

/// Create table request
#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    pub id: TableId,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub desc: Option<String>,
    pub schema: SchemaRef,
    pub region_numbers: Vec<u32>,
    pub primary_key_indices: Vec<usize>,
    pub create_if_not_exists: bool,
    pub table_options: HashMap<String, String>,
}

/// Open table request
#[derive(Debug, Clone)]
pub struct OpenTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub table_id: TableId,
}

/// Alter table request
#[derive(Debug)]
pub struct AlterTableRequest {
    pub catalog_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub alter_kind: AlterKind,
}

/// Add column request
#[derive(Debug)]
pub struct AddColumnRequest {
    pub column_schema: ColumnSchema,
    pub is_key: bool,
}

#[derive(Debug)]
pub enum AlterKind {
    AddColumns { columns: Vec<AddColumnRequest> },
    RemoveColumns { names: Vec<String> },
}

/// Drop table request
#[derive(Debug)]
pub struct DropTableRequest {}
