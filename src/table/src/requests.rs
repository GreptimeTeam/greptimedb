//! Table and TableEngine requests
use std::collections::HashMap;

use datatypes::prelude::VectorRef;
use datatypes::schema::{ColumnSchema, SchemaRef};

use crate::metadata::TableId;

/// Insert request
#[derive(Debug)]
pub struct InsertRequest {
    pub table_name: String,
    pub columns_values: HashMap<String, VectorRef>,
}

/// Create table request
#[derive(Debug, Clone)]
pub struct CreateTableRequest {
    pub id: TableId,
    pub catalog_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub desc: Option<String>,
    pub schema: SchemaRef,
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

#[derive(Debug)]
pub enum AlterKind {
    AddColumn { new_column: ColumnSchema },
}

/// Drop table request
#[derive(Debug)]
pub struct DropTableRequest {}
