//! Table and TableEngine requests
use std::collections::HashMap;

use datatypes::prelude::VectorRef;
use datatypes::schema::SchemaRef;

use crate::metadata::TableId;

/// Insert request
pub struct InsertRequest {
    pub table_name: String,
    pub columns_values: HashMap<String, VectorRef>,
}

/// Create table request
#[derive(Debug)]
pub struct CreateTableRequest {
    pub name: String,
    pub desc: Option<String>,
    pub schema: SchemaRef,
    // TODO(yingwen): 1. Add catalog_name/schema_name and other infos 2. Support create_if_not_exists.
}

/// Open table request
#[derive(Debug)]
pub struct OpenTableRequest {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    // TODO(yingwen): TableId could be recovered from the table metadata.
    pub table_id: TableId,
}

/// Alter table request
#[derive(Debug)]
pub struct AlterTableRequest {}

/// Drop table request
#[derive(Debug)]
pub struct DropTableRequest {}
