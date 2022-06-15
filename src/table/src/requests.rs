//! Table and TableEngine requests
use std::collections::HashMap;

use datatypes::prelude::VectorRef;
use datatypes::schema::SchemaRef;

/// Insert request
pub struct InsertRequest {
    pub table_name: String,
    pub columns_values: HashMap<String, VectorRef>,
}

/// Create table request
pub struct CreateTableRequest {
    pub name: String,
    pub desc: Option<String>,
    pub schema: SchemaRef,
}

/// Alter table request
pub struct AlterTableRequest {}

/// Drop table request
pub struct DropTableRequest {}
