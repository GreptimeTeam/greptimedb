use chrono::DateTime;
use chrono::Utc;
use datatypes::schema::Schema as TableSchema;
use std::collections::HashMap;
use std::sync::Arc;

pub type TableId = u64;
pub type TableVersion = u64;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableIdent {
    pub table_id: TableId,
    pub version: TableVersion,
}

#[derive(Debug)]
pub struct TableInfo {
    pub ident: TableIdent,
    pub name: String,
    pub desc: Option<String>,
    pub meta: TableMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TableMeta {
    pub schema: Arc<TableSchema>,
    pub engine: String,
    pub engine_options: HashMap<String, String>,
    pub options: HashMap<String, String>,
    pub created_on: DateTime<Utc>,
}

/// Table abstraction.
#[async_trait::async_trait]
pub trait Table: Send + Sync {}
