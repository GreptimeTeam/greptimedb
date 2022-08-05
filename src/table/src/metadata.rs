use std::collections::HashMap;

use chrono::{DateTime, Utc};
use datatypes::schema::SchemaRef;
use derive_builder::Builder;
use store_api::storage::ColumnId;

pub type TableId = u64;
pub type TableVersion = u64;

/// Indicates whether and how a filter expression can be handled by a
/// Table for table scans.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterPushDownType {
    /// The expression cannot be used by the provider.
    Unsupported,
    /// The expression can be used to help minimise the data retrieved,
    /// but the provider cannot guarantee that all returned tuples
    /// satisfy the filter. The Filter plan node containing this expression
    /// will be preserved.
    Inexact,
    /// The provider guarantees that all returned data satisfies this
    /// filter expression. The Filter plan node containing this expression
    /// will be removed.
    Exact,
}

/// Indicates the type of this table for metadata/catalog purposes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableType {
    /// An ordinary physical table.
    Base,
    /// A non-materialised table that itself uses a query internally to provide data.
    View,
    /// A transient table.
    Temporary,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableIdent {
    pub table_id: TableId,
    pub version: TableVersion,
}

#[derive(Clone, Debug, Builder)]
#[builder(pattern = "owned")]
pub struct TableMeta {
    pub schema: SchemaRef,
    pub primary_key_indices: Vec<usize>,
    #[builder(default, setter(into))]
    pub engine: String,
    pub next_column_id: ColumnId,
    #[builder(default)]
    pub engine_options: HashMap<String, String>,
    #[builder(default)]
    pub options: HashMap<String, String>,
    #[builder(default = "Utc::now()")]
    pub created_on: DateTime<Utc>,
}

impl TableMeta {
    //TODO(dennis): we can keep this vector in table metadata to avoid construction.
    pub fn row_key_column_names(&self) -> Vec<&String> {
        let mut names = Vec::new();

        // It's safe to unwrap here, the created table ensure the timestamp index exists.
        let ts_column = self.schema.timestamp_column().unwrap();
        let ts_index = self.schema.timestamp_index().unwrap();
        names.push(&ts_column.name);

        for index in &self.primary_key_indices {
            if *index != ts_index {
                let column_schema = &self.schema.column_schemas()[*index];
                names.push(&column_schema.name);
            }
        }

        names
    }

    //TODO(dennis): we can keep this vector in table metadata to avoid construction.
    pub fn value_column_names(&self) -> Vec<&String> {
        let mut names = Vec::new();

        let ts_index = self.schema.timestamp_index().unwrap();
        let primary_key_indices = &self.primary_key_indices;

        for (index, column_schema) in self.schema.column_schemas().iter().enumerate() {
            if index != ts_index && !primary_key_indices.contains(&index) {
                names.push(&column_schema.name);
            }
        }

        names
    }
}

#[derive(Clone, Debug, Builder)]
#[builder(pattern = "owned")]
pub struct TableInfo {
    #[builder(default, setter(into))]
    pub ident: TableIdent,
    #[builder(setter(into))]
    pub name: String,
    #[builder(default, setter(into))]
    pub desc: Option<String>,
    pub meta: TableMeta,
    #[builder(default = "TableType::Base")]
    pub table_type: TableType,
}

impl TableInfoBuilder {
    pub fn new<S: Into<String>>(name: S, meta: TableMeta) -> Self {
        Self {
            name: Some(name.into()),
            meta: Some(meta),
            ..Default::default()
        }
    }

    pub fn table_id(mut self, id: impl Into<TableId>) -> Self {
        let ident = self.ident.get_or_insert_with(TableIdent::default);
        ident.table_id = id.into();
        self
    }

    pub fn table_version(mut self, version: impl Into<TableVersion>) -> Self {
        let ident = self.ident.get_or_insert_with(TableIdent::default);
        ident.version = version.into();
        self
    }
}

impl TableIdent {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            version: 0,
        }
    }
}

impl From<TableId> for TableIdent {
    fn from(table_id: TableId) -> Self {
        Self::new(table_id)
    }
}
