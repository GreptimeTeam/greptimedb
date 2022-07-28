use std::collections::HashMap;

use chrono::{DateTime, Utc};
use datatypes::schema::SchemaRef;

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
    #[builder(default, setter(into))]
    pub engine: String,
    #[builder(default)]
    pub engine_options: HashMap<String, String>,
    #[builder(default)]
    pub options: HashMap<String, String>,
    #[builder(default = "Utc::now()")]
    pub created_on: DateTime<Utc>,
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
        let mut this = Self::default();

        this.name = Some(name.into());
        this.meta = Some(meta);

        this
    }

    pub fn table_id(mut self, id: impl Into<TableId>) -> Self {
        if self.ident.is_none() {
            self.ident = Some(TableIdent::default());
        }

        self.ident.as_mut().unwrap().table_id = id.into();
        self
    }

    pub fn table_version(mut self, version: impl Into<TableVersion>) -> Self {
        if self.ident.is_none() {
            self.ident = Some(TableIdent::default());
        }

        self.ident.as_mut().unwrap().version = version.into();
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
