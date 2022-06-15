use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use datatypes::schema::SchemaRef;

pub type TableId = u64;
pub type TableVersion = u64;

/// Indicates whether and how a filter expression can be handled by a
/// Table for table scans.
#[derive(Debug, Clone, PartialEq)]
pub enum TableProviderFilterPushDown {
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

#[derive(Clone, Debug)]
pub struct TableMeta {
    pub schema: SchemaRef,
    pub engine: String,
    pub engine_options: HashMap<String, String>,
    pub options: HashMap<String, String>,
    pub created_on: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub ident: TableIdent,
    pub name: String,
    pub desc: Option<String>,
    pub meta: TableMeta,
    pub table_type: TableType,
}

impl TableIdent {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            version: 0,
        }
    }
}

pub struct TableMetaBuilder {
    schema: SchemaRef,
    engine: String,
    engine_options: HashMap<String, String>,
    options: HashMap<String, String>,
}

impl TableMetaBuilder {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            engine: "".to_string(),
            engine_options: HashMap::default(),
            options: HashMap::default(),
        }
    }

    pub fn engine(mut self, engine: impl Into<String>) -> Self {
        self.engine = engine.into();
        self
    }

    pub fn table_option(mut self, name: &str, val: &str) -> Self {
        self.options.insert(name.to_string(), val.to_string());
        self
    }

    pub fn engine_option(mut self, name: &str, val: &str) -> Self {
        self.engine_options
            .insert(name.to_string(), val.to_string());
        self
    }

    pub fn build(self) -> TableMeta {
        TableMeta {
            schema: self.schema,
            engine: self.engine,
            engine_options: self.engine_options,
            options: self.options,
            // TODO(dennis): use time utilities helper function
            created_on: Utc.from_utc_datetime(&NaiveDateTime::from_timestamp(0, 0)),
        }
    }
}

pub struct TableInfoBuilder {
    ident: TableIdent,
    name: String,
    desc: Option<String>,
    meta: TableMeta,
    table_type: TableType,
}

impl TableInfoBuilder {
    pub fn new(name: impl Into<String>, meta: TableMeta) -> Self {
        Self {
            ident: TableIdent::new(0),
            name: name.into(),
            desc: None,
            meta,
            table_type: TableType::Base,
        }
    }

    pub fn table_id(mut self, id: impl Into<TableId>) -> Self {
        self.ident.table_id = id.into();
        self
    }

    pub fn table_version(mut self, version: impl Into<TableVersion>) -> Self {
        self.ident.version = version.into();
        self
    }

    pub fn table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    pub fn metadata(mut self, meta: TableMeta) -> Self {
        self.meta = meta;
        self
    }

    pub fn desc(mut self, desc: Option<String>) -> Self {
        self.desc = desc;
        self
    }

    pub fn build(self) -> TableInfo {
        TableInfo {
            ident: self.ident,
            name: self.name,
            desc: self.desc,
            meta: self.meta,
            table_type: self.table_type,
        }
    }
}
