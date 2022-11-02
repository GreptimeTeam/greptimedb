use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
pub use datatypes::error::{Error as ConvertError, Result as ConvertResult};
use datatypes::schema::{RawSchema, Schema, SchemaRef};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use store_api::storage::ColumnId;

pub type TableId = u32;
pub type TableVersion = u64;

/// Indicates whether and how a filter expression can be handled by a
/// Table for table scans.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    /// An ordinary physical table.
    Base,
    /// A non-materialised table that itself uses a query internally to provide data.
    View,
    /// A transient table.
    Temporary,
}

/// Identifier of the table.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableIdent {
    pub table_id: TableId,
    pub version: TableVersion,
}

#[derive(Clone, Debug, Builder, PartialEq)]
#[builder(pattern = "mutable")]
pub struct TableMeta {
    pub schema: SchemaRef,
    pub primary_key_indices: Vec<usize>,
    #[builder(default = "self.default_value_indices()?")]
    pub value_indices: Vec<usize>,
    #[builder(default, setter(into))]
    pub engine: String,
    pub next_column_id: ColumnId,
    /// Options for table engine.
    #[builder(default)]
    pub engine_options: HashMap<String, String>,
    /// Table options.
    #[builder(default)]
    pub options: HashMap<String, String>,
    #[builder(default = "Utc::now()")]
    pub created_on: DateTime<Utc>,
}

impl TableMetaBuilder {
    fn default_value_indices(&self) -> Result<Vec<usize>, String> {
        match (&self.primary_key_indices, &self.schema) {
            (Some(v), Some(schema)) => {
                let column_schemas = schema.column_schemas();
                Ok((0..column_schemas.len())
                    .filter(|idx| !v.contains(idx))
                    .collect())
            }
            _ => Err("Missing primary_key_indices or schema to create value_indices".to_string()),
        }
    }
}

impl TableMeta {
    pub fn row_key_column_names(&self) -> impl Iterator<Item = &String> {
        let columns_schemas = &self.schema.column_schemas();
        self.primary_key_indices
            .iter()
            .map(|idx| &columns_schemas[*idx].name)
    }

    pub fn value_column_names(&self) -> impl Iterator<Item = &String> {
        let columns_schemas = &self.schema.column_schemas();
        self.value_indices
            .iter()
            .map(|idx| &columns_schemas[*idx].name)
    }
}

#[derive(Clone, Debug, PartialEq, Builder)]
#[builder(pattern = "owned")]
pub struct TableInfo {
    #[builder(default, setter(into))]
    pub ident: TableIdent,
    #[builder(setter(into))]
    pub name: String,
    /// Comment of the table.
    #[builder(default, setter(into))]
    pub desc: Option<String>,
    #[builder(default, setter(into))]
    pub catalog_name: String,
    #[builder(default, setter(into))]
    pub schema_name: String,
    pub meta: TableMeta,
    #[builder(default = "TableType::Base")]
    pub table_type: TableType,
}

pub type TableInfoRef = Arc<TableInfo>;

impl TableInfoBuilder {
    pub fn new<S: Into<String>>(name: S, meta: TableMeta) -> Self {
        Self {
            name: Some(name.into()),
            meta: Some(meta),
            ..Default::default()
        }
    }

    pub fn table_id(mut self, id: TableId) -> Self {
        let ident = self.ident.get_or_insert_with(TableIdent::default);
        ident.table_id = id;
        self
    }

    pub fn table_version(mut self, version: TableVersion) -> Self {
        let ident = self.ident.get_or_insert_with(TableIdent::default);
        ident.version = version;
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

/// Struct used to serialize and deserialize [`TableMeta`].
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RawTableMeta {
    pub schema: RawSchema,
    pub primary_key_indices: Vec<usize>,
    pub value_indices: Vec<usize>,
    pub engine: String,
    pub next_column_id: ColumnId,
    pub engine_options: HashMap<String, String>,
    pub options: HashMap<String, String>,
    pub created_on: DateTime<Utc>,
}

impl From<TableMeta> for RawTableMeta {
    fn from(meta: TableMeta) -> RawTableMeta {
        RawTableMeta {
            schema: RawSchema::from(&*meta.schema),
            primary_key_indices: meta.primary_key_indices,
            value_indices: meta.value_indices,
            engine: meta.engine,
            next_column_id: meta.next_column_id,
            engine_options: meta.engine_options,
            options: meta.options,
            created_on: meta.created_on,
        }
    }
}

impl TryFrom<RawTableMeta> for TableMeta {
    type Error = ConvertError;

    fn try_from(raw: RawTableMeta) -> ConvertResult<TableMeta> {
        Ok(TableMeta {
            schema: Arc::new(Schema::try_from(raw.schema)?),
            primary_key_indices: raw.primary_key_indices,
            value_indices: raw.value_indices,
            engine: raw.engine,
            next_column_id: raw.next_column_id,
            engine_options: raw.engine_options,
            options: raw.options,
            created_on: raw.created_on,
        })
    }
}

/// Struct used to serialize and deserialize [`TableInfo`].
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct RawTableInfo {
    pub ident: TableIdent,
    pub name: String,
    pub desc: Option<String>,
    pub catalog_name: String,
    pub schema_name: String,
    pub meta: RawTableMeta,
    pub table_type: TableType,
}

impl From<TableInfo> for RawTableInfo {
    fn from(info: TableInfo) -> RawTableInfo {
        RawTableInfo {
            ident: info.ident,
            name: info.name,
            desc: info.desc,
            catalog_name: info.catalog_name,
            schema_name: info.schema_name,
            meta: RawTableMeta::from(info.meta),
            table_type: info.table_type,
        }
    }
}

impl TryFrom<RawTableInfo> for TableInfo {
    type Error = ConvertError;

    fn try_from(raw: RawTableInfo) -> ConvertResult<TableInfo> {
        Ok(TableInfo {
            ident: raw.ident,
            name: raw.name,
            desc: raw.desc,
            catalog_name: raw.catalog_name,
            schema_name: raw.schema_name,
            meta: TableMeta::try_from(raw.meta)?,
            table_type: raw.table_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};

    use super::*;

    fn new_test_schema() -> Schema {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), false),
        ];
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .timestamp_index(Some(1))
            .version(123)
            .build()
            .unwrap()
    }

    #[test]
    fn test_raw_convert() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![1])
            .value_indices(vec![0])
            .engine("engine")
            .next_column_id(2)
            .build()
            .unwrap();
        let info = TableInfoBuilder::default()
            .table_id(10)
            .table_version(5)
            .name("mytable")
            .meta(meta)
            .build()
            .unwrap();

        let raw = RawTableInfo::from(info.clone());
        let info_new = TableInfo::try_from(raw).unwrap();

        assert_eq!(info, info_new);
    }
}
