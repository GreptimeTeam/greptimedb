use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
pub use datatypes::error::{Error as ConvertError, Result as ConvertResult};
use datatypes::schema::{ColumnSchema, RawSchema, Schema, SchemaBuilder, SchemaRef};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::storage::{ColumnDescriptor, ColumnDescriptorBuilder, ColumnId};

use crate::error::{self, Result};
use crate::requests::{AddColumnRequest, AlterKind};

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
    /// Unique id of this table.
    pub table_id: TableId,
    /// Version of the table, bumped when metadata (such as schema) of the table
    /// being changed.
    pub version: TableVersion,
}

#[derive(Clone, Debug, Builder, PartialEq)]
#[builder(pattern = "mutable")]
pub struct TableMeta {
    pub schema: SchemaRef,
    /// The indices of columns in primary key. Note that the index of timestamp column
    /// is not included in these indices.
    pub primary_key_indices: Vec<usize>,
    #[builder(default = "self.default_value_indices()?")]
    pub value_indices: Vec<usize>,
    #[builder(default, setter(into))]
    pub engine: String,
    #[builder(default, setter(into))]
    pub region_numbers: Vec<u32>,
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
    fn default_value_indices(&self) -> std::result::Result<Vec<usize>, String> {
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

    /// Returns the new [TableMetaBuilder] after applying given `alter_kind`.
    ///
    /// The returned builder would derive the next column id of this meta.
    pub fn builder_with_alter_kind(
        &self,
        table_name: &str,
        alter_kind: &AlterKind,
    ) -> Result<TableMetaBuilder> {
        match alter_kind {
            AlterKind::AddColumns { columns } => self.add_columns(table_name, columns),
            AlterKind::RemoveColumns { names } => self.remove_columns(table_name, names),
        }
    }

    /// Allocate a new column for the table.
    ///
    /// This method would bump the `next_column_id` of the meta.
    pub fn alloc_new_column(
        &mut self,
        table_name: &str,
        new_column: &ColumnSchema,
    ) -> Result<ColumnDescriptor> {
        let desc = ColumnDescriptorBuilder::new(
            self.next_column_id as ColumnId,
            &new_column.name,
            new_column.data_type.clone(),
        )
        .is_nullable(new_column.is_nullable())
        .default_constraint(new_column.default_constraint().cloned())
        .build()
        .context(error::BuildColumnDescriptorSnafu {
            table_name,
            column_name: &new_column.name,
        })?;

        // Bump next column id.
        self.next_column_id += 1;

        Ok(desc)
    }

    fn new_meta_builder(&self) -> TableMetaBuilder {
        let mut builder = TableMetaBuilder::default();
        builder
            .engine(&self.engine)
            .engine_options(self.engine_options.clone())
            .options(self.options.clone())
            .created_on(self.created_on)
            .next_column_id(self.next_column_id);

        builder
    }

    fn add_columns(
        &self,
        table_name: &str,
        requests: &[AddColumnRequest],
    ) -> Result<TableMetaBuilder> {
        let table_schema = &self.schema;
        let mut meta_builder = self.new_meta_builder();

        // Check whether columns to add are already existing.
        for request in requests {
            let column_name = &request.column_schema.name;
            ensure!(
                table_schema.column_schema_by_name(column_name).is_none(),
                error::ColumnExistsSnafu {
                    column_name,
                    table_name,
                }
            );
        }

        // Collect names of columns to add for error message.
        let mut column_names = Vec::with_capacity(requests.len());
        let mut primary_key_indices = self.primary_key_indices.clone();
        let mut columns = Vec::with_capacity(table_schema.num_columns() + requests.len());
        columns.extend_from_slice(table_schema.column_schemas());
        // Append new columns to the end of column list.
        for request in requests {
            column_names.push(request.column_schema.name.clone());
            if request.is_key {
                // If a key column is added, we also need to store its index in primary_key_indices.
                primary_key_indices.push(columns.len());
            }
            columns.push(request.column_schema.clone());
        }

        let mut builder = SchemaBuilder::try_from(columns)
            .with_context(|_| error::SchemaBuildSnafu {
                msg: format!(
                    "Failed to convert column schemas into schema for table {}",
                    table_name
                ),
            })?
            // Also bump the schema version.
            .version(table_schema.version() + 1);
        for (k, v) in table_schema.metadata().iter() {
            builder = builder.add_metadata(k, v);
        }
        let new_schema = builder.build().with_context(|_| error::SchemaBuildSnafu {
            msg: format!(
                "Table {} cannot add new columns {:?}",
                table_name, column_names
            ),
        })?;

        // value_indices would be generated automatically.
        meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(primary_key_indices);

        Ok(meta_builder)
    }

    fn remove_columns(
        &self,
        table_name: &str,
        column_names: &[String],
    ) -> Result<TableMetaBuilder> {
        let table_schema = &self.schema;
        let column_names: HashSet<_> = column_names.iter().collect();
        let mut meta_builder = self.new_meta_builder();

        let timestamp_index = table_schema.timestamp_index();
        // Check whether columns are existing and not in primary key index.
        for column_name in &column_names {
            if let Some(index) = table_schema.column_index_by_name(column_name) {
                // This is a linear search, but since there won't be too much columns, the performance should
                // be acceptable.
                ensure!(
                    !self.primary_key_indices.contains(&index),
                    error::RemoveColumnInIndexSnafu {
                        column_name: *column_name,
                        table_name,
                    }
                );

                if let Some(ts_index) = timestamp_index {
                    // Not allowed to remove column in timestamp index.
                    ensure!(
                        index != ts_index,
                        error::RemoveColumnInIndexSnafu {
                            column_name: table_schema.column_name_by_index(ts_index),
                            table_name,
                        }
                    );
                }
            } else {
                return error::ColumnNotExistsSnafu {
                    column_name: *column_name,
                    table_name,
                }
                .fail()?;
            }
        }

        // Collect columns after removal.
        let columns: Vec<_> = table_schema
            .column_schemas()
            .iter()
            .filter(|column_schema| !column_names.contains(&column_schema.name))
            .cloned()
            .collect();

        let mut builder = SchemaBuilder::try_from_columns(columns)
            .with_context(|_| error::SchemaBuildSnafu {
                msg: format!(
                    "Failed to convert column schemas into schema for table {}",
                    table_name
                ),
            })?
            // Also bump the schema version.
            .version(table_schema.version() + 1);
        for (k, v) in table_schema.metadata().iter() {
            builder = builder.add_metadata(k, v);
        }
        let new_schema = builder.build().with_context(|_| error::SchemaBuildSnafu {
            msg: format!(
                "Table {} cannot add remove columns {:?}",
                table_name, column_names
            ),
        })?;

        // Rebuild the indices of primary key columns.
        let primary_key_indices = self
            .primary_key_indices
            .iter()
            .map(|idx| table_schema.column_name_by_index(*idx))
            // This unwrap is safe since we don't allow removing a primary key column.
            .map(|name| new_schema.column_index_by_name(name).unwrap())
            .collect();

        meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(primary_key_indices);

        Ok(meta_builder)
    }
}

#[derive(Clone, Debug, PartialEq, Builder)]
#[builder(pattern = "owned")]
pub struct TableInfo {
    /// Id and version of the table.
    #[builder(default, setter(into))]
    pub ident: TableIdent,
    /// Name of the table.
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
    pub region_numbers: Vec<u32>,
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
            region_numbers: meta.region_numbers,
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
            region_numbers: vec![],
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
    use common_error::prelude::*;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};

    use super::*;

    fn new_test_schema() -> Schema {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), false)
                .with_time_index(true),
            ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
        ];
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .version(123)
            .build()
            .unwrap()
    }

    #[test]
    fn test_raw_convert() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
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

    fn add_columns_to_meta(meta: &TableMeta) -> TableMeta {
        let new_tag = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_field = ColumnSchema::new("my_field", ConcreteDataType::string_datatype(), true);
        let alter_kind = AlterKind::AddColumns {
            columns: vec![
                AddColumnRequest {
                    column_schema: new_tag,
                    is_key: true,
                },
                AddColumnRequest {
                    column_schema: new_field,
                    is_key: false,
                },
            ],
        };

        let builder = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap();
        builder.build().unwrap()
    }

    #[test]
    fn test_add_columns() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let new_meta = add_columns_to_meta(&meta);

        let names: Vec<String> = new_meta
            .schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.name.clone())
            .collect();
        assert_eq!(&["col1", "ts", "col2", "my_tag", "my_field"], &names[..]);
        assert_eq!(&[0, 3], &new_meta.primary_key_indices[..]);
        assert_eq!(&[1, 2, 4], &new_meta.value_indices[..]);
    }

    #[test]
    fn test_remove_columns() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema.clone())
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();
        // Add more columns so we have enough candidate columns to remove.
        let meta = add_columns_to_meta(&meta);

        let alter_kind = AlterKind::RemoveColumns {
            names: vec![String::from("col2"), String::from("my_field")],
        };
        let new_meta = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap()
            .build()
            .unwrap();

        let names: Vec<String> = new_meta
            .schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.name.clone())
            .collect();
        assert_eq!(&["col1", "ts", "my_tag"], &names[..]);
        assert_eq!(&[0, 2], &new_meta.primary_key_indices[..]);
        assert_eq!(&[1], &new_meta.value_indices[..]);
        assert_eq!(
            schema.timestamp_column(),
            new_meta.schema.timestamp_column()
        );
    }

    #[test]
    fn test_remove_multiple_columns_before_timestamp() {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("col3", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), false)
                .with_time_index(true),
        ];
        let schema = Arc::new(
            SchemaBuilder::try_from(column_schemas)
                .unwrap()
                .version(123)
                .build()
                .unwrap(),
        );
        let meta = TableMetaBuilder::default()
            .schema(schema.clone())
            .primary_key_indices(vec![1])
            .engine("engine")
            .next_column_id(4)
            .build()
            .unwrap();

        // Remove columns in reverse order to test whether timestamp index is valid.
        let alter_kind = AlterKind::RemoveColumns {
            names: vec![String::from("col3"), String::from("col1")],
        };
        let new_meta = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap()
            .build()
            .unwrap();

        let names: Vec<String> = new_meta
            .schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.name.clone())
            .collect();
        assert_eq!(&["col2", "ts"], &names[..]);
        assert_eq!(&[0], &new_meta.primary_key_indices[..]);
        assert_eq!(&[1], &new_meta.value_indices[..]);
        assert_eq!(
            schema.timestamp_column(),
            new_meta.schema.timestamp_column()
        );
    }

    #[test]
    fn test_add_existing_column() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let alter_kind = AlterKind::AddColumns {
            columns: vec![AddColumnRequest {
                column_schema: ColumnSchema::new("col1", ConcreteDataType::string_datatype(), true),
                is_key: false,
            }],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::TableColumnExists, err.status_code());
    }

    #[test]
    fn test_remove_unknown_column() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let alter_kind = AlterKind::RemoveColumns {
            names: vec![String::from("unknown")],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::TableColumnNotFound, err.status_code());
    }

    #[test]
    fn test_remove_key_column() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        // Remove column in primary key.
        let alter_kind = AlterKind::RemoveColumns {
            names: vec![String::from("col1")],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());

        // Remove timestamp column.
        let alter_kind = AlterKind::RemoveColumns {
            names: vec![String::from("ts")],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_alloc_new_column() {
        let schema = Arc::new(new_test_schema());
        let mut meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();
        assert_eq!(3, meta.next_column_id);

        let column_schema = ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true);
        let desc = meta.alloc_new_column("test_table", &column_schema).unwrap();

        assert_eq!(4, meta.next_column_id);
        assert_eq!(column_schema.name, desc.name);
    }
}
