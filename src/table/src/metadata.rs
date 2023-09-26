// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::AddColumnLocation;
use datafusion_expr::TableProviderFilterPushDown;
pub use datatypes::error::{Error as ConvertError, Result as ConvertResult};
use datatypes::schema::{ColumnSchema, RawSchema, Schema, SchemaBuilder, SchemaRef};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use store_api::storage::{ColumnDescriptor, ColumnDescriptorBuilder, ColumnId, RegionId};

use crate::error::{self, Result};
use crate::requests::{AddColumnRequest, AlterKind, TableOptions};

pub type TableId = u32;
pub type TableVersion = u64;

/// Indicates whether and how a filter expression can be handled by a
/// Table for table scans.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
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

impl From<TableProviderFilterPushDown> for FilterPushDownType {
    fn from(value: TableProviderFilterPushDown) -> Self {
        match value {
            TableProviderFilterPushDown::Unsupported => FilterPushDownType::Unsupported,
            TableProviderFilterPushDown::Inexact => FilterPushDownType::Inexact,
            TableProviderFilterPushDown::Exact => FilterPushDownType::Exact,
        }
    }
}

impl From<FilterPushDownType> for TableProviderFilterPushDown {
    fn from(value: FilterPushDownType) -> Self {
        match value {
            FilterPushDownType::Unsupported => TableProviderFilterPushDown::Unsupported,
            FilterPushDownType::Inexact => TableProviderFilterPushDown::Inexact,
            FilterPushDownType::Exact => TableProviderFilterPushDown::Exact,
        }
    }
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

impl std::fmt::Display for TableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableType::Base => f.write_str("BASE TABLE"),
            TableType::Temporary => f.write_str("TEMPORARY"),
            TableType::View => f.write_str("VIEW"),
        }
    }
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

/// The table metadata
/// Note: if you add new fields to this struct, please ensure 'new_meta_builder' function works.
/// TODO(dennis): find a better way to ensure 'new_meta_builder' works when adding new fields.
#[derive(Clone, Debug, Builder, PartialEq, Eq)]
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
    /// Table options.
    #[builder(default)]
    pub options: TableOptions,
    #[builder(default = "Utc::now()")]
    pub created_on: DateTime<Utc>,
    #[builder(default = "Vec::new()")]
    pub partition_key_indices: Vec<usize>,
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

    pub fn new_external_table() -> Self {
        Self {
            primary_key_indices: Some(Vec::new()),
            value_indices: Some(Vec::new()),
            region_numbers: Some(Vec::new()),
            next_column_id: Some(0),
            ..Default::default()
        }
    }
}

/// The result after splitting requests by column location info.
struct SplitResult<'a> {
    /// column requests should be added at first place.
    columns_at_first: Vec<&'a AddColumnRequest>,
    /// column requests should be added after already exist columns.
    columns_at_after: HashMap<String, Vec<&'a AddColumnRequest>>,
    /// column requests should be added at last place.
    columns_at_last: Vec<&'a AddColumnRequest>,
    /// all column names should be added.
    column_names: Vec<String>,
}

impl TableMeta {
    pub fn row_key_column_names(&self) -> impl Iterator<Item = &String> {
        let columns_schemas = &self.schema.column_schemas();
        self.primary_key_indices
            .iter()
            .map(|idx| &columns_schemas[*idx].name)
    }

    pub fn field_column_names(&self) -> impl Iterator<Item = &String> {
        // `value_indices` is wrong under distributed mode. Use the logic copied from DESC TABLE
        let columns_schemas = self.schema.column_schemas();
        let primary_key_indices = &self.primary_key_indices;
        columns_schemas
            .iter()
            .enumerate()
            .filter(|(i, cs)| !primary_key_indices.contains(i) && !cs.is_time_index())
            .map(|(_, cs)| &cs.name)
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
            AlterKind::DropColumns { names } => self.remove_columns(table_name, names),
            // No need to rebuild table meta when renaming tables.
            AlterKind::RenameTable { .. } => {
                let mut meta_builder = TableMetaBuilder::default();
                let _ = meta_builder
                    .schema(self.schema.clone())
                    .primary_key_indices(self.primary_key_indices.clone())
                    .engine(self.engine.clone())
                    .next_column_id(self.next_column_id);
                Ok(meta_builder)
            }
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
        let _ = builder
            .engine(&self.engine)
            .options(self.options.clone())
            .created_on(self.created_on)
            .region_numbers(self.region_numbers.clone())
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
        let original_primary_key_indices: HashSet<&usize> =
            self.primary_key_indices.iter().collect();

        let mut names = HashSet::with_capacity(requests.len());

        for col_to_add in requests {
            ensure!(
                names.insert(&col_to_add.column_schema.name),
                error::InvalidAlterRequestSnafu {
                    table: table_name,
                    err: format!(
                        "add column {} more than once",
                        col_to_add.column_schema.name
                    ),
                }
            );

            ensure!(
                !table_schema.contains_column(&col_to_add.column_schema.name),
                error::ColumnExistsSnafu {
                    table_name,
                    column_name: col_to_add.column_schema.name.to_string()
                },
            );

            ensure!(
                col_to_add.column_schema.is_nullable()
                    || col_to_add.column_schema.default_constraint().is_some(),
                error::InvalidAlterRequestSnafu {
                    table: table_name,
                    err: format!(
                        "no default value for column {}",
                        col_to_add.column_schema.name
                    ),
                },
            );
        }

        let SplitResult {
            columns_at_first,
            columns_at_after,
            columns_at_last,
            column_names,
        } = self.split_requests_by_column_location(table_name, requests)?;
        let mut primary_key_indices = Vec::with_capacity(self.primary_key_indices.len());
        let mut columns = Vec::with_capacity(table_schema.num_columns() + requests.len());
        // add new columns with FIRST, and in reverse order of requests.
        columns_at_first.iter().rev().for_each(|request| {
            if request.is_key {
                // If a key column is added, we also need to store its index in primary_key_indices.
                primary_key_indices.push(columns.len());
            }
            columns.push(request.column_schema.clone());
        });
        // add existed columns in original order and handle new columns with AFTER.
        for (index, column_schema) in table_schema.column_schemas().iter().enumerate() {
            if original_primary_key_indices.contains(&index) {
                primary_key_indices.push(columns.len());
            }
            columns.push(column_schema.clone());
            if let Some(requests) = columns_at_after.get(&column_schema.name) {
                requests.iter().rev().for_each(|request| {
                    if request.is_key {
                        // If a key column is added, we also need to store its index in primary_key_indices.
                        primary_key_indices.push(columns.len());
                    }
                    columns.push(request.column_schema.clone());
                });
            }
        }
        // add new columns without location info to last.
        columns_at_last.iter().for_each(|request| {
            if request.is_key {
                // If a key column is added, we also need to store its index in primary_key_indices.
                primary_key_indices.push(columns.len());
            }
            columns.push(request.column_schema.clone());
        });

        let mut builder = SchemaBuilder::try_from(columns)
            .with_context(|_| error::SchemaBuildSnafu {
                msg: format!("Failed to convert column schemas into schema for table {table_name}"),
            })?
            // Also bump the schema version.
            .version(table_schema.version() + 1);
        for (k, v) in table_schema.metadata().iter() {
            builder = builder.add_metadata(k, v);
        }
        let new_schema = builder.build().with_context(|_| error::SchemaBuildSnafu {
            msg: format!("Table {table_name} cannot add new columns {column_names:?}"),
        })?;

        // value_indices would be generated automatically.
        let _ = meta_builder
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
                msg: format!("Failed to convert column schemas into schema for table {table_name}"),
            })?
            // Also bump the schema version.
            .version(table_schema.version() + 1);
        for (k, v) in table_schema.metadata().iter() {
            builder = builder.add_metadata(k, v);
        }
        let new_schema = builder.build().with_context(|_| error::SchemaBuildSnafu {
            msg: format!("Table {table_name} cannot add remove columns {column_names:?}"),
        })?;

        // Rebuild the indices of primary key columns.
        let primary_key_indices = self
            .primary_key_indices
            .iter()
            .map(|idx| table_schema.column_name_by_index(*idx))
            // This unwrap is safe since we don't allow removing a primary key column.
            .map(|name| new_schema.column_index_by_name(name).unwrap())
            .collect();

        let _ = meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(primary_key_indices);

        Ok(meta_builder)
    }

    /// Split requests into different groups using column location info.
    fn split_requests_by_column_location<'a>(
        &self,
        table_name: &str,
        requests: &'a [AddColumnRequest],
    ) -> Result<SplitResult<'a>> {
        let table_schema = &self.schema;
        let mut columns_at_first = Vec::new();
        let mut columns_at_after = HashMap::new();
        let mut columns_at_last = Vec::new();
        let mut column_names = Vec::with_capacity(requests.len());
        for request in requests {
            // Check whether columns to add are already existing.
            let column_name = &request.column_schema.name;
            column_names.push(column_name.clone());
            ensure!(
                table_schema.column_schema_by_name(column_name).is_none(),
                error::ColumnExistsSnafu {
                    column_name,
                    table_name,
                }
            );
            match request.location.as_ref() {
                Some(AddColumnLocation::First) => {
                    columns_at_first.push(request);
                }
                Some(AddColumnLocation::After { column_name }) => {
                    ensure!(
                        table_schema.column_schema_by_name(column_name).is_some(),
                        error::ColumnNotExistsSnafu {
                            column_name,
                            table_name,
                        }
                    );
                    columns_at_after
                        .entry(column_name.clone())
                        .or_insert(Vec::new())
                        .push(request);
                }
                None => {
                    columns_at_last.push(request);
                }
            }
        }
        Ok(SplitResult {
            columns_at_first,
            columns_at_after,
            columns_at_last,
            column_names,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Builder)]
#[builder(pattern = "owned")]
pub struct TableInfo {
    /// Id and version of the table.
    #[builder(default, setter(into))]
    pub ident: TableIdent,

    // TODO(LFC): Remove the catalog, schema and table names from TableInfo.
    /// Name of the table.
    #[builder(setter(into))]
    pub name: String,
    /// Comment of the table.
    #[builder(default, setter(into))]
    pub desc: Option<String>,
    #[builder(default = "DEFAULT_CATALOG_NAME.to_string()", setter(into))]
    pub catalog_name: String,
    #[builder(default = "DEFAULT_SCHEMA_NAME.to_string()", setter(into))]
    pub schema_name: String,
    pub meta: TableMeta,
    #[builder(default = "TableType::Base")]
    pub table_type: TableType,
}

pub type TableInfoRef = Arc<TableInfo>;

impl TableInfo {
    pub fn table_id(&self) -> TableId {
        self.ident.table_id
    }

    pub fn region_ids(&self) -> Vec<RegionId> {
        self.meta
            .region_numbers
            .iter()
            .map(|id| RegionId::new(self.table_id(), *id))
            .collect()
    }
    /// Returns the full table name in the form of `{catalog}.{schema}.{table}`.
    pub fn full_table_name(&self) -> String {
        common_catalog::format_full_table_name(&self.catalog_name, &self.schema_name, &self.name)
    }
}

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
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct RawTableMeta {
    pub schema: RawSchema,
    pub primary_key_indices: Vec<usize>,
    pub value_indices: Vec<usize>,
    pub engine: String,
    pub next_column_id: ColumnId,
    pub region_numbers: Vec<u32>,
    pub options: TableOptions,
    pub created_on: DateTime<Utc>,
    #[serde(default)]
    pub partition_key_indices: Vec<usize>,
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
            options: meta.options,
            created_on: meta.created_on,
            partition_key_indices: meta.partition_key_indices,
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
            region_numbers: raw.region_numbers,
            next_column_id: raw.next_column_id,
            options: raw.options,
            created_on: raw.created_on,
            partition_key_indices: raw.partition_key_indices,
        })
    }
}

/// Struct used to serialize and deserialize [`TableInfo`].
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
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

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};

    use super::*;

    fn new_test_schema() -> Schema {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
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
                    location: None,
                },
                AddColumnRequest {
                    column_schema: new_field,
                    is_key: false,
                    location: None,
                },
            ],
        };

        let builder = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap();
        builder.build().unwrap()
    }

    fn add_columns_to_meta_with_location(meta: &TableMeta) -> TableMeta {
        let new_tag = ColumnSchema::new("my_tag_first", ConcreteDataType::string_datatype(), true);
        let new_field = ColumnSchema::new(
            "my_field_after_ts",
            ConcreteDataType::string_datatype(),
            true,
        );
        let alter_kind = AlterKind::AddColumns {
            columns: vec![
                AddColumnRequest {
                    column_schema: new_tag,
                    is_key: true,
                    location: Some(AddColumnLocation::First),
                },
                AddColumnRequest {
                    column_schema: new_field,
                    is_key: false,
                    location: Some(AddColumnLocation::After {
                        column_name: "ts".to_string(),
                    }),
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
        assert_eq!(meta.region_numbers, new_meta.region_numbers);

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

        let alter_kind = AlterKind::DropColumns {
            names: vec![String::from("col2"), String::from("my_field")],
        };
        let new_meta = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(meta.region_numbers, new_meta.region_numbers);

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
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
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
        let alter_kind = AlterKind::DropColumns {
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
                location: None,
            }],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::TableColumnExists, err.status_code());
    }

    #[test]
    fn test_add_invalid_column() {
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
                column_schema: ColumnSchema::new(
                    "weny",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                is_key: false,
                location: None,
            }],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
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

        let alter_kind = AlterKind::DropColumns {
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
        let alter_kind = AlterKind::DropColumns {
            names: vec![String::from("col1")],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());

        // Remove timestamp column.
        let alter_kind = AlterKind::DropColumns {
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

    #[test]
    fn test_add_columns_with_location() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let new_meta = add_columns_to_meta_with_location(&meta);
        assert_eq!(meta.region_numbers, new_meta.region_numbers);

        let names: Vec<String> = new_meta
            .schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.name.clone())
            .collect();
        assert_eq!(
            &["my_tag_first", "col1", "ts", "my_field_after_ts", "col2"],
            &names[..]
        );
        assert_eq!(&[0, 1], &new_meta.primary_key_indices[..]);
        assert_eq!(&[2, 3, 4], &new_meta.value_indices[..]);
    }
}
