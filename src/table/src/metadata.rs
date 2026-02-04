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
use common_macro::ToMetaBuilder;
use common_query::AddColumnLocation;
use datafusion_expr::TableProviderFilterPushDown;
pub use datatypes::error::{Error as ConvertError, Result as ConvertResult};
use datatypes::schema::{
    ColumnSchema, FulltextOptions, Schema, SchemaBuilder, SchemaRef, SkippingIndexOptions,
};
use derive_builder::Builder;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::PHYSICAL_TABLE_METADATA_KEY;
use store_api::mito_engine_options::{
    APPEND_MODE_KEY, COMPACTION_TYPE, COMPACTION_TYPE_TWCS, SST_FORMAT_KEY,
};
use store_api::region_request::{SetRegionOption, UnsetRegionOption};
use store_api::storage::{ColumnDescriptor, ColumnDescriptorBuilder, ColumnId};

use crate::error::{self, Result};
use crate::requests::{
    AddColumnRequest, AlterKind, ModifyColumnTypeRequest, SetDefaultRequest, SetIndexOption,
    TableOptions, UnsetIndexOption,
};
use crate::table_reference::TableReference;

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

impl From<TableType> for datafusion::datasource::TableType {
    fn from(t: TableType) -> datafusion::datasource::TableType {
        match t {
            TableType::Base => datafusion::datasource::TableType::Base,
            TableType::View => datafusion::datasource::TableType::View,
            TableType::Temporary => datafusion::datasource::TableType::Temporary,
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

/// The table metadata.
///
/// Note: if you add new fields to this struct, please ensure 'new_meta_builder' function works.
#[derive(Clone, Debug, Builder, PartialEq, Eq, ToMetaBuilder, Serialize)]
#[builder(pattern = "mutable", custom_constructor)]
pub struct TableMeta {
    pub schema: SchemaRef,
    /// The indices of columns in primary key. Note that the index of timestamp column
    /// is not included in these indices.
    pub primary_key_indices: Vec<usize>,
    #[builder(default = "self.default_value_indices()?")]
    pub value_indices: Vec<usize>,
    #[builder(default, setter(into))]
    pub engine: String,
    pub next_column_id: ColumnId,
    /// Table options.
    #[builder(default)]
    pub options: TableOptions,
    #[builder(default = "Utc::now()")]
    pub created_on: DateTime<Utc>,
    #[builder(default = "self.default_updated_on()")]
    pub updated_on: DateTime<Utc>,
    #[builder(default = "Vec::new()")]
    pub partition_key_indices: Vec<usize>,
    #[builder(default = "Vec::new()")]
    pub column_ids: Vec<ColumnId>,
}

impl TableMeta {
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![])),
            primary_key_indices: vec![],
            value_indices: vec![],
            engine: "".to_string(),
            next_column_id: 0,
            options: TableOptions::default(),
            created_on: Utc::now(),
            updated_on: Utc::now(),
            partition_key_indices: vec![],
            column_ids: vec![],
        }
    }
}

impl<'de> Deserialize<'de> for TableMeta {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawTableMeta {
            schema: SchemaRef,
            primary_key_indices: Vec<usize>,
            value_indices: Vec<usize>,
            engine: String,
            next_column_id: ColumnId,
            options: TableOptions,
            created_on: DateTime<Utc>,
            updated_on: Option<DateTime<Utc>>,
            #[serde(default)]
            partition_key_indices: Vec<usize>,
            #[serde(default)]
            column_ids: Vec<ColumnId>,
        }

        let RawTableMeta {
            schema,
            primary_key_indices,
            value_indices,
            engine,
            next_column_id,
            options,
            created_on,
            updated_on,
            partition_key_indices,
            column_ids,
        } = RawTableMeta::deserialize(deserializer)?;

        Ok(Self {
            schema,
            primary_key_indices,
            value_indices,
            engine,
            next_column_id,
            options,
            created_on,
            updated_on: updated_on.unwrap_or(created_on),
            partition_key_indices,
            column_ids,
        })
    }
}

impl TableMetaBuilder {
    /// Note: Please always use [new_meta_builder] to create new [TableMetaBuilder].
    #[cfg(any(test, feature = "testing"))]
    pub fn empty() -> Self {
        Self {
            schema: None,
            primary_key_indices: None,
            value_indices: None,
            engine: None,
            next_column_id: None,
            options: None,
            created_on: None,
            updated_on: None,
            partition_key_indices: None,
            column_ids: None,
        }
    }
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

    fn default_updated_on(&self) -> DateTime<Utc> {
        self.created_on.unwrap_or_default()
    }

    pub fn new_external_table() -> Self {
        Self {
            schema: None,
            primary_key_indices: Some(Vec::new()),
            value_indices: Some(Vec::new()),
            engine: None,
            next_column_id: Some(0),
            options: None,
            created_on: None,
            updated_on: None,
            partition_key_indices: None,
            column_ids: None,
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

    pub fn partition_column_names(&self) -> impl Iterator<Item = &String> {
        let columns_schemas = &self.schema.column_schemas();
        self.partition_key_indices
            .iter()
            .map(|idx| &columns_schemas[*idx].name)
    }

    pub fn partition_columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        self.partition_key_indices
            .iter()
            .map(|idx| &self.schema.column_schemas()[*idx])
    }

    /// Returns the new [TableMetaBuilder] after applying given `alter_kind`.
    ///
    /// The returned builder would derive the next column id of this meta.
    pub fn builder_with_alter_kind(
        &self,
        table_name: &str,
        alter_kind: &AlterKind,
    ) -> Result<TableMetaBuilder> {
        let mut builder = match alter_kind {
            AlterKind::AddColumns { columns } => self.add_columns(table_name, columns),
            AlterKind::DropColumns { names } => self.remove_columns(table_name, names),
            AlterKind::ModifyColumnTypes { columns } => {
                self.modify_column_types(table_name, columns)
            }
            // No need to rebuild table meta when renaming tables.
            AlterKind::RenameTable { .. } => Ok(self.new_meta_builder()),
            AlterKind::SetTableOptions { options } => self.set_table_options(options),
            AlterKind::UnsetTableOptions { keys } => self.unset_table_options(keys),
            AlterKind::SetIndexes { options } => self.set_indexes(table_name, options),
            AlterKind::UnsetIndexes { options } => self.unset_indexes(table_name, options),
            AlterKind::DropDefaults { names } => self.drop_defaults(table_name, names),
            AlterKind::SetDefaults { defaults } => self.set_defaults(table_name, defaults),
        }?;
        let _ = builder.updated_on(Utc::now());
        Ok(builder)
    }

    /// Creates a [TableMetaBuilder] with modified table options.
    fn set_table_options(&self, requests: &[SetRegionOption]) -> Result<TableMetaBuilder> {
        let mut new_options = self.options.clone();

        for request in requests {
            match request {
                SetRegionOption::Ttl(new_ttl) => {
                    new_options.ttl = *new_ttl;
                }
                SetRegionOption::Twsc(key, value) => {
                    if !value.is_empty() {
                        new_options.extra_options.insert(key.clone(), value.clone());
                        // Ensure node restart correctly.
                        new_options.extra_options.insert(
                            COMPACTION_TYPE.to_string(),
                            COMPACTION_TYPE_TWCS.to_string(),
                        );
                    } else {
                        // Invalidate the previous change option if an empty value has been set.
                        new_options.extra_options.remove(key.as_str());
                    }
                }
                SetRegionOption::Format(value) => {
                    new_options
                        .extra_options
                        .insert(SST_FORMAT_KEY.to_string(), value.clone());
                }
                SetRegionOption::AppendMode(value) => {
                    new_options
                        .extra_options
                        .insert(APPEND_MODE_KEY.to_string(), value.to_string());
                }
            }
        }
        let mut builder = self.new_meta_builder();
        builder.options(new_options);

        Ok(builder)
    }

    fn unset_table_options(&self, requests: &[UnsetRegionOption]) -> Result<TableMetaBuilder> {
        let requests = requests.iter().map(Into::into).collect::<Vec<_>>();
        self.set_table_options(&requests)
    }

    fn set_indexes(
        &self,
        table_name: &str,
        requests: &[SetIndexOption],
    ) -> Result<TableMetaBuilder> {
        let table_schema = &self.schema;
        let mut set_index_options: HashMap<&str, Vec<_>> = HashMap::new();
        for request in requests {
            let column_name = request.column_name();
            table_schema
                .column_index_by_name(column_name)
                .with_context(|| error::ColumnNotExistsSnafu {
                    column_name,
                    table_name,
                })?;
            set_index_options
                .entry(column_name)
                .or_default()
                .push(request);
        }

        let mut meta_builder = self.new_meta_builder();
        let mut columns: Vec<_> = Vec::with_capacity(table_schema.column_schemas().len());
        for mut column in table_schema.column_schemas().iter().cloned() {
            if let Some(request) = set_index_options.get(column.name.as_str()) {
                for request in request {
                    self.set_index(&mut column, request)?;
                }
            }
            columns.push(column);
        }

        let mut builder = SchemaBuilder::try_from_columns(columns)
            .with_context(|_| error::SchemaBuildSnafu {
                msg: format!("Failed to convert column schemas into schema for table {table_name}"),
            })?
            .version(table_schema.version() + 1);

        for (k, v) in table_schema.metadata().iter() {
            builder = builder.add_metadata(k, v);
        }

        let new_schema = builder.build().with_context(|_| {
            let column_names = requests
                .iter()
                .map(|request| request.column_name())
                .collect::<Vec<_>>();
            error::SchemaBuildSnafu {
                msg: format!(
                    "Table {table_name} cannot set index options with columns {column_names:?}",
                ),
            }
        })?;
        let _ = meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(self.primary_key_indices.clone());

        Ok(meta_builder)
    }

    fn unset_indexes(
        &self,
        table_name: &str,
        requests: &[UnsetIndexOption],
    ) -> Result<TableMetaBuilder> {
        let table_schema = &self.schema;
        let mut set_index_options: HashMap<&str, Vec<_>> = HashMap::new();
        for request in requests {
            let column_name = request.column_name();
            table_schema
                .column_index_by_name(column_name)
                .with_context(|| error::ColumnNotExistsSnafu {
                    column_name,
                    table_name,
                })?;
            set_index_options
                .entry(column_name)
                .or_default()
                .push(request);
        }

        let mut meta_builder = self.new_meta_builder();
        let mut columns: Vec<_> = Vec::with_capacity(table_schema.column_schemas().len());
        for mut column in table_schema.column_schemas().iter().cloned() {
            if let Some(request) = set_index_options.get(column.name.as_str()) {
                for request in request {
                    self.unset_index(&mut column, request)?;
                }
            }
            columns.push(column);
        }

        let mut builder = SchemaBuilder::try_from_columns(columns)
            .with_context(|_| error::SchemaBuildSnafu {
                msg: format!("Failed to convert column schemas into schema for table {table_name}"),
            })?
            .version(table_schema.version() + 1);

        for (k, v) in table_schema.metadata().iter() {
            builder = builder.add_metadata(k, v);
        }

        let new_schema = builder.build().with_context(|_| {
            let column_names = requests
                .iter()
                .map(|request| request.column_name())
                .collect::<Vec<_>>();
            error::SchemaBuildSnafu {
                msg: format!(
                    "Table {table_name} cannot set index options with columns {column_names:?}",
                ),
            }
        })?;
        let _ = meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(self.primary_key_indices.clone());

        Ok(meta_builder)
    }

    fn set_index(&self, column_schema: &mut ColumnSchema, request: &SetIndexOption) -> Result<()> {
        match request {
            SetIndexOption::Fulltext {
                column_name,
                options,
            } => {
                ensure!(
                    column_schema.data_type.is_string(),
                    error::InvalidColumnOptionSnafu {
                        column_name,
                        msg: "FULLTEXT index only supports string type",
                    }
                );

                let current_fulltext_options = column_schema
                    .fulltext_options()
                    .context(error::SetFulltextOptionsSnafu { column_name })?;
                set_column_fulltext_options(
                    column_schema,
                    column_name,
                    options,
                    current_fulltext_options,
                )?;
            }
            SetIndexOption::Inverted { column_name } => {
                debug_assert_eq!(column_schema.name, *column_name);
                column_schema.set_inverted_index(true);
            }
            SetIndexOption::Skipping {
                column_name,
                options,
            } => {
                set_column_skipping_index_options(column_schema, column_name, options)?;
            }
        }

        Ok(())
    }

    fn unset_index(
        &self,
        column_schema: &mut ColumnSchema,
        request: &UnsetIndexOption,
    ) -> Result<()> {
        match request {
            UnsetIndexOption::Fulltext { column_name } => {
                let current_fulltext_options = column_schema
                    .fulltext_options()
                    .context(error::SetFulltextOptionsSnafu { column_name })?;
                unset_column_fulltext_options(
                    column_schema,
                    column_name,
                    current_fulltext_options.clone(),
                )?
            }
            UnsetIndexOption::Inverted { .. } => {
                column_schema.set_inverted_index(false);
            }
            UnsetIndexOption::Skipping { column_name } => {
                unset_column_skipping_index_options(column_schema, column_name)?;
            }
        }

        Ok(())
    }

    // TODO(yingwen): Remove this.
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

    /// Create a [`TableMetaBuilder`] from the current TableMeta.
    fn new_meta_builder(&self) -> TableMetaBuilder {
        let mut builder = TableMetaBuilder::from(self);
        // Manually remove value_indices.
        builder.value_indices = None;
        builder
    }

    // TODO(yingwen): Tests add if not exists.
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
        let mut new_columns = Vec::with_capacity(requests.len());
        for col_to_add in requests {
            if let Some(column_schema) =
                table_schema.column_schema_by_name(&col_to_add.column_schema.name)
            {
                // If the column already exists.
                ensure!(
                    col_to_add.add_if_not_exists,
                    error::ColumnExistsSnafu {
                        table_name,
                        column_name: &col_to_add.column_schema.name
                    },
                );

                // Checks if the type is the same
                ensure!(
                    column_schema.data_type == col_to_add.column_schema.data_type,
                    error::InvalidAlterRequestSnafu {
                        table: table_name,
                        err: format!(
                            "column {} already exists with different type {:?}",
                            col_to_add.column_schema.name, column_schema.data_type,
                        ),
                    }
                );
            } else {
                // A new column.
                // Ensures we only add a column once.
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

                new_columns.push(col_to_add.clone());
            }
        }
        let requests = &new_columns[..];

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

        let partition_key_indices = self
            .partition_key_indices
            .iter()
            .map(|idx| table_schema.column_name_by_index(*idx))
            // This unwrap is safe since we only add new columns.
            .map(|name| new_schema.column_index_by_name(name).unwrap())
            .collect();

        // value_indices would be generated automatically.
        let _ = meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(primary_key_indices)
            .partition_key_indices(partition_key_indices);

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

                ensure!(
                    !self.partition_key_indices.contains(&index),
                    error::RemovePartitionColumnSnafu {
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

        let partition_key_indices = self
            .partition_key_indices
            .iter()
            .map(|idx| table_schema.column_name_by_index(*idx))
            // This unwrap is safe since we don't allow removing a partition key column.
            .map(|name| new_schema.column_index_by_name(name).unwrap())
            .collect();

        let _ = meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(primary_key_indices)
            .partition_key_indices(partition_key_indices);

        Ok(meta_builder)
    }

    fn modify_column_types(
        &self,
        table_name: &str,
        requests: &[ModifyColumnTypeRequest],
    ) -> Result<TableMetaBuilder> {
        let table_schema = &self.schema;
        let mut meta_builder = self.new_meta_builder();

        let mut modify_column_types = HashMap::with_capacity(requests.len());
        let timestamp_index = table_schema.timestamp_index();

        for col_to_change in requests {
            let change_column_name = &col_to_change.column_name;

            let index = table_schema
                .column_index_by_name(change_column_name)
                .with_context(|| error::ColumnNotExistsSnafu {
                    column_name: change_column_name,
                    table_name,
                })?;

            let column = &table_schema.column_schemas()[index];

            ensure!(
                !self.primary_key_indices.contains(&index),
                error::InvalidAlterRequestSnafu {
                    table: table_name,
                    err: format!(
                        "Not allowed to change primary key index column '{}'",
                        column.name
                    )
                }
            );

            if let Some(ts_index) = timestamp_index {
                // Not allowed to change column datatype in timestamp index.
                ensure!(
                    index != ts_index,
                    error::InvalidAlterRequestSnafu {
                        table: table_name,
                        err: format!(
                            "Not allowed to change timestamp index column '{}' datatype",
                            column.name
                        )
                    }
                );
            }

            ensure!(
                modify_column_types
                    .insert(&col_to_change.column_name, col_to_change)
                    .is_none(),
                error::InvalidAlterRequestSnafu {
                    table: table_name,
                    err: format!(
                        "change column datatype {} more than once",
                        col_to_change.column_name
                    ),
                }
            );

            ensure!(
                column
                    .data_type
                    .can_arrow_type_cast_to(&col_to_change.target_type),
                error::InvalidAlterRequestSnafu {
                    table: table_name,
                    err: format!(
                        "column '{}' cannot be cast automatically to type '{}'",
                        col_to_change.column_name, col_to_change.target_type,
                    ),
                }
            );

            ensure!(
                column.is_nullable(),
                error::InvalidAlterRequestSnafu {
                    table: table_name,
                    err: format!(
                        "column '{}' must be nullable to ensure safe conversion.",
                        col_to_change.column_name,
                    ),
                }
            );
        }
        // Collect columns after changed.

        let mut columns: Vec<_> = Vec::with_capacity(table_schema.column_schemas().len());
        for mut column in table_schema.column_schemas().iter().cloned() {
            if let Some(change_column) = modify_column_types.get(&column.name) {
                column.data_type = change_column.target_type.clone();
                let new_default = if let Some(default_value) = column.default_constraint() {
                    Some(
                        default_value
                            .cast_to_datatype(&change_column.target_type)
                            .with_context(|_| error::CastDefaultValueSnafu {
                                reason: format!(
                                    "Failed to cast default value from {:?} to type {:?}",
                                    default_value, &change_column.target_type
                                ),
                            })?,
                    )
                } else {
                    None
                };
                column = column
                    .clone()
                    .with_default_constraint(new_default.clone())
                    .with_context(|_| error::CastDefaultValueSnafu {
                        reason: format!("Failed to set new default: {:?}", new_default),
                    })?;
            }
            columns.push(column)
        }

        let mut builder = SchemaBuilder::try_from_columns(columns)
            .with_context(|_| error::SchemaBuildSnafu {
                msg: format!("Failed to convert column schemas into schema for table {table_name}"),
            })?
            // Also bump the schema version.
            .version(table_schema.version() + 1);
        for (k, v) in table_schema.metadata().iter() {
            builder = builder.add_metadata(k, v);
        }
        let new_schema = builder.build().with_context(|_| {
            let column_names: Vec<_> = requests
                .iter()
                .map(|request| &request.column_name)
                .collect();

            error::SchemaBuildSnafu {
                msg: format!(
                    "Table {table_name} cannot change datatype with columns {column_names:?}"
                ),
            }
        })?;

        let _ = meta_builder
            .schema(Arc::new(new_schema))
            .primary_key_indices(self.primary_key_indices.clone());

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

    fn drop_defaults(&self, table_name: &str, column_names: &[String]) -> Result<TableMetaBuilder> {
        let table_schema = &self.schema;
        let mut meta_builder = self.new_meta_builder();
        let mut columns = Vec::with_capacity(table_schema.num_columns());
        for column_schema in table_schema.column_schemas() {
            if let Some(name) = column_names.iter().find(|s| **s == column_schema.name) {
                // Drop default constraint.
                ensure!(
                    column_schema.default_constraint().is_some(),
                    error::InvalidAlterRequestSnafu {
                        table: table_name,
                        err: format!("column {name} does not have a default value"),
                    }
                );
                if !column_schema.is_nullable() {
                    return error::InvalidAlterRequestSnafu {
                        table: table_name,
                        err: format!(
                            "column {name} is not nullable and `default` cannot be dropped",
                        ),
                    }
                    .fail();
                }
                let new_column_schema = column_schema.clone();
                let new_column_schema = new_column_schema
                    .with_default_constraint(None)
                    .with_context(|_| error::SchemaBuildSnafu {
                        msg: format!("Table {table_name} cannot drop default values"),
                    })?;
                columns.push(new_column_schema);
            } else {
                columns.push(column_schema.clone());
            }
        }

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
            msg: format!("Table {table_name} cannot drop default values"),
        })?;

        let _ = meta_builder.schema(Arc::new(new_schema));

        Ok(meta_builder)
    }

    fn set_defaults(
        &self,
        table_name: &str,
        set_defaults: &[SetDefaultRequest],
    ) -> Result<TableMetaBuilder> {
        let table_schema = &self.schema;
        let mut meta_builder = self.new_meta_builder();
        let mut columns = Vec::with_capacity(table_schema.num_columns());
        for column_schema in table_schema.column_schemas() {
            if let Some(set_default) = set_defaults
                .iter()
                .find(|s| s.column_name == column_schema.name)
            {
                let new_column_schema = column_schema.clone();
                let new_column_schema = new_column_schema
                    .with_default_constraint(set_default.default_constraint.clone())
                    .with_context(|_| error::SchemaBuildSnafu {
                        msg: format!("Table {table_name} cannot set default values"),
                    })?;
                columns.push(new_column_schema);
            } else {
                columns.push(column_schema.clone());
            }
        }

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
            msg: format!("Table {table_name} cannot set default values"),
        })?;

        let _ = meta_builder.schema(Arc::new(new_schema));

        Ok(meta_builder)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Builder, Serialize, Deserialize)]
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

    /// Returns the full table name in the form of `{catalog}.{schema}.{table}`.
    pub fn full_table_name(&self) -> String {
        common_catalog::format_full_table_name(&self.catalog_name, &self.schema_name, &self.name)
    }

    pub fn get_db_string(&self) -> String {
        common_catalog::build_db_string(&self.catalog_name, &self.schema_name)
    }

    /// Returns true when the table is the metric engine's physical table.
    pub fn is_physical_table(&self) -> bool {
        self.meta
            .options
            .extra_options
            .contains_key(PHYSICAL_TABLE_METADATA_KEY)
    }

    /// Return true if the table's TTL is `instant`.
    pub fn is_ttl_instant_table(&self) -> bool {
        self.meta
            .options
            .ttl
            .map(|t| t.is_instant())
            .unwrap_or(false)
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

impl TableInfo {
    /// Returns the map of column name to column id.
    ///
    /// Note: This method may return an empty map for older versions that did not include this field.
    pub fn name_to_ids(&self) -> Option<HashMap<String, ColumnId>> {
        let column_schemas = self.meta.schema.column_schemas();
        if self.meta.column_ids.len() != column_schemas.len() {
            None
        } else {
            Some(
                self.meta
                    .column_ids
                    .iter()
                    .enumerate()
                    .map(|(index, id)| (column_schemas[index].name.clone(), *id))
                    .collect(),
            )
        }
    }

    /// Sort the columns in [TableInfo], logical tables require it.
    pub fn sort_columns(&mut self) {
        let column_schemas = self.meta.schema.column_schemas();
        let primary_keys = self
            .meta
            .primary_key_indices
            .iter()
            .map(|index| column_schemas[*index].name.clone())
            .collect::<HashSet<_>>();

        let name_to_ids = self.name_to_ids().unwrap_or_default();
        let mut column_schemas = column_schemas.to_vec();
        column_schemas.sort_unstable_by(|a, b| a.name.cmp(&b.name));

        // Compute new indices of sorted columns
        let mut primary_key_indices = Vec::with_capacity(primary_keys.len());
        let mut value_indices = Vec::with_capacity(column_schemas.len() - primary_keys.len());
        let mut column_ids = Vec::with_capacity(column_schemas.len());
        for (index, column_schema) in column_schemas.iter().enumerate() {
            if primary_keys.contains(&column_schema.name) {
                primary_key_indices.push(index);
            } else {
                value_indices.push(index);
            }
            if let Some(id) = name_to_ids.get(&column_schema.name) {
                column_ids.push(*id);
            }
        }

        // Overwrite table meta
        self.meta.schema = Arc::new(Schema::new_with_version(
            column_schemas,
            self.meta.schema.version(),
        ));
        self.meta.primary_key_indices = primary_key_indices;
        self.meta.value_indices = value_indices;
        self.meta.column_ids = column_ids;
    }

    /// Extracts region options from table info.
    ///
    /// All "region options" are actually a copy of table options for redundancy.
    pub fn to_region_options(&self) -> HashMap<String, String> {
        HashMap::from(&self.meta.options)
    }

    /// Returns the table reference.
    pub fn table_ref(&self) -> TableReference<'_> {
        TableReference::full(
            self.catalog_name.as_str(),
            self.schema_name.as_str(),
            self.name.as_str(),
        )
    }
}

/// Set column fulltext options if it passed the validation.
///
/// Options allowed to modify:
/// * backend
///
/// Options not allowed to modify:
/// * analyzer
/// * case_sensitive
fn set_column_fulltext_options(
    column_schema: &mut ColumnSchema,
    column_name: &str,
    options: &FulltextOptions,
    current_options: Option<FulltextOptions>,
) -> Result<()> {
    if let Some(current_options) = current_options {
        ensure!(
            current_options.analyzer == options.analyzer
                && current_options.case_sensitive == options.case_sensitive,
            error::InvalidColumnOptionSnafu {
                column_name,
                msg: format!(
                    "Cannot change analyzer or case_sensitive if FULLTEXT index is set before. Previous analyzer: {}, previous case_sensitive: {}",
                    current_options.analyzer, current_options.case_sensitive
                ),
            }
        );
    }

    column_schema
        .set_fulltext_options(options)
        .context(error::SetFulltextOptionsSnafu { column_name })?;

    Ok(())
}

fn unset_column_fulltext_options(
    column_schema: &mut ColumnSchema,
    column_name: &str,
    current_options: Option<FulltextOptions>,
) -> Result<()> {
    ensure!(
        current_options
            .as_ref()
            .is_some_and(|options| options.enable),
        error::InvalidColumnOptionSnafu {
            column_name,
            msg: "FULLTEXT index already disabled".to_string(),
        }
    );

    let mut options = current_options.unwrap();
    options.enable = false;
    column_schema
        .set_fulltext_options(&options)
        .context(error::SetFulltextOptionsSnafu { column_name })?;

    Ok(())
}

fn set_column_skipping_index_options(
    column_schema: &mut ColumnSchema,
    column_name: &str,
    options: &SkippingIndexOptions,
) -> Result<()> {
    column_schema
        .set_skipping_options(options)
        .context(error::SetSkippingOptionsSnafu { column_name })?;

    Ok(())
}

fn unset_column_skipping_index_options(
    column_schema: &mut ColumnSchema,
    column_name: &str,
) -> Result<()> {
    column_schema
        .unset_skipping_options()
        .context(error::UnsetSkippingOptionsSnafu { column_name })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{
        ColumnSchema, FulltextAnalyzer, FulltextBackend, Schema, SchemaBuilder,
    };

    use super::*;
    use crate::Error;

    /// Create a test schema with 3 columns: `[col1 int32, ts timestampmills, col2 int32]`.
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

    fn add_columns_to_meta(meta: &TableMeta) -> TableMeta {
        let new_tag = ColumnSchema::new("my_tag", ConcreteDataType::string_datatype(), true);
        let new_field = ColumnSchema::new("my_field", ConcreteDataType::string_datatype(), true);
        let alter_kind = AlterKind::AddColumns {
            columns: vec![
                AddColumnRequest {
                    column_schema: new_tag,
                    is_key: true,
                    location: None,
                    add_if_not_exists: false,
                },
                AddColumnRequest {
                    column_schema: new_field,
                    is_key: false,
                    location: None,
                    add_if_not_exists: false,
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
        let yet_another_field = ColumnSchema::new(
            "yet_another_field_after_ts",
            ConcreteDataType::int64_datatype(),
            true,
        );
        let alter_kind = AlterKind::AddColumns {
            columns: vec![
                AddColumnRequest {
                    column_schema: new_tag,
                    is_key: true,
                    location: Some(AddColumnLocation::First),
                    add_if_not_exists: false,
                },
                AddColumnRequest {
                    column_schema: new_field,
                    is_key: false,
                    location: Some(AddColumnLocation::After {
                        column_name: "ts".to_string(),
                    }),
                    add_if_not_exists: false,
                },
                AddColumnRequest {
                    column_schema: yet_another_field,
                    is_key: true,
                    location: Some(AddColumnLocation::After {
                        column_name: "ts".to_string(),
                    }),
                    add_if_not_exists: false,
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
        let meta = TableMetaBuilder::empty()
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
    fn test_add_columns_multiple_times() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let alter_kind = AlterKind::AddColumns {
            columns: vec![
                AddColumnRequest {
                    column_schema: ColumnSchema::new(
                        "col3",
                        ConcreteDataType::int32_datatype(),
                        true,
                    ),
                    is_key: true,
                    location: None,
                    add_if_not_exists: true,
                },
                AddColumnRequest {
                    column_schema: ColumnSchema::new(
                        "col3",
                        ConcreteDataType::int32_datatype(),
                        true,
                    ),
                    is_key: true,
                    location: None,
                    add_if_not_exists: true,
                },
            ],
        };
        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_remove_columns() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
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
        let meta = TableMetaBuilder::empty()
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
        let meta = TableMetaBuilder::empty()
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
                add_if_not_exists: false,
            }],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::TableColumnExists, err.status_code());

        // Add if not exists
        let alter_kind = AlterKind::AddColumns {
            columns: vec![AddColumnRequest {
                column_schema: ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
                is_key: true,
                location: None,
                add_if_not_exists: true,
            }],
        };
        let new_meta = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(
            meta.schema.column_schemas(),
            new_meta.schema.column_schemas()
        );
        assert_eq!(meta.schema.version() + 1, new_meta.schema.version());
    }

    #[test]
    fn test_add_different_type_column() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        // Add if not exists, but different type.
        let alter_kind = AlterKind::AddColumns {
            columns: vec![AddColumnRequest {
                column_schema: ColumnSchema::new("col1", ConcreteDataType::string_datatype(), true),
                is_key: false,
                location: None,
                add_if_not_exists: true,
            }],
        };
        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());
    }

    #[test]
    fn test_add_invalid_column() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        // Not nullable and no default value.
        let alter_kind = AlterKind::AddColumns {
            columns: vec![AddColumnRequest {
                column_schema: ColumnSchema::new(
                    "weny",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                is_key: false,
                location: None,
                add_if_not_exists: false,
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
        let meta = TableMetaBuilder::empty()
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
    fn test_change_unknown_column_data_type() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let alter_kind = AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnTypeRequest {
                column_name: "unknown".to_string(),
                target_type: ConcreteDataType::string_datatype(),
            }],
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
        let meta = TableMetaBuilder::empty()
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
    fn test_remove_partition_column() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![])
            .partition_key_indices(vec![0])
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
        assert_matches!(err, Error::RemovePartitionColumn { .. });
    }

    #[test]
    fn test_change_key_column_data_type() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        // Remove column in primary key.
        let alter_kind = AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnTypeRequest {
                column_name: "col1".to_string(),
                target_type: ConcreteDataType::string_datatype(),
            }],
        };

        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(StatusCode::InvalidArguments, err.status_code());

        // Remove timestamp column.
        let alter_kind = AlterKind::ModifyColumnTypes {
            columns: vec![ModifyColumnTypeRequest {
                column_name: "ts".to_string(),
                target_type: ConcreteDataType::string_datatype(),
            }],
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
        let mut meta = TableMetaBuilder::empty()
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
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            // partition col: col1, col2
            .partition_key_indices(vec![0, 2])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let new_meta = add_columns_to_meta_with_location(&meta);
        let names: Vec<String> = new_meta
            .schema
            .column_schemas()
            .iter()
            .map(|column_schema| column_schema.name.clone())
            .collect();
        assert_eq!(
            &[
                "my_tag_first",               // primary key column
                "col1",                       // partition column
                "ts",                         // timestamp column
                "yet_another_field_after_ts", // primary key column
                "my_field_after_ts",          // value column
                "col2",                       // partition column
            ],
            &names[..]
        );
        assert_eq!(&[0, 1, 3], &new_meta.primary_key_indices[..]);
        assert_eq!(&[2, 4, 5], &new_meta.value_indices[..]);
        assert_eq!(&[1, 5], &new_meta.partition_key_indices[..]);
    }

    #[test]
    fn test_modify_column_fulltext_options() {
        let schema = Arc::new(new_test_schema());
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .engine("engine")
            .next_column_id(3)
            .build()
            .unwrap();

        let alter_kind = AlterKind::SetIndexes {
            options: vec![SetIndexOption::Fulltext {
                column_name: "col1".to_string(),
                options: FulltextOptions::default(),
            }],
        };
        let err = meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .err()
            .unwrap();
        assert_eq!(
            "Invalid column option, column name: col1, error: FULLTEXT index only supports string type",
            err.to_string()
        );

        // Add a string column and make it fulltext indexed
        let new_meta = add_columns_to_meta_with_location(&meta);
        let alter_kind = AlterKind::SetIndexes {
            options: vec![SetIndexOption::Fulltext {
                column_name: "my_tag_first".to_string(),
                options: FulltextOptions::new_unchecked(
                    true,
                    FulltextAnalyzer::Chinese,
                    true,
                    FulltextBackend::Bloom,
                    1000,
                    0.01,
                ),
            }],
        };
        let new_meta = new_meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap()
            .build()
            .unwrap();
        let column_schema = new_meta
            .schema
            .column_schema_by_name("my_tag_first")
            .unwrap();
        let fulltext_options = column_schema.fulltext_options().unwrap().unwrap();
        assert!(fulltext_options.enable);
        assert_eq!(
            datatypes::schema::FulltextAnalyzer::Chinese,
            fulltext_options.analyzer
        );
        assert!(fulltext_options.case_sensitive);

        let alter_kind = AlterKind::UnsetIndexes {
            options: vec![UnsetIndexOption::Fulltext {
                column_name: "my_tag_first".to_string(),
            }],
        };
        let new_meta = new_meta
            .builder_with_alter_kind("my_table", &alter_kind)
            .unwrap()
            .build()
            .unwrap();
        let column_schema = new_meta
            .schema
            .column_schema_by_name("my_tag_first")
            .unwrap();
        let fulltext_options = column_schema.fulltext_options().unwrap().unwrap();
        assert!(!fulltext_options.enable);
    }

    #[test]
    fn test_table_info_serde_compatibility() {
        // "serialized" is generated by the following codes before this refactor (PR 7626):
        //
        // ```Rust
        // serde_json::to_string(&RawTableInfo::from(TableInfo {
        //     ident: TableIdent {
        //         table_id: 1024,
        //         version: 1,
        //     },
        //     name: "foo".to_string(),
        //     desc: Some("my table".to_string()),
        //     catalog_name: "greptime".to_string(),
        //     schema_name: "public".to_string(),
        //     meta: TableMeta {
        //         schema: Arc::new(new_test_schema()),
        //         primary_key_indices: vec![0],
        //         value_indices: vec![1, 2],
        //         engine: "mito".to_string(),
        //         next_column_id: 3,
        //         options: TableOptions {
        //             ttl: Some(common_time::TimeToLive::Duration(
        //                 std::time::Duration::from_secs(3600),
        //             )),
        //             ..Default::default()
        //         },
        //         created_on: DateTime::<Utc>::MIN_UTC,
        //         updated_on: DateTime::<Utc>::MAX_UTC,
        //         partition_key_indices: vec![2],
        //         column_ids: vec![0, 1, 2],
        //     },
        //     table_type: TableType::Base,
        // }))
        // ```
        let serialized = r#"{"ident":{"table_id":1024,"version":1},"name":"foo","desc":"my table","catalog_name":"greptime","schema_name":"public","meta":{"schema":{"column_schemas":[{"name":"col1","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"ts","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},{"name":"col2","data_type":{"Int32":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}}],"timestamp_index":1,"version":123},"primary_key_indices":[0],"value_indices":[1,2],"engine":"mito","next_column_id":3,"options":{"write_buffer_size":null,"ttl":"1h","skip_wal":false,"extra_options":{}},"created_on":"-262143-01-01T00:00:00Z","updated_on":"+262142-12-31T23:59:59.999999999Z","partition_key_indices":[2],"column_ids":[0,1,2]},"table_type":"Base"}"#;

        let actual: TableInfo = serde_json::from_str(serialized).unwrap();
        let expected = TableInfo {
            ident: TableIdent {
                table_id: 1024,
                version: 1,
            },
            name: "foo".to_string(),
            desc: Some("my table".to_string()),
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            meta: TableMeta {
                schema: Arc::new(new_test_schema()),
                primary_key_indices: vec![0],
                value_indices: vec![1, 2],
                engine: "mito".to_string(),
                next_column_id: 3,
                options: TableOptions {
                    ttl: Some(common_time::TimeToLive::Duration(
                        std::time::Duration::from_secs(3600),
                    )),
                    ..Default::default()
                },
                created_on: DateTime::<Utc>::MIN_UTC,
                updated_on: DateTime::<Utc>::MAX_UTC,
                partition_key_indices: vec![2],
                column_ids: vec![0, 1, 2],
            },
            table_type: TableType::Base,
        };
        assert_eq!(actual, expected);
    }
}
