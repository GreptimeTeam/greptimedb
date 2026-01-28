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

//! Schema definition data structures for Export/Import V2.
//!
//! Schema definitions are stored as JSON for:
//! - Version-agnostic format (can handle schema evolution)
//! - Programmatic processing (direct deserialization)
//! - Extensibility (easy to add new fields)

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Schema directory name within snapshot.
pub const SCHEMA_DIR: &str = "schema";

/// Optional DDL file name within schema directory.
pub const DDL_FILE: &str = "ddl.sql";

/// Optional DDL directory name within schema directory.
pub const DDL_DIR: &str = "ddl";

/// Schema definition file name.
pub const SCHEMAS_FILE: &str = "schemas.json";

/// Table definition file name.
pub const TABLES_FILE: &str = "tables.json";

/// View definition file name.
pub const VIEWS_FILE: &str = "views.json";

/// Column semantic type in GreptimeDB.
///
/// GreptimeDB categorizes columns into three semantic types:
/// - Timestamp: The time index column (exactly one per table)
/// - Tag: Primary key columns (zero or more)
/// - Field: Data columns (zero or more)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SemanticType {
    /// Time index column (TIMESTAMP semantic).
    Timestamp,
    /// Primary key / tag column (TAG semantic).
    Tag,
    /// Field column (FIELD semantic).
    Field,
}

impl SemanticType {
    /// Parses semantic type from information_schema.columns.semantic_type value.
    pub fn from_info_schema(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "TIME INDEX" | "TIMESTAMP" => Some(Self::Timestamp),
            "PRIMARY KEY" | "TAG" => Some(Self::Tag),
            "FIELD" => Some(Self::Field),
            _ => None,
        }
    }
}

impl fmt::Display for SemanticType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SemanticType::Timestamp => write!(f, "TIMESTAMP"),
            SemanticType::Tag => write!(f, "TAG"),
            SemanticType::Field => write!(f, "FIELD"),
        }
    }
}

/// Column definition in JSON format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// Column name.
    pub name: String,
    /// GreptimeDB internal data type (e.g., "TimestampMillisecond", "String", "Float64").
    pub data_type: String,
    /// SQL-compatible data type name (e.g., "TIMESTAMP", "STRING", "DOUBLE").
    pub sql_type: String,
    /// Semantic type (TIMESTAMP, TAG, FIELD).
    pub semantic_type: SemanticType,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Default value expression (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    /// Column comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// Table options from create_options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableOptions {
    /// Storage engine (e.g., "mito", "metric").
    pub engine: String,
    /// TTL setting (e.g., "30d").
    /// Extracted for convenience as it's the most commonly used option.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    /// All other table options (write_buffer_size, skip_wal, compaction_type, etc.).
    /// This ensures forward compatibility with future GreptimeDB versions.
    #[serde(flatten)]
    pub extra: HashMap<String, String>,
}

/// Table definition in JSON format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDefinition {
    /// Catalog name.
    pub catalog: String,
    /// Schema (database) name.
    pub schema: String,
    /// Table name.
    pub name: String,
    /// Table type (e.g., "BASE TABLE").
    pub table_type: String,
    /// Table ID (internal identifier).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_id: Option<u64>,
    /// Column definitions in order.
    pub columns: Vec<ColumnDefinition>,
    /// Primary key column names (in order).
    pub primary_keys: Vec<String>,
    /// Time index column name.
    pub time_index: String,
    /// Table options.
    pub options: TableOptions,
    /// Table comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl TableDefinition {
    /// Returns the time index column definition.
    pub fn time_index_column(&self) -> Option<&ColumnDefinition> {
        self.columns
            .iter()
            .find(|c| c.semantic_type == SemanticType::Timestamp)
    }

    /// Returns primary key column definitions in order.
    pub fn primary_key_columns(&self) -> Vec<&ColumnDefinition> {
        self.primary_keys
            .iter()
            .filter_map(|pk| self.columns.iter().find(|c| &c.name == pk))
            .collect()
    }

    /// Returns field column definitions.
    pub fn field_columns(&self) -> Vec<&ColumnDefinition> {
        self.columns
            .iter()
            .filter(|c| c.semantic_type == SemanticType::Field)
            .collect()
    }
}

/// View definition in JSON format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDefinition {
    /// Catalog name.
    pub catalog: String,
    /// Schema (database) name.
    pub schema: String,
    /// View name.
    pub name: String,
    /// View definition SQL (the SELECT statement).
    pub definition: String,
}

/// Schema (database) definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    /// Catalog name.
    pub catalog: String,
    /// Schema (database) name.
    pub name: String,
    /// Schema options (if any).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

/// Container for all schema definitions in a snapshot.
///
/// This represents the complete schema state of a catalog,
/// including databases (schemas), tables, and views.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// Schema (database) definitions.
    pub schemas: Vec<SchemaDefinition>,
    /// Table definitions.
    pub tables: Vec<TableDefinition>,
    /// View definitions.
    pub views: Vec<ViewDefinition>,
}

impl SchemaSnapshot {
    /// Creates an empty schema snapshot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a schema definition.
    pub fn add_schema(&mut self, schema: SchemaDefinition) {
        self.schemas.push(schema);
    }

    /// Adds a table definition.
    pub fn add_table(&mut self, table: TableDefinition) {
        self.tables.push(table);
    }

    /// Adds a view definition.
    pub fn add_view(&mut self, view: ViewDefinition) {
        self.views.push(view);
    }

    /// Returns tables for a specific schema.
    pub fn tables_in_schema(&self, schema: &str) -> Vec<&TableDefinition> {
        self.tables.iter().filter(|t| t.schema == schema).collect()
    }

    /// Returns views for a specific schema.
    pub fn views_in_schema(&self, schema: &str) -> Vec<&ViewDefinition> {
        self.views.iter().filter(|v| v.schema == schema).collect()
    }

    /// Filters the snapshot to only include specified schemas.
    pub fn filter_schemas(&self, schemas: &[String]) -> Self {
        Self {
            schemas: self
                .schemas
                .iter()
                .filter(|s| schemas.contains(&s.name))
                .cloned()
                .collect(),
            tables: self
                .tables
                .iter()
                .filter(|t| schemas.contains(&t.schema))
                .cloned()
                .collect(),
            views: self
                .views
                .iter()
                .filter(|v| schemas.contains(&v.schema))
                .cloned()
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semantic_type_parsing() {
        assert_eq!(
            SemanticType::from_info_schema("TIME INDEX"),
            Some(SemanticType::Timestamp)
        );
        assert_eq!(
            SemanticType::from_info_schema("TIMESTAMP"),
            Some(SemanticType::Timestamp)
        );
        assert_eq!(
            SemanticType::from_info_schema("PRIMARY KEY"),
            Some(SemanticType::Tag)
        );
        assert_eq!(
            SemanticType::from_info_schema("TAG"),
            Some(SemanticType::Tag)
        );
        assert_eq!(
            SemanticType::from_info_schema("FIELD"),
            Some(SemanticType::Field)
        );
        assert_eq!(SemanticType::from_info_schema("unknown"), None);
    }

    #[test]
    fn test_table_definition_helpers() {
        let table = TableDefinition {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            name: "metrics".to_string(),
            table_type: "BASE TABLE".to_string(),
            table_id: Some(1),
            columns: vec![
                ColumnDefinition {
                    name: "ts".to_string(),
                    data_type: "TimestampMillisecond".to_string(),
                    sql_type: "TIMESTAMP(3)".to_string(),
                    semantic_type: SemanticType::Timestamp,
                    nullable: false,
                    default_value: None,
                    comment: None,
                },
                ColumnDefinition {
                    name: "host".to_string(),
                    data_type: "String".to_string(),
                    sql_type: "STRING".to_string(),
                    semantic_type: SemanticType::Tag,
                    nullable: false,
                    default_value: None,
                    comment: None,
                },
                ColumnDefinition {
                    name: "cpu".to_string(),
                    data_type: "Float64".to_string(),
                    sql_type: "DOUBLE".to_string(),
                    semantic_type: SemanticType::Field,
                    nullable: true,
                    default_value: None,
                    comment: None,
                },
            ],
            primary_keys: vec!["host".to_string()],
            time_index: "ts".to_string(),
            options: TableOptions {
                engine: "mito".to_string(),
                ..Default::default()
            },
            comment: None,
        };

        assert_eq!(table.time_index_column().unwrap().name, "ts");
        assert_eq!(table.primary_key_columns().len(), 1);
        assert_eq!(table.primary_key_columns()[0].name, "host");
        assert_eq!(table.field_columns().len(), 1);
        assert_eq!(table.field_columns()[0].name, "cpu");
    }

    #[test]
    fn test_schema_snapshot_filter() {
        let mut snapshot = SchemaSnapshot::new();
        snapshot.add_schema(SchemaDefinition {
            catalog: "greptime".to_string(),
            name: "public".to_string(),
            options: HashMap::new(),
        });
        snapshot.add_schema(SchemaDefinition {
            catalog: "greptime".to_string(),
            name: "private".to_string(),
            options: HashMap::new(),
        });

        let filtered = snapshot.filter_schemas(&["public".to_string()]);
        assert_eq!(filtered.schemas.len(), 1);
        assert_eq!(filtered.schemas[0].name, "public");
    }
}
