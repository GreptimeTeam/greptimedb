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

//! Schema extraction from information_schema.
//!
//! This module queries the GreptimeDB information_schema to extract:
//! - Schema (database) definitions
//! - Table definitions (columns, primary keys, time index, options)
//! - View definitions

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use snafu::ResultExt;

use super::error::{
    DatabaseSnafu, EmptyResultSnafu, InvalidSemanticTypeSnafu, NoTimeIndexSnafu, Result,
    SchemaNotFoundSnafu, UnexpectedValueTypeSnafu,
};
use super::schema::{
    ColumnDefinition, SchemaDefinition, SchemaSnapshot, SemanticType, TableDefinition,
    TableOptions, ViewDefinition,
};
use crate::database::DatabaseClient;

/// System schemas that should be excluded from export.
const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog"];

/// Extracts schema definitions from information_schema.
pub struct SchemaExtractor<'a> {
    client: &'a DatabaseClient,
    catalog: &'a str,
}

impl<'a> SchemaExtractor<'a> {
    /// Creates a new schema extractor.
    pub fn new(client: &'a DatabaseClient, catalog: &'a str) -> Self {
        Self { client, catalog }
    }

    /// Extracts all schema information for the given schemas.
    ///
    /// If `schemas` is None, extracts all non-system schemas.
    pub async fn extract(&self, schemas: Option<&[String]>) -> Result<SchemaSnapshot> {
        let mut snapshot = SchemaSnapshot::new();

        // Get list of schemas to export
        let schema_names = match schemas {
            Some(names) => self.validate_schemas(names).await?,
            None => self.get_all_schemas().await?,
        };

        for schema_name in &schema_names {
            // Extract schema definition
            let schema_def = self.extract_schema_definition(schema_name).await?;
            snapshot.add_schema(schema_def);

            // Extract tables
            let table_names = self.get_table_names(schema_name).await?;
            for table_name in &table_names {
                let table_def = self
                    .extract_table_definition(schema_name, table_name)
                    .await?;
                snapshot.add_table(table_def);
            }

            // Extract views
            let views = self.get_views(schema_name).await?;
            for (view_name, definition) in views {
                snapshot.add_view(ViewDefinition {
                    catalog: self.catalog.to_string(),
                    schema: schema_name.clone(),
                    name: view_name,
                    definition,
                });
            }
        }

        Ok(snapshot)
    }

    /// Gets all non-system schemas in the catalog.
    async fn get_all_schemas(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE catalog_name = '{}'",
            self.catalog
        );

        let records = self.query(&sql).await?;
        let mut schemas = Vec::new();

        for row in records {
            let name = extract_string(&row, 0)?;
            if !SYSTEM_SCHEMAS.contains(&name.as_str()) {
                schemas.push(name);
            }
        }

        Ok(schemas)
    }

    /// Validates that all specified schemas exist.
    async fn validate_schemas(&self, schemas: &[String]) -> Result<Vec<String>> {
        let all_schemas = self.get_all_schemas().await?;

        for schema in schemas {
            if !all_schemas.iter().any(|s| s.eq_ignore_ascii_case(schema)) {
                return SchemaNotFoundSnafu {
                    catalog: self.catalog,
                    schema,
                }
                .fail();
            }
        }

        // Return with original case from database
        Ok(schemas
            .iter()
            .filter_map(|s| {
                all_schemas
                    .iter()
                    .find(|a| a.eq_ignore_ascii_case(s))
                    .cloned()
            })
            .collect())
    }

    /// Extracts schema (database) definition.
    async fn extract_schema_definition(&self, schema: &str) -> Result<SchemaDefinition> {
        let sql = format!(
            "SELECT schema_name, options FROM information_schema.schemata \
             WHERE catalog_name = '{}' AND schema_name = '{}'",
            self.catalog, schema
        );

        let records = self.query(&sql).await?;
        if records.is_empty() {
            return SchemaNotFoundSnafu {
                catalog: self.catalog,
                schema,
            }
            .fail();
        }

        let name = extract_string(&records[0], 0)?;
        let options_str = extract_optional_string(&records[0], 1);

        let options = if let Some(opts) = options_str {
            parse_options(&opts)
        } else {
            HashMap::new()
        };

        Ok(SchemaDefinition {
            catalog: self.catalog.to_string(),
            name,
            options,
        })
    }

    /// Gets all table names in a schema (excluding physical tables from metric engine).
    async fn get_table_names(&self, schema: &str) -> Result<Vec<String>> {
        // First, get all physical tables (those with __tsid column) in one query
        let physical_tables = self.get_physical_tables(schema).await?;

        // Then get all tables
        let sql = format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND table_type = 'BASE TABLE'",
            self.catalog, schema
        );

        let records = self.query(&sql).await?;
        let mut tables = Vec::new();

        for row in records {
            let name = extract_string(&row, 0)?;
            // Skip physical tables
            if !physical_tables.contains(&name) {
                tables.push(name);
            }
        }

        Ok(tables)
    }

    /// Gets all physical tables in a schema (batch query).
    async fn get_physical_tables(&self, schema: &str) -> Result<std::collections::HashSet<String>> {
        let sql = format!(
            "SELECT DISTINCT table_name FROM information_schema.columns \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND column_name = '__tsid'",
            self.catalog, schema
        );

        let records = self.query(&sql).await?;
        let mut physical_tables = HashSet::new();

        for row in records {
            let name = extract_string(&row, 0)?;
            physical_tables.insert(name);
        }

        Ok(physical_tables)
    }

    /// Extracts full table definition.
    async fn extract_table_definition(&self, schema: &str, table: &str) -> Result<TableDefinition> {
        // Get table info
        let table_info = self.get_table_info(schema, table).await?;

        // Get columns
        let columns = self.get_columns(schema, table).await?;

        // Identify time index
        let time_index = columns
            .iter()
            .find(|c| c.semantic_type == SemanticType::Timestamp)
            .map(|c| c.name.clone())
            .ok_or_else(|| {
                NoTimeIndexSnafu {
                    table: format!("{}.{}", schema, table),
                }
                .build()
            })?;

        // Identify primary keys (TAG columns in order)
        let primary_keys: Vec<String> = columns
            .iter()
            .filter(|c| c.semantic_type == SemanticType::Tag)
            .map(|c| c.name.clone())
            .collect();

        Ok(TableDefinition {
            catalog: self.catalog.to_string(),
            schema: schema.to_string(),
            name: table.to_string(),
            table_type: table_info.table_type,
            table_id: table_info.table_id,
            columns,
            primary_keys,
            time_index,
            options: table_info.options,
            comment: table_info.comment,
        })
    }

    /// Gets table metadata from information_schema.tables.
    async fn get_table_info(&self, schema: &str, table: &str) -> Result<TableInfo> {
        let sql = format!(
            "SELECT table_type, table_id, engine, create_options, table_comment \
             FROM information_schema.tables \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}'",
            self.catalog, schema, table
        );

        let records = self.query(&sql).await?;
        if records.is_empty() {
            return EmptyResultSnafu.fail();
        }

        let row = &records[0];
        let table_type = extract_string(row, 0)?;
        let table_id = extract_optional_number(row, 1);
        let engine = extract_optional_string(row, 2).unwrap_or_else(|| "mito".to_string());
        let create_options = extract_optional_string(row, 3);
        let comment = extract_optional_string(row, 4);

        let options = parse_table_options(&engine, create_options.as_deref());

        Ok(TableInfo {
            table_type,
            table_id,
            options,
            comment,
        })
    }

    /// Gets columns for a table from information_schema.columns.
    async fn get_columns(&self, schema: &str, table: &str) -> Result<Vec<ColumnDefinition>> {
        let sql = format!(
            "SELECT column_name, greptime_data_type, data_type, semantic_type, \
                    is_nullable, column_default, column_comment \
             FROM information_schema.columns \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}' \
             ORDER BY ordinal_position",
            self.catalog, schema, table
        );

        let records = self.query(&sql).await?;
        let mut columns = Vec::new();

        for row in &records {
            let name = extract_string(row, 0)?;
            let data_type = extract_string(row, 1)?;
            let sql_type = extract_string(row, 2)?;
            let semantic_type_str = extract_string(row, 3)?;
            let is_nullable = extract_string(row, 4)?.to_lowercase() == "yes";
            let default_value = extract_optional_string(row, 5);
            let comment = extract_optional_string(row, 6);

            let semantic_type =
                SemanticType::from_info_schema(&semantic_type_str).ok_or_else(|| {
                    InvalidSemanticTypeSnafu {
                        value: semantic_type_str.clone(),
                    }
                    .build()
                })?;

            columns.push(ColumnDefinition {
                name,
                data_type,
                sql_type,
                semantic_type,
                nullable: is_nullable,
                default_value,
                comment,
            });
        }

        Ok(columns)
    }

    /// Gets views for a schema.
    async fn get_views(&self, schema: &str) -> Result<Vec<(String, String)>> {
        let sql = format!(
            "SELECT table_name, view_definition FROM information_schema.views \
             WHERE table_catalog = '{}' AND table_schema = '{}'",
            self.catalog, schema
        );

        let records = self.query(&sql).await?;
        let mut views = Vec::new();

        for row in &records {
            let name = extract_string(row, 0)?;
            let definition = extract_string(row, 1)?;
            views.push((name, definition));
        }

        Ok(views)
    }

    /// Executes a SQL query and returns the results.
    async fn query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        self.client
            .sql_in_public(sql)
            .await
            .context(DatabaseSnafu)?
            .ok_or_else(|| EmptyResultSnafu.build())
    }
}

/// Internal struct for table metadata.
struct TableInfo {
    table_type: String,
    table_id: Option<u64>,
    options: TableOptions,
    comment: Option<String>,
}

/// Extracts a string value from a row.
fn extract_string(row: &[Value], index: usize) -> Result<String> {
    match row.get(index) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Null) => Ok(String::new()),
        _ => UnexpectedValueTypeSnafu.fail(),
    }
}

/// Extracts an optional string value from a row.
fn extract_optional_string(row: &[Value], index: usize) -> Option<String> {
    match row.get(index) {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// Extracts an optional number value from a row.
fn extract_optional_number(row: &[Value], index: usize) -> Option<u64> {
    match row.get(index) {
        Some(Value::Number(n)) => n.as_u64(),
        _ => None,
    }
}

/// Parses options string into a HashMap.
fn parse_options(options_str: &str) -> HashMap<String, String> {
    // Options are stored as JSON or key=value pairs
    if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(options_str) {
        return map;
    }

    // Fallback: parse as key=value pairs (space-separated, as per TableOptions::Display)
    let mut options = HashMap::new();
    for part in options_str.split_whitespace() {
        if let Some((key, value)) = part.split_once('=') {
            options.insert(key.to_string(), value.to_string());
        }
    }
    options
}

/// Parses table options from engine and create_options.
fn parse_table_options(engine: &str, create_options: Option<&str>) -> TableOptions {
    let mut options = TableOptions {
        engine: engine.to_string(),
        ttl: None,
        extra: HashMap::new(),
    };

    if let Some(opts_str) = create_options {
        let parsed = parse_options(opts_str);

        // Extract TTL if present (most commonly used option)
        options.ttl = parsed.get("ttl").cloned();

        // Store all options in extra, excluding ttl to avoid duplicate JSON keys
        // when serialized alongside the dedicated ttl field.
        options.extra = parsed;
        options.extra.remove("ttl");
    }

    options
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_options_json() {
        let opts = r#"{"ttl": "30d", "custom": "value"}"#;
        let parsed = parse_options(opts);
        assert_eq!(parsed.get("ttl"), Some(&"30d".to_string()));
        assert_eq!(parsed.get("custom"), Some(&"value".to_string()));
    }

    #[test]
    fn test_parse_options_key_value() {
        // Format matches TableOptions::Display output (space-separated)
        let opts = "ttl=30d custom=value";
        let parsed = parse_options(opts);
        assert_eq!(parsed.get("ttl"), Some(&"30d".to_string()));
        assert_eq!(parsed.get("custom"), Some(&"value".to_string()));
    }

    #[test]
    fn test_parse_table_options() {
        let options = parse_table_options("mito", Some(r#"{"ttl": "7d"}"#));
        assert_eq!(options.engine, "mito");
        assert_eq!(options.ttl, Some("7d".to_string()));
        assert_eq!(options.extra.get("ttl"), None);
    }

    #[test]
    fn test_parse_table_options_with_extra() {
        let options = parse_table_options(
            "mito",
            Some("ttl=30d write_buffer_size=64MB skip_wal=false"),
        );
        assert_eq!(options.engine, "mito");
        assert_eq!(options.ttl, Some("30d".to_string()));
        assert_eq!(options.extra.get("ttl"), None);
        assert_eq!(
            options.extra.get("write_buffer_size"),
            Some(&"64MB".to_string())
        );
        assert_eq!(options.extra.get("skip_wal"), Some(&"false".to_string()));
    }

    #[test]
    fn test_parse_table_options_forward_compat() {
        // Simulate future option that doesn't exist yet
        let options = parse_table_options("mito", Some("ttl=7d future_option=value"));
        assert_eq!(options.ttl, Some("7d".to_string()));
        assert_eq!(options.extra.get("ttl"), None);
        assert_eq!(
            options.extra.get("future_option"),
            Some(&"value".to_string())
        );
    }
}
