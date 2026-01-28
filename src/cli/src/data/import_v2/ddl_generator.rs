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

//! DDL generation from JSON schema definitions.

use std::collections::HashSet;

use store_api::metric_engine_consts::{
    LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
};

use super::error::{InvalidColumnDefinitionSnafu, Result};
use crate::data::export_v2::schema::{
    ColumnDefinition, SchemaDefinition, SchemaSnapshot, SemanticType, TableDefinition,
    ViewDefinition,
};

/// Generates DDL statements from schema definitions.
pub struct DdlGenerator<'a> {
    schema_snapshot: &'a SchemaSnapshot,
}

impl<'a> DdlGenerator<'a> {
    /// Creates a new DDL generator.
    pub fn new(schema_snapshot: &'a SchemaSnapshot) -> Self {
        Self { schema_snapshot }
    }

    /// Generates all DDL statements for the specified schemas.
    ///
    /// Returns statements in dependency order:
    /// 1. CREATE DATABASE statements
    /// 2. CREATE TABLE statements
    /// 3. CREATE VIEW statements (after tables they depend on)
    pub fn generate(&self, schemas: &[String]) -> Result<Vec<String>> {
        let mut statements = Vec::new();

        // Filter snapshot to only include specified schemas
        let filtered = self.schema_snapshot.filter_schemas(schemas);

        // 1. CREATE DATABASE statements
        for schema_def in &filtered.schemas {
            statements.push(self.generate_create_database(schema_def));
        }

        // 2. CREATE TABLE statements
        let mut existing_tables = HashSet::new();
        for table_def in &filtered.tables {
            existing_tables.insert((table_def.schema.clone(), table_def.name.clone()));
        }

        let mut generated_physical = HashSet::new();
        for table_def in &filtered.tables {
            if table_def.options.engine == METRIC_ENGINE_NAME
                && let Some(physical_name) = table_def.options.extra.get(LOGICAL_TABLE_METADATA_KEY)
            {
                let key = (table_def.schema.clone(), physical_name.clone());
                if !existing_tables.contains(&key) && generated_physical.insert(key.clone()) {
                    let physical_def =
                        self.build_physical_table_definition(table_def, physical_name);
                    statements.push(self.generate_create_table(&physical_def)?);
                }
            }

            statements.push(self.generate_create_table(table_def)?);
        }

        // 3. CREATE VIEW statements
        for view_def in &filtered.views {
            statements.push(self.generate_create_view(view_def));
        }

        Ok(statements)
    }

    /// Generates CREATE DATABASE statement.
    fn generate_create_database(&self, schema: &SchemaDefinition) -> String {
        // Use quoted identifiers to handle special characters
        format!(
            "CREATE DATABASE IF NOT EXISTS \"{}\"",
            escape_identifier(&schema.name)
        )
    }

    /// Generates CREATE TABLE statement.
    fn generate_create_table(&self, table: &TableDefinition) -> Result<String> {
        let mut sql = String::new();

        // CREATE TABLE header
        sql.push_str(&format!(
            "CREATE TABLE IF NOT EXISTS \"{}\".\"{}\" (\n",
            escape_identifier(&table.schema),
            escape_identifier(&table.name)
        ));

        // Column definitions
        let column_defs: Vec<String> = table
            .columns
            .iter()
            .map(|col| self.generate_column_definition(col, &table.name))
            .collect::<Result<Vec<_>>>()?;

        sql.push_str(&column_defs.join(",\n"));

        // TIME INDEX constraint
        sql.push_str(&format!(
            ",\n  TIME INDEX (\"{}\")",
            escape_identifier(&table.time_index)
        ));

        // PRIMARY KEY constraint (if any)
        if !table.primary_keys.is_empty() {
            let pk_list = table
                .primary_keys
                .iter()
                .map(|k| format!("\"{}\"", escape_identifier(k)))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(",\n  PRIMARY KEY ({})", pk_list));
        }

        sql.push_str("\n)");

        // ENGINE clause
        sql.push_str(&format!("\nENGINE = {}", table.options.engine));

        // WITH options
        let with_options = self.generate_with_options(table);
        if !with_options.is_empty() {
            sql.push_str(&format!("\nWITH (\n  {}\n)", with_options.join(",\n  ")));
        }

        Ok(sql)
    }

    fn build_physical_table_definition(
        &self,
        logical_table: &TableDefinition,
        physical_name: &str,
    ) -> TableDefinition {
        let mut extra = logical_table.options.extra.clone();
        extra.remove(LOGICAL_TABLE_METADATA_KEY);
        extra.insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), "true".to_string());

        let mut table = logical_table.clone();
        table.name = physical_name.to_string();
        table.options.engine = METRIC_ENGINE_NAME.to_string();
        table.options.extra = extra;
        table
    }

    /// Generates column definition.
    fn generate_column_definition(
        &self,
        col: &ColumnDefinition,
        table_name: &str,
    ) -> Result<String> {
        let mut def = format!("  \"{}\" {}", escape_identifier(&col.name), col.sql_type);

        // Semantic type annotation (for GreptimeDB)
        match col.semantic_type {
            SemanticType::Tag => {
                // Tags are implicitly part of PRIMARY KEY, no annotation needed
            }
            SemanticType::Timestamp => {
                // Time index is specified separately
            }
            SemanticType::Field => {
                // Fields are the default, no annotation needed
            }
        }

        // NULL/NOT NULL
        if !col.nullable {
            def.push_str(" NOT NULL");
        } else {
            def.push_str(" NULL");
        }

        // DEFAULT value
        if let Some(ref default) = col.default_value {
            let trimmed = default.trim();
            if trimmed.is_empty() {
                if is_string_like_type(&col.sql_type) {
                    def.push_str(" DEFAULT ''");
                } else {
                    return InvalidColumnDefinitionSnafu {
                        table: table_name.to_string(),
                        reason: format!("empty default value for column '{}'", col.name),
                    }
                    .fail();
                }
            } else {
                let formatted = format_default_value(trimmed, &col.sql_type);
                def.push_str(&format!(" DEFAULT {}", formatted));
            }
        }

        // COMMENT
        if let Some(ref comment) = col.comment {
            def.push_str(&format!(" COMMENT '{}'", escape_string(comment)));
        }

        Ok(def)
    }

    /// Generates WITH options for CREATE TABLE.
    fn generate_with_options(&self, table: &TableDefinition) -> Vec<String> {
        let mut options = Vec::new();

        // TTL is extracted separately for convenience.
        if let Some(ref ttl) = table.options.ttl {
            options.push(format!("ttl = '{}'", escape_string(ttl)));
        }

        // Add all extra options (including write_buffer_size, skip_wal, compaction_type, etc.)
        for (key, value) in &table.options.extra {
            // Backward compatibility: older snapshots may have ttl in extra.
            if key == "ttl" && table.options.ttl.is_some() {
                continue;
            }
            // Handle special option key formats
            let option_key = if key == "compaction_type" {
                "compaction.type"
            } else {
                key.as_str()
            };
            let formatted_key = format_option_key(option_key);
            options.push(format!("{} = '{}'", formatted_key, escape_string(value)));
        }

        options
    }

    /// Generates CREATE VIEW statement.
    fn generate_create_view(&self, view: &ViewDefinition) -> String {
        let definition =
            extract_view_query(&view.definition).unwrap_or_else(|| view.definition.trim());
        format!(
            "CREATE VIEW IF NOT EXISTS \"{}\".\"{}\" AS {}",
            escape_identifier(&view.schema),
            escape_identifier(&view.name),
            definition
        )
    }
}

/// Escapes a SQL identifier (table/column name).
fn escape_identifier(s: &str) -> String {
    s.replace('"', "\"\"")
}

fn format_default_value(default: &str, sql_type: &str) -> String {
    if is_null_literal(default) {
        return "NULL".to_string();
    }
    if is_function_default(default) {
        return default.to_string();
    }
    if is_already_quoted(default) {
        return default.to_string();
    }

    let needs_quote = is_string_like_type(sql_type) || is_temporal_type(sql_type);
    if needs_quote {
        format!("'{}'", escape_string(default))
    } else {
        default.to_string()
    }
}

fn is_string_like_type(sql_type: &str) -> bool {
    let upper = sql_type.trim().to_ascii_uppercase();
    upper.starts_with("STRING")
        || upper.starts_with("VARCHAR")
        || upper.starts_with("CHAR")
        || upper.starts_with("TEXT")
        || upper.starts_with("TINYTEXT")
        || upper.starts_with("BINARY")
        || upper.starts_with("VARBINARY")
        || upper.starts_with("JSON")
}

fn is_temporal_type(sql_type: &str) -> bool {
    let upper = sql_type.trim().to_ascii_uppercase();
    upper.starts_with("TIMESTAMP")
        || upper.starts_with("DATE")
        || upper.starts_with("TIME")
        || upper.starts_with("DATETIME")
        || upper.starts_with("DURATION")
        || upper.starts_with("INTERVAL")
}

fn is_function_default(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.contains('(') {
        return true;
    }

    matches!(
        trimmed.to_ascii_lowercase().as_str(),
        "current_timestamp"
            | "now"
            | "localtime"
            | "localtimestamp"
            | "current_date"
            | "current_time"
    )
}

fn is_null_literal(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("null")
}

fn is_already_quoted(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() >= 2
        && ((trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"')))
}

fn extract_view_query(definition: &str) -> Option<&str> {
    let trimmed = definition.trim_start();
    let tokens = tokenize(trimmed);
    if tokens.is_empty() {
        return None;
    }
    if !token_eq(trimmed, tokens[0], "CREATE") {
        return None;
    }

    let mut saw_view = false;
    for (start, end) in tokens.iter().skip(1) {
        if !saw_view {
            if token_eq(trimmed, (*start, *end), "VIEW") {
                saw_view = true;
            }
            continue;
        }

        if token_eq(trimmed, (*start, *end), "AS") {
            let rest = &trimmed[*end..];
            return Some(rest.trim_start());
        }
    }

    None
}

fn tokenize(input: &str) -> Vec<(usize, usize)> {
    let mut tokens = Vec::new();
    let mut start = None;
    for (i, ch) in input.char_indices() {
        if ch.is_whitespace() {
            if let Some(s) = start {
                tokens.push((s, i));
                start = None;
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(s) = start {
        tokens.push((s, input.len()));
    }
    tokens
}

fn token_eq(input: &str, token: (usize, usize), keyword: &str) -> bool {
    input[token.0..token.1].eq_ignore_ascii_case(keyword)
}

/// Formats a table option key for use in WITH clauses.
fn format_option_key(key: &str) -> String {
    let is_simple = key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if is_simple {
        key.to_string()
    } else {
        format!("'{}'", key.replace('\'', "''"))
    }
}

/// Escapes a SQL string literal.
fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use store_api::metric_engine_consts::{
        LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME, PHYSICAL_TABLE_METADATA_KEY,
    };

    use super::*;
    use crate::data::export_v2::schema::TableOptions;

    #[test]
    fn test_generate_create_database() {
        let snapshot = SchemaSnapshot::default();
        let generator = DdlGenerator::new(&snapshot);

        let schema = SchemaDefinition {
            catalog: "greptime".to_string(),
            name: "public".to_string(),
            options: HashMap::new(),
        };

        let ddl = generator.generate_create_database(&schema);
        assert_eq!(ddl, "CREATE DATABASE IF NOT EXISTS \"public\"");
    }

    #[test]
    fn test_generate_create_table() {
        let snapshot = SchemaSnapshot::default();
        let generator = DdlGenerator::new(&snapshot);

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
                    comment: Some("CPU usage".to_string()),
                },
            ],
            primary_keys: vec!["host".to_string()],
            time_index: "ts".to_string(),
            options: TableOptions {
                engine: "mito".to_string(),
                ttl: Some("7d".to_string()),
                ..Default::default()
            },
            comment: None,
        };

        let ddl = generator.generate_create_table(&table).unwrap();

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS \"public\".\"metrics\""));
        assert!(ddl.contains("\"ts\" TIMESTAMP(3) NOT NULL"));
        assert!(ddl.contains("\"host\" STRING NOT NULL"));
        assert!(ddl.contains("\"cpu\" DOUBLE NULL COMMENT 'CPU usage'"));
        assert!(ddl.contains("TIME INDEX (\"ts\")"));
        assert!(ddl.contains("PRIMARY KEY (\"host\")"));
        assert!(ddl.contains("ENGINE = mito"));
        assert!(ddl.contains("ttl = '7d'"));
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("normal"), "normal");
        assert_eq!(escape_identifier("with\"quote"), "with\"\"quote");
    }

    #[test]
    fn test_format_option_key() {
        assert_eq!(format_option_key("ttl"), "ttl");
        assert_eq!(format_option_key("compaction.type"), "'compaction.type'");
        assert_eq!(
            format_option_key("compaction.override"),
            "'compaction.override'"
        );
        assert_eq!(format_option_key("with-dash"), "'with-dash'");
    }

    #[test]
    fn test_extract_view_query() {
        let def = "CREATE VIEW metrics_view AS SELECT * FROM metrics WHERE cpu > 0.5";
        assert_eq!(
            extract_view_query(def),
            Some("SELECT * FROM metrics WHERE cpu > 0.5")
        );

        let def = "CREATE OR REPLACE VIEW metrics_view AS SELECT 1";
        assert_eq!(extract_view_query(def), Some("SELECT 1"));

        let def = "SELECT * FROM metrics";
        assert_eq!(extract_view_query(def), None);
    }

    #[test]
    fn test_escape_string() {
        assert_eq!(escape_string("normal"), "normal");
        assert_eq!(escape_string("it's"), "it''s");
    }

    #[test]
    fn test_generate_column_definition_defaults() {
        let snapshot = SchemaSnapshot::default();
        let generator = DdlGenerator::new(&snapshot);

        let col = ColumnDefinition {
            name: "name".to_string(),
            data_type: "String".to_string(),
            sql_type: "STRING".to_string(),
            semantic_type: SemanticType::Field,
            nullable: true,
            default_value: Some("hello".to_string()),
            comment: None,
        };
        let ddl = generator
            .generate_column_definition(&col, "metrics")
            .unwrap();
        assert!(ddl.contains("DEFAULT 'hello'"));

        let col = ColumnDefinition {
            name: "value".to_string(),
            data_type: "Float64".to_string(),
            sql_type: "DOUBLE".to_string(),
            semantic_type: SemanticType::Field,
            nullable: true,
            default_value: Some("0".to_string()),
            comment: None,
        };
        let ddl = generator
            .generate_column_definition(&col, "metrics")
            .unwrap();
        assert!(ddl.contains("DEFAULT 0"));

        let col = ColumnDefinition {
            name: "ts".to_string(),
            data_type: "TimestampMillisecond".to_string(),
            sql_type: "TIMESTAMP(3)".to_string(),
            semantic_type: SemanticType::Timestamp,
            nullable: false,
            default_value: Some("2024-01-01T00:00:00Z".to_string()),
            comment: None,
        };
        let ddl = generator
            .generate_column_definition(&col, "metrics")
            .unwrap();
        assert!(ddl.contains("DEFAULT '2024-01-01T00:00:00Z'"));

        let col = ColumnDefinition {
            name: "created_at".to_string(),
            data_type: "TimestampMillisecond".to_string(),
            sql_type: "TIMESTAMP(3)".to_string(),
            semantic_type: SemanticType::Timestamp,
            nullable: false,
            default_value: Some("current_timestamp()".to_string()),
            comment: None,
        };
        let ddl = generator
            .generate_column_definition(&col, "metrics")
            .unwrap();
        assert!(ddl.contains("DEFAULT current_timestamp()"));

        let col = ColumnDefinition {
            name: "empty_str".to_string(),
            data_type: "String".to_string(),
            sql_type: "STRING".to_string(),
            semantic_type: SemanticType::Field,
            nullable: true,
            default_value: Some("".to_string()),
            comment: None,
        };
        let ddl = generator
            .generate_column_definition(&col, "metrics")
            .unwrap();
        assert!(ddl.contains("DEFAULT ''"));

        let col = ColumnDefinition {
            name: "null_str".to_string(),
            data_type: "String".to_string(),
            sql_type: "STRING".to_string(),
            semantic_type: SemanticType::Field,
            nullable: true,
            default_value: Some("NULL".to_string()),
            comment: None,
        };
        let ddl = generator
            .generate_column_definition(&col, "metrics")
            .unwrap();
        assert!(ddl.contains("DEFAULT NULL"));
    }

    #[test]
    fn test_generate_adds_metric_physical_table() {
        let mut snapshot = SchemaSnapshot::default();
        snapshot.add_schema(SchemaDefinition {
            catalog: "greptime".to_string(),
            name: "public".to_string(),
            options: HashMap::new(),
        });

        let columns = vec![
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
                nullable: true,
                default_value: None,
                comment: None,
            },
            ColumnDefinition {
                name: "region_name".to_string(),
                data_type: "String".to_string(),
                sql_type: "STRING".to_string(),
                semantic_type: SemanticType::Tag,
                nullable: true,
                default_value: None,
                comment: None,
            },
            ColumnDefinition {
                name: "cpu".to_string(),
                data_type: "Float64".to_string(),
                sql_type: "DOUBLE".to_string(),
                semantic_type: SemanticType::Field,
                nullable: true,
                default_value: Some("0".to_string()),
                comment: None,
            },
        ];

        let mut options = TableOptions {
            engine: METRIC_ENGINE_NAME.to_string(),
            ..Default::default()
        };
        options.extra.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            "metrics_physical".to_string(),
        );

        snapshot.add_table(TableDefinition {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            name: "metrics_logical".to_string(),
            table_type: "BASE TABLE".to_string(),
            table_id: None,
            columns,
            primary_keys: vec!["host".to_string(), "region_name".to_string()],
            time_index: "ts".to_string(),
            options,
            comment: None,
        });

        let generator = DdlGenerator::new(&snapshot);
        let statements = generator.generate(&["public".to_string()]).unwrap();

        assert_eq!(statements.len(), 3);
        assert!(statements[1].contains("\"metrics_physical\""));
        assert!(statements[1].contains("ENGINE = metric"));
        assert!(statements[1].contains("physical_metric_table"));
        assert!(statements[2].contains("\"metrics_logical\""));
        assert!(statements[2].contains("on_physical_table"));
    }

    #[test]
    fn test_generate_skips_existing_metric_physical_table() {
        let mut snapshot = SchemaSnapshot::default();
        snapshot.add_schema(SchemaDefinition {
            catalog: "greptime".to_string(),
            name: "public".to_string(),
            options: HashMap::new(),
        });

        let columns = vec![
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
                nullable: true,
                default_value: None,
                comment: None,
            },
            ColumnDefinition {
                name: "region_name".to_string(),
                data_type: "String".to_string(),
                sql_type: "STRING".to_string(),
                semantic_type: SemanticType::Tag,
                nullable: true,
                default_value: None,
                comment: None,
            },
            ColumnDefinition {
                name: "cpu".to_string(),
                data_type: "Float64".to_string(),
                sql_type: "DOUBLE".to_string(),
                semantic_type: SemanticType::Field,
                nullable: true,
                default_value: Some("0".to_string()),
                comment: None,
            },
        ];

        let mut physical_options = TableOptions {
            engine: METRIC_ENGINE_NAME.to_string(),
            ..Default::default()
        };
        physical_options
            .extra
            .insert(PHYSICAL_TABLE_METADATA_KEY.to_string(), "true".to_string());

        snapshot.add_table(TableDefinition {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            name: "metrics_physical".to_string(),
            table_type: "BASE TABLE".to_string(),
            table_id: None,
            columns: columns.clone(),
            primary_keys: vec!["host".to_string(), "region_name".to_string()],
            time_index: "ts".to_string(),
            options: physical_options,
            comment: None,
        });

        let mut logical_options = TableOptions {
            engine: METRIC_ENGINE_NAME.to_string(),
            ..Default::default()
        };
        logical_options.extra.insert(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            "metrics_physical".to_string(),
        );

        snapshot.add_table(TableDefinition {
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            name: "metrics_logical".to_string(),
            table_type: "BASE TABLE".to_string(),
            table_id: None,
            columns,
            primary_keys: vec!["host".to_string(), "region_name".to_string()],
            time_index: "ts".to_string(),
            options: logical_options,
            comment: None,
        });

        let generator = DdlGenerator::new(&snapshot);
        let statements = generator.generate(&["public".to_string()]).unwrap();

        assert_eq!(statements.len(), 3);
        let physical_count = statements
            .iter()
            .filter(|stmt| stmt.contains("\"metrics_physical\""))
            .count();
        assert_eq!(physical_count, 1);
    }
}
