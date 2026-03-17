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
//! For V2 DDL-only snapshots, extractor only persists the schema index.

use std::collections::HashMap;

use serde_json::Value;
use snafu::ResultExt;

use crate::data::export_v2::error::{
    DatabaseSnafu, EmptyResultSnafu, Result, SchemaNotFoundSnafu, UnexpectedValueTypeSnafu,
};
use crate::data::export_v2::schema::{SchemaDefinition, SchemaSnapshot};
use crate::data::sql::escape_sql_literal;
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

    /// Extracts the schema index for the given schemas.
    ///
    /// If `schemas` is None, extracts all non-system schemas.
    pub async fn extract(&self, schemas: Option<&[String]>) -> Result<SchemaSnapshot> {
        let mut snapshot = SchemaSnapshot::new();

        let schema_names = match schemas {
            Some(names) => self.validate_schemas(names).await?,
            None => self.get_all_schemas().await?,
        };

        for schema_name in &schema_names {
            let schema_def = self.extract_schema_definition(schema_name).await?;
            snapshot.add_schema(schema_def);
        }

        Ok(snapshot)
    }

    /// Gets all non-system schemas in the catalog.
    async fn get_all_schemas(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE catalog_name = '{}'",
            escape_sql_literal(self.catalog)
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
            escape_sql_literal(self.catalog),
            escape_sql_literal(schema)
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
        let options = extract_optional_string(&records[0], 1)
            .map(|opts| parse_options(&opts))
            .unwrap_or_default();

        Ok(SchemaDefinition {
            catalog: self.catalog.to_string(),
            name,
            options,
        })
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

/// Extracts a string value from a row.
fn extract_string(row: &[Value], index: usize) -> Result<String> {
    match row.get(index) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Null) => UnexpectedValueTypeSnafu.fail(),
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

/// Parses options string into a HashMap.
fn parse_options(options_str: &str) -> HashMap<String, String> {
    if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(options_str) {
        return map;
    }

    let mut options = HashMap::new();
    for line in options_str.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some((key, value)) = parse_quoted_option_line(trimmed) {
            options.insert(key, value);
            continue;
        }

        for part in trimmed.split_whitespace() {
            if let Some((key, value)) = part.split_once('=') {
                options.insert(key.to_string(), value.to_string());
            }
        }
    }
    options
}

fn parse_quoted_option_line(line: &str) -> Option<(String, String)> {
    let key = line.strip_prefix('\'')?;
    let (key, rest) = key.split_once("'='")?;
    let value = rest.strip_suffix('\'')?;
    Some((key.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

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
        let opts = "ttl=30d custom=value";
        let parsed = parse_options(opts);
        assert_eq!(parsed.get("ttl"), Some(&"30d".to_string()));
        assert_eq!(parsed.get("custom"), Some(&"value".to_string()));
    }

    #[test]
    fn test_parse_options_schema_display_format() {
        let opts = "'ttl'='30d'\n'custom'='value with spaces'\n";
        let parsed = parse_options(opts);
        assert_eq!(parsed.get("ttl"), Some(&"30d".to_string()));
        assert_eq!(parsed.get("custom"), Some(&"value with spaces".to_string()));
    }

    #[test]
    fn test_extract_string_rejects_null() {
        let row = vec![Value::Null];
        assert!(extract_string(&row, 0).is_err());
    }
}
