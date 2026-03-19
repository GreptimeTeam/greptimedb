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

//! Minimal schema index structures for Export/Import V2.
//!
//! The canonical schema representation is the per-schema DDL file under
//! `schema/ddl/`. `schemas.json` only records which schemas exist in a snapshot.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Schema directory name within snapshot.
pub const SCHEMA_DIR: &str = "schema";

/// DDL directory name within schema directory.
pub const DDL_DIR: &str = "ddl";

/// Schema definition file name.
pub const SCHEMAS_FILE: &str = "schemas.json";

/// Schema (database) definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaDefinition {
    /// Catalog name.
    pub catalog: String,
    /// Schema (database) name.
    pub name: String,
    /// Schema options (if any).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

/// Minimal schema index stored in a snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaSnapshot {
    /// Schema (database) definitions.
    pub schemas: Vec<SchemaDefinition>,
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

    /// Filters the snapshot to only include specified schemas.
    pub fn filter_schemas(&self, schemas: &[String]) -> Self {
        Self {
            schemas: self
                .schemas
                .iter()
                .filter(|s| schemas.contains(&s.name))
                .cloned()
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
