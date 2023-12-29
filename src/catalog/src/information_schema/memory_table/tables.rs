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

use std::sync::Arc;

use common_catalog::consts::MITO_ENGINE;
use datatypes::prelude::{ConcreteDataType, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::vectors::{Int64Vector, StringVector};

use crate::information_schema::table_names::*;

const UNKNOWN: &str = "unknown";

/// Find the schema and columns by the table_name, only valid for memory tables.
/// Safety: the user MUST ensure the table schema exists, panic otherwise.
pub fn get_schema_columns(table_name: &str) -> (SchemaRef, Vec<VectorRef>) {
    let (column_schemas, columns): (_, Vec<VectorRef>) = match table_name {
        COLUMN_PRIVILEGES => (
            string_columns(&[
                "GRANTEE",
                "TABLE_CATALOG",
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "COLUMN_NAME",
                "PRIVILEGE_TYPE",
                "IS_GRANTABLE",
            ]),
            vec![],
        ),

        COLUMN_STATISTICS => (
            string_columns(&[
                "SCHEMA_NAME",
                "TABLE_NAME",
                "COLUMN_NAME",
                // TODO(dennis): It must be a JSON type, but we don't support it yet
                "HISTOGRAM",
            ]),
            vec![],
        ),

        ENGINES => (
            string_columns(&[
                "ENGINE",
                "SUPPORT",
                "COMMENT",
                "TRANSACTIONS",
                "XA",
                "SAVEPOINTS",
            ]),
            vec![
                Arc::new(StringVector::from(vec![MITO_ENGINE])),
                Arc::new(StringVector::from(vec!["DEFAULT"])),
                Arc::new(StringVector::from(vec![
                    "Storage engine for time-series data",
                ])),
                Arc::new(StringVector::from(vec!["NO"])),
                Arc::new(StringVector::from(vec!["NO"])),
                Arc::new(StringVector::from(vec!["NO"])),
            ],
        ),

        BUILD_INFO => (
            string_columns(&[
                "GIT_BRANCH",
                "GIT_COMMIT",
                "GIT_COMMIT_SHORT",
                "GIT_DIRTY",
                "PKG_VERSION",
            ]),
            vec![
                Arc::new(StringVector::from(vec![
                    build_data::get_git_branch().unwrap_or_else(|_| UNKNOWN.to_string())
                ])),
                Arc::new(StringVector::from(vec![
                    build_data::get_git_commit().unwrap_or_else(|_| UNKNOWN.to_string())
                ])),
                Arc::new(StringVector::from(vec![
                    build_data::get_git_commit_short().unwrap_or_else(|_| UNKNOWN.to_string())
                ])),
                Arc::new(StringVector::from(vec![
                    build_data::get_git_dirty().map_or(UNKNOWN.to_string(), |v| v.to_string())
                ])),
                Arc::new(StringVector::from(vec![option_env!("CARGO_PKG_VERSION")])),
            ],
        ),

        CHARACTER_SETS => (
            vec![
                string_column("CHARACTER_SET_NAME"),
                string_column("DEFAULT_COLLATE_NAME"),
                string_column("DESCRIPTION"),
                bigint_column("MAXLEN"),
            ],
            vec![
                Arc::new(StringVector::from(vec!["utf8"])),
                Arc::new(StringVector::from(vec!["utf8_bin"])),
                Arc::new(StringVector::from(vec!["UTF-8 Unicode"])),
                Arc::new(Int64Vector::from_slice([4])),
            ],
        ),

        COLLATIONS => (
            vec![
                string_column("COLLATION_NAME"),
                string_column("CHARACTER_SET_NAME"),
                bigint_column("ID"),
                string_column("IS_DEFAULT"),
                string_column("IS_COMPILED"),
                bigint_column("SORTLEN"),
            ],
            vec![
                Arc::new(StringVector::from(vec!["utf8_bin"])),
                Arc::new(StringVector::from(vec!["utf8"])),
                Arc::new(Int64Vector::from_slice([1])),
                Arc::new(StringVector::from(vec!["Yes"])),
                Arc::new(StringVector::from(vec!["Yes"])),
                Arc::new(Int64Vector::from_slice([1])),
            ],
        ),

        COLLATION_CHARACTER_SET_APPLICABILITY => (
            vec![
                string_column("COLLATION_NAME"),
                string_column("CHARACTER_SET_NAME"),
            ],
            vec![
                Arc::new(StringVector::from(vec!["utf8_bin"])),
                Arc::new(StringVector::from(vec!["utf8"])),
            ],
        ),

        CHECK_CONSTRAINTS => (
            string_columns(&[
                "CONSTRAINT_CATALOG",
                "CONSTRAINT_SCHEMA",
                "CONSTRAINT_NAME",
                "CHECK_CLAUSE",
            ]),
            // Not support check constraints yet
            vec![],
        ),

        EVENTS => (
            vec![
                string_column("EVENT_CATALOG"),
                string_column("EVENT_SCHEMA"),
                string_column("EVENT_NAME"),
                string_column("DEFINER"),
                string_column("TIME_ZONE"),
                string_column("EVENT_BODY"),
                string_column("EVENT_DEFINITION"),
                string_column("EVENT_TYPE"),
                datetime_column("EXECUTE_AT"),
                bigint_column("INTERVAL_VALUE"),
                string_column("INTERVAL_FIELD"),
                string_column("SQL_MODE"),
                datetime_column("STARTS"),
                datetime_column("ENDS"),
                string_column("STATUS"),
                string_column("ON_COMPLETION"),
                datetime_column("CREATED"),
                datetime_column("LAST_ALTERED"),
                datetime_column("LAST_EXECUTED"),
                string_column("EVENT_COMMENT"),
                bigint_column("ORIGINATOR"),
                string_column("CHARACTER_SET_CLIENT"),
                string_column("COLLATION_CONNECTION"),
                string_column("DATABASE_COLLATION"),
            ],
            vec![],
        ),

        FILES => (
            vec![
                bigint_column("FILE_ID"),
                string_column("FILE_NAME"),
                string_column("FILE_TYPE"),
                string_column("TABLESPACE_NAME"),
                string_column("TABLE_CATALOG"),
                string_column("TABLE_SCHEMA"),
                string_column("TABLE_NAME"),
                string_column("LOGFILE_GROUP_NAME"),
                bigint_column("LOGFILE_GROUP_NUMBER"),
                string_column("ENGINE"),
                string_column("FULLTEXT_KEYS"),
                bigint_column("DELETED_ROWS"),
                bigint_column("UPDATE_COUNT"),
                bigint_column("FREE_EXTENTS"),
                bigint_column("TOTAL_EXTENTS"),
                bigint_column("EXTENT_SIZE"),
                bigint_column("INITIAL_SIZE"),
                bigint_column("MAXIMUM_SIZE"),
                bigint_column("AUTOEXTEND_SIZE"),
                datetime_column("CREATION_TIME"),
                datetime_column("LAST_UPDATE_TIME"),
                datetime_column("LAST_ACCESS_TIME"),
                datetime_column("RECOVER_TIME"),
                bigint_column("TRANSACTION_COUNTER"),
                string_column("VERSION"),
                string_column("ROW_FORMAT"),
                bigint_column("TABLE_ROWS"),
                bigint_column("AVG_ROW_LENGTH"),
                bigint_column("DATA_LENGTH"),
                bigint_column("MAX_DATA_LENGTH"),
                bigint_column("INDEX_LENGTH"),
                bigint_column("DATA_FREE"),
                datetime_column("CREATE_TIME"),
                datetime_column("UPDATE_TIME"),
                datetime_column("CHECK_TIME"),
                string_column("CHECKSUM"),
                string_column("STATUS"),
                bigint_column("EXTRA"),
            ],
            vec![],
        ),

        _ => unreachable!("Unknown table in information_schema: {}", table_name),
    };

    (Arc::new(Schema::new(column_schemas)), columns)
}

fn string_columns(names: &[&'static str]) -> Vec<ColumnSchema> {
    names.iter().map(|name| string_column(name)).collect()
}

fn string_column(name: &str) -> ColumnSchema {
    ColumnSchema::new(
        str::to_lowercase(name),
        ConcreteDataType::string_datatype(),
        false,
    )
}

fn bigint_column(name: &str) -> ColumnSchema {
    ColumnSchema::new(
        str::to_lowercase(name),
        ConcreteDataType::int64_datatype(),
        false,
    )
}

fn datetime_column(name: &str) -> ColumnSchema {
    ColumnSchema::new(
        str::to_lowercase(name),
        ConcreteDataType::datetime_datatype(),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_columns() {
        let columns = ["a", "b", "c"];
        let column_schemas = string_columns(&columns);

        assert_eq!(3, column_schemas.len());
        for (i, name) in columns.iter().enumerate() {
            let cs = column_schemas.get(i).unwrap();

            assert_eq!(*name, cs.name);
            assert_eq!(ConcreteDataType::string_datatype(), cs.data_type);
        }
    }
}
