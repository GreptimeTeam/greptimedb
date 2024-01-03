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
                string_column("EXTRA"),
            ],
            vec![],
        ),

        OPTIMIZER_TRACE => (
            vec![
                string_column("QUERY"),
                string_column("TRACE"),
                bigint_column("MISSING_BYTES_BEYOND_MAX_MEM_SIZE"),
                bigint_column("INSUFFICIENT_PRIVILEGES"),
            ],
            vec![],
        ),

        // MySQL(https://dev.mysql.com/doc/refman/8.2/en/information-schema-parameters-table.html)
        // has the spec that is different from
        // PostgreSQL(https://www.postgresql.org/docs/current/infoschema-parameters.html).
        // Follow `MySQL` spec here.
        PARAMETERS => (
            vec![
                string_column("SPECIFIC_CATALOG"),
                string_column("SPECIFIC_SCHEMA"),
                string_column("SPECIFIC_NAME"),
                bigint_column("ORDINAL_POSITION"),
                string_column("PARAMETER_MODE"),
                string_column("PARAMETER_NAME"),
                string_column("DATA_TYPE"),
                bigint_column("CHARACTER_MAXIMUM_LENGTH"),
                bigint_column("CHARACTER_OCTET_LENGTH"),
                bigint_column("NUMERIC_PRECISION"),
                bigint_column("NUMERIC_SCALE"),
                bigint_column("DATETIME_PRECISION"),
                string_column("CHARACTER_SET_NAME"),
                string_column("COLLATION_NAME"),
                string_column("DTD_IDENTIFIER"),
                string_column("ROUTINE_TYPE"),
            ],
            vec![],
        ),

        PROFILING => (
            vec![
                bigint_column("QUERY_ID"),
                bigint_column("SEQ"),
                string_column("STATE"),
                bigint_column("DURATION"),
                bigint_column("CPU_USER"),
                bigint_column("CPU_SYSTEM"),
                bigint_column("CONTEXT_VOLUNTARY"),
                bigint_column("CONTEXT_INVOLUNTARY"),
                bigint_column("BLOCK_OPS_IN"),
                bigint_column("BLOCK_OPS_OUT"),
                bigint_column("MESSAGES_SENT"),
                bigint_column("MESSAGES_RECEIVED"),
                bigint_column("PAGE_FAULTS_MAJOR"),
                bigint_column("PAGE_FAULTS_MINOR"),
                bigint_column("SWAPS"),
                string_column("SOURCE_FUNCTION"),
                string_column("SOURCE_FILE"),
                bigint_column("SOURCE_LINE"),
            ],
            vec![],
        ),

        // TODO: _Must_ reimplement this table when foreign key constraint is supported.
        REFERENTIAL_CONSTRAINTS => (
            vec![
                string_column("CONSTRAINT_CATALOG"),
                string_column("CONSTRAINT_SCHEMA"),
                string_column("CONSTRAINT_NAME"),
                string_column("UNIQUE_CONSTRAINT_CATALOG"),
                string_column("UNIQUE_CONSTRAINT_SCHEMA"),
                string_column("UNIQUE_CONSTRAINT_NAME"),
                string_column("MATCH_OPTION"),
                string_column("UPDATE_RULE"),
                string_column("DELETE_RULE"),
                string_column("TABLE_NAME"),
                string_column("REFERENCED_TABLE_NAME"),
            ],
            vec![],
        ),

        ROUTINES => (
            vec![
                string_column("SPECIFIC_NAME"),
                string_column("ROUTINE_CATALOG"),
                string_column("ROUTINE_SCHEMA"),
                string_column("ROUTINE_NAME"),
                string_column("ROUTINE_TYPE"),
                string_column("DATA_TYPE"),
                bigint_column("CHARACTER_MAXIMUM_LENGTH"),
                bigint_column("CHARACTER_OCTET_LENGTH"),
                bigint_column("NUMERIC_PRECISION"),
                bigint_column("NUMERIC_SCALE"),
                bigint_column("DATETIME_PRECISION"),
                string_column("CHARACTER_SET_NAME"),
                string_column("COLLATION_NAME"),
                string_column("DTD_IDENTIFIER"),
                string_column("ROUTINE_BODY"),
                string_column("ROUTINE_DEFINITION"),
                string_column("EXTERNAL_NAME"),
                string_column("EXTERNAL_LANGUAGE"),
                string_column("PARAMETER_STYLE"),
                string_column("IS_DETERMINISTIC"),
                string_column("SQL_DATA_ACCESS"),
                string_column("SQL_PATH"),
                string_column("SECURITY_TYPE"),
                datetime_column("CREATED"),
                datetime_column("LAST_ALTERED"),
                string_column("SQL_MODE"),
                string_column("ROUTINE_COMMENT"),
                string_column("DEFINER"),
                string_column("CHARACTER_SET_CLIENT"),
                string_column("COLLATION_CONNECTION"),
                string_column("DATABASE_COLLATION"),
            ],
            vec![],
        ),

        SCHEMA_PRIVILEGES => (
            vec![
                string_column("GRANTEE"),
                string_column("TABLE_CATALOG"),
                string_column("TABLE_SCHEMA"),
                string_column("PRIVILEGE_TYPE"),
                string_column("IS_GRANTABLE"),
            ],
            vec![],
        ),

        TABLE_PRIVILEGES => (
            vec![
                string_column("GRANTEE"),
                string_column("TABLE_CATALOG"),
                string_column("TABLE_SCHEMA"),
                string_column("TABLE_NAME"),
                string_column("PRIVILEGE_TYPE"),
                string_column("IS_GRANTABLE"),
            ],
            vec![],
        ),

        TRIGGERS => (
            vec![
                string_column("TRIGGER_CATALOG"),
                string_column("TRIGGER_SCHEMA"),
                string_column("TRIGGER_NAME"),
                string_column("EVENT_MANIPULATION"),
                string_column("EVENT_OBJECT_CATALOG"),
                string_column("EVENT_OBJECT_SCHEMA"),
                string_column("EVENT_OBJECT_TABLE"),
                bigint_column("ACTION_ORDER"),
                string_column("ACTION_CONDITION"),
                string_column("ACTION_STATEMENT"),
                string_column("ACTION_ORIENTATION"),
                string_column("ACTION_TIMING"),
                string_column("ACTION_REFERENCE_OLD_TABLE"),
                string_column("ACTION_REFERENCE_NEW_TABLE"),
                string_column("ACTION_REFERENCE_OLD_ROW"),
                string_column("ACTION_REFERENCE_NEW_ROW"),
                datetime_column("CREATED"),
                string_column("SQL_MODE"),
                string_column("DEFINER"),
                string_column("CHARACTER_SET_CLIENT"),
                string_column("COLLATION_CONNECTION"),
                string_column("DATABASE_COLLATION"),
            ],
            vec![],
        ),

        // TODO: Considering store internal metrics in `global_status` and
        // `session_status` tables.
        GLOBAL_STATUS => (
            vec![
                string_column("VARIABLE_NAME"),
                string_column("VARIABLE_VALUE"),
            ],
            vec![],
        ),

        SESSION_STATUS => (
            vec![
                string_column("VARIABLE_NAME"),
                string_column("VARIABLE_VALUE"),
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
