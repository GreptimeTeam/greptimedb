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

//! Scripts table
use std::collections::HashMap;
use std::sync::Arc;

use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, SCRIPTS_TABLE_ID};
use common_catalog::format_full_table_name;
use common_query::Output;
use common_recordbatch::util as record_util;
use common_telemetry::logging;
use common_time::util;
use datatypes::prelude::{ConcreteDataType, ScalarVector};
use datatypes::schema::{ColumnSchema, RawSchema};
use datatypes::vectors::{StringVector, TimestampMillisecondVector, Vector, VectorRef};
use query::parser::QueryLanguageParser;
use query::QueryEngineRef;
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use table::requests::{CreateTableRequest, InsertRequest, TableOptions};

use crate::error::{
    CastTypeSnafu, CollectRecordsSnafu, FindScriptSnafu, FindScriptsTableSnafu, InsertScriptSnafu,
    RegisterScriptsTableSnafu, Result, ScriptNotFoundSnafu, ScriptsTableNotFoundSnafu,
};

pub const SCRIPTS_TABLE_NAME: &str = "scripts";

pub struct ScriptsTable {
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    name: String,
}

impl ScriptsTable {
    pub async fn new(
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
    ) -> Result<Self> {
        let schema = build_scripts_schema();
        // TODO(dennis): we put scripts table into default catalog and schema.
        // maybe put into system catalog?
        let request = CreateTableRequest {
            id: SCRIPTS_TABLE_ID,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: SCRIPTS_TABLE_NAME.to_string(),
            desc: Some("Scripts table".to_string()),
            schema,
            region_numbers: vec![0],
            //schema and name as primary key
            primary_key_indices: vec![0, 1],
            create_if_not_exists: true,
            table_options: TableOptions::default(),
        };

        catalog_manager
            .register_system_table(RegisterSystemTableRequest {
                create_table_request: request,
                open_hook: None,
            })
            .await
            .context(RegisterScriptsTableSnafu)?;

        Ok(Self {
            catalog_manager,
            query_engine,
            name: format_full_table_name(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                SCRIPTS_TABLE_NAME,
            ),
        })
    }

    pub async fn insert(&self, schema: &str, name: &str, script: &str) -> Result<()> {
        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(8);
        columns_values.insert(
            "schema".to_string(),
            Arc::new(StringVector::from(vec![schema])) as _,
        );
        columns_values.insert(
            "name".to_string(),
            Arc::new(StringVector::from(vec![name])) as _,
        );
        columns_values.insert(
            "script".to_string(),
            Arc::new(StringVector::from(vec![script])) as _,
        );
        // TODO(dennis): we only supports python right now.
        columns_values.insert(
            "engine".to_string(),
            Arc::new(StringVector::from(vec!["python"])) as _,
        );
        // Timestamp in key part is intentionally left to 0
        columns_values.insert(
            "timestamp".to_string(),
            Arc::new(TimestampMillisecondVector::from_slice([0])) as _,
        );
        let now = util::current_time_millis();
        columns_values.insert(
            "gmt_created".to_string(),
            Arc::new(TimestampMillisecondVector::from_slice([now])) as _,
        );
        columns_values.insert(
            "gmt_modified".to_string(),
            Arc::new(TimestampMillisecondVector::from_slice([now])) as _,
        );
        let table = self
            .catalog_manager
            .table(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                SCRIPTS_TABLE_NAME,
            )
            .await
            .context(FindScriptsTableSnafu)?
            .context(ScriptsTableNotFoundSnafu)?;

        let _ = table
            .insert(InsertRequest {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: SCRIPTS_TABLE_NAME.to_string(),
                columns_values,
                region_number: 0,
            })
            .await
            .context(InsertScriptSnafu { name })?;

        logging::info!("Inserted script: name={} into scripts table.", name);

        Ok(())
    }

    pub async fn find_script_by_name(&self, schema: &str, name: &str) -> Result<String> {
        // FIXME(dennis): SQL injection
        // TODO(dennis): we use sql to find the script, the better way is use a function
        //               such as `find_record_by_primary_key` in table_engine.
        let sql = format!(
            "select script from {} where schema='{}' and name='{}'",
            self.name(),
            schema,
            name
        );
        let stmt = QueryLanguageParser::parse_sql(&sql).unwrap();

        let plan = self
            .query_engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();

        let stream = match self
            .query_engine
            .execute(plan, QueryContext::arc())
            .await
            .context(FindScriptSnafu { name })?
        {
            Output::Stream(stream) => stream,
            _ => unreachable!(),
        };
        let records = record_util::collect(stream)
            .await
            .context(CollectRecordsSnafu)?;

        ensure!(!records.is_empty(), ScriptNotFoundSnafu { name });

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].num_columns(), 1);

        let script_column = records[0].column(0);
        let script_column = script_column
            .as_any()
            .downcast_ref::<StringVector>()
            .with_context(|| CastTypeSnafu {
                msg: format!(
                    "can't downcast {:?} array into string vector",
                    script_column.data_type()
                ),
            })?;

        assert_eq!(script_column.len(), 1);
        Ok(script_column.get_data(0).unwrap().to_string())
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Build scripts table
fn build_scripts_schema() -> RawSchema {
    let cols = vec![
        ColumnSchema::new(
            "schema".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "name".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "script".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "engine".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            "timestamp".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
        ColumnSchema::new(
            "gmt_created".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
        ColumnSchema::new(
            "gmt_modified".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        ),
    ];

    RawSchema::new(cols)
}
