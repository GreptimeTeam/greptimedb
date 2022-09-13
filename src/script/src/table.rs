//! Scripts table
use std::collections::HashMap;
use std::sync::Arc;

use catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, SCRIPTS_TABLE_ID};
use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use common_recordbatch::util as record_util;
use common_telemetry::logging;
use common_time::timestamp::Timestamp;
use common_time::util;
use datatypes::arrow::array::Utf8Array;
use datatypes::prelude::ConcreteDataType;
use datatypes::prelude::ScalarVector;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};
use datatypes::vectors::{StringVector, TimestampVector, VectorRef};
use query::{Output, QueryEngineRef};
use snafu::{ensure, OptionExt, ResultExt};
use table::requests::{CreateTableRequest, InsertRequest};

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
        let schema = Arc::new(build_scripts_schema());
        // TODO(dennis): we put scripts table into default catalog and schema.
        // maybe put into system catalog?
        let request = CreateTableRequest {
            id: SCRIPTS_TABLE_ID,
            catalog_name: Some(DEFAULT_CATALOG_NAME.to_string()),
            schema_name: Some(DEFAULT_SCHEMA_NAME.to_string()),
            table_name: SCRIPTS_TABLE_NAME.to_string(),
            desc: Some("Scripts table".to_string()),
            schema,
            // name and timestamp as primary key
            primary_key_indices: vec![0, 3],
            create_if_not_exists: true,
            table_options: HashMap::default(),
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
            name: catalog::format_full_table_name(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                SCRIPTS_TABLE_NAME,
            ),
        })
    }

    pub async fn insert(&self, name: &str, script: &str) -> Result<()> {
        let mut columns_values: HashMap<String, VectorRef> = HashMap::with_capacity(7);
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
            Arc::new(TimestampVector::from_slice(&[Timestamp::from_millis(0)])) as _,
        );
        columns_values.insert(
            "gmt_created".to_string(),
            Arc::new(TimestampVector::from_slice(&[Timestamp::from_millis(
                util::current_time_millis(),
            )])) as _,
        );
        columns_values.insert(
            "gmt_modified".to_string(),
            Arc::new(TimestampVector::from_slice(&[Timestamp::from_millis(
                util::current_time_millis(),
            )])) as _,
        );

        let table = self
            .catalog_manager
            .table(
                Some(DEFAULT_CATALOG_NAME),
                Some(DEFAULT_SCHEMA_NAME),
                SCRIPTS_TABLE_NAME,
            )
            .context(FindScriptsTableSnafu)?
            .context(ScriptsTableNotFoundSnafu)?;

        let _ = table
            .insert(InsertRequest {
                table_name: SCRIPTS_TABLE_NAME.to_string(),
                columns_values,
            })
            .await
            .context(InsertScriptSnafu { name })?;

        logging::info!("Inserted script: name={} into scripts table.", name);

        Ok(())
    }

    pub async fn find_script_by_name(&self, name: &str) -> Result<String> {
        // FIXME(dennis): SQL injection
        // TODO(dennis): we use sql to find the script, the better way is use a function
        //               such as `find_record_by_primary_key` in table_engine.
        let sql = format!("select script from {} where name='{}'", self.name(), name);

        let plan = self
            .query_engine
            .sql_to_plan(&sql)
            .context(FindScriptSnafu { name })?;

        let stream = match self
            .query_engine
            .execute(&plan)
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
        assert_eq!(records[0].df_recordbatch.num_columns(), 1);

        let record = &records[0].df_recordbatch;

        let script_column = record
            .column(0)
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .context(CastTypeSnafu {
                msg: format!(
                    "can't downcast {:?} array into utf8 array",
                    record.column(0).data_type()
                ),
            })?;

        assert_eq!(script_column.len(), 1);
        Ok(script_column.value(0).to_string())
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Build scripts table
fn build_scripts_schema() -> Schema {
    let cols = vec![
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
            ConcreteDataType::timestamp_millis_datatype(),
            false,
        ),
        ColumnSchema::new(
            "gmt_created".to_string(),
            ConcreteDataType::timestamp_millis_datatype(),
            false,
        ),
        ColumnSchema::new(
            "gmt_modified".to_string(),
            ConcreteDataType::timestamp_millis_datatype(),
            false,
        ),
    ];

    SchemaBuilder::from(cols)
        .timestamp_index(3)
        .build()
        .unwrap()
}
