//! Scripts table
use std::collections::HashMap;
use std::sync::Arc;

use catalog::consts::{INFORMATION_SCHEMA_NAME, SCRIPTS_TABLE_ID, SYSTEM_CATALOG_NAME};
use common_telemetry::logging;
use common_time::util;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder, SchemaRef};
use datatypes::vectors::{Int64Vector, StringVector, VectorRef};
use snafu::ResultExt;
use table::engine::{EngineContext, TableEngineRef};
use table::requests::{CreateTableRequest, InsertRequest, OpenTableRequest};
use table::TableRef;

use crate::error::{CreateScriptsTableSnafu, InsertScriptSnafu, OpenScriptsTableSnafu, Result};

pub const SCRIPTS_TABLE_NAME: &str = "scripts";

pub struct ScriptsTable {
    _schema: SchemaRef,
    table: TableRef,
}

impl ScriptsTable {
    pub async fn new(engine: TableEngineRef) -> Result<Self> {
        let request = OpenTableRequest {
            catalog_name: SYSTEM_CATALOG_NAME.to_string(),
            schema_name: INFORMATION_SCHEMA_NAME.to_string(),
            table_name: SCRIPTS_TABLE_NAME.to_string(),
            table_id: SCRIPTS_TABLE_ID,
        };
        let schema = Arc::new(build_scripts_schema());
        let ctx = EngineContext::default();

        if let Some(table) = engine
            .open_table(&ctx, request)
            .await
            .context(OpenScriptsTableSnafu)?
        {
            Ok(Self {
                table,
                _schema: schema,
            })
        } else {
            let request = CreateTableRequest {
                id: SCRIPTS_TABLE_ID,
                catalog_name: Some(SYSTEM_CATALOG_NAME.to_string()),
                schema_name: Some(INFORMATION_SCHEMA_NAME.to_string()),
                table_name: SCRIPTS_TABLE_NAME.to_string(),
                desc: Some("Scripts table".to_string()),
                schema: schema.clone(),
                // name and timestamp as primary key
                primary_key_indices: vec![0, 3],
                create_if_not_exists: true,
                table_options: HashMap::default(),
            };

            let table = engine
                .create_table(&ctx, request)
                .await
                .context(CreateScriptsTableSnafu)?;
            Ok(Self {
                table,
                _schema: schema,
            })
        }
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
            Arc::new(Int64Vector::from_slice(&[0])) as _,
        );
        columns_values.insert(
            "gmt_created".to_string(),
            Arc::new(Int64Vector::from_slice(&[util::current_time_millis()])) as _,
        );
        columns_values.insert(
            "gmt_modified".to_string(),
            Arc::new(Int64Vector::from_slice(&[util::current_time_millis()])) as _,
        );

        let _ = self
            .table
            .insert(InsertRequest {
                table_name: SCRIPTS_TABLE_NAME.to_string(),
                columns_values: HashMap::default(),
            })
            .await
            .context(InsertScriptSnafu)?;

        logging::info!("Inserted script: {} into scripts table.", name);

        Ok(())
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
            ConcreteDataType::int64_datatype(),
            false,
        ),
        ColumnSchema::new(
            "gmt_created".to_string(),
            ConcreteDataType::int64_datatype(),
            false,
        ),
        ColumnSchema::new(
            "gmt_modified".to_string(),
            ConcreteDataType::int64_datatype(),
            false,
        ),
    ];

    SchemaBuilder::from(cols)
        .timestamp_index(3)
        .build()
        .unwrap()
}
