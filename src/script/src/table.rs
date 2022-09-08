//! Scripts table
use std::collections::HashMap;
use std::sync::Arc;

use catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, SCRIPTS_TABLE_ID};
use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use common_telemetry::logging;
use common_time::util;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};
use datatypes::vectors::{Int64Vector, StringVector, VectorRef};
use snafu::{OptionExt, ResultExt};
use table::requests::{CreateTableRequest, InsertRequest};

use crate::error::{
    FindScriptsTableSnafu, InsertScriptSnafu, RegisterScriptsTableSnafu, Result,
    ScriptsTableNotFoundSnafu,
};

pub const SCRIPTS_TABLE_NAME: &str = "scripts";

pub struct ScriptsTable {
    catalog_manager: CatalogManagerRef,
    name: String,
}

impl ScriptsTable {
    pub async fn new(catalog_manager: CatalogManagerRef) -> Result<Self> {
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
            .context(InsertScriptSnafu)?;

        logging::info!("Inserted script: name={} into scripts table.", name);

        Ok(())
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
