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

//! Util functions for adapter

use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::column_def::options_from_column_schema;
use api::v1::{ColumnDataType, ColumnDataTypeExtension, CreateTableExpr, SemanticType};
use common_error::ext::BoxedError;
use common_meta::key::table_info::TableInfoValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use itertools::Itertools;
use operator::expr_helper;
use session::context::QueryContextBuilder;
use snafu::{OptionExt, ResultExt};
use table::table_reference::TableReference;

use crate::adapter::table_source::TableDesc;
use crate::adapter::{TableName, WorkerHandle, AUTO_CREATED_PLACEHOLDER_TS_COL};
use crate::error::{Error, ExternalSnafu, UnexpectedSnafu};
use crate::repr::{ColumnType, RelationDesc, RelationType};
use crate::StreamingEngine;
impl StreamingEngine {
    /// Get a worker handle for creating flow, using round robin to select a worker
    pub(crate) async fn get_worker_handle_for_create_flow(&self) -> &WorkerHandle {
        let use_idx = {
            let mut selector = self.worker_selector.lock().await;
            if *selector >= self.worker_handles.len() {
                *selector = 0
            };
            let use_idx = *selector;
            *selector += 1;
            use_idx
        };
        // Safety: selector is always in bound
        &self.worker_handles[use_idx]
    }

    /// Create table from given schema(will adjust to add auto column if needed), return true if table is created
    pub(crate) async fn create_table_from_relation(
        &self,
        flow_name: &str,
        table_name: &TableName,
        relation_desc: &RelationDesc,
    ) -> Result<bool, Error> {
        if self.fetch_table_pk_schema(table_name).await?.is_some() {
            return Ok(false);
        }
        let (pks, tys, _) = self.adjust_auto_created_table_schema(relation_desc).await?;

        //create sink table using pks, column types and is_ts_auto

        let proto_schema = column_schemas_to_proto(tys.clone(), &pks)?;

        // create sink table
        let create_expr = expr_helper::create_table_expr_by_column_schemas(
            &TableReference {
                catalog: &table_name[0],
                schema: &table_name[1],
                table: &table_name[2],
            },
            &proto_schema,
            "mito",
            Some(&format!("Sink table for flow {}", flow_name)),
        )
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

        self.submit_create_sink_table_ddl(create_expr).await?;
        Ok(true)
    }

    /// Try fetch table with adjusted schema(added auto column if needed)
    pub(crate) async fn try_fetch_existing_table(
        &self,
        table_name: &TableName,
    ) -> Result<Option<(bool, Vec<api::v1::ColumnSchema>)>, Error> {
        if let Some((primary_keys, time_index, schema)) =
            self.fetch_table_pk_schema(table_name).await?
        {
            // check if the last column is the auto created timestamp column, hence the table is auto created from
            // flow's plan type
            let is_auto_create = {
                let correct_name = schema
                    .last()
                    .map(|s| s.name == AUTO_CREATED_PLACEHOLDER_TS_COL)
                    .unwrap_or(false);
                let correct_time_index = time_index == Some(schema.len() - 1);
                correct_name && correct_time_index
            };
            let proto_schema = column_schemas_to_proto(schema, &primary_keys)?;
            Ok(Some((is_auto_create, proto_schema)))
        } else {
            Ok(None)
        }
    }

    /// submit a create table ddl
    pub(crate) async fn submit_create_sink_table_ddl(
        &self,
        mut create_table: CreateTableExpr,
    ) -> Result<(), Error> {
        let stmt_exec = {
            self.frontend_invoker
                .read()
                .await
                .as_ref()
                .map(|f| f.statement_executor())
        }
        .context(UnexpectedSnafu {
            reason: "Failed to get statement executor",
        })?;
        let ctx = Arc::new(
            QueryContextBuilder::default()
                .current_catalog(create_table.catalog_name.clone())
                .current_schema(create_table.schema_name.clone())
                .build(),
        );
        stmt_exec
            .create_table_inner(&mut create_table, None, ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        Ok(())
    }
}

pub fn table_info_value_to_relation_desc(
    table_info_value: TableInfoValue,
) -> Result<TableDesc, Error> {
    let raw_schema = table_info_value.table_info.meta.schema;
    let (column_types, col_names): (Vec<_>, Vec<_>) = raw_schema
        .column_schemas
        .clone()
        .into_iter()
        .map(|col| {
            (
                ColumnType {
                    nullable: col.is_nullable(),
                    scalar_type: col.data_type,
                },
                Some(col.name),
            )
        })
        .unzip();

    let key = table_info_value.table_info.meta.primary_key_indices;
    let keys = vec![crate::repr::Key::from(key)];

    let time_index = raw_schema.timestamp_index;
    let relation_desc = RelationDesc {
        typ: RelationType {
            column_types,
            keys,
            time_index,
            // by default table schema's column are all non-auto
            auto_columns: vec![],
        },
        names: col_names,
    };
    let default_values = raw_schema
        .column_schemas
        .iter()
        .map(|c| {
            c.default_constraint().cloned().or_else(|| {
                if c.is_nullable() {
                    Some(ColumnDefaultConstraint::null_value())
                } else {
                    None
                }
            })
        })
        .collect_vec();

    Ok(TableDesc::new(relation_desc, default_values))
}

pub fn from_proto_to_data_type(
    column_schema: &api::v1::ColumnSchema,
) -> Result<ConcreteDataType, Error> {
    let wrapper =
        ColumnDataTypeWrapper::try_new(column_schema.datatype, column_schema.datatype_extension)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
    let cdt = ConcreteDataType::from(wrapper);

    Ok(cdt)
}

/// convert `ColumnSchema` lists to it's corresponding proto type
pub fn column_schemas_to_proto(
    column_schemas: Vec<ColumnSchema>,
    primary_keys: &[String],
) -> Result<Vec<api::v1::ColumnSchema>, Error> {
    let column_datatypes: Vec<(ColumnDataType, Option<ColumnDataTypeExtension>)> = column_schemas
        .iter()
        .map(|c| {
            ColumnDataTypeWrapper::try_from(c.data_type.clone())
                .map(|w| w.to_parts())
                .map_err(BoxedError::new)
                .context(ExternalSnafu)
        })
        .try_collect()?;

    let ret = column_schemas
        .iter()
        .zip(column_datatypes)
        .map(|(schema, datatype)| {
            let semantic_type = if schema.is_time_index() {
                SemanticType::Timestamp
            } else if primary_keys.contains(&schema.name) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            } as i32;

            api::v1::ColumnSchema {
                column_name: schema.name.clone(),
                datatype: datatype.0 as i32,
                semantic_type,
                datatype_extension: datatype.1,
                options: options_from_column_schema(schema),
            }
        })
        .collect();
    Ok(ret)
}

/// Convert `RelationDesc` to `ColumnSchema` list,
/// if the column name is not present, use `col_{idx}` as the column name
pub fn relation_desc_to_column_schemas_with_fallback(schema: &RelationDesc) -> Vec<ColumnSchema> {
    schema
        .typ()
        .column_types
        .clone()
        .into_iter()
        .enumerate()
        .map(|(idx, typ)| {
            let name = schema
                .names
                .get(idx)
                .cloned()
                .flatten()
                .unwrap_or(format!("col_{}", idx));
            let ret = ColumnSchema::new(name, typ.scalar_type, typ.nullable);
            if schema.typ().time_index == Some(idx) {
                ret.with_time_index(true)
            } else {
                ret
            }
        })
        .collect_vec()
}
