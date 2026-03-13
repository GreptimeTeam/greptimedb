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

use async_trait::async_trait;
use chrono::Utc;
use common_catalog::format_full_table_name;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::tracing::info;
use datatypes::schema::{COMMENT_KEY as COLUMN_COMMENT_KEY, Schema};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::TableId;
use strum::AsRefStr;
use table::metadata::TableInfo;
use table::requests::COMMENT_KEY as TABLE_COMMENT_KEY;
use table::table_name::TableName;

use crate::cache_invalidator::Context;
use crate::ddl::DdlContext;
use crate::ddl::utils::map_to_procedure_error;
use crate::error::{ColumnNotFoundSnafu, FlowNotFoundSnafu, Result, TableNotFoundSnafu};
use crate::instruction::CacheIdent;
use crate::key::flow::flow_info::{FlowInfoKey, FlowInfoValue};
use crate::key::table_info::{TableInfoKey, TableInfoValue};
use crate::key::table_name::TableNameKey;
use crate::key::{DeserializedValueWithBytes, FlowId, MetadataKey, MetadataValue};
use crate::lock_key::{CatalogLock, FlowNameLock, SchemaLock, TableNameLock};
use crate::rpc::ddl::{CommentObjectType, CommentOnTask};
use crate::rpc::store::PutRequest;

pub struct CommentOnProcedure {
    pub context: DdlContext,
    pub data: CommentOnData,
}

impl CommentOnProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CommentOn";

    pub fn new(task: CommentOnTask, context: DdlContext) -> Self {
        Self {
            context,
            data: CommentOnData::new(task),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self { context, data })
    }

    pub async fn on_prepare(&mut self) -> Result<Status> {
        match self.data.object_type {
            CommentObjectType::Table | CommentObjectType::Column => {
                self.prepare_table_or_column().await?;
            }
            CommentObjectType::Flow => {
                self.prepare_flow().await?;
            }
        }

        // Fast path: if comment is unchanged, skip update
        if self.data.is_unchanged {
            let object_desc = match self.data.object_type {
                CommentObjectType::Table => format!(
                    "table {}",
                    format_full_table_name(
                        &self.data.catalog_name,
                        &self.data.schema_name,
                        &self.data.object_name,
                    )
                ),
                CommentObjectType::Column => format!(
                    "column {}.{}",
                    format_full_table_name(
                        &self.data.catalog_name,
                        &self.data.schema_name,
                        &self.data.object_name,
                    ),
                    self.data.column_name.as_ref().unwrap()
                ),
                CommentObjectType::Flow => {
                    format!("flow {}.{}", self.data.catalog_name, self.data.object_name)
                }
            };
            info!("Comment unchanged for {}, skipping update", object_desc);
            return Ok(Status::done());
        }

        self.data.state = CommentOnState::UpdateMetadata;
        Ok(Status::executing(true))
    }

    async fn prepare_table_or_column(&mut self) -> Result<()> {
        let table_name_key = TableNameKey::new(
            &self.data.catalog_name,
            &self.data.schema_name,
            &self.data.object_name,
        );

        let table_id = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .get(table_name_key)
            .await?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_full_table_name(
                    &self.data.catalog_name,
                    &self.data.schema_name,
                    &self.data.object_name,
                ),
            })?
            .table_id();

        let table_info = self
            .context
            .table_metadata_manager
            .table_info_manager()
            .get(table_id)
            .await?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_full_table_name(
                    &self.data.catalog_name,
                    &self.data.schema_name,
                    &self.data.object_name,
                ),
            })?;

        // For column comments, validate the column exists
        if self.data.object_type == CommentObjectType::Column {
            let column_name = self.data.column_name.as_ref().unwrap();
            let column_exists = table_info
                .table_info
                .meta
                .schema
                .column_schemas()
                .iter()
                .any(|col| &col.name == column_name);

            ensure!(
                column_exists,
                ColumnNotFoundSnafu {
                    column_name,
                    column_id: 0u32, // column_id is not known here
                }
            );
        }

        self.data.table_id = Some(table_id);

        // Check if comment is unchanged for early exit optimization
        match self.data.object_type {
            CommentObjectType::Table => {
                let current_comment = &table_info.table_info.desc;
                if &self.data.comment == current_comment {
                    self.data.is_unchanged = true;
                }
            }
            CommentObjectType::Column => {
                let column_name = self.data.column_name.as_ref().unwrap();
                let column_schema = table_info
                    .table_info
                    .meta
                    .schema
                    .column_schemas()
                    .iter()
                    .find(|col| &col.name == column_name)
                    .unwrap(); // Safe: validated above

                let current_comment = column_schema.metadata().get(COLUMN_COMMENT_KEY);
                if self.data.comment.as_deref() == current_comment.map(String::as_str) {
                    self.data.is_unchanged = true;
                }
            }
            CommentObjectType::Flow => {
                // this branch is handled in `prepare_flow`
            }
        }

        self.data.table_info = Some(table_info);

        Ok(())
    }

    async fn prepare_flow(&mut self) -> Result<()> {
        let flow_name_value = self
            .context
            .flow_metadata_manager
            .flow_name_manager()
            .get(&self.data.catalog_name, &self.data.object_name)
            .await?
            .with_context(|| FlowNotFoundSnafu {
                flow_name: &self.data.object_name,
            })?;

        let flow_id = flow_name_value.flow_id();
        let flow_info = self
            .context
            .flow_metadata_manager
            .flow_info_manager()
            .get_raw(flow_id)
            .await?
            .with_context(|| FlowNotFoundSnafu {
                flow_name: &self.data.object_name,
            })?;

        self.data.flow_id = Some(flow_id);

        // Check if comment is unchanged for early exit optimization
        let current_comment = &flow_info.get_inner_ref().comment;
        let new_comment = self.data.comment.as_deref().unwrap_or("");
        if new_comment == current_comment.as_str() {
            self.data.is_unchanged = true;
        }

        self.data.flow_info = Some(flow_info);

        Ok(())
    }

    pub async fn on_update_metadata(&mut self) -> Result<Status> {
        match self.data.object_type {
            CommentObjectType::Table => {
                self.update_table_comment().await?;
            }
            CommentObjectType::Column => {
                self.update_column_comment().await?;
            }
            CommentObjectType::Flow => {
                self.update_flow_comment().await?;
            }
        }

        self.data.state = CommentOnState::InvalidateCache;
        Ok(Status::executing(true))
    }

    async fn update_table_comment(&mut self) -> Result<()> {
        let table_info_value = self.data.table_info.as_ref().unwrap();
        let mut new_table_info = table_info_value.table_info.clone();

        new_table_info.desc = self.data.comment.clone();

        // Sync comment to table options
        sync_table_comment_option(
            &mut new_table_info.meta.options,
            new_table_info.desc.as_deref(),
        );

        self.update_table_info(table_info_value, new_table_info)
            .await?;

        info!(
            "Updated comment for table {}.{}.{}",
            self.data.catalog_name, self.data.schema_name, self.data.object_name
        );

        Ok(())
    }

    async fn update_column_comment(&mut self) -> Result<()> {
        let table_info_value = self.data.table_info.as_ref().unwrap();
        let mut new_table_info = table_info_value.table_info.clone();

        let column_name = self.data.column_name.as_ref().unwrap();
        let mut column_schemas = new_table_info.meta.schema.column_schemas().to_vec();
        let column_schema = column_schemas
            .iter_mut()
            .find(|col| &col.name == column_name)
            .unwrap(); // Safe: validated in prepare

        update_column_comment_metadata(column_schema, self.data.comment.clone());

        new_table_info.meta.schema = Arc::new(Schema::new_with_version(
            column_schemas,
            new_table_info.meta.schema.version(),
        ));
        self.update_table_info(table_info_value, new_table_info)
            .await?;

        info!(
            "Updated comment for column {}.{}.{}.{}",
            self.data.catalog_name, self.data.schema_name, self.data.object_name, column_name
        );

        Ok(())
    }

    async fn update_flow_comment(&mut self) -> Result<()> {
        let flow_id = self.data.flow_id.unwrap();
        let flow_info_value = self.data.flow_info.as_ref().unwrap();

        let mut new_flow_info = flow_info_value.get_inner_ref().clone();
        new_flow_info.comment = self.data.comment.clone().unwrap_or_default();
        new_flow_info.updated_time = Utc::now();

        let raw_value = new_flow_info.try_as_raw_value()?;

        self.context
            .table_metadata_manager
            .kv_backend()
            .put(
                PutRequest::new()
                    .with_key(FlowInfoKey::new(flow_id).to_bytes())
                    .with_value(raw_value),
            )
            .await?;

        info!(
            "Updated comment for flow {}.{}",
            self.data.catalog_name, self.data.object_name
        );

        Ok(())
    }

    async fn update_table_info(
        &self,
        current_table_info: &DeserializedValueWithBytes<TableInfoValue>,
        new_table_info: TableInfo,
    ) -> Result<()> {
        let table_id = current_table_info.table_info.ident.table_id;
        let new_table_info_value = current_table_info.update(new_table_info);
        let raw_value = new_table_info_value.try_as_raw_value()?;

        self.context
            .table_metadata_manager
            .kv_backend()
            .put(
                PutRequest::new()
                    .with_key(TableInfoKey::new(table_id).to_bytes())
                    .with_value(raw_value),
            )
            .await?;

        Ok(())
    }

    pub async fn on_invalidate_cache(&mut self) -> Result<Status> {
        let cache_invalidator = &self.context.cache_invalidator;

        match self.data.object_type {
            CommentObjectType::Table | CommentObjectType::Column => {
                let table_id = self.data.table_id.unwrap();
                let table_name = TableName::new(
                    self.data.catalog_name.clone(),
                    self.data.schema_name.clone(),
                    self.data.object_name.clone(),
                );

                let cache_ident = vec![
                    CacheIdent::TableId(table_id),
                    CacheIdent::TableName(table_name),
                ];

                cache_invalidator
                    .invalidate(&Context::default(), &cache_ident)
                    .await?;
            }
            CommentObjectType::Flow => {
                let flow_id = self.data.flow_id.unwrap();
                let cache_ident = vec![CacheIdent::FlowId(flow_id)];

                cache_invalidator
                    .invalidate(&Context::default(), &cache_ident)
                    .await?;
            }
        }

        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for CommentOnProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            CommentOnState::Prepare => self.on_prepare().await,
            CommentOnState::UpdateMetadata => self.on_update_metadata().await,
            CommentOnState::InvalidateCache => self.on_invalidate_cache().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog = &self.data.catalog_name;
        let schema = &self.data.schema_name;

        let lock_key = match self.data.object_type {
            CommentObjectType::Table | CommentObjectType::Column => {
                vec![
                    CatalogLock::Read(catalog).into(),
                    SchemaLock::read(catalog, schema).into(),
                    TableNameLock::new(catalog, schema, &self.data.object_name).into(),
                ]
            }
            CommentObjectType::Flow => {
                vec![
                    CatalogLock::Read(catalog).into(),
                    FlowNameLock::new(catalog, &self.data.object_name).into(),
                ]
            }
        };

        LockKey::new(lock_key)
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum CommentOnState {
    Prepare,
    UpdateMetadata,
    InvalidateCache,
}

/// The data of comment on procedure.
#[derive(Debug, Serialize, Deserialize)]
pub struct CommentOnData {
    state: CommentOnState,
    catalog_name: String,
    schema_name: String,
    object_type: CommentObjectType,
    object_name: String,
    /// Column name (only for Column comments)
    column_name: Option<String>,
    comment: Option<String>,
    /// Cached table ID (for Table/Column)
    #[serde(skip_serializing_if = "Option::is_none")]
    table_id: Option<TableId>,
    /// Cached table info (for Table/Column)
    #[serde(skip)]
    table_info: Option<DeserializedValueWithBytes<TableInfoValue>>,
    /// Cached flow ID (for Flow)
    #[serde(skip_serializing_if = "Option::is_none")]
    flow_id: Option<FlowId>,
    /// Cached flow info (for Flow)
    #[serde(skip)]
    flow_info: Option<DeserializedValueWithBytes<FlowInfoValue>>,
    /// Whether the comment is unchanged (optimization for early exit)
    #[serde(skip)]
    is_unchanged: bool,
}

impl CommentOnData {
    pub fn new(task: CommentOnTask) -> Self {
        Self {
            state: CommentOnState::Prepare,
            catalog_name: task.catalog_name,
            schema_name: task.schema_name,
            object_type: task.object_type,
            object_name: task.object_name,
            column_name: task.column_name,
            comment: task.comment,
            table_id: None,
            table_info: None,
            flow_id: None,
            flow_info: None,
            is_unchanged: false,
        }
    }
}

fn update_column_comment_metadata(
    column_schema: &mut datatypes::schema::ColumnSchema,
    comment: Option<String>,
) {
    match comment {
        Some(value) => {
            column_schema
                .mut_metadata()
                .insert(COLUMN_COMMENT_KEY.to_string(), value);
        }
        None => {
            column_schema.mut_metadata().remove(COLUMN_COMMENT_KEY);
        }
    }
}

fn sync_table_comment_option(options: &mut table::requests::TableOptions, comment: Option<&str>) {
    match comment {
        Some(value) => {
            options
                .extra_options
                .insert(TABLE_COMMENT_KEY.to_string(), value.to_string());
        }
        None => {
            options.extra_options.remove(TABLE_COMMENT_KEY);
        }
    }
}
