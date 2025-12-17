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

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::tracing::info;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use strum::AsRefStr;

use crate::cache_invalidator::Context;
use crate::ddl::DdlContext;
use crate::ddl::utils::map_to_procedure_error;
use crate::error::{Result, SchemaNotFoundSnafu};
use crate::instruction::CacheIdent;
use crate::key::DeserializedValueWithBytes;
use crate::key::schema_name::{SchemaName, SchemaNameKey, SchemaNameValue};
use crate::lock_key::{CatalogLock, SchemaLock};
use crate::rpc::ddl::UnsetDatabaseOption::{self};
use crate::rpc::ddl::{AlterDatabaseKind, AlterDatabaseTask, SetDatabaseOption};

pub struct AlterDatabaseProcedure {
    pub context: DdlContext,
    pub data: AlterDatabaseData,
}

fn build_new_schema_value(
    mut value: SchemaNameValue,
    alter_kind: &AlterDatabaseKind,
) -> Result<SchemaNameValue> {
    match alter_kind {
        AlterDatabaseKind::SetDatabaseOptions(options) => {
            for option in options.0.iter() {
                match option {
                    SetDatabaseOption::Ttl(ttl) => {
                        value.ttl = Some(*ttl);
                    }
                    SetDatabaseOption::Other(key, val) => {
                        value.extra_options.insert(key.clone(), val.clone());
                    }
                }
            }
        }
        AlterDatabaseKind::UnsetDatabaseOptions(keys) => {
            for key in keys.0.iter() {
                match key {
                    UnsetDatabaseOption::Ttl => value.ttl = None,
                    UnsetDatabaseOption::Other(key) => {
                        value.extra_options.remove(key);
                    }
                }
            }
        }
    }
    Ok(value)
}

impl AlterDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::AlterDatabase";

    pub fn new(task: AlterDatabaseTask, context: DdlContext) -> Result<Self> {
        Ok(Self {
            context,
            data: AlterDatabaseData::new(task)?,
        })
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self { context, data })
    }

    pub async fn on_prepare(&mut self) -> Result<Status> {
        let value = self
            .context
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey::new(self.data.catalog(), self.data.schema()))
            .await?;

        ensure!(
            value.is_some(),
            SchemaNotFoundSnafu {
                table_schema: self.data.schema(),
            }
        );

        self.data.schema_value = value;
        self.data.state = AlterDatabaseState::UpdateMetadata;

        Ok(Status::executing(true))
    }

    pub async fn on_update_metadata(&mut self) -> Result<Status> {
        let schema_name = SchemaNameKey::new(self.data.catalog(), self.data.schema());

        // Safety: schema_value is not None.
        let current_schema_value = self.data.schema_value.as_ref().unwrap();

        let new_schema_value = build_new_schema_value(
            current_schema_value.get_inner_ref().clone(),
            &self.data.kind,
        )?;

        self.context
            .table_metadata_manager
            .schema_manager()
            .update(schema_name, current_schema_value, &new_schema_value)
            .await?;

        info!("Updated database metadata for schema {schema_name}");
        self.data.state = AlterDatabaseState::InvalidateSchemaCache;
        Ok(Status::executing(true))
    }

    pub async fn on_invalidate_schema_cache(&mut self) -> Result<Status> {
        let cache_invalidator = &self.context.cache_invalidator;
        cache_invalidator
            .invalidate(
                &Context::default(),
                &[CacheIdent::SchemaName(SchemaName {
                    catalog_name: self.data.catalog().to_string(),
                    schema_name: self.data.schema().to_string(),
                })],
            )
            .await?;

        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for AlterDatabaseProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            AlterDatabaseState::Prepare => self.on_prepare().await,
            AlterDatabaseState::UpdateMetadata => self.on_update_metadata().await,
            AlterDatabaseState::InvalidateSchemaCache => self.on_invalidate_schema_cache().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog = self.data.catalog();
        let schema = self.data.schema();

        let lock_key = vec![
            CatalogLock::Read(catalog).into(),
            SchemaLock::write(catalog, schema).into(),
        ];

        LockKey::new(lock_key)
    }
}

#[derive(Debug, Serialize, Deserialize, AsRefStr)]
enum AlterDatabaseState {
    Prepare,
    UpdateMetadata,
    InvalidateSchemaCache,
}

/// The data of alter database procedure.
#[derive(Debug, Serialize, Deserialize)]
pub struct AlterDatabaseData {
    state: AlterDatabaseState,
    kind: AlterDatabaseKind,
    catalog_name: String,
    schema_name: String,
    schema_value: Option<DeserializedValueWithBytes<SchemaNameValue>>,
}

impl AlterDatabaseData {
    pub fn new(task: AlterDatabaseTask) -> Result<Self> {
        Ok(Self {
            state: AlterDatabaseState::Prepare,
            kind: AlterDatabaseKind::try_from(task.alter_expr.kind.unwrap())?,
            catalog_name: task.alter_expr.catalog_name,
            schema_name: task.alter_expr.schema_name,
            schema_value: None,
        })
    }

    pub fn catalog(&self) -> &str {
        &self.catalog_name
    }

    pub fn schema(&self) -> &str {
        &self.schema_name
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::ddl::alter_database::build_new_schema_value;
    use crate::key::schema_name::SchemaNameValue;
    use crate::rpc::ddl::{
        AlterDatabaseKind, SetDatabaseOption, SetDatabaseOptions, UnsetDatabaseOption,
        UnsetDatabaseOptions,
    };

    #[test]
    fn test_build_new_schema_value() {
        let set_ttl = AlterDatabaseKind::SetDatabaseOptions(SetDatabaseOptions(vec![
            SetDatabaseOption::Ttl(Duration::from_secs(10).into()),
        ]));
        let current_schema_value = SchemaNameValue::default();
        let new_schema_value =
            build_new_schema_value(current_schema_value.clone(), &set_ttl).unwrap();
        assert_eq!(new_schema_value.ttl, Some(Duration::from_secs(10).into()));

        let unset_ttl_alter_kind =
            AlterDatabaseKind::UnsetDatabaseOptions(UnsetDatabaseOptions(vec![
                UnsetDatabaseOption::Ttl,
            ]));
        let new_schema_value =
            build_new_schema_value(current_schema_value, &unset_ttl_alter_kind).unwrap();
        assert_eq!(new_schema_value.ttl, None);
    }

    #[test]
    fn test_build_new_schema_value_with_compaction_options() {
        let set_compaction = AlterDatabaseKind::SetDatabaseOptions(SetDatabaseOptions(vec![
            SetDatabaseOption::Other("compaction.type".to_string(), "twcs".to_string()),
            SetDatabaseOption::Other("compaction.twcs.time_window".to_string(), "1d".to_string()),
        ]));

        let current_schema_value = SchemaNameValue::default();
        let new_schema_value =
            build_new_schema_value(current_schema_value.clone(), &set_compaction).unwrap();

        assert_eq!(
            new_schema_value.extra_options.get("compaction.type"),
            Some(&"twcs".to_string())
        );
        assert_eq!(
            new_schema_value
                .extra_options
                .get("compaction.twcs.time_window"),
            Some(&"1d".to_string())
        );

        let unset_compaction = AlterDatabaseKind::UnsetDatabaseOptions(UnsetDatabaseOptions(vec![
            UnsetDatabaseOption::Other("compaction.type".to_string()),
        ]));

        let new_schema_value = build_new_schema_value(new_schema_value, &unset_compaction).unwrap();

        assert_eq!(new_schema_value.extra_options.get("compaction.type"), None);
        assert_eq!(
            new_schema_value
                .extra_options
                .get("compaction.twcs.time_window"),
            Some(&"1d".to_string())
        );
    }
}
