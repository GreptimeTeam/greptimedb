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

#[cfg(feature = "enterprise")]
pub mod trigger;

use std::collections::{HashMap, HashSet};
use std::result;

use api::helper::{from_pb_time_ranges, to_pb_time_ranges};
use api::v1::alter_database_expr::Kind as PbAlterDatabaseKind;
use api::v1::meta::ddl_task_request::Task;
use api::v1::meta::{
    AlterDatabaseTask as PbAlterDatabaseTask, AlterTableTask as PbAlterTableTask,
    AlterTableTasks as PbAlterTableTasks, CreateDatabaseTask as PbCreateDatabaseTask,
    CreateFlowTask as PbCreateFlowTask, CreateTableTask as PbCreateTableTask,
    CreateTableTasks as PbCreateTableTasks, CreateViewTask as PbCreateViewTask,
    DdlTaskRequest as PbDdlTaskRequest, DdlTaskResponse as PbDdlTaskResponse,
    DropDatabaseTask as PbDropDatabaseTask, DropFlowTask as PbDropFlowTask,
    DropTableTask as PbDropTableTask, DropTableTasks as PbDropTableTasks,
    DropViewTask as PbDropViewTask, Partition, ProcedureId,
    TruncateTableTask as PbTruncateTableTask,
};
use api::v1::{
    AlterDatabaseExpr, AlterTableExpr, CreateDatabaseExpr, CreateFlowExpr, CreateTableExpr,
    CreateViewExpr, DropDatabaseExpr, DropFlowExpr, DropTableExpr, DropViewExpr, ExpireAfter,
    Option as PbOption, QueryContext as PbQueryContext, TruncateTableExpr,
};
use base64::engine::general_purpose;
use base64::Engine as _;
use common_error::ext::BoxedError;
use common_time::{DatabaseTimeToLive, Timestamp, Timezone};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DefaultOnNull};
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use table::metadata::{RawTableInfo, TableId};
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::error::{
    self, ConvertTimeRangesSnafu, ExternalSnafu, InvalidSetDatabaseOptionSnafu,
    InvalidTimeZoneSnafu, InvalidUnsetDatabaseOptionSnafu, Result,
};
use crate::key::FlowId;

/// DDL tasks
#[derive(Debug, Clone)]
pub enum DdlTask {
    CreateTable(CreateTableTask),
    DropTable(DropTableTask),
    AlterTable(AlterTableTask),
    TruncateTable(TruncateTableTask),
    CreateLogicalTables(Vec<CreateTableTask>),
    DropLogicalTables(Vec<DropTableTask>),
    AlterLogicalTables(Vec<AlterTableTask>),
    CreateDatabase(CreateDatabaseTask),
    DropDatabase(DropDatabaseTask),
    AlterDatabase(AlterDatabaseTask),
    CreateFlow(CreateFlowTask),
    DropFlow(DropFlowTask),
    #[cfg(feature = "enterprise")]
    DropTrigger(trigger::DropTriggerTask),
    CreateView(CreateViewTask),
    DropView(DropViewTask),
    #[cfg(feature = "enterprise")]
    CreateTrigger(trigger::CreateTriggerTask),
}

impl DdlTask {
    /// Creates a [`DdlTask`] to create a flow.
    pub fn new_create_flow(expr: CreateFlowTask) -> Self {
        DdlTask::CreateFlow(expr)
    }

    /// Creates a [`DdlTask`] to drop a flow.
    pub fn new_drop_flow(expr: DropFlowTask) -> Self {
        DdlTask::DropFlow(expr)
    }

    /// Creates a [`DdlTask`] to drop a view.
    pub fn new_drop_view(expr: DropViewTask) -> Self {
        DdlTask::DropView(expr)
    }

    /// Creates a [`DdlTask`] to create a table.
    pub fn new_create_table(
        expr: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> Self {
        DdlTask::CreateTable(CreateTableTask::new(expr, partitions, table_info))
    }

    /// Creates a [`DdlTask`] to create several logical tables.
    pub fn new_create_logical_tables(table_data: Vec<(CreateTableExpr, RawTableInfo)>) -> Self {
        DdlTask::CreateLogicalTables(
            table_data
                .into_iter()
                .map(|(expr, table_info)| CreateTableTask::new(expr, Vec::new(), table_info))
                .collect(),
        )
    }

    /// Creates a [`DdlTask`] to alter several logical tables.
    pub fn new_alter_logical_tables(table_data: Vec<AlterTableExpr>) -> Self {
        DdlTask::AlterLogicalTables(
            table_data
                .into_iter()
                .map(|alter_table| AlterTableTask { alter_table })
                .collect(),
        )
    }

    /// Creates a [`DdlTask`] to drop a table.
    pub fn new_drop_table(
        catalog: String,
        schema: String,
        table: String,
        table_id: TableId,
        drop_if_exists: bool,
    ) -> Self {
        DdlTask::DropTable(DropTableTask {
            catalog,
            schema,
            table,
            table_id,
            drop_if_exists,
        })
    }

    /// Creates a [`DdlTask`] to create a database.
    pub fn new_create_database(
        catalog: String,
        schema: String,
        create_if_not_exists: bool,
        options: HashMap<String, String>,
    ) -> Self {
        DdlTask::CreateDatabase(CreateDatabaseTask {
            catalog,
            schema,
            create_if_not_exists,
            options,
        })
    }

    /// Creates a [`DdlTask`] to drop a database.
    pub fn new_drop_database(catalog: String, schema: String, drop_if_exists: bool) -> Self {
        DdlTask::DropDatabase(DropDatabaseTask {
            catalog,
            schema,
            drop_if_exists,
        })
    }

    /// Creates a [`DdlTask`] to alter a database.
    pub fn new_alter_database(alter_expr: AlterDatabaseExpr) -> Self {
        DdlTask::AlterDatabase(AlterDatabaseTask { alter_expr })
    }

    /// Creates a [`DdlTask`] to alter a table.
    pub fn new_alter_table(alter_table: AlterTableExpr) -> Self {
        DdlTask::AlterTable(AlterTableTask { alter_table })
    }

    /// Creates a [`DdlTask`] to truncate a table.
    pub fn new_truncate_table(
        catalog: String,
        schema: String,
        table: String,
        table_id: TableId,
        time_ranges: Vec<(Timestamp, Timestamp)>,
    ) -> Self {
        DdlTask::TruncateTable(TruncateTableTask {
            catalog,
            schema,
            table,
            table_id,
            time_ranges,
        })
    }

    /// Creates a [`DdlTask`] to create a view.
    pub fn new_create_view(create_view: CreateViewExpr, view_info: RawTableInfo) -> Self {
        DdlTask::CreateView(CreateViewTask {
            create_view,
            view_info,
        })
    }
}

impl TryFrom<Task> for DdlTask {
    type Error = error::Error;
    fn try_from(task: Task) -> Result<Self> {
        match task {
            Task::CreateTableTask(create_table) => {
                Ok(DdlTask::CreateTable(create_table.try_into()?))
            }
            Task::DropTableTask(drop_table) => Ok(DdlTask::DropTable(drop_table.try_into()?)),
            Task::AlterTableTask(alter_table) => Ok(DdlTask::AlterTable(alter_table.try_into()?)),
            Task::TruncateTableTask(truncate_table) => {
                Ok(DdlTask::TruncateTable(truncate_table.try_into()?))
            }
            Task::CreateTableTasks(create_tables) => {
                let tasks = create_tables
                    .tasks
                    .into_iter()
                    .map(|task| task.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Ok(DdlTask::CreateLogicalTables(tasks))
            }
            Task::DropTableTasks(drop_tables) => {
                let tasks = drop_tables
                    .tasks
                    .into_iter()
                    .map(|task| task.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Ok(DdlTask::DropLogicalTables(tasks))
            }
            Task::AlterTableTasks(alter_tables) => {
                let tasks = alter_tables
                    .tasks
                    .into_iter()
                    .map(|task| task.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Ok(DdlTask::AlterLogicalTables(tasks))
            }
            Task::CreateDatabaseTask(create_database) => {
                Ok(DdlTask::CreateDatabase(create_database.try_into()?))
            }
            Task::DropDatabaseTask(drop_database) => {
                Ok(DdlTask::DropDatabase(drop_database.try_into()?))
            }
            Task::AlterDatabaseTask(alter_database) => {
                Ok(DdlTask::AlterDatabase(alter_database.try_into()?))
            }
            Task::CreateFlowTask(create_flow) => Ok(DdlTask::CreateFlow(create_flow.try_into()?)),
            Task::DropFlowTask(drop_flow) => Ok(DdlTask::DropFlow(drop_flow.try_into()?)),
            Task::CreateViewTask(create_view) => Ok(DdlTask::CreateView(create_view.try_into()?)),
            Task::DropViewTask(drop_view) => Ok(DdlTask::DropView(drop_view.try_into()?)),
            Task::CreateTriggerTask(create_trigger) => {
                #[cfg(feature = "enterprise")]
                return Ok(DdlTask::CreateTrigger(create_trigger.try_into()?));
                #[cfg(not(feature = "enterprise"))]
                {
                    let _ = create_trigger;
                    crate::error::UnsupportedSnafu {
                        operation: "create trigger",
                    }
                    .fail()
                }
            }
            Task::DropTriggerTask(drop_trigger) => {
                #[cfg(feature = "enterprise")]
                return Ok(DdlTask::DropTrigger(drop_trigger.try_into()?));
                #[cfg(not(feature = "enterprise"))]
                {
                    let _ = drop_trigger;
                    crate::error::UnsupportedSnafu {
                        operation: "drop trigger",
                    }
                    .fail()
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SubmitDdlTaskRequest {
    pub query_context: QueryContextRef,
    pub task: DdlTask,
}

impl TryFrom<SubmitDdlTaskRequest> for PbDdlTaskRequest {
    type Error = error::Error;

    fn try_from(request: SubmitDdlTaskRequest) -> Result<Self> {
        let task = match request.task {
            DdlTask::CreateTable(task) => Task::CreateTableTask(task.try_into()?),
            DdlTask::DropTable(task) => Task::DropTableTask(task.into()),
            DdlTask::AlterTable(task) => Task::AlterTableTask(task.try_into()?),
            DdlTask::TruncateTable(task) => Task::TruncateTableTask(task.try_into()?),
            DdlTask::CreateLogicalTables(tasks) => {
                let tasks = tasks
                    .into_iter()
                    .map(|task| task.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Task::CreateTableTasks(PbCreateTableTasks { tasks })
            }
            DdlTask::DropLogicalTables(tasks) => {
                let tasks = tasks
                    .into_iter()
                    .map(|task| task.into())
                    .collect::<Vec<_>>();

                Task::DropTableTasks(PbDropTableTasks { tasks })
            }
            DdlTask::AlterLogicalTables(tasks) => {
                let tasks = tasks
                    .into_iter()
                    .map(|task| task.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Task::AlterTableTasks(PbAlterTableTasks { tasks })
            }
            DdlTask::CreateDatabase(task) => Task::CreateDatabaseTask(task.try_into()?),
            DdlTask::DropDatabase(task) => Task::DropDatabaseTask(task.try_into()?),
            DdlTask::AlterDatabase(task) => Task::AlterDatabaseTask(task.try_into()?),
            DdlTask::CreateFlow(task) => Task::CreateFlowTask(task.into()),
            DdlTask::DropFlow(task) => Task::DropFlowTask(task.into()),
            DdlTask::CreateView(task) => Task::CreateViewTask(task.try_into()?),
            DdlTask::DropView(task) => Task::DropViewTask(task.into()),
            #[cfg(feature = "enterprise")]
            DdlTask::CreateTrigger(task) => Task::CreateTriggerTask(task.try_into()?),
            #[cfg(feature = "enterprise")]
            DdlTask::DropTrigger(task) => Task::DropTriggerTask(task.into()),
        };

        Ok(Self {
            header: None,
            query_context: Some((*request.query_context).clone().into()),
            task: Some(task),
        })
    }
}

#[derive(Debug, Default)]
pub struct SubmitDdlTaskResponse {
    pub key: Vec<u8>,
    // `table_id`s for `CREATE TABLE` or `CREATE LOGICAL TABLES` task.
    pub table_ids: Vec<TableId>,
}

impl TryFrom<PbDdlTaskResponse> for SubmitDdlTaskResponse {
    type Error = error::Error;

    fn try_from(resp: PbDdlTaskResponse) -> Result<Self> {
        let table_ids = resp.table_ids.into_iter().map(|t| t.id).collect();
        Ok(Self {
            key: resp.pid.map(|pid| pid.key).unwrap_or_default(),
            table_ids,
        })
    }
}

impl From<SubmitDdlTaskResponse> for PbDdlTaskResponse {
    fn from(val: SubmitDdlTaskResponse) -> Self {
        Self {
            pid: Some(ProcedureId { key: val.key }),
            table_ids: val
                .table_ids
                .into_iter()
                .map(|id| api::v1::TableId { id })
                .collect(),
            ..Default::default()
        }
    }
}

/// A `CREATE VIEW` task.
#[derive(Debug, PartialEq, Clone)]
pub struct CreateViewTask {
    pub create_view: CreateViewExpr,
    pub view_info: RawTableInfo,
}

impl CreateViewTask {
    /// Returns the [`TableReference`] of view.
    pub fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.create_view.catalog_name,
            schema: &self.create_view.schema_name,
            table: &self.create_view.view_name,
        }
    }

    /// Returns the encoded logical plan
    pub fn raw_logical_plan(&self) -> &Vec<u8> {
        &self.create_view.logical_plan
    }

    /// Returns the view definition in SQL
    pub fn view_definition(&self) -> &str {
        &self.create_view.definition
    }

    /// Returns the resolved table names in view's logical plan
    pub fn table_names(&self) -> HashSet<TableName> {
        self.create_view
            .table_names
            .iter()
            .map(|t| t.clone().into())
            .collect()
    }

    /// Returns the view's columns
    pub fn columns(&self) -> &Vec<String> {
        &self.create_view.columns
    }

    /// Returns the original logical plan's columns
    pub fn plan_columns(&self) -> &Vec<String> {
        &self.create_view.plan_columns
    }
}

impl TryFrom<PbCreateViewTask> for CreateViewTask {
    type Error = error::Error;

    fn try_from(pb: PbCreateViewTask) -> Result<Self> {
        let view_info = serde_json::from_slice(&pb.view_info).context(error::SerdeJsonSnafu)?;

        Ok(CreateViewTask {
            create_view: pb.create_view.context(error::InvalidProtoMsgSnafu {
                err_msg: "expected create view",
            })?,
            view_info,
        })
    }
}

impl TryFrom<CreateViewTask> for PbCreateViewTask {
    type Error = error::Error;

    fn try_from(task: CreateViewTask) -> Result<PbCreateViewTask> {
        Ok(PbCreateViewTask {
            create_view: Some(task.create_view),
            view_info: serde_json::to_vec(&task.view_info).context(error::SerdeJsonSnafu)?,
        })
    }
}

impl Serialize for CreateViewTask {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let view_info = serde_json::to_vec(&self.view_info)
            .map_err(|err| serde::ser::Error::custom(err.to_string()))?;

        let pb = PbCreateViewTask {
            create_view: Some(self.create_view.clone()),
            view_info,
        };
        let buf = pb.encode_to_vec();
        let encoded = general_purpose::STANDARD_NO_PAD.encode(buf);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for CreateViewTask {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let buf = general_purpose::STANDARD_NO_PAD
            .decode(encoded)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;
        let expr: PbCreateViewTask = PbCreateViewTask::decode(&*buf)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        let expr = CreateViewTask::try_from(expr)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        Ok(expr)
    }
}

/// A `DROP VIEW` task.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct DropViewTask {
    pub catalog: String,
    pub schema: String,
    pub view: String,
    pub view_id: TableId,
    pub drop_if_exists: bool,
}

impl DropViewTask {
    /// Returns the [`TableReference`] of view.
    pub fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.catalog,
            schema: &self.schema,
            table: &self.view,
        }
    }
}

impl TryFrom<PbDropViewTask> for DropViewTask {
    type Error = error::Error;

    fn try_from(pb: PbDropViewTask) -> Result<Self> {
        let expr = pb.drop_view.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected drop view",
        })?;

        Ok(DropViewTask {
            catalog: expr.catalog_name,
            schema: expr.schema_name,
            view: expr.view_name,
            view_id: expr
                .view_id
                .context(error::InvalidProtoMsgSnafu {
                    err_msg: "expected view_id",
                })?
                .id,
            drop_if_exists: expr.drop_if_exists,
        })
    }
}

impl From<DropViewTask> for PbDropViewTask {
    fn from(task: DropViewTask) -> Self {
        PbDropViewTask {
            drop_view: Some(DropViewExpr {
                catalog_name: task.catalog,
                schema_name: task.schema,
                view_name: task.view,
                view_id: Some(api::v1::TableId { id: task.view_id }),
                drop_if_exists: task.drop_if_exists,
            }),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DropTableTask {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub table_id: TableId,
    #[serde(default)]
    pub drop_if_exists: bool,
}

impl DropTableTask {
    pub fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.catalog,
            schema: &self.schema,
            table: &self.table,
        }
    }

    pub fn table_name(&self) -> TableName {
        TableName {
            catalog_name: self.catalog.to_string(),
            schema_name: self.schema.to_string(),
            table_name: self.table.to_string(),
        }
    }
}

impl TryFrom<PbDropTableTask> for DropTableTask {
    type Error = error::Error;

    fn try_from(pb: PbDropTableTask) -> Result<Self> {
        let drop_table = pb.drop_table.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected drop table",
        })?;

        Ok(Self {
            catalog: drop_table.catalog_name,
            schema: drop_table.schema_name,
            table: drop_table.table_name,
            table_id: drop_table
                .table_id
                .context(error::InvalidProtoMsgSnafu {
                    err_msg: "expected table_id",
                })?
                .id,
            drop_if_exists: drop_table.drop_if_exists,
        })
    }
}

impl From<DropTableTask> for PbDropTableTask {
    fn from(task: DropTableTask) -> Self {
        PbDropTableTask {
            drop_table: Some(DropTableExpr {
                catalog_name: task.catalog,
                schema_name: task.schema,
                table_name: task.table,
                table_id: Some(api::v1::TableId { id: task.table_id }),
                drop_if_exists: task.drop_if_exists,
            }),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct CreateTableTask {
    pub create_table: CreateTableExpr,
    pub partitions: Vec<Partition>,
    pub table_info: RawTableInfo,
}

impl TryFrom<PbCreateTableTask> for CreateTableTask {
    type Error = error::Error;

    fn try_from(pb: PbCreateTableTask) -> Result<Self> {
        let table_info = serde_json::from_slice(&pb.table_info).context(error::SerdeJsonSnafu)?;

        Ok(CreateTableTask::new(
            pb.create_table.context(error::InvalidProtoMsgSnafu {
                err_msg: "expected create table",
            })?,
            pb.partitions,
            table_info,
        ))
    }
}

impl TryFrom<CreateTableTask> for PbCreateTableTask {
    type Error = error::Error;

    fn try_from(task: CreateTableTask) -> Result<Self> {
        Ok(PbCreateTableTask {
            table_info: serde_json::to_vec(&task.table_info).context(error::SerdeJsonSnafu)?,
            create_table: Some(task.create_table),
            partitions: task.partitions,
        })
    }
}

impl CreateTableTask {
    pub fn new(
        expr: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> CreateTableTask {
        CreateTableTask {
            create_table: expr,
            partitions,
            table_info,
        }
    }

    pub fn table_name(&self) -> TableName {
        let table = &self.create_table;

        TableName {
            catalog_name: table.catalog_name.to_string(),
            schema_name: table.schema_name.to_string(),
            table_name: table.table_name.to_string(),
        }
    }

    pub fn table_ref(&self) -> TableReference {
        let table = &self.create_table;

        TableReference {
            catalog: &table.catalog_name,
            schema: &table.schema_name,
            table: &table.table_name,
        }
    }

    /// Sets the `table_info`'s table_id.
    pub fn set_table_id(&mut self, table_id: TableId) {
        self.table_info.ident.table_id = table_id;
    }

    /// Sort the columns in [CreateTableExpr] and [RawTableInfo].
    ///
    /// This function won't do any check or verification. Caller should
    /// ensure this task is valid.
    pub fn sort_columns(&mut self) {
        // sort create table expr
        // sort column_defs by name
        self.create_table
            .column_defs
            .sort_unstable_by(|a, b| a.name.cmp(&b.name));

        self.table_info.sort_columns();
    }
}

impl Serialize for CreateTableTask {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let table_info = serde_json::to_vec(&self.table_info)
            .map_err(|err| serde::ser::Error::custom(err.to_string()))?;

        let pb = PbCreateTableTask {
            create_table: Some(self.create_table.clone()),
            partitions: self.partitions.clone(),
            table_info,
        };
        let buf = pb.encode_to_vec();
        let encoded = general_purpose::STANDARD_NO_PAD.encode(buf);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for CreateTableTask {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let buf = general_purpose::STANDARD_NO_PAD
            .decode(encoded)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;
        let expr: PbCreateTableTask = PbCreateTableTask::decode(&*buf)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        let expr = CreateTableTask::try_from(expr)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        Ok(expr)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AlterTableTask {
    // TODO(CookiePieWw): Replace proto struct with user-defined struct
    pub alter_table: AlterTableExpr,
}

impl AlterTableTask {
    pub fn validate(&self) -> Result<()> {
        self.alter_table
            .kind
            .as_ref()
            .context(error::UnexpectedSnafu {
                err_msg: "'kind' is absent",
            })?;
        Ok(())
    }

    pub fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.alter_table.catalog_name,
            schema: &self.alter_table.schema_name,
            table: &self.alter_table.table_name,
        }
    }

    pub fn table_name(&self) -> TableName {
        let table = &self.alter_table;

        TableName {
            catalog_name: table.catalog_name.to_string(),
            schema_name: table.schema_name.to_string(),
            table_name: table.table_name.to_string(),
        }
    }
}

impl TryFrom<PbAlterTableTask> for AlterTableTask {
    type Error = error::Error;

    fn try_from(pb: PbAlterTableTask) -> Result<Self> {
        let alter_table = pb.alter_table.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected alter_table",
        })?;

        Ok(AlterTableTask { alter_table })
    }
}

impl TryFrom<AlterTableTask> for PbAlterTableTask {
    type Error = error::Error;

    fn try_from(task: AlterTableTask) -> Result<Self> {
        Ok(PbAlterTableTask {
            alter_table: Some(task.alter_table),
        })
    }
}

impl Serialize for AlterTableTask {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let pb = PbAlterTableTask {
            alter_table: Some(self.alter_table.clone()),
        };
        let buf = pb.encode_to_vec();
        let encoded = general_purpose::STANDARD_NO_PAD.encode(buf);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for AlterTableTask {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        let buf = general_purpose::STANDARD_NO_PAD
            .decode(encoded)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;
        let expr: PbAlterTableTask = PbAlterTableTask::decode(&*buf)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        let expr = AlterTableTask::try_from(expr)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        Ok(expr)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TruncateTableTask {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub table_id: TableId,
    pub time_ranges: Vec<(Timestamp, Timestamp)>,
}

impl TruncateTableTask {
    pub fn table_ref(&self) -> TableReference {
        TableReference {
            catalog: &self.catalog,
            schema: &self.schema,
            table: &self.table,
        }
    }

    pub fn table_name(&self) -> TableName {
        TableName {
            catalog_name: self.catalog.to_string(),
            schema_name: self.schema.to_string(),
            table_name: self.table.to_string(),
        }
    }
}

impl TryFrom<PbTruncateTableTask> for TruncateTableTask {
    type Error = error::Error;

    fn try_from(pb: PbTruncateTableTask) -> Result<Self> {
        let truncate_table = pb.truncate_table.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected truncate table",
        })?;

        Ok(Self {
            catalog: truncate_table.catalog_name,
            schema: truncate_table.schema_name,
            table: truncate_table.table_name,
            table_id: truncate_table
                .table_id
                .context(error::InvalidProtoMsgSnafu {
                    err_msg: "expected table_id",
                })?
                .id,
            time_ranges: truncate_table
                .time_ranges
                .map(from_pb_time_ranges)
                .transpose()
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?
                .unwrap_or_default(),
        })
    }
}

impl TryFrom<TruncateTableTask> for PbTruncateTableTask {
    type Error = error::Error;

    fn try_from(task: TruncateTableTask) -> Result<Self> {
        Ok(PbTruncateTableTask {
            truncate_table: Some(TruncateTableExpr {
                catalog_name: task.catalog,
                schema_name: task.schema,
                table_name: task.table,
                table_id: Some(api::v1::TableId { id: task.table_id }),
                time_ranges: Some(
                    to_pb_time_ranges(&task.time_ranges).context(ConvertTimeRangesSnafu)?,
                ),
            }),
        })
    }
}

#[serde_as]
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct CreateDatabaseTask {
    pub catalog: String,
    pub schema: String,
    pub create_if_not_exists: bool,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    pub options: HashMap<String, String>,
}

impl TryFrom<PbCreateDatabaseTask> for CreateDatabaseTask {
    type Error = error::Error;

    fn try_from(pb: PbCreateDatabaseTask) -> Result<Self> {
        let CreateDatabaseExpr {
            catalog_name,
            schema_name,
            create_if_not_exists,
            options,
        } = pb.create_database.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected create database",
        })?;

        Ok(CreateDatabaseTask {
            catalog: catalog_name,
            schema: schema_name,
            create_if_not_exists,
            options,
        })
    }
}

impl TryFrom<CreateDatabaseTask> for PbCreateDatabaseTask {
    type Error = error::Error;

    fn try_from(
        CreateDatabaseTask {
            catalog,
            schema,
            create_if_not_exists,
            options,
        }: CreateDatabaseTask,
    ) -> Result<Self> {
        Ok(PbCreateDatabaseTask {
            create_database: Some(CreateDatabaseExpr {
                catalog_name: catalog,
                schema_name: schema,
                create_if_not_exists,
                options,
            }),
        })
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DropDatabaseTask {
    pub catalog: String,
    pub schema: String,
    pub drop_if_exists: bool,
}

impl TryFrom<PbDropDatabaseTask> for DropDatabaseTask {
    type Error = error::Error;

    fn try_from(pb: PbDropDatabaseTask) -> Result<Self> {
        let DropDatabaseExpr {
            catalog_name,
            schema_name,
            drop_if_exists,
        } = pb.drop_database.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected drop database",
        })?;

        Ok(DropDatabaseTask {
            catalog: catalog_name,
            schema: schema_name,
            drop_if_exists,
        })
    }
}

impl TryFrom<DropDatabaseTask> for PbDropDatabaseTask {
    type Error = error::Error;

    fn try_from(
        DropDatabaseTask {
            catalog,
            schema,
            drop_if_exists,
        }: DropDatabaseTask,
    ) -> Result<Self> {
        Ok(PbDropDatabaseTask {
            drop_database: Some(DropDatabaseExpr {
                catalog_name: catalog,
                schema_name: schema,
                drop_if_exists,
            }),
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct AlterDatabaseTask {
    pub alter_expr: AlterDatabaseExpr,
}

impl TryFrom<AlterDatabaseTask> for PbAlterDatabaseTask {
    type Error = error::Error;

    fn try_from(task: AlterDatabaseTask) -> Result<Self> {
        Ok(PbAlterDatabaseTask {
            task: Some(task.alter_expr),
        })
    }
}

impl TryFrom<PbAlterDatabaseTask> for AlterDatabaseTask {
    type Error = error::Error;

    fn try_from(pb: PbAlterDatabaseTask) -> Result<Self> {
        let alter_expr = pb.task.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected alter database",
        })?;

        Ok(AlterDatabaseTask { alter_expr })
    }
}

impl TryFrom<PbAlterDatabaseKind> for AlterDatabaseKind {
    type Error = error::Error;

    fn try_from(pb: PbAlterDatabaseKind) -> Result<Self> {
        match pb {
            PbAlterDatabaseKind::SetDatabaseOptions(options) => {
                Ok(AlterDatabaseKind::SetDatabaseOptions(SetDatabaseOptions(
                    options
                        .set_database_options
                        .into_iter()
                        .map(SetDatabaseOption::try_from)
                        .collect::<Result<Vec<_>>>()?,
                )))
            }
            PbAlterDatabaseKind::UnsetDatabaseOptions(options) => Ok(
                AlterDatabaseKind::UnsetDatabaseOptions(UnsetDatabaseOptions(
                    options
                        .keys
                        .iter()
                        .map(|key| UnsetDatabaseOption::try_from(key.as_str()))
                        .collect::<Result<Vec<_>>>()?,
                )),
            ),
        }
    }
}

const TTL_KEY: &str = "ttl";

impl TryFrom<PbOption> for SetDatabaseOption {
    type Error = error::Error;

    fn try_from(PbOption { key, value }: PbOption) -> Result<Self> {
        match key.to_ascii_lowercase().as_str() {
            TTL_KEY => {
                let ttl = DatabaseTimeToLive::from_humantime_or_str(&value)
                    .map_err(|_| InvalidSetDatabaseOptionSnafu { key, value }.build())?;

                Ok(SetDatabaseOption::Ttl(ttl))
            }
            _ => InvalidSetDatabaseOptionSnafu { key, value }.fail(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum SetDatabaseOption {
    Ttl(DatabaseTimeToLive),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum UnsetDatabaseOption {
    Ttl,
}

impl TryFrom<&str> for UnsetDatabaseOption {
    type Error = error::Error;

    fn try_from(key: &str) -> Result<Self> {
        match key.to_ascii_lowercase().as_str() {
            TTL_KEY => Ok(UnsetDatabaseOption::Ttl),
            _ => InvalidUnsetDatabaseOptionSnafu { key }.fail(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SetDatabaseOptions(pub Vec<SetDatabaseOption>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct UnsetDatabaseOptions(pub Vec<UnsetDatabaseOption>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum AlterDatabaseKind {
    SetDatabaseOptions(SetDatabaseOptions),
    UnsetDatabaseOptions(UnsetDatabaseOptions),
}

impl AlterDatabaseTask {
    pub fn catalog(&self) -> &str {
        &self.alter_expr.catalog_name
    }

    pub fn schema(&self) -> &str {
        &self.alter_expr.catalog_name
    }
}

/// Create flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateFlowTask {
    pub catalog_name: String,
    pub flow_name: String,
    pub source_table_names: Vec<TableName>,
    pub sink_table_name: TableName,
    pub or_replace: bool,
    pub create_if_not_exists: bool,
    /// Duration in seconds. Data older than this duration will not be used.
    pub expire_after: Option<i64>,
    pub comment: String,
    pub sql: String,
    pub flow_options: HashMap<String, String>,
}

impl TryFrom<PbCreateFlowTask> for CreateFlowTask {
    type Error = error::Error;

    fn try_from(pb: PbCreateFlowTask) -> Result<Self> {
        let CreateFlowExpr {
            catalog_name,
            flow_name,
            source_table_names,
            sink_table_name,
            or_replace,
            create_if_not_exists,
            expire_after,
            comment,
            sql,
            flow_options,
        } = pb.create_flow.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected create_flow",
        })?;

        Ok(CreateFlowTask {
            catalog_name,
            flow_name,
            source_table_names: source_table_names.into_iter().map(Into::into).collect(),
            sink_table_name: sink_table_name
                .context(error::InvalidProtoMsgSnafu {
                    err_msg: "expected sink_table_name",
                })?
                .into(),
            or_replace,
            create_if_not_exists,
            expire_after: expire_after.map(|e| e.value),
            comment,
            sql,
            flow_options,
        })
    }
}

impl From<CreateFlowTask> for PbCreateFlowTask {
    fn from(
        CreateFlowTask {
            catalog_name,
            flow_name,
            source_table_names,
            sink_table_name,
            or_replace,
            create_if_not_exists,
            expire_after,
            comment,
            sql,
            flow_options,
        }: CreateFlowTask,
    ) -> Self {
        PbCreateFlowTask {
            create_flow: Some(CreateFlowExpr {
                catalog_name,
                flow_name,
                source_table_names: source_table_names.into_iter().map(Into::into).collect(),
                sink_table_name: Some(sink_table_name.into()),
                or_replace,
                create_if_not_exists,
                expire_after: expire_after.map(|value| ExpireAfter { value }),
                comment,
                sql,
                flow_options,
            }),
        }
    }
}

/// Drop flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropFlowTask {
    pub catalog_name: String,
    pub flow_name: String,
    pub flow_id: FlowId,
    pub drop_if_exists: bool,
}

impl TryFrom<PbDropFlowTask> for DropFlowTask {
    type Error = error::Error;

    fn try_from(pb: PbDropFlowTask) -> Result<Self> {
        let DropFlowExpr {
            catalog_name,
            flow_name,
            flow_id,
            drop_if_exists,
        } = pb.drop_flow.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected drop_flow",
        })?;
        let flow_id = flow_id
            .context(error::InvalidProtoMsgSnafu {
                err_msg: "expected flow_id",
            })?
            .id;
        Ok(DropFlowTask {
            catalog_name,
            flow_name,
            flow_id,
            drop_if_exists,
        })
    }
}

impl From<DropFlowTask> for PbDropFlowTask {
    fn from(
        DropFlowTask {
            catalog_name,
            flow_name,
            flow_id,
            drop_if_exists,
        }: DropFlowTask,
    ) -> Self {
        PbDropFlowTask {
            drop_flow: Some(DropFlowExpr {
                catalog_name,
                flow_name,
                flow_id: Some(api::v1::FlowId { id: flow_id }),
                drop_if_exists,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryContext {
    pub(crate) current_catalog: String,
    pub(crate) current_schema: String,
    pub(crate) timezone: String,
    pub(crate) extensions: HashMap<String, String>,
    pub(crate) channel: u8,
}

impl QueryContext {
    /// Get the current catalog
    pub fn current_catalog(&self) -> &str {
        &self.current_catalog
    }

    /// Get the current schema
    pub fn current_schema(&self) -> &str {
        &self.current_schema
    }

    /// Get the timezone
    pub fn timezone(&self) -> &str {
        &self.timezone
    }

    /// Get the extensions
    pub fn extensions(&self) -> &HashMap<String, String> {
        &self.extensions
    }

    /// Get the channel
    pub fn channel(&self) -> u8 {
        self.channel
    }
}

/// Lightweight query context for flow operations containing only essential fields.
/// This is a subset of QueryContext that includes only the fields actually needed
/// for flow creation and execution.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FlowQueryContext {
    /// Current catalog name - needed for flow metadata and recovery
    pub(crate) catalog: String,
    /// Current schema name - needed for table resolution during flow execution
    pub(crate) schema: String,
    /// Timezone for timestamp operations in the flow
    pub(crate) timezone: String,
}

impl<'de> Deserialize<'de> for FlowQueryContext {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Support both QueryContext format and FlowQueryContext format
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ContextCompat {
            Flow(FlowQueryContextHelper),
            Full(QueryContext),
        }

        #[derive(Deserialize)]
        struct FlowQueryContextHelper {
            catalog: String,
            schema: String,
            timezone: String,
        }

        match ContextCompat::deserialize(deserializer)? {
            ContextCompat::Flow(helper) => Ok(FlowQueryContext {
                catalog: helper.catalog,
                schema: helper.schema,
                timezone: helper.timezone,
            }),
            ContextCompat::Full(full_ctx) => Ok(full_ctx.into()),
        }
    }
}

impl From<QueryContextRef> for QueryContext {
    fn from(query_context: QueryContextRef) -> Self {
        QueryContext {
            current_catalog: query_context.current_catalog().to_string(),
            current_schema: query_context.current_schema().to_string(),
            timezone: query_context.timezone().to_string(),
            extensions: query_context.extensions(),
            channel: query_context.channel() as u8,
        }
    }
}

impl TryFrom<QueryContext> for session::context::QueryContext {
    type Error = error::Error;
    fn try_from(value: QueryContext) -> std::result::Result<Self, Self::Error> {
        Ok(QueryContextBuilder::default()
            .current_catalog(value.current_catalog)
            .current_schema(value.current_schema)
            .timezone(Timezone::from_tz_string(&value.timezone).context(InvalidTimeZoneSnafu)?)
            .extensions(value.extensions)
            .channel((value.channel as u32).into())
            .build())
    }
}

impl From<QueryContext> for PbQueryContext {
    fn from(
        QueryContext {
            current_catalog,
            current_schema,
            timezone,
            extensions,
            channel,
        }: QueryContext,
    ) -> Self {
        PbQueryContext {
            current_catalog,
            current_schema,
            timezone,
            extensions,
            channel: channel as u32,
            snapshot_seqs: None,
            explain: None,
        }
    }
}

impl From<QueryContext> for FlowQueryContext {
    fn from(ctx: QueryContext) -> Self {
        Self {
            catalog: ctx.current_catalog,
            schema: ctx.current_schema,
            timezone: ctx.timezone,
        }
    }
}

impl From<QueryContextRef> for FlowQueryContext {
    fn from(ctx: QueryContextRef) -> Self {
        Self {
            catalog: ctx.current_catalog().to_string(),
            schema: ctx.current_schema().to_string(),
            timezone: ctx.timezone().to_string(),
        }
    }
}

impl From<FlowQueryContext> for QueryContext {
    fn from(flow_ctx: FlowQueryContext) -> Self {
        Self {
            current_catalog: flow_ctx.catalog,
            current_schema: flow_ctx.schema,
            timezone: flow_ctx.timezone,
            extensions: HashMap::new(),
            channel: 0, // Use default channel for flows
        }
    }
}

impl From<FlowQueryContext> for PbQueryContext {
    fn from(flow_ctx: FlowQueryContext) -> Self {
        let query_ctx: QueryContext = flow_ctx.into();
        query_ctx.into()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{AlterTableExpr, ColumnDef, CreateTableExpr, SemanticType};
    use datatypes::schema::{ColumnSchema, RawSchema, SchemaBuilder};
    use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
    use store_api::storage::ConcreteDataType;
    use table::metadata::{RawTableInfo, RawTableMeta, TableType};
    use table::test_util::table_info::test_table_info;

    use super::{AlterTableTask, CreateTableTask, *};

    #[test]
    fn test_basic_ser_de_create_table_task() {
        let schema = SchemaBuilder::default().build().unwrap();
        let table_info = test_table_info(1025, "foo", "bar", "baz", Arc::new(schema));
        let task = CreateTableTask::new(
            CreateTableExpr::default(),
            Vec::new(),
            RawTableInfo::from(table_info),
        );

        let output = serde_json::to_vec(&task).unwrap();

        let de = serde_json::from_slice(&output).unwrap();
        assert_eq!(task, de);
    }

    #[test]
    fn test_basic_ser_de_alter_table_task() {
        let task = AlterTableTask {
            alter_table: AlterTableExpr::default(),
        };

        let output = serde_json::to_vec(&task).unwrap();

        let de = serde_json::from_slice(&output).unwrap();
        assert_eq!(task, de);
    }

    #[test]
    fn test_sort_columns() {
        // construct RawSchema
        let raw_schema = RawSchema {
            column_schemas: vec![
                ColumnSchema::new(
                    "column3".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                ColumnSchema::new(
                    "column1".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                ColumnSchema::new(
                    "column2".to_string(),
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
            ],
            timestamp_index: Some(1),
            version: 0,
        };

        // construct RawTableMeta
        let raw_table_meta = RawTableMeta {
            schema: raw_schema,
            primary_key_indices: vec![0],
            value_indices: vec![2],
            engine: METRIC_ENGINE_NAME.to_string(),
            next_column_id: 0,
            region_numbers: vec![0],
            options: Default::default(),
            created_on: Default::default(),
            partition_key_indices: Default::default(),
            column_ids: Default::default(),
        };

        // construct RawTableInfo
        let raw_table_info = RawTableInfo {
            ident: Default::default(),
            meta: raw_table_meta,
            name: Default::default(),
            desc: Default::default(),
            catalog_name: Default::default(),
            schema_name: Default::default(),
            table_type: TableType::Base,
        };

        // construct create table expr
        let create_table_expr = CreateTableExpr {
            column_defs: vec![
                ColumnDef {
                    name: "column3".to_string(),
                    semantic_type: SemanticType::Tag as i32,
                    ..Default::default()
                },
                ColumnDef {
                    name: "column1".to_string(),
                    semantic_type: SemanticType::Timestamp as i32,
                    ..Default::default()
                },
                ColumnDef {
                    name: "column2".to_string(),
                    semantic_type: SemanticType::Field as i32,
                    ..Default::default()
                },
            ],
            primary_keys: vec!["column3".to_string()],
            ..Default::default()
        };

        let mut create_table_task =
            CreateTableTask::new(create_table_expr, Vec::new(), raw_table_info);

        // Call the sort_columns method
        create_table_task.sort_columns();

        // Assert that the columns are sorted correctly
        assert_eq!(
            create_table_task.create_table.column_defs[0].name,
            "column1".to_string()
        );
        assert_eq!(
            create_table_task.create_table.column_defs[1].name,
            "column2".to_string()
        );
        assert_eq!(
            create_table_task.create_table.column_defs[2].name,
            "column3".to_string()
        );

        // Assert that the table_info is updated correctly
        assert_eq!(
            create_table_task.table_info.meta.schema.timestamp_index,
            Some(0)
        );
        assert_eq!(
            create_table_task.table_info.meta.primary_key_indices,
            vec![2]
        );
        assert_eq!(create_table_task.table_info.meta.value_indices, vec![0, 1]);
    }

    #[test]
    fn test_flow_query_context_conversion_from_query_context() {
        use std::collections::HashMap;
        let mut extensions = HashMap::new();
        extensions.insert("key1".to_string(), "value1".to_string());
        extensions.insert("key2".to_string(), "value2".to_string());

        let query_ctx = QueryContext {
            current_catalog: "test_catalog".to_string(),
            current_schema: "test_schema".to_string(),
            timezone: "UTC".to_string(),
            extensions,
            channel: 5,
        };

        let flow_ctx: FlowQueryContext = query_ctx.into();

        assert_eq!(flow_ctx.catalog, "test_catalog");
        assert_eq!(flow_ctx.schema, "test_schema");
        assert_eq!(flow_ctx.timezone, "UTC");
    }

    #[test]
    fn test_flow_query_context_conversion_to_query_context() {
        let flow_ctx = FlowQueryContext {
            catalog: "prod_catalog".to_string(),
            schema: "public".to_string(),
            timezone: "America/New_York".to_string(),
        };

        let query_ctx: QueryContext = flow_ctx.clone().into();

        assert_eq!(query_ctx.current_catalog, "prod_catalog");
        assert_eq!(query_ctx.current_schema, "public");
        assert_eq!(query_ctx.timezone, "America/New_York");
        assert!(query_ctx.extensions.is_empty());
        assert_eq!(query_ctx.channel, 0);

        // Test roundtrip conversion
        let flow_ctx_roundtrip: FlowQueryContext = query_ctx.into();
        assert_eq!(flow_ctx, flow_ctx_roundtrip);
    }

    #[test]
    fn test_flow_query_context_conversion_from_query_context_ref() {
        use common_time::Timezone;
        use session::context::QueryContextBuilder;

        let session_ctx = QueryContextBuilder::default()
            .current_catalog("session_catalog".to_string())
            .current_schema("session_schema".to_string())
            .timezone(Timezone::from_tz_string("Europe/London").unwrap())
            .build();

        let session_ctx_ref = Arc::new(session_ctx);
        let flow_ctx: FlowQueryContext = session_ctx_ref.into();

        assert_eq!(flow_ctx.catalog, "session_catalog");
        assert_eq!(flow_ctx.schema, "session_schema");
        assert_eq!(flow_ctx.timezone, "Europe/London");
    }

    #[test]
    fn test_flow_query_context_serialization() {
        let flow_ctx = FlowQueryContext {
            catalog: "test_catalog".to_string(),
            schema: "test_schema".to_string(),
            timezone: "UTC".to_string(),
        };

        let serialized = serde_json::to_string(&flow_ctx).unwrap();
        let deserialized: FlowQueryContext = serde_json::from_str(&serialized).unwrap();

        assert_eq!(flow_ctx, deserialized);

        // Verify JSON structure
        let json_value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(json_value["catalog"], "test_catalog");
        assert_eq!(json_value["schema"], "test_schema");
        assert_eq!(json_value["timezone"], "UTC");
    }

    #[test]
    fn test_flow_query_context_conversion_to_pb() {
        let flow_ctx = FlowQueryContext {
            catalog: "pb_catalog".to_string(),
            schema: "pb_schema".to_string(),
            timezone: "Asia/Tokyo".to_string(),
        };

        let pb_ctx: PbQueryContext = flow_ctx.into();

        assert_eq!(pb_ctx.current_catalog, "pb_catalog");
        assert_eq!(pb_ctx.current_schema, "pb_schema");
        assert_eq!(pb_ctx.timezone, "Asia/Tokyo");
        assert!(pb_ctx.extensions.is_empty());
        assert_eq!(pb_ctx.channel, 0);
        assert!(pb_ctx.snapshot_seqs.is_none());
        assert!(pb_ctx.explain.is_none());
    }
}
