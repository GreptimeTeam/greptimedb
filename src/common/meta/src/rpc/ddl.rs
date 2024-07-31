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

use std::collections::{HashMap, HashSet};
use std::result;

use api::v1::meta::ddl_task_request::Task;
use api::v1::meta::{
    AlterTableTask as PbAlterTableTask, AlterTableTasks as PbAlterTableTasks,
    CreateDatabaseTask as PbCreateDatabaseTask, CreateFlowTask as PbCreateFlowTask,
    CreateTableTask as PbCreateTableTask, CreateTableTasks as PbCreateTableTasks,
    CreateViewTask as PbCreateViewTask, DdlTaskRequest as PbDdlTaskRequest,
    DdlTaskResponse as PbDdlTaskResponse, DropDatabaseTask as PbDropDatabaseTask,
    DropFlowTask as PbDropFlowTask, DropTableTask as PbDropTableTask,
    DropTableTasks as PbDropTableTasks, DropViewTask as PbDropViewTask, Partition, ProcedureId,
    TruncateTableTask as PbTruncateTableTask,
};
use api::v1::{
    AlterExpr, CreateDatabaseExpr, CreateFlowExpr, CreateTableExpr, CreateViewExpr,
    DropDatabaseExpr, DropFlowExpr, DropTableExpr, DropViewExpr, ExpireAfter,
    QueryContext as PbQueryContext, TruncateTableExpr,
};
use base64::engine::general_purpose;
use base64::Engine as _;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DefaultOnNull};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::metadata::{RawTableInfo, TableId};
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::error::{self, Result};
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
    CreateFlow(CreateFlowTask),
    DropFlow(DropFlowTask),
    CreateView(CreateViewTask),
    DropView(DropViewTask),
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
    pub fn new_alter_logical_tables(table_data: Vec<AlterExpr>) -> Self {
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

    /// Creates a [`DdlTask`] to alter a table.
    pub fn new_alter_table(alter_table: AlterExpr) -> Self {
        DdlTask::AlterTable(AlterTableTask { alter_table })
    }

    /// Creates a [`DdlTask`] to truncate a table.
    pub fn new_truncate_table(
        catalog: String,
        schema: String,
        table: String,
        table_id: TableId,
    ) -> Self {
        DdlTask::TruncateTable(TruncateTableTask {
            catalog,
            schema,
            table,
            table_id,
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
            Task::CreateFlowTask(create_flow) => Ok(DdlTask::CreateFlow(create_flow.try_into()?)),
            Task::DropFlowTask(drop_flow) => Ok(DdlTask::DropFlow(drop_flow.try_into()?)),
            Task::CreateViewTask(create_view) => Ok(DdlTask::CreateView(create_view.try_into()?)),
            Task::DropViewTask(drop_view) => Ok(DdlTask::DropView(drop_view.try_into()?)),
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
            DdlTask::CreateFlow(task) => Task::CreateFlowTask(task.into()),
            DdlTask::DropFlow(task) => Task::DropFlowTask(task.into()),
            DdlTask::CreateView(task) => Task::CreateViewTask(task.try_into()?),
            DdlTask::DropView(task) => Task::DropViewTask(task.into()),
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
    pub alter_table: AlterExpr,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryContext {
    current_catalog: String,
    current_schema: String,
    timezone: String,
    extensions: HashMap<String, String>,
    channel: u8,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{AlterExpr, ColumnDef, CreateTableExpr, SemanticType};
    use datatypes::schema::{ColumnSchema, RawSchema, SchemaBuilder};
    use store_api::metric_engine_consts::METRIC_ENGINE_NAME;
    use store_api::storage::ConcreteDataType;
    use table::metadata::{RawTableInfo, RawTableMeta, TableType};
    use table::test_util::table_info::test_table_info;

    use super::{AlterTableTask, CreateTableTask};

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
            alter_table: AlterExpr::default(),
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
        assert_eq!(create_table_task.table_info.meta.value_indices, vec![1]);
    }
}
