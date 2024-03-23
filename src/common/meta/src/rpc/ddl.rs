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

use std::result;

use api::v1::meta::ddl_task_request::Task;
use api::v1::meta::{
    AlterTableTask as PbAlterTableTask, AlterTableTasks as PbAlterTableTasks,
    CreateTableTask as PbCreateTableTask, CreateTableTasks as PbCreateTableTasks,
    DdlTaskRequest as PbDdlTaskRequest, DdlTaskResponse as PbDdlTaskResponse,
    DropTableTask as PbDropTableTask, DropTableTasks as PbDropTableTasks, Partition, ProcedureId,
    TruncateTableTask as PbTruncateTableTask,
};
use api::v1::{AlterExpr, CreateTableExpr, DropTableExpr, SemanticType, TruncateTableExpr};
use base64::engine::general_purpose;
use base64::Engine as _;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::metadata::{RawTableInfo, TableId};
use table::table_reference::TableReference;

use crate::error::{self, Result};
use crate::table_name::TableName;

#[derive(Debug, Clone)]
pub enum DdlTask {
    CreateTable(CreateTableTask),
    DropTable(DropTableTask),
    AlterTable(AlterTableTask),
    TruncateTable(TruncateTableTask),
    CreateLogicalTables(Vec<CreateTableTask>),
    DropLogicalTables(Vec<DropTableTask>),
    AlterLogicalTables(Vec<AlterTableTask>),
}

impl DdlTask {
    pub fn new_create_table(
        expr: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> Self {
        DdlTask::CreateTable(CreateTableTask::new(expr, partitions, table_info))
    }

    pub fn new_create_logical_tables(table_data: Vec<(CreateTableExpr, RawTableInfo)>) -> Self {
        DdlTask::CreateLogicalTables(
            table_data
                .into_iter()
                .map(|(expr, table_info)| CreateTableTask::new(expr, Vec::new(), table_info))
                .collect(),
        )
    }

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

    pub fn new_alter_table(alter_table: AlterExpr) -> Self {
        DdlTask::AlterTable(AlterTableTask { alter_table })
    }

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
        }
    }
}

#[derive(Clone)]
pub struct SubmitDdlTaskRequest {
    pub task: DdlTask,
}

impl TryFrom<SubmitDdlTaskRequest> for PbDdlTaskRequest {
    type Error = error::Error;

    fn try_from(request: SubmitDdlTaskRequest) -> Result<Self> {
        let task = match request.task {
            DdlTask::CreateTable(task) => Task::CreateTableTask(task.try_into()?),
            DdlTask::DropTable(task) => Task::DropTableTask(task.try_into()?),
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
                    .map(|task| task.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Task::DropTableTasks(PbDropTableTasks { tasks })
            }
            DdlTask::AlterLogicalTables(tasks) => {
                let tasks = tasks
                    .into_iter()
                    .map(|task| task.try_into())
                    .collect::<Result<Vec<_>>>()?;

                Task::AlterTableTasks(PbAlterTableTasks { tasks })
            }
        };

        Ok(Self {
            header: None,
            task: Some(task),
        })
    }
}

#[derive(Debug, Default)]
pub struct SubmitDdlTaskResponse {
    pub key: Vec<u8>,
    // For create physical table
    // TODO(jeremy): remove it?
    pub table_id: Option<TableId>,
    // For create multi logical tables
    pub table_ids: Vec<TableId>,
}

impl TryFrom<PbDdlTaskResponse> for SubmitDdlTaskResponse {
    type Error = error::Error;

    fn try_from(resp: PbDdlTaskResponse) -> Result<Self> {
        let table_id = resp.table_id.map(|t| t.id);
        let table_ids = resp.table_ids.into_iter().map(|t| t.id).collect();
        Ok(Self {
            key: resp.pid.map(|pid| pid.key).unwrap_or_default(),
            table_id,
            table_ids,
        })
    }
}

impl From<SubmitDdlTaskResponse> for PbDdlTaskResponse {
    fn from(val: SubmitDdlTaskResponse) -> Self {
        Self {
            pid: Some(ProcedureId { key: val.key }),
            table_id: val
                .table_id
                .map(|table_id| api::v1::meta::TableId { id: table_id }),
            table_ids: val
                .table_ids
                .into_iter()
                .map(|id| api::v1::meta::TableId { id })
                .collect(),
            ..Default::default()
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

impl TryFrom<DropTableTask> for PbDropTableTask {
    type Error = error::Error;

    fn try_from(task: DropTableTask) -> Result<Self> {
        Ok(PbDropTableTask {
            drop_table: Some(DropTableExpr {
                catalog_name: task.catalog,
                schema_name: task.schema,
                table_name: task.table,
                table_id: Some(api::v1::TableId { id: task.table_id }),
                drop_if_exists: task.drop_if_exists,
            }),
        })
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

        // compute new indices of sorted columns
        // this part won't do any check or verification.
        let mut primary_key_indices = Vec::with_capacity(self.create_table.primary_keys.len());
        let mut value_indices =
            Vec::with_capacity(self.create_table.column_defs.len() - primary_key_indices.len() - 1);
        let mut timestamp_index = None;
        for (index, col) in self.create_table.column_defs.iter().enumerate() {
            if self.create_table.primary_keys.contains(&col.name) {
                primary_key_indices.push(index);
            } else if col.semantic_type == SemanticType::Timestamp as i32 {
                timestamp_index = Some(index);
            } else {
                value_indices.push(index);
            }
        }

        // overwrite table info
        self.table_info
            .meta
            .schema
            .column_schemas
            .sort_unstable_by(|a, b| a.name.cmp(&b.name));
        self.table_info.meta.schema.timestamp_index = timestamp_index;
        self.table_info.meta.primary_key_indices = primary_key_indices;
        self.table_info.meta.value_indices = value_indices;
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
            err_msg: "expected drop table",
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::{AlterExpr, CreateTableExpr};
    use datatypes::schema::SchemaBuilder;
    use table::metadata::RawTableInfo;
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
}
