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

use api::v1::meta::submit_ddl_task_request::Task;
use api::v1::meta::{
    CreateTableTask as PbCreateTableTask, Partition,
    SubmitDdlTaskRequest as PbSubmitDdlTaskRequest,
    SubmitDdlTaskResponse as PbSubmitDdlTaskResponse,
};
use api::v1::CreateTableExpr;
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use crate::error::{self, Result};
use crate::table_name::TableName;

#[derive(Debug)]
pub enum DdlTask {
    CreateTable(CreateTableTask),
}

impl DdlTask {
    pub fn new_create_table(
        expr: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> Self {
        DdlTask::CreateTable(CreateTableTask::new(expr, partitions, table_info))
    }
}

impl TryFrom<Task> for DdlTask {
    type Error = error::Error;
    fn try_from(task: Task) -> Result<Self> {
        match task {
            Task::CreateTableTask(create_table) => {
                Ok(DdlTask::CreateTable(create_table.try_into()?))
            }
        }
    }
}

pub struct SubmitDdlTaskRequest {
    pub task: DdlTask,
}

impl TryFrom<SubmitDdlTaskRequest> for PbSubmitDdlTaskRequest {
    type Error = error::Error;

    fn try_from(request: SubmitDdlTaskRequest) -> Result<Self> {
        let task = match request.task {
            DdlTask::CreateTable(task) => Task::CreateTableTask(PbCreateTableTask {
                table_info: serde_json::to_vec(&task.table_info).context(error::SerdeJsonSnafu)?,
                create_table: Some(task.create_table),
                partitions: task.partitions,
            }),
        };
        Ok(Self {
            header: None,
            task: Some(task),
        })
    }
}

pub struct SubmitDdlTaskResponse {
    pub key: Vec<u8>,
    pub table_id: TableId,
}

impl TryFrom<PbSubmitDdlTaskResponse> for SubmitDdlTaskResponse {
    type Error = error::Error;

    fn try_from(resp: PbSubmitDdlTaskResponse) -> Result<Self> {
        let table_id = resp.table_id.context(error::InvalidProtoMsgSnafu {
            err_msg: "expected table_id",
        })?;
        Ok(Self {
            key: resp.key,
            table_id: table_id.id,
        })
    }
}

#[derive(Debug, PartialEq)]
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
        serializer.serialize_bytes(&buf)
    }
}

impl<'de> Deserialize<'de> for CreateTableTask {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf = Vec::<u8>::deserialize(deserializer)?;
        let expr: PbCreateTableTask = PbCreateTableTask::decode(&*buf)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        let expr = CreateTableTask::try_from(expr)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        Ok(expr)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::CreateTableExpr;
    use datatypes::schema::SchemaBuilder;
    use table::metadata::RawTableInfo;
    use table::test_util::table_info::test_table_info;

    use super::CreateTableTask;

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
}
