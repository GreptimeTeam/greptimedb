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
use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use table::engine::TableReference;
use table::metadata::RawTableInfo;

use crate::error::{self, Result};
use crate::table_name::TableName;

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("owned bytes")
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v)
    }
}

#[derive(Debug)]
pub enum DdlTask {
    CreateTable(CreateTableTask),
}

impl DdlTask {
    pub fn new_create_table(
        expr: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: RawTableInfo,
    ) -> Result<Self> {
        let table_info = serde_json::to_vec(&table_info).context(error::SerdeJsonSnafu)?;
        Ok(DdlTask::CreateTable(CreateTableTask::new(
            expr, partitions, table_info,
        )))
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

impl From<DdlTask> for PbSubmitDdlTaskRequest {
    fn from(task: DdlTask) -> Self {
        let task = match task {
            DdlTask::CreateTable(task) => Task::CreateTableTask(PbCreateTableTask {
                table_info: task.table_info,
                create_table: Some(task.create_table),
                partitions: task.partitions,
            }),
        };
        Self {
            header: None,
            task: Some(task),
        }
    }
}

pub struct SubmitDdlTaskResponse {
    pub key: Vec<u8>,
}

impl From<PbSubmitDdlTaskResponse> for SubmitDdlTaskResponse {
    fn from(resp: PbSubmitDdlTaskResponse) -> Self {
        Self { key: resp.key }
    }
}

#[derive(Debug)]
pub struct CreateTableTask {
    pub create_table: CreateTableExpr,
    pub partitions: Vec<Partition>,
    pub table_info: Vec<u8>,
}

impl TryFrom<PbCreateTableTask> for CreateTableTask {
    type Error = error::Error;
    fn try_from(pb: PbCreateTableTask) -> Result<Self> {
        Ok(CreateTableTask::new(
            pb.create_table.context(error::InfoCorruptedSnafu {
                err_msg: "expected create table",
            })?,
            pb.partitions,
            pb.table_info,
        ))
    }
}

impl CreateTableTask {
    pub fn new(
        expr: CreateTableExpr,
        partitions: Vec<Partition>,
        table_info: Vec<u8>,
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
        let pb = PbCreateTableTask {
            create_table: Some(self.create_table.clone()),
            partitions: self.partitions.clone(),
            table_info: self.table_info.clone(),
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
        let buf = deserializer.deserialize_byte_buf(BytesVisitor)?;
        let expr: PbCreateTableTask = PbCreateTableTask::decode(&*buf)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        let expr = CreateTableTask::try_from(expr)
            .map_err(|err| serde::de::Error::custom(err.to_string()))?;

        Ok(expr)
    }
}
