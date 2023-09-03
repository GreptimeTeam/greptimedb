// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use store_api::storage::RegionId;

use crate::ident::TableIdent;
use crate::{ClusterId, DatanodeId};

#[derive(Eq, Hash, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct RegionIdent {
    pub cluster_id: ClusterId,
    pub datanode_id: DatanodeId,
    pub table_ident: TableIdent,
    pub region_number: u32,
}

impl RegionIdent {
    pub fn get_region_id(&self) -> RegionId {
        RegionId::new(self.table_ident.table_id, self.region_number)
    }
}

impl Display for RegionIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RegionIdent(datanode_id='{}.{}', table_id='{}', table_name='{}.{}.{}', table_engine='{}', region_no='{}')",
            self.cluster_id,
            self.datanode_id,
            self.table_ident.table_id,
            self.table_ident.catalog,
            self.table_ident.schema,
            self.table_ident.table,
            self.table_ident.engine,
            self.region_number
        )
    }
}

impl From<RegionIdent> for TableIdent {
    fn from(region_ident: RegionIdent) -> Self {
        region_ident.table_ident
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct SimpleReply {
    pub result: bool,
    pub error: Option<String>,
}

impl Display for SimpleReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "(result={}, error={:?})", self.result, self.error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Instruction {
    OpenRegion(RegionIdent),
    CloseRegion(RegionIdent),
    InvalidateTableCache(TableIdent),
}

impl Display for Instruction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenRegion(region) => write!(f, "Instruction::OpenRegion({})", region),
            Self::CloseRegion(region) => write!(f, "Instruction::CloseRegion({})", region),
            Self::InvalidateTableCache(table) => write!(f, "Instruction::Invalidate({})", table),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstructionReply {
    OpenRegion(SimpleReply),
    CloseRegion(SimpleReply),
    InvalidateTableCache(SimpleReply),
}

impl Display for InstructionReply {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenRegion(reply) => write!(f, "InstructionReply::OpenRegion({})", reply),
            Self::CloseRegion(reply) => write!(f, "InstructionReply::CloseRegion({})", reply),
            Self::InvalidateTableCache(reply) => {
                write!(f, "InstructionReply::Invalidate({})", reply)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_instruction() {
        let open_region = Instruction::OpenRegion(RegionIdent {
            cluster_id: 1,
            datanode_id: 2,
            table_ident: TableIdent {
                catalog: "foo".to_string(),
                schema: "bar".to_string(),
                table: "hi".to_string(),
                table_id: 1024,
                engine: "mito".to_string(),
            },
            region_number: 1,
        });

        let serialized = serde_json::to_string(&open_region).unwrap();

        assert_eq!(
            r#"{"type":"open_region","cluster_id":1,"datanode_id":2,"table_ident":{"catalog":"foo","schema":"bar","table":"hi","table_id":1024,"engine":"mito"},"region_number":1}"#,
            serialized
        );

        let close_region = Instruction::CloseRegion(RegionIdent {
            cluster_id: 1,
            datanode_id: 2,
            table_ident: TableIdent {
                catalog: "foo".to_string(),
                schema: "bar".to_string(),
                table: "hi".to_string(),
                table_id: 1024,
                engine: "mito".to_string(),
            },
            region_number: 1,
        });

        let serialized = serde_json::to_string(&close_region).unwrap();

        assert_eq!(
            r#"{"type":"close_region","cluster_id":1,"datanode_id":2,"table_ident":{"catalog":"foo","schema":"bar","table":"hi","table_id":1024,"engine":"mito"},"region_number":1}"#,
            serialized
        );
    }
}
