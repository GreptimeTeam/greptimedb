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

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Instruction {
    OpenRegion {
        catalog: String,
        schema: String,
        table: String,
        table_id: u32,
        engine: String,
        // it will open all regions if region_number is None
        region_number: Option<u32>,
    },
    CloseRegion {
        catalog: String,
        schema: String,
        table: String,
        table_id: u32,
        engine: String,
        // it will close all regions if region_number is None
        region_number: Option<u32>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InstructionReply {
    OpenRegion { result: bool, error: Option<String> },
    CloseRegion { result: bool, error: Option<String> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_instruction() {
        let open_region = Instruction::OpenRegion {
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            table: "hi".to_string(),
            table_id: 1024,
            engine: "mito".to_string(),
            region_number: Some(1),
        };

        let serialized = serde_json::to_string(&open_region).unwrap();

        assert_eq!(
            r#"{"type":"open_region","catalog":"foo","schema":"bar","table":"hi","table_id":1024,"engine":"mito","region_number":1}"#,
            serialized
        );

        let close_region = Instruction::CloseRegion {
            catalog: "foo".to_string(),
            schema: "bar".to_string(),
            table: "hi".to_string(),
            table_id: 1024,
            engine: "mito".to_string(),
            region_number: Some(1),
        };

        let serialized = serde_json::to_string(&close_region).unwrap();

        assert_eq!(
            r#"{"type":"close_region","catalog":"foo","schema":"bar","table":"hi","table_id":1024,"engine":"mito","region_number":1}"#,
            serialized
        );
    }
}
