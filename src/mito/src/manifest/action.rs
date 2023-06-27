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

use std::io::{BufRead, BufReader};

use serde::{Deserialize, Serialize};
use serde_json as json;
use snafu::{ensure, OptionExt, ResultExt};
use storage::error::{
    DecodeJsonSnafu, DecodeMetaActionListSnafu, Error as StorageError,
    ManifestProtocolForbidReadSnafu, ReadlineSnafu,
};
use storage::manifest::helper;
use store_api::manifest::action::{ProtocolAction, ProtocolVersion, VersionHeader};
use store_api::manifest::{ManifestVersion, MetaAction};
use table::metadata::{RawTableInfo, TableIdent};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TableChange {
    pub table_info: RawTableInfo,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TableRemove {
    pub table_ident: TableIdent,
    pub table_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TableMetaAction {
    Protocol(ProtocolAction),
    // Boxed TableChange to reduce the total size of enum
    Change(Box<TableChange>),
    Remove(TableRemove),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TableMetaActionList {
    pub actions: Vec<TableMetaAction>,
    pub prev_version: ManifestVersion,
}

impl TableMetaActionList {
    pub fn with_action(action: TableMetaAction) -> Self {
        Self {
            actions: vec![action],
            prev_version: 0,
        }
    }

    pub fn new(actions: Vec<TableMetaAction>) -> Self {
        Self {
            actions,
            prev_version: 0,
        }
    }
}

impl MetaAction for TableMetaActionList {
    type Error = StorageError;

    fn set_protocol(&mut self, action: ProtocolAction) {
        // The protocol action should be the first action in action list by convention.
        self.actions.insert(0, TableMetaAction::Protocol(action));
    }

    fn set_prev_version(&mut self, version: ManifestVersion) {
        self.prev_version = version;
    }

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        helper::encode_actions(self.prev_version, &self.actions)
    }

    /// TODO(dennis): duplicated code with RegionMetaActionList::decode, try to refactor it.
    fn decode(
        bs: &[u8],
        reader_version: ProtocolVersion,
    ) -> Result<(Self, Option<ProtocolAction>), Self::Error> {
        let mut lines = BufReader::new(bs).lines();

        let mut action_list = TableMetaActionList {
            actions: Vec::default(),
            prev_version: 0,
        };

        {
            let first_line = lines
                .next()
                .with_context(|| DecodeMetaActionListSnafu {
                    msg: format!(
                        "Invalid content in manifest: {}",
                        std::str::from_utf8(bs).unwrap_or("**invalid bytes**")
                    ),
                })?
                .context(ReadlineSnafu)?;

            // Decode prev_version
            let v: VersionHeader = json::from_str(&first_line).context(DecodeJsonSnafu)?;
            action_list.prev_version = v.prev_version;
        }

        // Decode actions
        let mut protocol_action = None;
        let mut actions = Vec::default();
        for line in lines {
            let line = &line.context(ReadlineSnafu)?;
            let action: TableMetaAction = json::from_str(line).context(DecodeJsonSnafu)?;

            if let TableMetaAction::Protocol(p) = &action {
                ensure!(
                    p.is_readable(reader_version),
                    ManifestProtocolForbidReadSnafu {
                        min_version: p.min_reader_version,
                        supported_version: reader_version,
                    }
                );
                protocol_action = Some(p.clone());
            }

            actions.push(action);
        }
        action_list.actions = actions;

        Ok((action_list, protocol_action))
    }
}

#[cfg(test)]
mod tests {
    use common_telemetry::logging;

    use super::*;
    use crate::table::test_util;

    #[test]
    fn test_encode_decode_action_list() {
        common_telemetry::init_default_ut_logging();
        let mut protocol = ProtocolAction::new();
        protocol.min_reader_version = 1;

        let table_info = RawTableInfo::from(test_util::build_test_table_info());

        let mut action_list = TableMetaActionList::new(vec![
            TableMetaAction::Protocol(protocol.clone()),
            TableMetaAction::Change(Box::new(TableChange { table_info })),
        ]);
        action_list.set_prev_version(3);

        let bs = action_list.encode().unwrap();

        logging::debug!(
            "Encoded action list: \r\n{}",
            String::from_utf8(bs.clone()).unwrap()
        );

        let e = TableMetaActionList::decode(&bs, 0);
        assert!(e.is_err());
        assert_eq!(
            "Manifest protocol forbid to read, min_version: 1, supported_version: 0",
            format!("{}", e.err().unwrap())
        );

        let (decode_list, p) = TableMetaActionList::decode(&bs, 1).unwrap();
        assert_eq!(decode_list, action_list);
        assert_eq!(p.unwrap(), protocol);
    }

    // These tests are used to ensure backward compatibility of manifest files.
    // DO NOT modify the serialized string when they fail, check if your
    // modification to manifest-related structs is compatible with older manifests.
    #[test]
    fn test_table_manifest_compatibility() {
        let table_change = r#"{"table_info":{"ident":{"table_id":0,"version":0},"name":"demo","desc":null,"catalog_name":"greptime","schema_name":"public","meta":{"schema":{"column_schemas":[{"name":"host","data_type":{"String":null},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"cpu","data_type":{"Float64":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"memory","data_type":{"Float64":{}},"is_nullable":false,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"ts","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":true,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}}],"timestamp_index":3,"version":0},"primary_key_indices":[0],"value_indices":[1,2,3],"engine":"mito","next_column_id":1,"region_numbers":[],"engine_options":{},"options":{"write_buffer_size":null,"ttl":null,"extra_options":{}},"created_on":"2023-03-06T08:50:34.662020Z"},"table_type":"Base"}}"#;
        let _ = serde_json::from_str::<TableChange>(table_change).unwrap();

        let table_remove =
            r#"{"table_ident":{"table_id":42,"version":0},"table_name":"test_table"}"#;
        let _ = serde_json::from_str::<TableRemove>(table_remove).unwrap();

        let protocol_action = r#"{"min_reader_version":0,"min_writer_version":1}"#;
        let _ = serde_json::from_str::<ProtocolAction>(protocol_action).unwrap();
    }
}
