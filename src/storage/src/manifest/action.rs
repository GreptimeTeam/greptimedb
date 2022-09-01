use std::io::{BufRead, BufReader};

use serde::{Deserialize, Serialize};
use serde_json as json;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::manifest::action::{ProtocolAction, ProtocolVersion, VersionHeader};
use store_api::manifest::ManifestVersion;
use store_api::manifest::MetaAction;
use store_api::storage::RegionId;
use store_api::storage::SequenceNumber;

use crate::error::{
    self, DecodeJsonSnafu, DecodeMetaActionListSnafu, ManifestProtocolForbidReadSnafu,
    ReadlineSnafu, Result,
};
use crate::manifest::helper;
use crate::metadata::{ColumnFamilyMetadata, ColumnMetadata, VersionNumber};
use crate::sst::FileMeta;

/// Minimal data that could be used to persist and recover [RegionMetadata](crate::metadata::RegionMetadata).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawRegionMetadata {
    pub id: RegionId,
    pub name: String,
    pub columns: RawColumnsMetadata,
    pub column_families: RawColumnFamiliesMetadata,
    pub version: VersionNumber,
}

/// Minimal data that could be used to persist and recover [ColumnsMetadata](crate::metadata::ColumnsMetadata).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawColumnsMetadata {
    pub columns: Vec<ColumnMetadata>,
    pub row_key_end: usize,
    pub timestamp_key_index: usize,
    pub enable_version_column: bool,
    pub user_column_end: usize,
}

/// Minimal data that could be used to persist and recover [ColumnFamiliesMetadata](crate::metadata::ColumnFamiliesMetadata).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawColumnFamiliesMetadata {
    pub column_families: Vec<ColumnFamilyMetadata>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionChange {
    pub metadata: RawRegionMetadata,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionRemove {
    pub region_id: RegionId,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionEdit {
    pub region_version: VersionNumber,
    pub flushed_sequence: SequenceNumber,
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum RegionMetaAction {
    Protocol(ProtocolAction),
    Change(RegionChange),
    Remove(RegionRemove),
    Edit(RegionEdit),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionMetaActionList {
    pub actions: Vec<RegionMetaAction>,
    pub prev_version: ManifestVersion,
}

impl RegionMetaActionList {
    pub fn with_action(action: RegionMetaAction) -> Self {
        Self {
            actions: vec![action],
            prev_version: 0,
        }
    }

    pub fn new(actions: Vec<RegionMetaAction>) -> Self {
        Self {
            actions,
            prev_version: 0,
        }
    }
}

impl MetaAction for RegionMetaActionList {
    type Error = error::Error;

    fn set_prev_version(&mut self, version: ManifestVersion) {
        self.prev_version = version;
    }

    /// Encode self into json in the form of string lines, starts with prev_version and then action json list.
    fn encode(&self) -> Result<Vec<u8>> {
        helper::encode_actions(self.prev_version, &self.actions)
    }

    fn decode(
        bs: &[u8],
        reader_version: ProtocolVersion,
    ) -> Result<(Self, Option<ProtocolAction>)> {
        let mut lines = BufReader::new(bs).lines();

        let mut action_list = RegionMetaActionList {
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
            let action: RegionMetaAction = json::from_str(line).context(DecodeJsonSnafu)?;

            if let RegionMetaAction::Protocol(p) = &action {
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
    use crate::manifest::test_utils;

    #[test]
    fn test_encode_decode_action_list() {
        common_telemetry::init_default_ut_logging();
        let mut protocol = ProtocolAction::new();
        protocol.min_reader_version = 1;
        let mut action_list = RegionMetaActionList::new(vec![
            RegionMetaAction::Protocol(protocol.clone()),
            RegionMetaAction::Edit(test_utils::build_region_edit(
                99,
                &["test1", "test2"],
                &["test3"],
            )),
        ]);
        action_list.set_prev_version(3);

        let bs = action_list.encode().unwrap();
        // {"prev_version":3}
        // {"Protocol":{"min_reader_version":1,"min_writer_version":0}}
        // {"Edit":{"region_version":0,"flush_sequence":99,"files_to_add":[{"file_name":"test1","level":1},{"file_name":"test2","level":2}],"files_to_remove":[{"file_name":"test0","level":0}]}}

        logging::debug!(
            "Encoded action list: \r\n{}",
            String::from_utf8(bs.clone()).unwrap()
        );

        let e = RegionMetaActionList::decode(&bs, 0);
        assert!(e.is_err());
        assert_eq!(
            "Manifest protocol forbid to read, min_version: 1, supported_version: 0",
            format!("{}", e.err().unwrap())
        );

        let (decode_list, p) = RegionMetaActionList::decode(&bs, 1).unwrap();
        assert_eq!(decode_list, action_list);
        assert_eq!(p.unwrap(), protocol);
    }
}
