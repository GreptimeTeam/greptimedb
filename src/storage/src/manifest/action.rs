use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json as json;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::manifest::action::ProtocolAction;
use store_api::manifest::action::ProtocolVersion;
use store_api::manifest::ManifestVersion;
use store_api::manifest::MetaAction;
use store_api::manifest::Metadata;
use store_api::storage::RegionId;
use store_api::storage::SequenceNumber;

use crate::error::{
    DecodeJsonSnafu, DecodeRegionMetaActionListSnafu, EncodeJsonSnafu,
    ManifestProtocolForbideReadSnafu, Result, Utf8Snafu,
};
use crate::metadata::{RegionMetadataRef, VersionNumber};
use crate::sst::FileMeta;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionChange {
    pub metadata: RegionMetadataRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionRemove {
    pub region_id: RegionId,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionEdit {
    pub region_id: RegionId,
    pub region_version: VersionNumber,
    pub flush_sequence: SequenceNumber,
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionManifestData {
    pub region_meta: RegionMetadataRef,
    // TODO(dennis): version metadata
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RegionMetaActionList {
    pub actions: Vec<RegionMetaAction>,
    pub prev_version: ManifestVersion,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum RegionMetaAction {
    Protocol(ProtocolAction),
    Change(RegionChange),
    Remove(RegionRemove),
    Edit(RegionEdit),
}

const NEW_LINE: &str = "\r\n";
const PREV_VERSION_KEY: &str = "prev_version";

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

    /// Encode self into json in the form of string lines, starts with prev_version and then action json list.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut s = String::default();

        {
            // Encode prev_version
            let mut m = HashMap::new();
            m.insert(PREV_VERSION_KEY.to_string(), self.prev_version);
            s.push_str(&json::to_string(&m).context(EncodeJsonSnafu)?);
            s.push_str(NEW_LINE);
        }

        // Encode actions
        let action_strings = self
            .actions
            .iter()
            .map(|action| json::to_string(action).context(EncodeJsonSnafu))
            .collect::<Result<Vec<_>>>();
        s.push_str(&action_strings?.join(NEW_LINE));

        Ok(s.into_bytes())
    }

    pub(crate) fn decode(
        bs: &[u8],
        reader_version: ProtocolVersion,
    ) -> Result<(Self, Option<ProtocolAction>)> {
        let s = std::str::from_utf8(bs).context(Utf8Snafu)?;

        let lines: Vec<&str> = s.split(NEW_LINE).collect();

        ensure!(
            lines.len() > 1,
            DecodeRegionMetaActionListSnafu {
                msg: format!("invalid content: {}", s),
            }
        );

        let mut action_list = RegionMetaActionList {
            actions: Vec::default(),
            prev_version: 0,
        };

        {
            // Decode prev_version
            let m: HashMap<String, ManifestVersion> =
                json::from_str(lines[0]).context(DecodeJsonSnafu)?;
            action_list.prev_version =
                *m.get(PREV_VERSION_KEY)
                    .context(DecodeRegionMetaActionListSnafu {
                        msg: "missing prev_version",
                    })?;
        }

        // Decode actions
        let mut protocol_action = None;
        let mut actions = Vec::with_capacity(lines.len() - 1);
        for line in lines.iter().skip(1) {
            let action: RegionMetaAction = json::from_str(line).context(DecodeJsonSnafu)?;

            if let RegionMetaAction::Protocol(p) = &action {
                ensure!(
                    reader_version >= p.min_reader_version,
                    ManifestProtocolForbideReadSnafu {
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

impl Metadata for RegionManifestData {}

impl MetaAction for RegionMetaActionList {
    fn set_prev_version(&mut self, version: ManifestVersion) {
        self.prev_version = version;
    }
}

#[cfg(test)]
mod tests {
    use common_telemetry::logging;

    use super::*;

    #[test]
    fn test_encode_decode_action_list() {
        common_telemetry::init_default_ut_logging();
        let mut protocol = ProtocolAction::new();
        protocol.min_reader_version = 1;
        let mut action_list = RegionMetaActionList::new(vec![
            RegionMetaAction::Protocol(protocol.clone()),
            RegionMetaAction::Edit(RegionEdit {
                region_id: 1,
                region_version: 10,
                flush_sequence: 99,
                files_to_add: vec![
                    FileMeta {
                        file_path: "test1".to_string(),
                        level: 1,
                    },
                    FileMeta {
                        file_path: "test2".to_string(),
                        level: 2,
                    },
                ],
                files_to_remove: vec![FileMeta {
                    file_path: "test0".to_string(),
                    level: 0,
                }],
            }),
        ]);
        action_list.set_prev_version(3);

        let bs = action_list.encode().unwrap();
        // {"prev_version":3}
        // {"Protocol":{"min_reader_version":1,"min_writer_version":0}}
        // {"Edit":{"region_id":1,"region_version":10,"flush_sequence":99,"files_to_add":[{"file_path":"test1","level":1},{"file_path":"test2","level":2}],"files_to_remove":[{"file_path":"test0","level":0}]}}

        logging::debug!(
            "Encoded action list: \r\n{}",
            String::from_utf8(bs.clone()).unwrap()
        );

        let e = RegionMetaActionList::decode(&bs, 0);
        assert!(e.is_err());
        assert_eq!(
            "Manifest protocol forbide to read, min_version: 1, supported_version: 0",
            format!("{}", e.err().unwrap())
        );

        let (decode_list, p) = RegionMetaActionList::decode(&bs, 1).unwrap();
        assert_eq!(decode_list, action_list);
        assert_eq!(p.unwrap(), protocol);
    }
}
