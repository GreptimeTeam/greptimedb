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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TableChange {
    pub table_info: RawTableInfo,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TableRemove {
    pub table_ident: TableIdent,
    pub table_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum TableMetaAction {
    Protocol(ProtocolAction),
    // Boxed TableChange to reduce the total size of enum
    Change(Box<TableChange>),
    Remove(TableRemove),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
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
}
