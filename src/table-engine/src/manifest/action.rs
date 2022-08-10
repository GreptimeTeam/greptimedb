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
use store_api::manifest::ManifestVersion;
use store_api::manifest::MetaAction;
use table::metadata::{TableIdent, TableInfo};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TableChange {
    pub table_info: TableInfo,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
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
    pub fn new(actions: Vec<TableMetaAction>) -> Self {
        Self {
            actions,
            prev_version: 0,
        }
    }
}

impl MetaAction for TableMetaActionList {
    type Error = StorageError;

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
mod tests {}
