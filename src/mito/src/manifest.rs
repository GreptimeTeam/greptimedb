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

//! Table manifest service
pub mod action;

use storage::manifest::ManifestImpl;
use store_api::manifest::action::{ProtocolAction, ProtocolVersion};
use store_api::manifest::{Checkpoint, ManifestVersion};

use crate::manifest::action::TableMetaActionList;

#[derive(Debug, Clone)]
pub struct NoopCheckpoint {}

impl Checkpoint for NoopCheckpoint {
    type Error = storage::error::Error;

    fn set_protocol(&mut self, _action: ProtocolAction) {}

    fn last_version(&self) -> ManifestVersion {
        unreachable!();
    }

    fn encode(&self) -> Result<Vec<u8>, Self::Error> {
        unreachable!();
    }

    fn decode(_bs: &[u8], _reader_version: ProtocolVersion) -> Result<Self, Self::Error> {
        unreachable!();
    }
}

pub type TableManifest = ManifestImpl<NoopCheckpoint, TableMetaActionList>;

#[cfg(test)]
mod tests {
    use storage::manifest::MetaActionIteratorImpl;
    use store_api::manifest::action::ProtocolAction;
    use store_api::manifest::{Manifest, MetaActionIterator};
    use table::metadata::{RawTableInfo, TableInfo};

    use super::*;
    use crate::manifest::action::{TableChange, TableMetaAction, TableRemove};
    use crate::table::test_util;
    type TableManifestActionIter = MetaActionIteratorImpl<TableMetaActionList>;

    async fn assert_actions(
        iter: &mut TableManifestActionIter,
        protocol: &ProtocolAction,
        table_info: &TableInfo,
    ) {
        match iter.next_action().await.unwrap() {
            Some((v, action_list)) => {
                assert_eq!(v, 0);
                assert_eq!(2, action_list.actions.len());
                assert!(
                    matches!(&action_list.actions[0], TableMetaAction::Protocol(p) if *p == *protocol)
                );
                assert!(
                    matches!(&action_list.actions[1], TableMetaAction::Change(c) if TableInfo::try_from(c.table_info.clone()).unwrap() == *table_info)
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_table_manifest() {
        let (_dir, object_store) = test_util::new_test_object_store("test_table_manifest").await;

        let manifest = TableManifest::create("manifest/", object_store);

        let mut iter = manifest.scan(0, 100).await.unwrap();
        assert!(iter.next_action().await.unwrap().is_none());

        let protocol = ProtocolAction::new();
        let table_info = test_util::build_test_table_info();
        let action_list =
            TableMetaActionList::new(vec![TableMetaAction::Change(Box::new(TableChange {
                table_info: RawTableInfo::from(table_info.clone()),
            }))]);

        assert_eq!(0, manifest.update(action_list).await.unwrap());

        let mut iter = manifest.scan(0, 100).await.unwrap();
        assert_actions(&mut iter, &protocol, &table_info).await;
        assert!(iter.next_action().await.unwrap().is_none());

        // update another action
        let action_list = TableMetaActionList::new(vec![TableMetaAction::Remove(TableRemove {
            table_name: table_info.name.clone(),
            table_ident: table_info.ident.clone(),
        })]);
        assert_eq!(1, manifest.update(action_list).await.unwrap());
        let mut iter = manifest.scan(0, 100).await.unwrap();
        assert_actions(&mut iter, &protocol, &table_info).await;

        match iter.next_action().await.unwrap() {
            Some((v, action_list)) => {
                assert_eq!(v, 1);
                assert_eq!(1, action_list.actions.len());
                assert!(matches!(&action_list.actions[0],
                                 TableMetaAction::Remove(r) if r.table_name == table_info.name && r.table_ident == table_info.ident));
            }
            _ => unreachable!(),
        }
    }
}
