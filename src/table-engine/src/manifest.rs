//! Table manifest service
pub mod action;

use storage::manifest::ManifestImpl;

use crate::manifest::action::TableMetaActionList;

pub type TableManifest = ManifestImpl<TableMetaActionList>;

#[cfg(test)]
mod tests {
    use storage::manifest::MetaActionIteratorImpl;
    use store_api::manifest::action::ProtocolAction;
    use store_api::manifest::{Manifest, MetaActionIterator};
    use table::metadata::TableInfo;

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
                    matches!(&action_list.actions[1], TableMetaAction::Change(c) if c.table_info == *table_info)
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_table_manifest() {
        let (_dir, object_store) = test_util::new_test_object_store("test_table_manifest").await;

        let manifest = TableManifest::new("manifest/", object_store);

        let mut iter = manifest.scan(0, 100).await.unwrap();
        assert!(iter.next_action().await.unwrap().is_none());

        let protocol = ProtocolAction::new();
        let table_info = test_util::build_test_table_info();
        let action_list =
            TableMetaActionList::new(vec![TableMetaAction::Change(Box::new(TableChange {
                table_info: table_info.clone(),
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
