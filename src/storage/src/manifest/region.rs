//! Region manifest impl
use crate::manifest::action::*;
use crate::manifest::ManifestImpl;

pub type RegionManifest = ManifestImpl<RegionMetaActionList>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{backend::fs, ObjectStore};
    use store_api::manifest::action::ProtocolAction;
    use store_api::manifest::{Manifest, MetaActionIterator, MAX_VERSION};
    use tempdir::TempDir;

    use super::*;
    use crate::manifest::test_utils::*;
    use crate::metadata::RegionMetadata;

    #[tokio::test]
    async fn test_region_manifest() {
        common_telemetry::init_default_ut_logging();
        let tmp_dir = TempDir::new("test_region_manifest").unwrap();
        let object_store = ObjectStore::new(
            fs::Backend::build()
                .root(&tmp_dir.path().to_string_lossy())
                .finish()
                .await
                .unwrap(),
        );

        let manifest = RegionManifest::new("/manifest/", object_store);

        let region_meta = Arc::new(build_region_meta());

        assert!(manifest
            .scan(0, MAX_VERSION)
            .await
            .unwrap()
            .next_action()
            .await
            .unwrap()
            .is_none());

        manifest
            .update(RegionMetaActionList::with_action(RegionMetaAction::Change(
                RegionChange {
                    metadata: region_meta.as_ref().into(),
                    committed_sequence: 99,
                },
            )))
            .await
            .unwrap();

        let mut iter = manifest.scan(0, MAX_VERSION).await.unwrap();

        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(0, v);
        assert_eq!(2, action_list.actions.len());
        let protocol = &action_list.actions[0];
        assert!(matches!(
            protocol,
            RegionMetaAction::Protocol(ProtocolAction { .. })
        ));

        let action = &action_list.actions[1];

        match action {
            RegionMetaAction::Change(c) => {
                assert_eq!(
                    RegionMetadata::try_from(c.metadata.clone()).unwrap(),
                    *region_meta
                );
                assert_eq!(c.committed_sequence, 99);
            }
            _ => unreachable!(),
        }

        // Save some actions
        manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Edit(build_region_edit(1, &["f1"], &[])),
                RegionMetaAction::Edit(build_region_edit(2, &["f2", "f3"], &[])),
            ]))
            .await
            .unwrap();

        let mut iter = manifest.scan(0, MAX_VERSION).await.unwrap();
        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(0, v);
        assert_eq!(2, action_list.actions.len());
        let protocol = &action_list.actions[0];
        assert!(matches!(
            protocol,
            RegionMetaAction::Protocol(ProtocolAction { .. })
        ));

        let action = &action_list.actions[1];
        match action {
            RegionMetaAction::Change(c) => {
                assert_eq!(
                    RegionMetadata::try_from(c.metadata.clone()).unwrap(),
                    *region_meta
                );
                assert_eq!(c.committed_sequence, 99);
            }
            _ => unreachable!(),
        }

        let (v, action_list) = iter.next_action().await.unwrap().unwrap();
        assert_eq!(1, v);
        assert_eq!(2, action_list.actions.len());
        assert!(matches!(&action_list.actions[0], RegionMetaAction::Edit(_)));
        assert!(matches!(&action_list.actions[1], RegionMetaAction::Edit(_)));

        // Reach end
        assert!(iter.next_action().await.unwrap().is_none());
    }
}
