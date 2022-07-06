use serde::{Deserialize, Serialize};
use store_api::manifest::MetaAction;
use store_api::manifest::Metadata;
use store_api::storage::RegionId;

use crate::metadata::{RegionMetaImpl, VersionNumber};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileMeta {
    path: String,
    // metadata: ObjectMetadata,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegionChange {
    pub metadata: RegionMetaImpl,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegionRemove {
    pub region_id: RegionId,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegionEdit {
    pub region_id: RegionId,
    pub region_version: VersionNumber,
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegionMetaAction {
    Change(RegionChange),
    Remove(RegionRemove),
    Edit(RegionEdit),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegionManifestData {
    pub region_meta: RegionMetaImpl,
    //TODO(dennis): version metadata
}

impl Metadata for RegionManifestData {}

impl MetaAction for RegionMetaAction {
    type MetadataId = RegionId;
    fn metadata_id(&self) -> &RegionId {
        match self {
            RegionMetaAction::Change(c) => &c.metadata.metadata.id,
            RegionMetaAction::Remove(r) => &r.region_id,
            RegionMetaAction::Edit(e) => &e.region_id,
        }
    }
}
