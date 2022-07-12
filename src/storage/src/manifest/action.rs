use serde::{Deserialize, Serialize};
use serde_json as json;
use snafu::ResultExt;
use store_api::manifest::MetaAction;
use store_api::manifest::Metadata;
use store_api::storage::RegionId;

use crate::error::{DecodeJsonSnafu, EncodeJsonSnafu, Result, Utf8Snafu};
use crate::metadata::{RegionMetadataRef, VersionNumber};
use crate::sst::FileMeta;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegionChange {
    pub metadata: RegionMetadataRef,
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
    pub files_to_remove: Option<Vec<FileMeta>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegionManifestData {
    pub region_meta: RegionMetadataRef,
    // TODO(dennis): version metadata
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RegionMetaAction {
    Change(RegionChange),
    Remove(RegionRemove),
    Edit(RegionEdit),
}

impl RegionMetaAction {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        Ok(json::to_string(self).context(EncodeJsonSnafu)?.into_bytes())
    }

    pub(crate) fn decode(bs: &[u8]) -> Result<Self> {
        json::from_str(std::str::from_utf8(bs).context(Utf8Snafu)?).context(DecodeJsonSnafu)
    }
}

impl Metadata for RegionManifestData {}

impl MetaAction for RegionMetaAction {
    type MetadataId = RegionId;

    fn metadata_id(&self) -> RegionId {
        match self {
            RegionMetaAction::Change(c) => c.metadata.id,
            RegionMetaAction::Remove(r) => r.region_id,
            RegionMetaAction::Edit(e) => e.region_id,
        }
    }
}
