use crate::manifest::{MetadataIdRef, MetadataRef, VersionEditMetaRef};
pub enum MetaAction {
    Change(MetadataRef),
    Drop(MetadataIdRef),
    VersionEdit(VersionEditMetaRef),
}
