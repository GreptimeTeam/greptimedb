use std::fmt::Debug;
use std::sync::Arc;

use datatypes::error::Result;
use datatypes::serialize::Serializable;
use time::OffsetDateTime;

pub trait MetadataId: Serializable + Debug {}

pub trait Metadata: Serializable + Debug {
    /// Returns the metadata id.
    fn id(&self) -> MetadataIdRef;
}

pub trait FileMeta: Serializable + Debug {}

#[derive(Clone, Debug)]
pub struct File {
    pub path: String,
    pub size: Option<usize>,
    pub etag: Option<String>,
    pub metadata: Option<FileMetaRef>,
    pub deleted_time: Option<OffsetDateTime>,
    pub data_change: bool,
}

impl Serializable for File {
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>> {
        unimplemented!();
    }
}

pub trait VersionEditMeta: Serializable {
    /// Returns the metadata id.
    fn id(&self) -> MetadataIdRef;
    fn files_to_add(&self) -> &[File];
    fn files_to_remove(&self) -> &[File];
}

pub type FileMetaRef = Arc<dyn FileMeta>;
pub type MetadataRef = Arc<dyn Metadata>;
pub type MetadataIdRef = Arc<dyn MetadataId>;
pub type VersionEditMetaRef = Arc<dyn VersionEditMeta>;
