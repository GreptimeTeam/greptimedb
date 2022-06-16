use std::collections::BTreeMap;
use std::sync::RwLock;

use store_api::logstore::entry::{Id, Offset};

use crate::error::Result;
use crate::fs::file_name::FileName;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Location {
    pub file_name: FileName,
    pub offset: Offset,
}

#[allow(dead_code)]
impl Location {
    pub fn new(file_name: FileName, offset: Offset) -> Self {
        Self { file_name, offset }
    }
}

/// In-memory entry id to offset index.
pub trait EntryIndex {
    /// Add entry id to offset mapping.
    fn add_entry_id(&self, id: Id, loc: Location) -> Option<Location>;

    /// Find offset by entry id.
    fn find_offset_by_id(&self, id: Id) -> Result<Option<Location>>;
}

pub struct MemoryIndex {
    map: RwLock<BTreeMap<Id, Location>>,
}

#[allow(dead_code)]
impl MemoryIndex {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
        }
    }
}

impl EntryIndex for MemoryIndex {
    fn add_entry_id(&self, id: Id, loc: Location) -> Option<Location> {
        self.map.write().unwrap().insert(id, loc)
    }

    fn find_offset_by_id(&self, id: Id) -> Result<Option<Location>> {
        Ok(self.map.read().unwrap().get(&id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_entry() {
        let index = MemoryIndex::new();
        index.add_entry_id(1, Location::new(FileName::log(0), 1));
        assert_eq!(
            Location::new(FileName::log(0), 1),
            index.find_offset_by_id(1).unwrap().unwrap()
        );
        assert_eq!(None, index.find_offset_by_id(2).unwrap());
    }
}
