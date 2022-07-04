use store_api::logstore::entry::{Id, Offset};
use store_api::logstore::AppendResponse;

pub mod config;
mod crc;
mod entry;
mod file;
mod file_name;
mod index;
pub mod log;
mod namespace;

#[derive(Debug, PartialEq, Eq)]
pub struct AppendResponseImpl {
    entry_id: Id,
    offset: Offset,
}

impl AppendResponse for AppendResponseImpl {
    #[inline]
    fn entry_id(&self) -> Id {
        self.entry_id
    }

    #[inline]
    fn offset(&self) -> Offset {
        self.offset
    }
}
