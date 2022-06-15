use store_api::logstore::entry::{Id, Offset};
use store_api::logstore::AppendResponse;

mod config;
mod crc;
mod entry;
mod file;
mod file_name;
mod index;
mod log;
mod namespace;

#[derive(Debug, PartialEq, Eq)]
pub struct AppendResultImpl {
    entry_id: Id,
    offset: Offset,
}

impl AppendResponse for AppendResultImpl {
    #[inline]
    fn entry_id(&self) -> Id {
        self.entry_id
    }

    #[inline]
    fn offset(&self) -> Offset {
        self.offset
    }
}
