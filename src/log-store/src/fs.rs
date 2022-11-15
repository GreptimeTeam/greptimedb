// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use store_api::logstore::entry::{Id, Offset};
use store_api::logstore::AppendResponse;

mod chunk;
pub mod config;
mod crc;
mod entry;
mod file;
mod file_name;
mod index;
mod io;
pub mod log;
mod namespace;
pub mod noop;

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
