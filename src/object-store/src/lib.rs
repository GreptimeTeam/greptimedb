// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub use opendal::raw::{normalize_path as raw_normalize_path, HttpClient};
pub use opendal::{
    services, Builder as ObjectStoreBuilder, Entry, EntryMode, Error, ErrorKind, Lister, Metakey,
    Operator as ObjectStore, Reader, Result, Writer,
};

pub mod layers;
pub mod manager;
mod metrics;
pub mod test_util;
pub mod util;
