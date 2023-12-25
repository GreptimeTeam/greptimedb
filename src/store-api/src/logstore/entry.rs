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

use common_error::ext::ErrorExt;

use crate::logstore::namespace::Namespace;

/// An entry's id.
/// Different log store implementations may interpret the id to different meanings.
pub type Id = u64;

/// Entry is the minimal data storage unit through which users interact with the log store.
/// The log store implementation may have larger or smaller data storage unit than an entry.
pub trait Entry: Send + Sync {
    type Error: ErrorExt + Send + Sync;
    type Namespace: Namespace;

    /// Returns the contained data of the entry.
    fn data(&self) -> &[u8];

    /// Returns the id of the entry.
    /// Usually the namespace id is identical with the region id.
    fn id(&self) -> Id;

    /// Returns the namespace of the entry.
    fn namespace(&self) -> Self::Namespace;
}
