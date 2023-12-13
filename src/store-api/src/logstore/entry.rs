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

/// An entry's logical id, allocated by log store users.
pub type Id = u64;
/// An entry's physical offset in the underlying log store.
pub type Offset = usize;
// TODO(niebayes): consider removing `Epoch`.
pub type Epoch = u64;

/// Entry is the minimal data storage unit in `LogStore`.
pub trait Entry: Send + Sync {
    type Error: ErrorExt + Send + Sync;
    type Namespace: Namespace;

    /// Return contained data of entry.
    fn data(&self) -> &[u8];

    /// Return entry id that monotonically increments.
    fn id(&self) -> Id;

    fn namespace(&self) -> Self::Namespace;
}
