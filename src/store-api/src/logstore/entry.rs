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

use common_base::buffer::{Buffer, BufferMut};
use common_error::ext::ErrorExt;

use crate::logstore::namespace::Namespace;

pub type Offset = usize;
pub type Epoch = u64;
pub type Id = u64;

/// Entry is the minimal data storage unit in `LogStore`.
pub trait Entry: Encode + Send + Sync {
    type Error: ErrorExt + Send + Sync;
    type Namespace: Namespace;

    /// Return contained data of entry.
    fn data(&self) -> &[u8];

    /// Return entry id that monotonically increments.
    fn id(&self) -> Id;

    /// Return file offset of entry.
    fn offset(&self) -> Offset;

    fn set_id(&mut self, id: Id);

    /// Returns epoch of entry.
    fn epoch(&self) -> Epoch;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn namespace(&self) -> Self::Namespace;
}

pub trait Encode: Sized {
    type Error: ErrorExt + Send + Sync + 'static;

    /// Encodes item to given byte slice and return encoded size on success;
    /// # Panics
    /// If given buffer is not large enough to hold the encoded item.
    fn encode_to<T: BufferMut>(&self, buf: &mut T) -> Result<usize, Self::Error>;

    /// Decodes item from given buffer.
    fn decode<T: Buffer>(buf: &mut T) -> Result<Self, Self::Error>;

    /// Return the size in bytes of the encoded item;
    fn encoded_size(&self) -> usize;
}
