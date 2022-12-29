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

use std::hash::{Hash, Hasher};

use common_base::buffer::{Buffer, BufferMut};
use protobuf::Message;
use snafu::ResultExt;
use store_api::logstore::entry::{Encode, Entry as EntryTrait, Id};
use store_api::logstore::namespace::Namespace as NamespaceTrait;

use crate::error::{DecodeProtobufSnafu, EncodeProtobufSnafu, EncodeSnafu, Error};
use crate::raft_engine::protos::logstore::{Entry, Namespace};

pub mod log_store;

pub mod protos {
    include!(concat!(env!("OUT_DIR"), concat!("/", "protos/", "mod.rs")));
}

impl Hash for Namespace {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl NamespaceTrait for Namespace {
    fn id(&self) -> store_api::logstore::namespace::Id {
        self.id
    }
}

impl Encode for Entry {
    type Error = Error;

    fn encode_to<T: BufferMut>(&self, buf: &mut T) -> Result<usize, Self::Error> {
        let bytes = self.write_to_bytes().context(EncodeProtobufSnafu)?;
        buf.write_from_slice(&bytes).context(EncodeSnafu)?;
        Ok(bytes.len())
    }

    fn decode<T: Buffer>(buf: &mut T) -> Result<Self, Self::Error> {
        let mut bytes = Vec::with_capacity(buf.remaining_size());
        buf.read_to_slice(&mut bytes).context(EncodeSnafu)?;
        Entry::parse_from_bytes(&bytes).context(DecodeProtobufSnafu)
    }

    fn encoded_size(&self) -> usize {
        self.compute_size() as usize
    }
}

impl EntryTrait for Entry {
    type Error = Error;
    type Namespace = Namespace;

    fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    fn id(&self) -> Id {
        self.id
    }

    fn set_id(&mut self, id: Id) {
        self.id = id
    }

    fn namespace(&self) -> Self::Namespace {
        Namespace {
            id: self.namespace_id,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_engine::protos::logstore::LogStoreState;

    #[test]
    fn test_protos() {}
}
