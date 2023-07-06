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

pub trait Encoder {
    /// The type that is decoded.
    type Item;
    type Error: ErrorExt;

    /// Encodes a message into the bytes buffer.
    fn encode(&self, item: &Self::Item, dst: &mut Vec<u8>) -> Result<(), Self::Error>;
}

pub trait Decoder {
    /// The type that is decoded.
    type Item;
    type Error: ErrorExt;

    /// Decodes a message from the bytes buffer.
    fn decode(&self, src: &[u8]) -> Result<Self::Item, Self::Error>;
}
