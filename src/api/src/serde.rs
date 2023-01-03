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

pub use prost::DecodeError;
use prost::Message;

use crate::v1::meta::TableRouteValue;

macro_rules! impl_convert_with_bytes {
    ($data_type: ty) => {
        impl From<$data_type> for Vec<u8> {
            fn from(entity: $data_type) -> Self {
                entity.encode_to_vec()
            }
        }

        impl TryFrom<&[u8]> for $data_type {
            type Error = DecodeError;

            fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
                <$data_type>::decode(value.as_ref())
            }
        }
    };
}

impl_convert_with_bytes!(TableRouteValue);
