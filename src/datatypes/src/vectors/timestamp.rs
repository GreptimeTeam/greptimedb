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

use crate::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use crate::vectors::{PrimitiveVector, PrimitiveVectorBuilder};

pub type TimestampSecondVector = PrimitiveVector<TimestampSecondType>;
pub type TimestampSecondVectorBuilder = PrimitiveVectorBuilder<TimestampSecondType>;

pub type TimestampMillisecondVector = PrimitiveVector<TimestampMillisecondType>;
pub type TimestampMillisecondVectorBuilder = PrimitiveVectorBuilder<TimestampMillisecondType>;

pub type TimestampMicrosecondVector = PrimitiveVector<TimestampMicrosecondType>;
pub type TimestampMicrosecondVectorBuilder = PrimitiveVectorBuilder<TimestampMicrosecondType>;

pub type TimestampNanosecondVector = PrimitiveVector<TimestampNanosecondType>;
pub type TimestampNanosecondVectorBuilder = PrimitiveVectorBuilder<TimestampNanosecondType>;
