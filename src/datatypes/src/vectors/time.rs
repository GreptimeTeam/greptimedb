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

use crate::types::{TimeMicrosecondType, TimeMillisecondType, TimeNanosecondType, TimeSecondType};
use crate::vectors::{PrimitiveVector, PrimitiveVectorBuilder};

pub type TimeSecondVector = PrimitiveVector<TimeSecondType>;
pub type TimeSecondVectorBuilder = PrimitiveVectorBuilder<TimeSecondType>;

pub type TimeMillisecondVector = PrimitiveVector<TimeMillisecondType>;
pub type TimeMillisecondVectorBuilder = PrimitiveVectorBuilder<TimeMillisecondType>;

pub type TimeMicrosecondVector = PrimitiveVector<TimeMicrosecondType>;
pub type TimeMicrosecondVectorBuilder = PrimitiveVectorBuilder<TimeMicrosecondType>;

pub type TimeNanosecondVector = PrimitiveVector<TimeNanosecondType>;
pub type TimeNanosecondVectorBuilder = PrimitiveVectorBuilder<TimeNanosecondType>;
