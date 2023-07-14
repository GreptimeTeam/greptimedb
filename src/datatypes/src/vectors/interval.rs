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

use super::{PrimitiveVector, PrimitiveVectorBuilder};
use crate::types::{IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType};

pub type IntervalYearMonthVector = PrimitiveVector<IntervalYearMonthType>;
pub type IntervalYearMonthVectorBuilder = PrimitiveVectorBuilder<IntervalYearMonthType>;

pub type IntervalMonthDayNanoVector = PrimitiveVector<IntervalMonthDayNanoType>;
pub type IntervalMonthDayNanoVectorBuilder = PrimitiveVectorBuilder<IntervalMonthDayNanoType>;

pub type IntervalDayTimeVector = PrimitiveVector<IntervalDayTimeType>;
pub type IntervalDayTimeVectorBuilder = PrimitiveVectorBuilder<IntervalDayTimeType>;
