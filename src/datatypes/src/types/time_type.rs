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

//! TimeType represents the elapsed time since midnight in the unit of `TimeUnit`.

use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use common_time::timestamp::TimeUnit;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::scalars::ScalarVectorBuilder;
use crate::type_id::LogicalTypeId;
use crate::value::Value;
use crate::vectors::{BooleanVectorBuilder, MutableVector};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeType(TimeUnit);

impl TimeType {
    pub fn from_unit(unit: TimeUnit) -> Self {
        Self(unit)
    }

    pub fn unit(&self) -> &TimeUnit {
        &self.0
    }
}

impl DataType for TimeType {
    fn name(&self) -> &str {
        "Time"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::Time
    }

    fn default_value(&self) -> Value {
        todo!();
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Time64(self.0.as_arrow_time_unit())
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        todo!();
    }

    fn is_timestamp_compatible(&self) -> bool {
        false
    }
}
