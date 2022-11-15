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

use std::sync::Arc;

use arrow::datatypes::DataType as ArrowDataType;
use serde::{Deserialize, Serialize};

use crate::data_type::{DataType, DataTypeRef};
use crate::prelude::{LogicalTypeId, Value};
use crate::scalars::ScalarVectorBuilder;
use crate::vectors::{DateTimeVectorBuilder, MutableVector};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DateTimeType;

const DATE_TIME_TYPE_NAME: &str = "DateTime";

/// [DateTimeType] represents the seconds elapsed since UNIX EPOCH.
impl DataType for DateTimeType {
    fn name(&self) -> &str {
        DATE_TIME_TYPE_NAME
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::DateTime
    }

    fn default_value(&self) -> Value {
        Value::DateTime(Default::default())
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Date64
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(DateTimeVectorBuilder::with_capacity(capacity))
    }
}

impl DateTimeType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }

    pub fn name() -> &'static str {
        DATE_TIME_TYPE_NAME
    }
}
