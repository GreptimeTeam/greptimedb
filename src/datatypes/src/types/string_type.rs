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
use common_base::bytes::StringBytes;
use serde::{Deserialize, Serialize};

use crate::data_type::DataType;
use crate::prelude::{DataTypeRef, LogicalTypeId, Value};
use crate::scalars::ScalarVectorBuilder;
use crate::vectors::{MutableVector, StringVectorBuilder};

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StringType;

impl StringType {
    pub fn arc() -> DataTypeRef {
        Arc::new(Self)
    }
}

impl DataType for StringType {
    fn name(&self) -> &str {
        "String"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::String
    }

    fn default_value(&self) -> Value {
        StringBytes::default().into()
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        ArrowDataType::Utf8
    }

    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector> {
        Box::new(StringVectorBuilder::with_capacity(capacity))
    }
}
