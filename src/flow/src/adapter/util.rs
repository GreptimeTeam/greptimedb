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

//! Util functions for adapter

use api::helper::ColumnDataTypeWrapper;
use common_error::ext::BoxedError;
use datatypes::prelude::ConcreteDataType;
use snafu::ResultExt;

use crate::error::{Error, ExternalSnafu};

pub fn from_proto_to_data_type(
    column_schema: &api::v1::ColumnSchema,
) -> Result<ConcreteDataType, Error> {
    let wrapper = ColumnDataTypeWrapper::try_new(
        column_schema.datatype,
        column_schema.datatype_extension.clone(),
    )
    .map_err(BoxedError::new)
    .context(ExternalSnafu)?;
    let cdt = ConcreteDataType::from(wrapper);

    Ok(cdt)
}
