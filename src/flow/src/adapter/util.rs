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

use api::helper::ColumnDataTypeWrapper;
use api::v1::column_def::options_from_column_schema;
use api::v1::{ColumnDataType, ColumnDataTypeExtension, SemanticType};
use common_error::ext::BoxedError;
use datatypes::schema::ColumnSchema;
use itertools::Itertools;
use snafu::ResultExt;

use crate::error::{Error, ExternalSnafu};

/// convert `ColumnSchema` lists to it's corresponding proto type
pub fn column_schemas_to_proto(
    column_schemas: Vec<ColumnSchema>,
    primary_keys: &[String],
) -> Result<Vec<api::v1::ColumnSchema>, Error> {
    let column_datatypes: Vec<(ColumnDataType, Option<ColumnDataTypeExtension>)> = column_schemas
        .iter()
        .map(|c| {
            ColumnDataTypeWrapper::try_from(c.data_type.clone())
                .map(|w| w.to_parts())
                .map_err(BoxedError::new)
                .context(ExternalSnafu)
        })
        .try_collect()?;

    let ret = column_schemas
        .iter()
        .zip(column_datatypes)
        .map(|(schema, datatype)| {
            let semantic_type = if schema.is_time_index() {
                SemanticType::Timestamp
            } else if primary_keys.contains(&schema.name) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            } as i32;

            api::v1::ColumnSchema {
                column_name: schema.name.clone(),
                datatype: datatype.0 as i32,
                semantic_type,
                datatype_extension: datatype.1,
                options: options_from_column_schema(schema),
            }
        })
        .collect();
    Ok(ret)
}
