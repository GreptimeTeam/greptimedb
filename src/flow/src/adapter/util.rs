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
use datatypes::prelude::ConcreteDataType;
use common_meta::key::table_info::TableInfoValue;
use datatypes::schema::ColumnSchema;
use itertools::Itertools;
use snafu::ResultExt;

use crate::error::{Error, ExternalSnafu};
use crate::repr::{ColumnType, RelationDesc, RelationType};

pub fn table_info_value_to_relation_desc(
    table_info_value: TableInfoValue,
) -> Result<RelationDesc, Error> {
    let raw_schema = table_info_value.table_info.meta.schema;
    let (column_types, col_names): (Vec<_>, Vec<_>) = raw_schema
        .column_schemas
        .clone()
        .into_iter()
        .map(|col| {
            (
                ColumnType {
                    nullable: col.is_nullable(),
                    scalar_type: col.data_type,
                },
                Some(col.name),
            )
        })
        .unzip();

    let key = table_info_value.table_info.meta.primary_key_indices;
    let keys = vec![crate::repr::Key::from(key)];

    let time_index = raw_schema.timestamp_index;

    Ok(RelationDesc {
        typ: RelationType {
            column_types,
            keys,
            time_index,
            // by default table schema's column are all non-auto
            auto_columns: vec![],
        },
        names: col_names,
    })
}

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
