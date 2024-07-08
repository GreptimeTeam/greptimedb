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

use std::collections::HashMap;

use datatypes::schema::{
    ColumnDefaultConstraint, ColumnSchema, FulltextOptions, COMMENT_KEY, FULLTEXT_KEY,
};
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::helper::ColumnDataTypeWrapper;
use crate::v1::{ColumnDef, ColumnOptions, SemanticType};

/// Key used to store fulltext options in gRPC column options.
const FULLTEXT_GRPC_KEY: &str = "fulltext";

pub fn try_as_column_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
    let data_type = ColumnDataTypeWrapper::try_new(
        column_def.data_type,
        column_def.datatype_extension.clone(),
    )?;

    let constraint = if column_def.default_constraint.is_empty() {
        None
    } else {
        Some(
            ColumnDefaultConstraint::try_from(column_def.default_constraint.as_slice()).context(
                error::ConvertColumnDefaultConstraintSnafu {
                    column: &column_def.name,
                },
            )?,
        )
    };

    let mut metadata = HashMap::new();
    if !column_def.comment.is_empty() {
        metadata.insert(COMMENT_KEY.to_string(), column_def.comment.clone());
    }
    if let Some(options) = column_def.options.as_ref()
        && let Some(fulltext) = options.options.get(FULLTEXT_GRPC_KEY)
    {
        metadata.insert(FULLTEXT_KEY.to_string(), fulltext.to_string());
    }

    ColumnSchema::new(&column_def.name, data_type.into(), column_def.is_nullable)
        .with_metadata(metadata)
        .with_time_index(column_def.semantic_type() == SemanticType::Timestamp)
        .with_default_constraint(constraint)
        .context(error::InvalidColumnDefaultConstraintSnafu {
            column: &column_def.name,
        })
}

pub fn options_from_column_schema(column_schema: &ColumnSchema) -> Option<ColumnOptions> {
    let mut options = ColumnOptions::default();
    if let Some(fulltext) = column_schema.metadata().get(FULLTEXT_KEY) {
        options
            .options
            .insert(FULLTEXT_GRPC_KEY.to_string(), fulltext.to_string());
    }

    (!options.options.is_empty()).then_some(options)
}

pub fn contains_fulltext(options: &Option<ColumnOptions>) -> bool {
    options
        .as_ref()
        .map_or(false, |o| o.options.contains_key(FULLTEXT_GRPC_KEY))
}

pub fn options_with_fulltext(fulltext: &FulltextOptions) -> Result<Option<ColumnOptions>> {
    let mut options = ColumnOptions::default();

    let v = serde_json::to_string(fulltext).context(error::SerializeJsonSnafu)?;
    options.options.insert(FULLTEXT_GRPC_KEY.to_string(), v);

    Ok((!options.options.is_empty()).then_some(options))
}
