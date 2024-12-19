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
    ColumnDefaultConstraint, ColumnSchema, FulltextAnalyzer, FulltextOptions, COMMENT_KEY,
    FULLTEXT_KEY, INVERTED_INDEX_KEY, SKIPPING_INDEX_KEY,
};
use greptime_proto::v1::Analyzer;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::helper::ColumnDataTypeWrapper;
use crate::v1::{ColumnDef, ColumnOptions, SemanticType};

/// Key used to store fulltext options in gRPC column options.
const FULLTEXT_GRPC_KEY: &str = "fulltext";
/// Key used to store inverted index options in gRPC column options.
const INVERTED_INDEX_GRPC_KEY: &str = "inverted_index";
/// Key used to store skip index options in gRPC column options.
const SKIPPING_INDEX_GRPC_KEY: &str = "skipping_index";

/// Tries to construct a `ColumnSchema` from the given  `ColumnDef`.
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
    if let Some(options) = column_def.options.as_ref() {
        if let Some(fulltext) = options.options.get(FULLTEXT_GRPC_KEY) {
            metadata.insert(FULLTEXT_KEY.to_string(), fulltext.clone());
        }
        if let Some(inverted_index) = options.options.get(INVERTED_INDEX_GRPC_KEY) {
            metadata.insert(INVERTED_INDEX_KEY.to_string(), inverted_index.clone());
        }
        if let Some(skipping_index) = options.options.get(SKIPPING_INDEX_GRPC_KEY) {
            metadata.insert(SKIPPING_INDEX_KEY.to_string(), skipping_index.clone());
        }
    }

    ColumnSchema::new(&column_def.name, data_type.into(), column_def.is_nullable)
        .with_metadata(metadata)
        .with_time_index(column_def.semantic_type() == SemanticType::Timestamp)
        .with_default_constraint(constraint)
        .context(error::InvalidColumnDefaultConstraintSnafu {
            column: &column_def.name,
        })
}

/// Constructs a `ColumnOptions` from the given `ColumnSchema`.
pub fn options_from_column_schema(column_schema: &ColumnSchema) -> Option<ColumnOptions> {
    let mut options = ColumnOptions::default();
    if let Some(fulltext) = column_schema.metadata().get(FULLTEXT_KEY) {
        options
            .options
            .insert(FULLTEXT_GRPC_KEY.to_string(), fulltext.clone());
    }
    if let Some(inverted_index) = column_schema.metadata().get(INVERTED_INDEX_KEY) {
        options
            .options
            .insert(INVERTED_INDEX_GRPC_KEY.to_string(), inverted_index.clone());
    }
    if let Some(skipping_index) = column_schema.metadata().get(SKIPPING_INDEX_KEY) {
        options
            .options
            .insert(SKIPPING_INDEX_GRPC_KEY.to_string(), skipping_index.clone());
    }

    (!options.options.is_empty()).then_some(options)
}

/// Checks if the `ColumnOptions` contains fulltext options.
pub fn contains_fulltext(options: &Option<ColumnOptions>) -> bool {
    options
        .as_ref()
        .map_or(false, |o| o.options.contains_key(FULLTEXT_GRPC_KEY))
}

/// Tries to construct a `ColumnOptions` from the given `FulltextOptions`.
pub fn options_from_fulltext(fulltext: &FulltextOptions) -> Result<Option<ColumnOptions>> {
    let mut options = ColumnOptions::default();

    let v = serde_json::to_string(fulltext).context(error::SerializeJsonSnafu)?;
    options.options.insert(FULLTEXT_GRPC_KEY.to_string(), v);

    Ok((!options.options.is_empty()).then_some(options))
}

/// Tries to construct a `FulltextAnalyzer` from the given analyzer.
pub fn as_fulltext_option(analyzer: Analyzer) -> FulltextAnalyzer {
    match analyzer {
        Analyzer::English => FulltextAnalyzer::English,
        Analyzer::Chinese => FulltextAnalyzer::Chinese,
    }
}

#[cfg(test)]
mod tests {

    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::FulltextAnalyzer;

    use super::*;
    use crate::v1::ColumnDataType;

    #[test]
    fn test_try_as_column_schema() {
        let column_def = ColumnDef {
            name: "test".to_string(),
            data_type: ColumnDataType::String as i32,
            is_nullable: true,
            default_constraint: ColumnDefaultConstraint::Value("test_default".into())
                .try_into()
                .unwrap(),
            semantic_type: SemanticType::Field as i32,
            comment: "test_comment".to_string(),
            datatype_extension: None,
            options: Some(ColumnOptions {
                options: HashMap::from([
                    (
                        FULLTEXT_GRPC_KEY.to_string(),
                        "{\"enable\":true}".to_string(),
                    ),
                    (INVERTED_INDEX_GRPC_KEY.to_string(), "true".to_string()),
                ]),
            }),
        };

        let schema = try_as_column_schema(&column_def).unwrap();
        assert_eq!(schema.name, "test");
        assert_eq!(schema.data_type, ConcreteDataType::string_datatype());
        assert!(!schema.is_time_index());
        assert!(schema.is_nullable());
        assert_eq!(
            schema.default_constraint().unwrap(),
            &ColumnDefaultConstraint::Value("test_default".into())
        );
        assert_eq!(schema.metadata().get(COMMENT_KEY).unwrap(), "test_comment");
        assert_eq!(
            schema.fulltext_options().unwrap().unwrap(),
            FulltextOptions {
                enable: true,
                ..Default::default()
            }
        );
        assert!(schema.is_inverted_indexed());
    }

    #[test]
    fn test_options_from_column_schema() {
        let schema = ColumnSchema::new("test", ConcreteDataType::string_datatype(), true);
        let options = options_from_column_schema(&schema);
        assert!(options.is_none());

        let schema = ColumnSchema::new("test", ConcreteDataType::string_datatype(), true)
            .with_fulltext_options(FulltextOptions {
                enable: true,
                analyzer: FulltextAnalyzer::English,
                case_sensitive: false,
            })
            .unwrap()
            .set_inverted_index(true);
        let options = options_from_column_schema(&schema).unwrap();
        assert_eq!(
            options.options.get(FULLTEXT_GRPC_KEY).unwrap(),
            "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false}"
        );
        assert_eq!(
            options.options.get(INVERTED_INDEX_GRPC_KEY).unwrap(),
            "true"
        );
    }

    #[test]
    fn test_options_with_fulltext() {
        let fulltext = FulltextOptions {
            enable: true,
            analyzer: FulltextAnalyzer::English,
            case_sensitive: false,
        };
        let options = options_from_fulltext(&fulltext).unwrap().unwrap();
        assert_eq!(
            options.options.get(FULLTEXT_GRPC_KEY).unwrap(),
            "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false}"
        );
    }

    #[test]
    fn test_contains_fulltext() {
        let options = ColumnOptions {
            options: HashMap::from([(
                FULLTEXT_GRPC_KEY.to_string(),
                "{\"enable\":true}".to_string(),
            )]),
        };
        assert!(contains_fulltext(&Some(options)));

        let options = ColumnOptions {
            options: HashMap::new(),
        };
        assert!(!contains_fulltext(&Some(options)));

        assert!(!contains_fulltext(&None));
    }
}
