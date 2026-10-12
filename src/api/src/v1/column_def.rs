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

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datatypes::schema::{
    COMMENT_KEY, ColumnDefaultConstraint, ColumnSchema, FULLTEXT_KEY, FulltextAnalyzer,
    FulltextBackend, FulltextOptions, INVERTED_INDEX_KEY, Metadata, SKIPPING_INDEX_KEY,
    SkippingIndexOptions, SkippingIndexType,
};
use greptime_proto::v1::{
    Analyzer, FulltextBackend as PbFulltextBackend, SkippingIndexType as PbSkippingIndexType,
};
use snafu::ResultExt;

use crate::error::{self, ConvertColumnDefaultConstraintSnafu, Result};
use crate::helper::ColumnDataTypeWrapper;
use crate::v1::{ColumnDef, ColumnOptions, SemanticType};

/// Key used to store fulltext options in gRPC column options.
const FULLTEXT_GRPC_KEY: &str = "fulltext";
/// Key used to store inverted index options in gRPC column options.
const INVERTED_INDEX_GRPC_KEY: &str = "inverted_index";
/// Key used to store skip index options in gRPC column options.
const SKIPPING_INDEX_GRPC_KEY: &str = "skipping_index";

const COLUMN_OPTION_MAPPINGS: [(&str, &str); 5] = [
    (FULLTEXT_GRPC_KEY, FULLTEXT_KEY),
    (INVERTED_INDEX_GRPC_KEY, INVERTED_INDEX_KEY),
    (SKIPPING_INDEX_GRPC_KEY, SKIPPING_INDEX_KEY),
    (EXTENSION_TYPE_NAME_KEY, EXTENSION_TYPE_NAME_KEY),
    (EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_METADATA_KEY),
];

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
            metadata.insert(FULLTEXT_KEY.to_string(), fulltext.to_owned());
        }
        if let Some(inverted_index) = options.options.get(INVERTED_INDEX_GRPC_KEY) {
            metadata.insert(INVERTED_INDEX_KEY.to_string(), inverted_index.to_owned());
        }
        if let Some(skipping_index) = options.options.get(SKIPPING_INDEX_GRPC_KEY) {
            metadata.insert(SKIPPING_INDEX_KEY.to_string(), skipping_index.to_owned());
        }
        if let Some(extension_name) = options.options.get(EXTENSION_TYPE_NAME_KEY) {
            metadata.insert(EXTENSION_TYPE_NAME_KEY.to_string(), extension_name.clone());
        }
        if let Some(extension_metadata) = options.options.get(EXTENSION_TYPE_METADATA_KEY) {
            metadata.insert(
                EXTENSION_TYPE_METADATA_KEY.to_string(),
                extension_metadata.clone(),
            );
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

/// Tries to construct a `ColumnDef` from the given `ColumnSchema`.
///
/// TODO(weny): Add tests for this function.
pub fn try_as_column_def(column_schema: &ColumnSchema, is_primary_key: bool) -> Result<ColumnDef> {
    let column_datatype =
        ColumnDataTypeWrapper::try_from(column_schema.data_type.clone()).map(|w| w.to_parts())?;

    let semantic_type = if column_schema.is_time_index() {
        SemanticType::Timestamp
    } else if is_primary_key {
        SemanticType::Tag
    } else {
        SemanticType::Field
    } as i32;
    let comment = column_schema
        .metadata()
        .get(COMMENT_KEY)
        .cloned()
        .unwrap_or_default();

    let default_constraint = match column_schema.default_constraint() {
        None => vec![],
        Some(v) => v
            .clone()
            .try_into()
            .context(ConvertColumnDefaultConstraintSnafu {
                column: &column_schema.name,
            })?,
    };
    let options = options_from_column_schema(column_schema);
    Ok(ColumnDef {
        name: column_schema.name.clone(),
        data_type: column_datatype.0 as i32,
        is_nullable: column_schema.is_nullable(),
        default_constraint,
        semantic_type,
        comment,
        datatype_extension: column_datatype.1,
        options,
    })
}

/// Collect the [ColumnOptions] into the [Metadata] that can be used in, for example, [ColumnSchema].
pub fn collect_column_options(column_options: Option<&ColumnOptions>) -> Metadata {
    let Some(ColumnOptions { options }) = column_options else {
        return Metadata::default();
    };

    let mut metadata = Metadata::with_capacity(options.len());
    for (x, y) in COLUMN_OPTION_MAPPINGS {
        if let Some(v) = options.get(x) {
            metadata.insert(y.to_string(), v.clone());
        }
    }
    metadata
}

/// Constructs a `ColumnOptions` from the given `ColumnSchema`.
pub fn options_from_column_schema(column_schema: &ColumnSchema) -> Option<ColumnOptions> {
    let mut options = ColumnOptions::default();
    if let Some(fulltext) = column_schema.metadata().get(FULLTEXT_KEY) {
        options
            .options
            .insert(FULLTEXT_GRPC_KEY.to_string(), fulltext.to_owned());
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
    if let Some(extension_name) = column_schema.metadata().get(EXTENSION_TYPE_NAME_KEY) {
        options
            .options
            .insert(EXTENSION_TYPE_NAME_KEY.to_string(), extension_name.clone());
    }
    if let Some(extension_metadata) = column_schema.metadata().get(EXTENSION_TYPE_METADATA_KEY) {
        options.options.insert(
            EXTENSION_TYPE_METADATA_KEY.to_string(),
            extension_metadata.clone(),
        );
    }

    (!options.options.is_empty()).then_some(options)
}

/// Checks if the `ColumnOptions` contains fulltext options.
pub fn contains_fulltext(options: &Option<ColumnOptions>) -> bool {
    options
        .as_ref()
        .is_some_and(|o| o.options.contains_key(FULLTEXT_GRPC_KEY))
}

/// Checks if the `ColumnOptions` contains skipping index options.
pub fn contains_skipping(options: &Option<ColumnOptions>) -> bool {
    options
        .as_ref()
        .is_some_and(|o| o.options.contains_key(SKIPPING_INDEX_GRPC_KEY))
}

/// Tries to construct a `ColumnOptions` from the given `FulltextOptions`.
pub fn options_from_fulltext(fulltext: &FulltextOptions) -> Result<Option<ColumnOptions>> {
    let mut options = ColumnOptions::default();

    let v = serde_json::to_string(fulltext).context(error::SerializeJsonSnafu)?;
    options.options.insert(FULLTEXT_GRPC_KEY.to_string(), v);

    Ok((!options.options.is_empty()).then_some(options))
}

/// Tries to construct a `ColumnOptions` from the given `SkippingIndexOptions`.
pub fn options_from_skipping(skipping: &SkippingIndexOptions) -> Result<Option<ColumnOptions>> {
    let mut options = ColumnOptions::default();

    let v = serde_json::to_string(skipping).context(error::SerializeJsonSnafu)?;
    options
        .options
        .insert(SKIPPING_INDEX_GRPC_KEY.to_string(), v);

    Ok((!options.options.is_empty()).then_some(options))
}

/// Tries to construct a `ColumnOptions` for inverted index.
pub fn options_from_inverted() -> ColumnOptions {
    let mut options = ColumnOptions::default();
    options
        .options
        .insert(INVERTED_INDEX_GRPC_KEY.to_string(), "true".to_string());
    options
}

/// Tries to construct a `FulltextAnalyzer` from the given analyzer.
pub fn as_fulltext_option_analyzer(analyzer: Analyzer) -> FulltextAnalyzer {
    match analyzer {
        Analyzer::English => FulltextAnalyzer::English,
        Analyzer::Chinese => FulltextAnalyzer::Chinese,
    }
}

/// Tries to construct a `FulltextBackend` from the given backend.
pub fn as_fulltext_option_backend(backend: PbFulltextBackend) -> FulltextBackend {
    match backend {
        PbFulltextBackend::Bloom => FulltextBackend::Bloom,
        PbFulltextBackend::Tantivy => FulltextBackend::Tantivy,
    }
}

/// Tries to construct a `SkippingIndexType` from the given skipping index type.
pub fn as_skipping_index_type(skipping_index_type: PbSkippingIndexType) -> SkippingIndexType {
    match skipping_index_type {
        PbSkippingIndexType::BloomFilter => SkippingIndexType::BloomFilter,
    }
}

#[cfg(test)]
mod tests {

    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{FulltextAnalyzer, FulltextBackend};

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

        let mut schema = ColumnSchema::new("test", ConcreteDataType::string_datatype(), true)
            .with_fulltext_options(FulltextOptions::new_unchecked(
                true,
                FulltextAnalyzer::English,
                false,
                FulltextBackend::Bloom,
                10240,
                0.01,
            ))
            .unwrap();
        schema.set_inverted_index(true);
        let options = options_from_column_schema(&schema).unwrap();
        assert_eq!(
            options.options.get(FULLTEXT_GRPC_KEY).unwrap(),
            "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false,\"backend\":\"bloom\",\"granularity\":10240,\"false-positive-rate-in-10000\":100}"
        );
        assert_eq!(
            options.options.get(INVERTED_INDEX_GRPC_KEY).unwrap(),
            "true"
        );
    }

    #[test]
    fn test_options_with_fulltext() {
        let fulltext = FulltextOptions::new_unchecked(
            true,
            FulltextAnalyzer::English,
            false,
            FulltextBackend::Bloom,
            10240,
            0.01,
        );
        let options = options_from_fulltext(&fulltext).unwrap().unwrap();
        assert_eq!(
            options.options.get(FULLTEXT_GRPC_KEY).unwrap(),
            "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false,\"backend\":\"bloom\",\"granularity\":10240,\"false-positive-rate-in-10000\":100}"
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
