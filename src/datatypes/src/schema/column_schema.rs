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
use std::fmt;
use std::str::FromStr;

use arrow::datatypes::Field;
use arrow_schema::extension::{
    EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY, ExtensionType,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use sqlparser_derive::{Visit, VisitMut};

use crate::data_type::{ConcreteDataType, DataType};
use crate::error::{
    self, ArrowMetadataSnafu, Error, InvalidFulltextOptionSnafu, ParseExtendedTypeSnafu, Result,
};
use crate::schema::TYPE_KEY;
use crate::schema::constraint::ColumnDefaultConstraint;
use crate::value::Value;
use crate::vectors::VectorRef;

pub type Metadata = HashMap<String, String>;

/// Key used to store whether the column is time index in arrow field's metadata.
pub const TIME_INDEX_KEY: &str = "greptime:time_index";
pub const COMMENT_KEY: &str = "greptime:storage:comment";
/// Key used to store default constraint in arrow field's metadata.
const DEFAULT_CONSTRAINT_KEY: &str = "greptime:default_constraint";
/// Key used to store fulltext options in arrow field's metadata.
pub const FULLTEXT_KEY: &str = "greptime:fulltext";
/// Key used to store whether the column has inverted index in arrow field's metadata.
pub const INVERTED_INDEX_KEY: &str = "greptime:inverted_index";
/// Key used to store skip options in arrow field's metadata.
pub const SKIPPING_INDEX_KEY: &str = "greptime:skipping_index";
/// Key used to store vector index options in arrow field's metadata.
pub const VECTOR_INDEX_KEY: &str = "greptime:vector_index";

/// Keys used in fulltext options
pub const COLUMN_FULLTEXT_CHANGE_OPT_KEY_ENABLE: &str = "enable";
pub const COLUMN_FULLTEXT_OPT_KEY_ANALYZER: &str = "analyzer";
pub const COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE: &str = "case_sensitive";
pub const COLUMN_FULLTEXT_OPT_KEY_BACKEND: &str = "backend";
pub const COLUMN_FULLTEXT_OPT_KEY_GRANULARITY: &str = "granularity";
pub const COLUMN_FULLTEXT_OPT_KEY_FALSE_POSITIVE_RATE: &str = "false_positive_rate";

/// Keys used in SKIPPING index options
pub const COLUMN_SKIPPING_INDEX_OPT_KEY_GRANULARITY: &str = "granularity";
pub const COLUMN_SKIPPING_INDEX_OPT_KEY_FALSE_POSITIVE_RATE: &str = "false_positive_rate";
pub const COLUMN_SKIPPING_INDEX_OPT_KEY_TYPE: &str = "type";

pub const DEFAULT_GRANULARITY: u32 = 10240;

pub const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01;

/// Schema of a column, used as an immutable struct.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: ConcreteDataType,
    is_nullable: bool,
    is_time_index: bool,
    default_constraint: Option<ColumnDefaultConstraint>,
    metadata: Metadata,
}

impl fmt::Debug for ColumnSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.name,
            self.data_type,
            if self.is_nullable { "null" } else { "not null" },
        )?;

        if self.is_time_index {
            write!(f, " time_index")?;
        }

        // Add default constraint if present
        if let Some(default_constraint) = &self.default_constraint {
            write!(f, " default={:?}", default_constraint)?;
        }

        // Add metadata if present
        if !self.metadata.is_empty() {
            write!(f, " metadata={:?}", self.metadata)?;
        }

        Ok(())
    }
}

impl ColumnSchema {
    pub fn new<T: Into<String>>(
        name: T,
        data_type: ConcreteDataType,
        is_nullable: bool,
    ) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            data_type,
            is_nullable,
            is_time_index: false,
            default_constraint: None,
            metadata: Metadata::new(),
        }
    }

    #[inline]
    pub fn is_time_index(&self) -> bool {
        self.is_time_index
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    #[inline]
    pub fn default_constraint(&self) -> Option<&ColumnDefaultConstraint> {
        self.default_constraint.as_ref()
    }

    /// Check if the default constraint is a impure function.
    pub fn is_default_impure(&self) -> bool {
        self.default_constraint
            .as_ref()
            .map(|c| c.is_function())
            .unwrap_or(false)
    }

    #[inline]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    #[inline]
    pub fn mut_metadata(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Retrieve the column comment
    pub fn column_comment(&self) -> Option<&String> {
        self.metadata.get(COMMENT_KEY)
    }

    pub fn with_time_index(mut self, is_time_index: bool) -> Self {
        self.is_time_index = is_time_index;
        if is_time_index {
            let _ = self
                .metadata
                .insert(TIME_INDEX_KEY.to_string(), "true".to_string());
        } else {
            let _ = self.metadata.remove(TIME_INDEX_KEY);
        }
        self
    }

    /// Set the inverted index for the column.
    /// Similar to [with_inverted_index] but don't take the ownership.
    ///
    /// [with_inverted_index]: Self::with_inverted_index
    pub fn set_inverted_index(&mut self, value: bool) {
        match value {
            true => {
                self.metadata
                    .insert(INVERTED_INDEX_KEY.to_string(), value.to_string());
            }
            false => {
                self.metadata.remove(INVERTED_INDEX_KEY);
            }
        }
    }

    /// Set the inverted index for the column.
    /// Similar to [set_inverted_index] but take the ownership and return a owned value.
    ///
    /// [set_inverted_index]: Self::set_inverted_index
    pub fn with_inverted_index(mut self, value: bool) -> Self {
        self.set_inverted_index(value);
        self
    }

    pub fn is_inverted_indexed(&self) -> bool {
        self.metadata
            .get(INVERTED_INDEX_KEY)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    pub fn is_fulltext_indexed(&self) -> bool {
        self.fulltext_options()
            .unwrap_or_default()
            .map(|option| option.enable)
            .unwrap_or_default()
    }

    pub fn is_skipping_indexed(&self) -> bool {
        self.skipping_index_options().unwrap_or_default().is_some()
    }

    pub fn has_inverted_index_key(&self) -> bool {
        self.metadata.contains_key(INVERTED_INDEX_KEY)
    }

    /// Checks if this column has a vector index.
    pub fn is_vector_indexed(&self) -> bool {
        match self.vector_index_options() {
            Ok(opts) => opts.is_some(),
            Err(e) => {
                common_telemetry::warn!(
                    "Failed to deserialize vector_index_options for column '{}': {}",
                    self.name,
                    e
                );
                false
            }
        }
    }

    /// Gets the vector index options.
    pub fn vector_index_options(&self) -> Result<Option<VectorIndexOptions>> {
        match self.metadata.get(VECTOR_INDEX_KEY) {
            None => Ok(None),
            Some(json) => {
                let options =
                    serde_json::from_str(json).context(error::DeserializeSnafu { json })?;
                Ok(Some(options))
            }
        }
    }

    /// Sets the vector index options.
    pub fn set_vector_index_options(&mut self, options: &VectorIndexOptions) -> Result<()> {
        self.metadata.insert(
            VECTOR_INDEX_KEY.to_string(),
            serde_json::to_string(options).context(error::SerializeSnafu)?,
        );
        Ok(())
    }

    /// Removes the vector index options.
    pub fn unset_vector_index_options(&mut self) {
        self.metadata.remove(VECTOR_INDEX_KEY);
    }

    /// Sets vector index options and returns self for chaining.
    pub fn with_vector_index_options(mut self, options: &VectorIndexOptions) -> Result<Self> {
        self.set_vector_index_options(options)?;
        Ok(self)
    }

    /// Set default constraint.
    ///
    /// If a default constraint exists for the column, this method will
    /// validate it against the column's data type and nullability.
    pub fn with_default_constraint(
        mut self,
        default_constraint: Option<ColumnDefaultConstraint>,
    ) -> Result<Self> {
        if let Some(constraint) = &default_constraint {
            constraint.validate(&self.data_type, self.is_nullable)?;
        }

        self.default_constraint = default_constraint;
        Ok(self)
    }

    /// Set the nullablity to `true` of the column.
    /// Similar to [set_nullable] but take the ownership and return a owned value.
    ///
    /// [set_nullable]: Self::set_nullable
    pub fn with_nullable_set(mut self) -> Self {
        self.is_nullable = true;
        self
    }

    /// Set the nullability to `true` of the column.
    /// Similar to [with_nullable_set] but don't take the ownership
    ///
    /// [with_nullable_set]: Self::with_nullable_set
    pub fn set_nullable(&mut self) {
        self.is_nullable = true;
    }

    /// Set the `is_time_index` to `true` of the column.
    /// Similar to [with_time_index] but don't take the ownership.
    ///
    /// [with_time_index]: Self::with_time_index
    pub fn set_time_index(&mut self) {
        self.is_time_index = true;
    }

    /// Creates a new [`ColumnSchema`] with given metadata.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Creates a vector with default value for this column.
    ///
    /// If the column is `NOT NULL` but doesn't has `DEFAULT` value supplied, returns `Ok(None)`.
    pub fn create_default_vector(&self, num_rows: usize) -> Result<Option<VectorRef>> {
        match &self.default_constraint {
            Some(c) => c
                .create_default_vector(&self.data_type, self.is_nullable, num_rows)
                .map(Some),
            None => {
                if self.is_nullable {
                    // No default constraint, use null as default value.
                    // TODO(yingwen): Use NullVector once it supports setting logical type.
                    ColumnDefaultConstraint::null_value()
                        .create_default_vector(&self.data_type, self.is_nullable, num_rows)
                        .map(Some)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Creates a vector for padding.
    ///
    /// This method always returns a vector since it uses [DataType::default_value]
    /// to fill the vector. Callers should only use the created vector for padding
    /// and never read its content.
    pub fn create_default_vector_for_padding(&self, num_rows: usize) -> VectorRef {
        let padding_value = if self.is_nullable {
            Value::Null
        } else {
            // If the column is not null, use the data type's default value as it is
            // more efficient to acquire.
            self.data_type.default_value()
        };
        let value_ref = padding_value.as_value_ref();
        let mut mutable_vector = self.data_type.create_mutable_vector(num_rows);
        for _ in 0..num_rows {
            mutable_vector.push_value_ref(&value_ref);
        }
        mutable_vector.to_vector()
    }

    /// Creates a default value for this column.
    ///
    /// If the column is `NOT NULL` but doesn't has `DEFAULT` value supplied, returns `Ok(None)`.
    pub fn create_default(&self) -> Result<Option<Value>> {
        match &self.default_constraint {
            Some(c) => c
                .create_default(&self.data_type, self.is_nullable)
                .map(Some),
            None => {
                if self.is_nullable {
                    // No default constraint, use null as default value.
                    ColumnDefaultConstraint::null_value()
                        .create_default(&self.data_type, self.is_nullable)
                        .map(Some)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Creates an impure default value for this column, only if it have a impure default constraint.
    /// Otherwise, returns `Ok(None)`.
    pub fn create_impure_default(&self) -> Result<Option<Value>> {
        match &self.default_constraint {
            Some(c) => c.create_impure_default(&self.data_type),
            None => Ok(None),
        }
    }

    /// Retrieves the fulltext options for the column.
    pub fn fulltext_options(&self) -> Result<Option<FulltextOptions>> {
        match self.metadata.get(FULLTEXT_KEY) {
            None => Ok(None),
            Some(json) => {
                let options =
                    serde_json::from_str(json).context(error::DeserializeSnafu { json })?;
                Ok(Some(options))
            }
        }
    }

    pub fn with_fulltext_options(mut self, options: FulltextOptions) -> Result<Self> {
        self.metadata.insert(
            FULLTEXT_KEY.to_string(),
            serde_json::to_string(&options).context(error::SerializeSnafu)?,
        );
        Ok(self)
    }

    pub fn set_fulltext_options(&mut self, options: &FulltextOptions) -> Result<()> {
        self.metadata.insert(
            FULLTEXT_KEY.to_string(),
            serde_json::to_string(options).context(error::SerializeSnafu)?,
        );
        Ok(())
    }

    /// Retrieves the skipping index options for the column.
    pub fn skipping_index_options(&self) -> Result<Option<SkippingIndexOptions>> {
        match self.metadata.get(SKIPPING_INDEX_KEY) {
            None => Ok(None),
            Some(json) => {
                let options =
                    serde_json::from_str(json).context(error::DeserializeSnafu { json })?;
                Ok(Some(options))
            }
        }
    }

    pub fn with_skipping_options(mut self, options: SkippingIndexOptions) -> Result<Self> {
        self.metadata.insert(
            SKIPPING_INDEX_KEY.to_string(),
            serde_json::to_string(&options).context(error::SerializeSnafu)?,
        );
        Ok(self)
    }

    pub fn set_skipping_options(&mut self, options: &SkippingIndexOptions) -> Result<()> {
        self.metadata.insert(
            SKIPPING_INDEX_KEY.to_string(),
            serde_json::to_string(options).context(error::SerializeSnafu)?,
        );
        Ok(())
    }

    pub fn unset_skipping_options(&mut self) -> Result<()> {
        self.metadata.remove(SKIPPING_INDEX_KEY);
        Ok(())
    }

    pub fn extension_type<E>(&self) -> Result<Option<E>>
    where
        E: ExtensionType,
    {
        let extension_type_name = self.metadata.get(EXTENSION_TYPE_NAME_KEY);

        if extension_type_name.map(|s| s.as_str()) == Some(E::NAME) {
            let extension_metadata = self.metadata.get(EXTENSION_TYPE_METADATA_KEY);
            let extension_metadata =
                E::deserialize_metadata(extension_metadata.map(|s| s.as_str()))
                    .context(ArrowMetadataSnafu)?;

            let extension = E::try_new(&self.data_type.as_arrow_type(), extension_metadata)
                .context(ArrowMetadataSnafu)?;
            Ok(Some(extension))
        } else {
            Ok(None)
        }
    }

    pub fn with_extension_type<E>(&mut self, extension_type: &E) -> Result<()>
    where
        E: ExtensionType,
    {
        self.metadata
            .insert(EXTENSION_TYPE_NAME_KEY.to_string(), E::NAME.to_string());

        if let Some(extension_metadata) = extension_type.serialize_metadata() {
            self.metadata
                .insert(EXTENSION_TYPE_METADATA_KEY.to_string(), extension_metadata);
        }

        Ok(())
    }

    pub fn is_indexed(&self) -> bool {
        self.is_inverted_indexed() || self.is_fulltext_indexed() || self.is_skipping_indexed()
    }
}

/// Column extended type set in column schema's metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnExtType {
    /// Json type.
    Json,

    /// Vector type with dimension.
    Vector(u32),
}

impl fmt::Display for ColumnExtType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ColumnExtType::Json => write!(f, "Json"),
            ColumnExtType::Vector(dim) => write!(f, "Vector({})", dim),
        }
    }
}

impl FromStr for ColumnExtType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Json" => Ok(ColumnExtType::Json),
            _ if s.starts_with("Vector(") && s.ends_with(')') => s[7..s.len() - 1]
                .parse::<u32>()
                .map(ColumnExtType::Vector)
                .map_err(|_| "Invalid dimension for Vector".to_string()),
            _ => Err("Unknown variant".to_string()),
        }
    }
}

impl TryFrom<&Field> for ColumnSchema {
    type Error = Error;

    fn try_from(field: &Field) -> Result<ColumnSchema> {
        let mut data_type = ConcreteDataType::try_from(field.data_type())?;
        // Override the data type if it is specified in the metadata.
        if let Some(s) = field.metadata().get(TYPE_KEY) {
            let extype = ColumnExtType::from_str(s)
                .map_err(|_| ParseExtendedTypeSnafu { value: s }.build())?;
            match extype {
                ColumnExtType::Json => {
                    data_type = ConcreteDataType::json_datatype();
                }
                ColumnExtType::Vector(dim) => {
                    data_type = ConcreteDataType::vector_datatype(dim);
                }
            }
        }
        let mut metadata = field.metadata().clone();
        let default_constraint = match metadata.remove(DEFAULT_CONSTRAINT_KEY) {
            Some(json) => {
                Some(serde_json::from_str(&json).context(error::DeserializeSnafu { json })?)
            }
            None => None,
        };
        let mut is_time_index = metadata.contains_key(TIME_INDEX_KEY);
        if is_time_index && !data_type.is_timestamp() {
            // If the column is time index but the data type is not timestamp, it is invalid.
            // We set the time index to false and remove the metadata.
            // This is possible if we cast the time index column to another type. DataFusion will
            // keep the metadata:
            // https://github.com/apache/datafusion/pull/12951
            is_time_index = false;
            metadata.remove(TIME_INDEX_KEY);
            common_telemetry::debug!(
                "Column {} is not timestamp ({:?}) but has time index metadata",
                data_type,
                field.name(),
            );
        }

        Ok(ColumnSchema {
            name: field.name().clone(),
            data_type,
            is_nullable: field.is_nullable(),
            is_time_index,
            default_constraint,
            metadata,
        })
    }
}

impl TryFrom<&ColumnSchema> for Field {
    type Error = Error;

    fn try_from(column_schema: &ColumnSchema) -> Result<Field> {
        let mut metadata = column_schema.metadata.clone();
        if let Some(value) = &column_schema.default_constraint {
            // Adds an additional metadata to store the default constraint.
            let old = metadata.insert(
                DEFAULT_CONSTRAINT_KEY.to_string(),
                serde_json::to_string(&value).context(error::SerializeSnafu)?,
            );

            ensure!(
                old.is_none(),
                error::DuplicateMetaSnafu {
                    key: DEFAULT_CONSTRAINT_KEY,
                }
            );
        }

        Ok(Field::new(
            &column_schema.name,
            column_schema.data_type.as_arrow_type(),
            column_schema.is_nullable(),
        )
        .with_metadata(metadata))
    }
}

/// Fulltext options for a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(rename_all = "kebab-case")]
pub struct FulltextOptions {
    /// Whether the fulltext index is enabled.
    pub enable: bool,
    /// The fulltext analyzer to use.
    #[serde(default)]
    pub analyzer: FulltextAnalyzer,
    /// Whether the fulltext index is case-sensitive.
    #[serde(default)]
    pub case_sensitive: bool,
    /// The fulltext backend to use.
    #[serde(default)]
    pub backend: FulltextBackend,
    /// The granularity of the fulltext index (for bloom backend only)
    #[serde(default = "fulltext_options_default_granularity")]
    pub granularity: u32,
    /// The false positive rate of the fulltext index (for bloom backend only)
    #[serde(default = "index_options_default_false_positive_rate_in_10000")]
    pub false_positive_rate_in_10000: u32,
}

fn fulltext_options_default_granularity() -> u32 {
    DEFAULT_GRANULARITY
}

fn index_options_default_false_positive_rate_in_10000() -> u32 {
    (DEFAULT_FALSE_POSITIVE_RATE * 10000.0) as u32
}

impl FulltextOptions {
    /// Creates a new fulltext options.
    pub fn new(
        enable: bool,
        analyzer: FulltextAnalyzer,
        case_sensitive: bool,
        backend: FulltextBackend,
        granularity: u32,
        false_positive_rate: f64,
    ) -> Result<Self> {
        ensure!(
            0.0 < false_positive_rate && false_positive_rate <= 1.0,
            error::InvalidFulltextOptionSnafu {
                msg: format!(
                    "Invalid false positive rate: {false_positive_rate}, expected: 0.0 < rate <= 1.0"
                ),
            }
        );
        ensure!(
            granularity > 0,
            error::InvalidFulltextOptionSnafu {
                msg: format!("Invalid granularity: {granularity}, expected: positive integer"),
            }
        );
        Ok(Self::new_unchecked(
            enable,
            analyzer,
            case_sensitive,
            backend,
            granularity,
            false_positive_rate,
        ))
    }

    /// Creates a new fulltext options without checking `false_positive_rate` and `granularity`.
    pub fn new_unchecked(
        enable: bool,
        analyzer: FulltextAnalyzer,
        case_sensitive: bool,
        backend: FulltextBackend,
        granularity: u32,
        false_positive_rate: f64,
    ) -> Self {
        Self {
            enable,
            analyzer,
            case_sensitive,
            backend,
            granularity,
            false_positive_rate_in_10000: (false_positive_rate * 10000.0) as u32,
        }
    }

    /// Gets the false positive rate.
    pub fn false_positive_rate(&self) -> f64 {
        self.false_positive_rate_in_10000 as f64 / 10000.0
    }
}

impl Default for FulltextOptions {
    fn default() -> Self {
        Self::new_unchecked(
            false,
            FulltextAnalyzer::default(),
            false,
            FulltextBackend::default(),
            DEFAULT_GRANULARITY,
            DEFAULT_FALSE_POSITIVE_RATE,
        )
    }
}

impl fmt::Display for FulltextOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "enable={}", self.enable)?;
        if self.enable {
            write!(f, ", analyzer={}", self.analyzer)?;
            write!(f, ", case_sensitive={}", self.case_sensitive)?;
            write!(f, ", backend={}", self.backend)?;
            if self.backend == FulltextBackend::Bloom {
                write!(f, ", granularity={}", self.granularity)?;
                write!(f, ", false_positive_rate={}", self.false_positive_rate())?;
            }
        }
        Ok(())
    }
}

/// The backend of the fulltext index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Visit, VisitMut)]
#[serde(rename_all = "kebab-case")]
pub enum FulltextBackend {
    #[default]
    Bloom,
    Tantivy,
}

impl fmt::Display for FulltextBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FulltextBackend::Tantivy => write!(f, "tantivy"),
            FulltextBackend::Bloom => write!(f, "bloom"),
        }
    }
}

impl TryFrom<HashMap<String, String>> for FulltextOptions {
    type Error = Error;

    fn try_from(options: HashMap<String, String>) -> Result<Self> {
        let mut fulltext_options = FulltextOptions {
            enable: true,
            ..Default::default()
        };

        if let Some(enable) = options.get(COLUMN_FULLTEXT_CHANGE_OPT_KEY_ENABLE) {
            match enable.to_ascii_lowercase().as_str() {
                "true" => fulltext_options.enable = true,
                "false" => fulltext_options.enable = false,
                _ => {
                    return InvalidFulltextOptionSnafu {
                        msg: format!("{enable}, expected: 'true' | 'false'"),
                    }
                    .fail();
                }
            }
        };

        if let Some(analyzer) = options.get(COLUMN_FULLTEXT_OPT_KEY_ANALYZER) {
            match analyzer.to_ascii_lowercase().as_str() {
                "english" => fulltext_options.analyzer = FulltextAnalyzer::English,
                "chinese" => fulltext_options.analyzer = FulltextAnalyzer::Chinese,
                _ => {
                    return InvalidFulltextOptionSnafu {
                        msg: format!("{analyzer}, expected: 'English' | 'Chinese'"),
                    }
                    .fail();
                }
            }
        };

        if let Some(case_sensitive) = options.get(COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE) {
            match case_sensitive.to_ascii_lowercase().as_str() {
                "true" => fulltext_options.case_sensitive = true,
                "false" => fulltext_options.case_sensitive = false,
                _ => {
                    return InvalidFulltextOptionSnafu {
                        msg: format!("{case_sensitive}, expected: 'true' | 'false'"),
                    }
                    .fail();
                }
            }
        }

        if let Some(backend) = options.get(COLUMN_FULLTEXT_OPT_KEY_BACKEND) {
            match backend.to_ascii_lowercase().as_str() {
                "bloom" => fulltext_options.backend = FulltextBackend::Bloom,
                "tantivy" => fulltext_options.backend = FulltextBackend::Tantivy,
                _ => {
                    return InvalidFulltextOptionSnafu {
                        msg: format!("{backend}, expected: 'bloom' | 'tantivy'"),
                    }
                    .fail();
                }
            }
        }

        if fulltext_options.backend == FulltextBackend::Bloom {
            // Parse granularity with default value 10240
            let granularity = match options.get(COLUMN_FULLTEXT_OPT_KEY_GRANULARITY) {
                Some(value) => value
                    .parse::<u32>()
                    .ok()
                    .filter(|&v| v > 0)
                    .ok_or_else(|| {
                        error::InvalidFulltextOptionSnafu {
                            msg: format!(
                                "Invalid granularity: {value}, expected: positive integer"
                            ),
                        }
                        .build()
                    })?,
                None => DEFAULT_GRANULARITY,
            };
            fulltext_options.granularity = granularity;

            // Parse false positive rate with default value 0.01
            let false_positive_rate = match options.get(COLUMN_FULLTEXT_OPT_KEY_FALSE_POSITIVE_RATE)
            {
                Some(value) => value
                    .parse::<f64>()
                    .ok()
                    .filter(|&v| v > 0.0 && v <= 1.0)
                    .ok_or_else(|| {
                        error::InvalidFulltextOptionSnafu {
                            msg: format!(
                                "Invalid false positive rate: {value}, expected: 0.0 < rate <= 1.0"
                            ),
                        }
                        .build()
                    })?,
                None => DEFAULT_FALSE_POSITIVE_RATE,
            };
            fulltext_options.false_positive_rate_in_10000 = (false_positive_rate * 10000.0) as u32;
        }

        Ok(fulltext_options)
    }
}

/// Fulltext analyzer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, Visit, VisitMut)]
pub enum FulltextAnalyzer {
    #[default]
    English,
    Chinese,
}

impl fmt::Display for FulltextAnalyzer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FulltextAnalyzer::English => write!(f, "English"),
            FulltextAnalyzer::Chinese => write!(f, "Chinese"),
        }
    }
}

/// Skipping options for a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(rename_all = "kebab-case")]
pub struct SkippingIndexOptions {
    /// The granularity of the skip index.
    pub granularity: u32,
    /// The false positive rate of the skip index (in ten-thousandths, e.g., 100 = 1%).
    #[serde(default = "index_options_default_false_positive_rate_in_10000")]
    pub false_positive_rate_in_10000: u32,
    /// The type of the skip index.
    #[serde(default)]
    pub index_type: SkippingIndexType,
}

impl SkippingIndexOptions {
    /// Creates a new skipping index options without checking `false_positive_rate` and `granularity`.
    pub fn new_unchecked(
        granularity: u32,
        false_positive_rate: f64,
        index_type: SkippingIndexType,
    ) -> Self {
        Self {
            granularity,
            false_positive_rate_in_10000: (false_positive_rate * 10000.0) as u32,
            index_type,
        }
    }

    /// Creates a new skipping index options.
    pub fn new(
        granularity: u32,
        false_positive_rate: f64,
        index_type: SkippingIndexType,
    ) -> Result<Self> {
        ensure!(
            0.0 < false_positive_rate && false_positive_rate <= 1.0,
            error::InvalidSkippingIndexOptionSnafu {
                msg: format!(
                    "Invalid false positive rate: {false_positive_rate}, expected: 0.0 < rate <= 1.0"
                ),
            }
        );
        ensure!(
            granularity > 0,
            error::InvalidSkippingIndexOptionSnafu {
                msg: format!("Invalid granularity: {granularity}, expected: positive integer"),
            }
        );
        Ok(Self::new_unchecked(
            granularity,
            false_positive_rate,
            index_type,
        ))
    }

    /// Gets the false positive rate.
    pub fn false_positive_rate(&self) -> f64 {
        self.false_positive_rate_in_10000 as f64 / 10000.0
    }
}

impl Default for SkippingIndexOptions {
    fn default() -> Self {
        Self::new_unchecked(
            DEFAULT_GRANULARITY,
            DEFAULT_FALSE_POSITIVE_RATE,
            SkippingIndexType::default(),
        )
    }
}

impl fmt::Display for SkippingIndexOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "granularity={}", self.granularity)?;
        write!(f, ", false_positive_rate={}", self.false_positive_rate())?;
        write!(f, ", index_type={}", self.index_type)?;
        Ok(())
    }
}

/// Skip index types.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
pub enum SkippingIndexType {
    #[default]
    BloomFilter,
}

impl fmt::Display for SkippingIndexType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SkippingIndexType::BloomFilter => write!(f, "BLOOM"),
        }
    }
}

impl TryFrom<HashMap<String, String>> for SkippingIndexOptions {
    type Error = Error;

    fn try_from(options: HashMap<String, String>) -> Result<Self> {
        // Parse granularity with default value 1
        let granularity = match options.get(COLUMN_SKIPPING_INDEX_OPT_KEY_GRANULARITY) {
            Some(value) => value
                .parse::<u32>()
                .ok()
                .filter(|&v| v > 0)
                .ok_or_else(|| {
                    error::InvalidSkippingIndexOptionSnafu {
                        msg: format!("Invalid granularity: {value}, expected: positive integer"),
                    }
                    .build()
                })?,
            None => DEFAULT_GRANULARITY,
        };

        // Parse false positive rate with default value 100
        let false_positive_rate =
            match options.get(COLUMN_SKIPPING_INDEX_OPT_KEY_FALSE_POSITIVE_RATE) {
                Some(value) => value
                    .parse::<f64>()
                    .ok()
                    .filter(|&v| v > 0.0 && v <= 1.0)
                    .ok_or_else(|| {
                        error::InvalidSkippingIndexOptionSnafu {
                            msg: format!(
                                "Invalid false positive rate: {value}, expected: 0.0 < rate <= 1.0"
                            ),
                        }
                        .build()
                    })?,
                None => DEFAULT_FALSE_POSITIVE_RATE,
            };

        // Parse index type with default value BloomFilter
        let index_type = match options.get(COLUMN_SKIPPING_INDEX_OPT_KEY_TYPE) {
            Some(typ) => match typ.to_ascii_uppercase().as_str() {
                "BLOOM" => SkippingIndexType::BloomFilter,
                _ => {
                    return error::InvalidSkippingIndexOptionSnafu {
                        msg: format!("Invalid index type: {typ}, expected: 'BLOOM'"),
                    }
                    .fail();
                }
            },
            None => SkippingIndexType::default(),
        };

        Ok(SkippingIndexOptions::new_unchecked(
            granularity,
            false_positive_rate,
            index_type,
        ))
    }
}

/// Distance metric for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, Visit, VisitMut)]
#[serde(rename_all = "lowercase")]
pub enum VectorDistanceMetric {
    /// Squared Euclidean distance (L2^2).
    #[default]
    L2sq,
    /// Cosine distance (1 - cosine similarity).
    Cosine,
    /// Inner product (negative, for maximum inner product search).
    #[serde(alias = "ip")]
    InnerProduct,
}

impl fmt::Display for VectorDistanceMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorDistanceMetric::L2sq => write!(f, "l2sq"),
            VectorDistanceMetric::Cosine => write!(f, "cosine"),
            VectorDistanceMetric::InnerProduct => write!(f, "ip"),
        }
    }
}

impl std::str::FromStr for VectorDistanceMetric {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "l2sq" | "l2" | "euclidean" => Ok(VectorDistanceMetric::L2sq),
            "cosine" | "cos" => Ok(VectorDistanceMetric::Cosine),
            "inner_product" | "ip" | "dot" => Ok(VectorDistanceMetric::InnerProduct),
            _ => Err(format!(
                "Unknown distance metric: {}. Expected: l2sq, cosine, or ip",
                s
            )),
        }
    }
}

impl VectorDistanceMetric {
    /// Returns the metric as u8 for blob serialization.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::L2sq => 0,
            Self::Cosine => 1,
            Self::InnerProduct => 2,
        }
    }

    /// Parses metric from u8 (used when reading blob).
    pub fn try_from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::L2sq),
            1 => Some(Self::Cosine),
            2 => Some(Self::InnerProduct),
            _ => None,
        }
    }
}

/// Default HNSW connectivity parameter.
const DEFAULT_VECTOR_INDEX_CONNECTIVITY: u32 = 16;
/// Default expansion factor during index construction.
const DEFAULT_VECTOR_INDEX_EXPANSION_ADD: u32 = 128;
/// Default expansion factor during search.
const DEFAULT_VECTOR_INDEX_EXPANSION_SEARCH: u32 = 64;

fn default_vector_index_connectivity() -> u32 {
    DEFAULT_VECTOR_INDEX_CONNECTIVITY
}

fn default_vector_index_expansion_add() -> u32 {
    DEFAULT_VECTOR_INDEX_EXPANSION_ADD
}

fn default_vector_index_expansion_search() -> u32 {
    DEFAULT_VECTOR_INDEX_EXPANSION_SEARCH
}

/// Supported vector index engine types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Visit, VisitMut)]
#[serde(rename_all = "lowercase")]
pub enum VectorIndexEngineType {
    /// USearch HNSW implementation.
    #[default]
    Usearch,
    // Future: Vsag,
}

impl VectorIndexEngineType {
    /// Returns the engine type as u8 for blob serialization.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::Usearch => 0,
        }
    }

    /// Parses engine type from u8 (used when reading blob).
    pub fn try_from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Usearch),
            _ => None,
        }
    }
}

impl fmt::Display for VectorIndexEngineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usearch => write!(f, "usearch"),
        }
    }
}

impl std::str::FromStr for VectorIndexEngineType {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "usearch" => Ok(Self::Usearch),
            _ => Err(format!(
                "Unknown vector index engine: {}. Expected: usearch",
                s
            )),
        }
    }
}

/// Options for vector index (HNSW).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Visit, VisitMut)]
#[serde(rename_all = "kebab-case")]
pub struct VectorIndexOptions {
    /// Vector index engine type (default: usearch).
    #[serde(default)]
    pub engine: VectorIndexEngineType,
    /// Distance metric for similarity search.
    #[serde(default)]
    pub metric: VectorDistanceMetric,
    /// HNSW connectivity parameter (M in the paper).
    /// Higher values improve recall but increase memory usage.
    #[serde(default = "default_vector_index_connectivity")]
    pub connectivity: u32,
    /// Expansion factor during index construction (ef_construction).
    /// Higher values improve index quality but slow down construction.
    #[serde(default = "default_vector_index_expansion_add")]
    pub expansion_add: u32,
    /// Expansion factor during search (ef_search).
    /// Higher values improve recall but slow down search.
    #[serde(default = "default_vector_index_expansion_search")]
    pub expansion_search: u32,
}

impl Default for VectorIndexOptions {
    fn default() -> Self {
        Self {
            engine: VectorIndexEngineType::default(),
            metric: VectorDistanceMetric::default(),
            connectivity: DEFAULT_VECTOR_INDEX_CONNECTIVITY,
            expansion_add: DEFAULT_VECTOR_INDEX_EXPANSION_ADD,
            expansion_search: DEFAULT_VECTOR_INDEX_EXPANSION_SEARCH,
        }
    }
}

impl fmt::Display for VectorIndexOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "engine={}, metric={}, connectivity={}, expansion_add={}, expansion_search={}",
            self.engine, self.metric, self.connectivity, self.expansion_add, self.expansion_search
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};

    use super::*;
    use crate::value::Value;
    use crate::vectors::Int32Vector;

    #[test]
    fn test_column_schema() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("test", field.name());
        assert_eq!(ArrowDataType::Int32, *field.data_type());
        assert!(field.is_nullable());

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_default_constraint() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::from(99))))
            .unwrap();
        assert!(
            column_schema
                .metadata()
                .get(DEFAULT_CONSTRAINT_KEY)
                .is_none()
        );

        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("test", field.name());
        assert_eq!(ArrowDataType::Int32, *field.data_type());
        assert!(field.is_nullable());
        assert_eq!(
            "{\"Value\":{\"Int32\":99}}",
            field.metadata().get(DEFAULT_CONSTRAINT_KEY).unwrap()
        );

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_metadata() {
        let metadata = Metadata::from([
            ("k1".to_string(), "v1".to_string()),
            (COMMENT_KEY.to_string(), "test comment".to_string()),
        ]);
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_metadata(metadata)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        assert_eq!("v1", column_schema.metadata().get("k1").unwrap());
        assert_eq!("test comment", column_schema.column_comment().unwrap());
        assert!(
            column_schema
                .metadata()
                .get(DEFAULT_CONSTRAINT_KEY)
                .is_none()
        );

        let field = Field::try_from(&column_schema).unwrap();
        assert_eq!("v1", field.metadata().get("k1").unwrap());
        let _ = field.metadata().get(DEFAULT_CONSTRAINT_KEY).unwrap();

        let new_column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!(column_schema, new_column_schema);
    }

    #[test]
    fn test_column_schema_with_duplicate_metadata() {
        let metadata = Metadata::from([(DEFAULT_CONSTRAINT_KEY.to_string(), "v1".to_string())]);
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_metadata(metadata)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        assert!(Field::try_from(&column_schema).is_err());
    }

    #[test]
    fn test_column_schema_invalid_default_constraint() {
        assert!(
            ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false)
                .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
                .is_err()
        );
    }

    #[test]
    fn test_column_default_constraint_try_into_from() {
        let default_constraint = ColumnDefaultConstraint::Value(Value::from(42i64));

        let bytes: Vec<u8> = default_constraint.clone().try_into().unwrap();
        let from_value = ColumnDefaultConstraint::try_from(&bytes[..]).unwrap();

        assert_eq!(default_constraint, from_value);
    }

    #[test]
    fn test_column_schema_create_default_null() {
        // Implicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let v = column_schema.create_default_vector(5).unwrap().unwrap();
        assert_eq!(5, v.len());
        assert!(v.only_null());

        // Explicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        let v = column_schema.create_default_vector(5).unwrap().unwrap();
        assert_eq!(5, v.len());
        assert!(v.only_null());
    }

    #[test]
    fn test_column_schema_no_default() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false);
        assert!(column_schema.create_default_vector(5).unwrap().is_none());
    }

    #[test]
    fn test_create_default_vector_for_padding() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let vector = column_schema.create_default_vector_for_padding(4);
        assert!(vector.only_null());
        assert_eq!(4, vector.len());

        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false);
        let vector = column_schema.create_default_vector_for_padding(4);
        assert_eq!(4, vector.len());
        let expect: VectorRef = Arc::new(Int32Vector::from_slice([0, 0, 0, 0]));
        assert_eq!(expect, vector);
    }

    #[test]
    fn test_column_schema_single_create_default_null() {
        // Implicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true);
        let v = column_schema.create_default().unwrap().unwrap();
        assert!(v.is_null());

        // Explicit default null.
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::null_value()))
            .unwrap();
        let v = column_schema.create_default().unwrap().unwrap();
        assert!(v.is_null());
    }

    #[test]
    fn test_column_schema_single_create_default_not_null() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), true)
            .with_default_constraint(Some(ColumnDefaultConstraint::Value(Value::Int32(6))))
            .unwrap();
        let v = column_schema.create_default().unwrap().unwrap();
        assert_eq!(v, Value::Int32(6));
    }

    #[test]
    fn test_column_schema_single_no_default() {
        let column_schema = ColumnSchema::new("test", ConcreteDataType::int32_datatype(), false);
        assert!(column_schema.create_default().unwrap().is_none());
    }

    #[test]
    fn test_debug_for_column_schema() {
        let column_schema_int8 =
            ColumnSchema::new("test_column_1", ConcreteDataType::int8_datatype(), true);

        let column_schema_int32 =
            ColumnSchema::new("test_column_2", ConcreteDataType::int32_datatype(), false);

        let formatted_int8 = format!("{:?}", column_schema_int8);
        let formatted_int32 = format!("{:?}", column_schema_int32);
        assert_eq!(formatted_int8, "test_column_1 Int8 null");
        assert_eq!(formatted_int32, "test_column_2 Int32 not null");
    }

    #[test]
    fn test_from_field_to_column_schema() {
        let field = Field::new("test", ArrowDataType::Int32, true);
        let column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!("test", column_schema.name);
        assert_eq!(ConcreteDataType::int32_datatype(), column_schema.data_type);
        assert!(column_schema.is_nullable);
        assert!(!column_schema.is_time_index);
        assert!(column_schema.default_constraint.is_none());
        assert!(column_schema.metadata.is_empty());

        let field = Field::new("test", ArrowDataType::Binary, true);
        let field = field.with_metadata(Metadata::from([(
            TYPE_KEY.to_string(),
            ConcreteDataType::json_datatype().name(),
        )]));
        let column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!("test", column_schema.name);
        assert_eq!(ConcreteDataType::json_datatype(), column_schema.data_type);
        assert!(column_schema.is_nullable);
        assert!(!column_schema.is_time_index);
        assert!(column_schema.default_constraint.is_none());
        assert_eq!(
            column_schema.metadata.get(TYPE_KEY).unwrap(),
            &ConcreteDataType::json_datatype().name()
        );

        let field = Field::new("test", ArrowDataType::Binary, true);
        let field = field.with_metadata(Metadata::from([(
            TYPE_KEY.to_string(),
            ConcreteDataType::vector_datatype(3).name(),
        )]));
        let column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!("test", column_schema.name);
        assert_eq!(
            ConcreteDataType::vector_datatype(3),
            column_schema.data_type
        );
        assert!(column_schema.is_nullable);
        assert!(!column_schema.is_time_index);
        assert!(column_schema.default_constraint.is_none());
        assert_eq!(
            column_schema.metadata.get(TYPE_KEY).unwrap(),
            &ConcreteDataType::vector_datatype(3).name()
        );
    }

    #[test]
    fn test_column_schema_fix_time_index() {
        let field = Field::new(
            "test",
            ArrowDataType::Timestamp(TimeUnit::Second, None),
            false,
        );
        let field = field.with_metadata(Metadata::from([(
            TIME_INDEX_KEY.to_string(),
            "true".to_string(),
        )]));
        let column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!("test", column_schema.name);
        assert_eq!(
            ConcreteDataType::timestamp_second_datatype(),
            column_schema.data_type
        );
        assert!(!column_schema.is_nullable);
        assert!(column_schema.is_time_index);
        assert!(column_schema.default_constraint.is_none());
        assert_eq!(1, column_schema.metadata().len());

        let field = Field::new("test", ArrowDataType::Int32, false);
        let field = field.with_metadata(Metadata::from([(
            TIME_INDEX_KEY.to_string(),
            "true".to_string(),
        )]));
        let column_schema = ColumnSchema::try_from(&field).unwrap();
        assert_eq!("test", column_schema.name);
        assert_eq!(ConcreteDataType::int32_datatype(), column_schema.data_type);
        assert!(!column_schema.is_nullable);
        assert!(!column_schema.is_time_index);
        assert!(column_schema.default_constraint.is_none());
        assert!(column_schema.metadata.is_empty());
    }

    #[test]
    fn test_skipping_index_options_deserialization() {
        let original_options = "{\"granularity\":1024,\"false-positive-rate-in-10000\":10,\"index-type\":\"BloomFilter\"}";
        let options = serde_json::from_str::<SkippingIndexOptions>(original_options).unwrap();
        assert_eq!(1024, options.granularity);
        assert_eq!(SkippingIndexType::BloomFilter, options.index_type);
        assert_eq!(0.001, options.false_positive_rate());

        let options_str = serde_json::to_string(&options).unwrap();
        assert_eq!(options_str, original_options);
    }

    #[test]
    fn test_skipping_index_options_deserialization_v0_14_to_v0_15() {
        let options = "{\"granularity\":10240,\"index-type\":\"BloomFilter\"}";
        let options = serde_json::from_str::<SkippingIndexOptions>(options).unwrap();
        assert_eq!(10240, options.granularity);
        assert_eq!(SkippingIndexType::BloomFilter, options.index_type);
        assert_eq!(DEFAULT_FALSE_POSITIVE_RATE, options.false_positive_rate());

        let options_str = serde_json::to_string(&options).unwrap();
        assert_eq!(
            options_str,
            "{\"granularity\":10240,\"false-positive-rate-in-10000\":100,\"index-type\":\"BloomFilter\"}"
        );
    }

    #[test]
    fn test_fulltext_options_deserialization() {
        let original_options = "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false,\"backend\":\"bloom\",\"granularity\":1024,\"false-positive-rate-in-10000\":10}";
        let options = serde_json::from_str::<FulltextOptions>(original_options).unwrap();
        assert!(!options.case_sensitive);
        assert!(options.enable);
        assert_eq!(FulltextBackend::Bloom, options.backend);
        assert_eq!(FulltextAnalyzer::default(), options.analyzer);
        assert_eq!(1024, options.granularity);
        assert_eq!(0.001, options.false_positive_rate());

        let options_str = serde_json::to_string(&options).unwrap();
        assert_eq!(options_str, original_options);
    }

    #[test]
    fn test_fulltext_options_deserialization_v0_14_to_v0_15() {
        // 0.14 to 0.15
        let options = "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false,\"backend\":\"bloom\"}";
        let options = serde_json::from_str::<FulltextOptions>(options).unwrap();
        assert!(!options.case_sensitive);
        assert!(options.enable);
        assert_eq!(FulltextBackend::Bloom, options.backend);
        assert_eq!(FulltextAnalyzer::default(), options.analyzer);
        assert_eq!(DEFAULT_GRANULARITY, options.granularity);
        assert_eq!(DEFAULT_FALSE_POSITIVE_RATE, options.false_positive_rate());

        let options_str = serde_json::to_string(&options).unwrap();
        assert_eq!(
            options_str,
            "{\"enable\":true,\"analyzer\":\"English\",\"case-sensitive\":false,\"backend\":\"bloom\",\"granularity\":10240,\"false-positive-rate-in-10000\":100}"
        );
    }
}
