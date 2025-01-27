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

pub mod index;
pub mod transformer;

use std::collections::BTreeMap;

use crate::etl::error::{Error, Result};
use crate::etl::transform::index::Index;
use crate::etl::value::Value;

const TRANSFORM_FIELD: &str = "field";
const TRANSFORM_FIELDS: &str = "fields";
const TRANSFORM_TYPE: &str = "type";
const TRANSFORM_INDEX: &str = "index";
const TRANSFORM_DEFAULT: &str = "default";
const TRANSFORM_ON_FAILURE: &str = "on_failure";

use snafu::OptionExt;
pub use transformer::greptime::GreptimeTransformer;

use super::error::{
    KeyMustBeStringSnafu, TransformElementMustBeMapSnafu, TransformOnFailureInvalidValueSnafu,
    TransformTypeMustBeSetSnafu,
};
use super::field::Fields;
use super::processor::{yaml_new_field, yaml_new_fields, yaml_string};

pub trait Transformer: std::fmt::Debug + Sized + Send + Sync + 'static {
    type Output;
    type VecOutput;

    fn new(transforms: Transforms) -> Result<Self>;
    fn schemas(&self) -> &Vec<greptime_proto::v1::ColumnSchema>;
    fn transforms(&self) -> &Transforms;
    fn transforms_mut(&mut self) -> &mut Transforms;
    fn transform_mut(&self, val: &mut BTreeMap<String, Value>) -> Result<Self::VecOutput>;
}

/// On Failure behavior when transform fails
#[derive(Debug, Clone, Default, Copy)]
pub enum OnFailure {
    // Return None if transform fails
    #[default]
    Ignore,
    // Return default value of the field if transform fails
    // Default value depends on the type of the field, or explicitly set by user
    Default,
}

impl std::str::FromStr for OnFailure {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "ignore" => Ok(OnFailure::Ignore),
            "default" => Ok(OnFailure::Default),
            _ => TransformOnFailureInvalidValueSnafu { value: s }.fail(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Transforms {
    pub(crate) transforms: Vec<Transform>,
}

impl Transforms {
    pub fn transforms(&self) -> &Vec<Transform> {
        &self.transforms
    }
}

impl std::ops::Deref for Transforms {
    type Target = Vec<Transform>;

    fn deref(&self) -> &Self::Target {
        &self.transforms
    }
}

impl std::ops::DerefMut for Transforms {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.transforms
    }
}

impl TryFrom<&Vec<yaml_rust::Yaml>> for Transforms {
    type Error = Error;

    fn try_from(docs: &Vec<yaml_rust::Yaml>) -> Result<Self> {
        let mut transforms = Vec::with_capacity(100);
        let mut all_output_keys: Vec<String> = Vec::with_capacity(100);
        let mut all_required_keys = Vec::with_capacity(100);
        for doc in docs {
            let transform_builder: Transform = doc
                .as_hash()
                .context(TransformElementMustBeMapSnafu)?
                .try_into()?;
            let mut transform_output_keys = transform_builder
                .fields
                .iter()
                .map(|f| f.target_or_input_field().to_string())
                .collect();
            all_output_keys.append(&mut transform_output_keys);

            let mut transform_required_keys = transform_builder
                .fields
                .iter()
                .map(|f| f.input_field().to_string())
                .collect();
            all_required_keys.append(&mut transform_required_keys);

            transforms.push(transform_builder);
        }

        all_required_keys.sort();

        Ok(Transforms { transforms })
    }
}

/// only field is required
#[derive(Debug, Clone)]
pub struct Transform {
    pub fields: Fields,

    pub type_: Value,

    pub default: Option<Value>,

    pub index: Option<Index>,

    pub on_failure: Option<OnFailure>,
}

impl Default for Transform {
    fn default() -> Self {
        Transform {
            fields: Fields::default(),
            type_: Value::Null,
            default: None,
            index: None,
            on_failure: None,
        }
    }
}

impl Transform {
    pub(crate) fn get_default(&self) -> Option<&Value> {
        self.default.as_ref()
    }

    pub(crate) fn get_type_matched_default_val(&self) -> &Value {
        &self.type_
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for Transform {
    type Error = Error;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut type_ = Value::Null;
        let mut default = None;
        let mut index = None;
        let mut on_failure = None;

        for (k, v) in hash {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                TRANSFORM_FIELD => {
                    fields = Fields::one(yaml_new_field(v, TRANSFORM_FIELD)?);
                }

                TRANSFORM_FIELDS => {
                    fields = yaml_new_fields(v, TRANSFORM_FIELDS)?;
                }

                TRANSFORM_TYPE => {
                    let t = yaml_string(v, TRANSFORM_TYPE)?;
                    type_ = Value::parse_str_type(&t)?;
                }

                TRANSFORM_INDEX => {
                    let index_str = yaml_string(v, TRANSFORM_INDEX)?;
                    index = Some(index_str.try_into()?);
                }

                TRANSFORM_DEFAULT => {
                    default = Some(Value::try_from(v)?);
                }

                TRANSFORM_ON_FAILURE => {
                    let on_failure_str = yaml_string(v, TRANSFORM_ON_FAILURE)?;
                    on_failure = Some(on_failure_str.parse()?);
                }

                _ => {}
            }
        }
        let mut final_default = None;

        if let Some(default_value) = default {
            match (&type_, &default_value) {
                (Value::Null, _) => {
                    return TransformTypeMustBeSetSnafu {
                        fields: format!("{:?}", fields),
                        default: default_value.to_string(),
                    }
                    .fail();
                }
                (_, Value::Null) => {} // if default is not set, then it will be regarded as default null
                (_, _) => {
                    let target = type_.parse_str_value(default_value.to_str_value().as_str())?;
                    final_default = Some(target);
                }
            }
        }
        let builder = Transform {
            fields,
            type_,
            default: final_default,
            index,
            on_failure,
        };

        Ok(builder)
    }
}
