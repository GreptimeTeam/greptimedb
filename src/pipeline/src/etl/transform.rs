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

use itertools::Itertools;

use crate::etl::field::Fields;
use crate::etl::processor::{update_one_one_output_keys, yaml_field, yaml_fields, yaml_string};
use crate::etl::transform::index::Index;
use crate::etl::value::Value;

const TRANSFORM_FIELD: &str = "field";
const TRANSFORM_FIELDS: &str = "fields";
const TRANSFORM_TYPE: &str = "type";
const TRANSFORM_INDEX: &str = "index";
const TRANSFORM_DEFAULT: &str = "default";
const TRANSFORM_ON_FAILURE: &str = "on_failure";

pub use transformer::greptime::GreptimeTransformer;

pub trait Transformer: std::fmt::Display + Sized + Send + Sync + 'static {
    type Output;
    type VecOutput;

    fn new(transforms: Transforms) -> Result<Self, String>;
    fn schemas(&self) -> &Vec<greptime_proto::v1::ColumnSchema>;
    fn transforms(&self) -> &Transforms;
    fn transforms_mut(&mut self) -> &mut Transforms;
    fn transform(&self, val: Value) -> Result<Self::Output, String>;
    fn transform_mut(&self, val: &mut Vec<Value>) -> Result<Self::VecOutput, String>;
}

/// On Failure behavior when transform fails
#[derive(Debug, Clone, Default)]
pub enum OnFailure {
    // Return None if transform fails
    #[default]
    Ignore,
    // Return default value of the field if transform fails
    // Default value depends on the type of the field, or explicitly set by user
    Default,
}

impl std::str::FromStr for OnFailure {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ignore" => Ok(OnFailure::Ignore),
            "default" => Ok(OnFailure::Default),
            _ => Err(format!("invalid transform on_failure value: {}", s)),
        }
    }
}

impl std::fmt::Display for OnFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            OnFailure::Ignore => write!(f, "ignore"),
            OnFailure::Default => write!(f, "default"),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Transforms {
    transforms: Vec<Transform>,
    output_keys: Vec<String>,
    required_keys: Vec<String>,
}

impl Transforms {
    pub fn output_keys(&self) -> &Vec<String> {
        &self.output_keys
    }

    pub fn output_keys_mut(&mut self) -> &mut Vec<String> {
        &mut self.output_keys
    }

    pub fn required_keys_mut(&mut self) -> &mut Vec<String> {
        &mut self.required_keys
    }

    pub fn required_keys(&self) -> &Vec<String> {
        &self.required_keys
    }

    pub fn transforms(&self) -> &Vec<Transform> {
        &self.transforms
    }
}

impl std::fmt::Display for Transforms {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let transforms = self
            .transforms
            .iter()
            .map(|field| field.to_string())
            .join(", ");

        write!(f, "{}", transforms)
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
    type Error = String;

    fn try_from(docs: &Vec<yaml_rust::Yaml>) -> Result<Self, Self::Error> {
        let mut transforms = Vec::with_capacity(100);
        let mut all_output_keys: Vec<String> = Vec::with_capacity(100);
        let mut all_required_keys = Vec::with_capacity(100);
        for doc in docs {
            let transform: Transform = doc
                .as_hash()
                .ok_or("transform element must be a map".to_string())?
                .try_into()?;
            let mut transform_output_keys = transform
                .fields
                .iter()
                .map(|f| f.get_target_field().to_string())
                .collect();
            all_output_keys.append(&mut transform_output_keys);

            let mut transform_required_keys = transform
                .fields
                .iter()
                .map(|f| f.input_field.name.clone())
                .collect();
            all_required_keys.append(&mut transform_required_keys);

            transforms.push(transform);
        }

        all_required_keys.sort();

        Ok(Transforms {
            transforms,
            output_keys: all_output_keys,
            required_keys: all_required_keys,
        })
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

impl std::fmt::Display for Transform {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let index = if let Some(index) = &self.index {
            format!(", index: {}", index)
        } else {
            "".to_string()
        };

        let type_ = format!("type: {}", self.type_);
        let fields = format!("field(s): {}", self.fields);
        let default = if let Some(default) = &self.default {
            format!(", default: {}", default)
        } else {
            "".to_string()
        };

        let on_failure = if let Some(on_failure) = &self.on_failure {
            format!(", on_failure: {}", on_failure)
        } else {
            "".to_string()
        };

        write!(f, "{type_}{index}, {fields}{default}{on_failure}",)
    }
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
    fn with_fields(&mut self, mut fields: Fields) {
        update_one_one_output_keys(&mut fields);
        self.fields = fields;
    }

    fn with_type(&mut self, type_: Value) {
        self.type_ = type_;
    }

    fn try_default(&mut self, default: Value) -> Result<(), String> {
        match (&self.type_, &default) {
            (Value::Null, _) => Err(format!(
                "transform {} type MUST BE set before default {}",
                self.fields, &default,
            )),
            (_, Value::Null) => Ok(()), // if default is not set, then it will be regarded as default null
            (_, _) => {
                let target = self
                    .type_
                    .parse_str_value(default.to_str_value().as_str())?;
                self.default = Some(target);
                Ok(())
            }
        }
    }

    fn with_index(&mut self, index: Index) {
        self.index = Some(index);
    }

    fn with_on_failure(&mut self, on_failure: OnFailure) {
        self.on_failure = Some(on_failure);
    }

    pub(crate) fn get_default(&self) -> Option<&Value> {
        self.default.as_ref()
    }

    pub(crate) fn get_type_matched_default_val(&self) -> &Value {
        &self.type_
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for Transform {
    type Error = String;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut transform = Transform::default();

        let mut default_opt = None;

        for (k, v) in hash {
            let key = k.as_str().ok_or("key must be a string")?;
            match key {
                TRANSFORM_FIELD => {
                    transform.with_fields(Fields::one(yaml_field(v, TRANSFORM_FIELD)?));
                }

                TRANSFORM_FIELDS => {
                    transform.with_fields(yaml_fields(v, TRANSFORM_FIELDS)?);
                }

                TRANSFORM_TYPE => {
                    let t = yaml_string(v, TRANSFORM_TYPE)?;
                    transform.with_type(Value::parse_str_type(&t)?);
                }

                TRANSFORM_INDEX => {
                    let index = yaml_string(v, TRANSFORM_INDEX)?;
                    transform.with_index(index.try_into()?);
                }

                TRANSFORM_DEFAULT => {
                    default_opt = Some(Value::try_from(v)?);
                }

                TRANSFORM_ON_FAILURE => {
                    let on_failure = yaml_string(v, TRANSFORM_ON_FAILURE)?;
                    transform.with_on_failure(on_failure.parse()?);
                }

                _ => {}
            }
        }

        if let Some(default) = default_opt {
            transform.try_default(default)?;
        }

        Ok(transform)
    }
}
