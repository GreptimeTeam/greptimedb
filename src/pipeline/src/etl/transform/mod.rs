// Copyright 2024 Greptime Team
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
use crate::etl::processor::{yaml_field, yaml_fields, yaml_string};
use crate::etl::transform::index::Index;
use crate::etl::value::Value;

const TRANSFORM_FIELD: &str = "field";
const TRANSFORM_FIELDS: &str = "fields";
const TRANSFORM_TYPE: &str = "type";
const TRANSFORM_INDEX: &str = "index";
const TRANSFORM_DEFAULT: &str = "default";

pub use transformer::greptime::GreptimeTransformer;
// pub use transformer::noop::NoopTransformer;

pub trait Transformer: std::fmt::Display + Sized + Send + Sync + 'static {
    type Output;

    fn new(transforms: Transforms) -> Result<Self, String>;
    fn transform(&self, val: crate::etl::value::Value) -> Result<Self::Output, String>;
}

#[derive(Debug, Default, Clone)]
pub struct Transforms {
    transforms: Vec<Transform>,
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
        let mut transforms = vec![];

        for doc in docs {
            let transform: Transform = doc
                .as_hash()
                .ok_or("transform element must be a map".to_string())?
                .try_into()?;
            transforms.push(transform);
        }

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
}

impl std::fmt::Display for Transform {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let index = if let Some(index) = &self.index {
            format!(", index: {}", index)
        } else {
            "".to_string()
        };

        let fields = format!("field(s): {}", self.fields);
        let type_ = format!("type: {}", self.type_);

        write!(f, "{type_}{index}, {fields}")
    }
}

impl Default for Transform {
    fn default() -> Self {
        Transform {
            fields: Fields::default(),
            type_: Value::Null,
            default: None,
            index: None,
        }
    }
}

impl Transform {
    fn with_fields(&mut self, fields: Fields) {
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

    pub(crate) fn get_default(&self) -> Option<&Value> {
        self.default.as_ref()
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
                _ => {}
            }
        }

        if let Some(default) = default_opt {
            transform.try_default(default)?;
        }

        Ok(transform)
    }
}
