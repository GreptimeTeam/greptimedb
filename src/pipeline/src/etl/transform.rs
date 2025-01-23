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

use snafu::OptionExt;

use crate::etl::error::{Error, Result};
use crate::etl::find_key_index;
use crate::etl::processor::yaml_string;
use crate::etl::transform::index::Index;
use crate::etl::value::Value;

const TRANSFORM_FIELD: &str = "field";
const TRANSFORM_FIELDS: &str = "fields";
const TRANSFORM_TYPE: &str = "type";
const TRANSFORM_INDEX: &str = "index";
const TRANSFORM_DEFAULT: &str = "default";
const TRANSFORM_ON_FAILURE: &str = "on_failure";

pub use transformer::greptime::GreptimeTransformer;

use super::error::{
    KeyMustBeStringSnafu, TransformElementMustBeMapSnafu, TransformOnFailureInvalidValueSnafu,
    TransformTypeMustBeSetSnafu,
};
use super::field::{Fields, InputFieldInfo, OneInputOneOutputField};
use super::processor::{yaml_new_field, yaml_new_fields};

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
        todo!()
    }
}

/// only field is required
#[derive(Debug, Clone)]
pub struct Transform {
    pub real_fields: Vec<OneInputOneOutputField>,

    pub type_: Value,

    pub default: Option<Value>,

    pub index: Option<Index>,

    pub on_failure: Option<OnFailure>,
}

impl Default for Transform {
    fn default() -> Self {
        Transform {
            real_fields: Vec::new(),
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
