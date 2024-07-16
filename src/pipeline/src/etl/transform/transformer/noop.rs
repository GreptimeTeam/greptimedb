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

use crate::etl::transform::{Transformer, Transforms};
use crate::etl::value::Value;

pub struct NoopTransformer;

impl std::fmt::Display for NoopTransformer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NoopTransformer")
    }
}

impl Transformer for NoopTransformer {
    type Output = Value;

    fn new(_transforms: Transforms) -> Result<Self, String> {
        Ok(NoopTransformer)
    }

    fn transform(&self, val: Value) -> Result<Self::Output, String> {
        Ok(val)
    }

    fn transforms(&self) -> &Transforms {
        unimplemented!()
    }
}
