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

//! Script engine

use std::any::Any;
use std::collections::HashMap;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_query::Output;

#[async_trait]
pub trait Script {
    type Error: ErrorExt + Send + Sync;

    /// Returns the script engine name such as `python` etc.
    fn engine_name(&self) -> &str;

    fn as_any(&self) -> &dyn Any;

    /// Execute the script and returns the output.
    async fn execute(
        &self,
        params: HashMap<String, String>,
        ctx: EvalContext,
    ) -> std::result::Result<Output, Self::Error>;
}

#[async_trait]
pub trait ScriptEngine {
    type Error: ErrorExt + Send + Sync;
    type Script: Script<Error = Self::Error>;

    /// Returns the script engine name such as `python` etc.
    fn name(&self) -> &str;

    fn as_any(&self) -> &dyn Any;

    /// Compile a script text into a script instance.
    async fn compile(
        &self,
        script: &str,
        ctx: CompileContext,
    ) -> std::result::Result<Self::Script, Self::Error>;
}

/// Evaluate script context
#[derive(Debug, Default)]
pub struct EvalContext {}

/// Compile script context
#[derive(Debug, Default)]
pub struct CompileContext {}
