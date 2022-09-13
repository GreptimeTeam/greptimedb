//! Script engine

use std::any::Any;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use query::Output;

#[async_trait]
pub trait Script {
    type Error: ErrorExt + Send + Sync;

    /// Returns the script engine name such as `python` etc.
    fn engine_name(&self) -> &str;

    fn as_any(&self) -> &dyn Any;

    /// Execute the script and returns the output.
    async fn execute(&self, ctx: EvalContext) -> std::result::Result<Output, Self::Error>;
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

/// Evalute script context
#[derive(Debug, Default)]
pub struct EvalContext {}

/// Compile script context
#[derive(Debug, Default)]
pub struct CompileContext {}
