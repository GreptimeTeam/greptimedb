/// Query engine global state
use datafusion::prelude::ExecutionContext;

#[derive(Clone, Default)]
pub(crate) struct QueryEngineState {
    df_context: ExecutionContext,
}

impl QueryEngineState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub(crate) fn df_context(&self) -> &ExecutionContext {
        &self.df_context
    }
}
