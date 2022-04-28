/// Query engine execution context
use crate::query_engine::state::QueryEngineState;

#[derive(Debug)]
pub struct QueryContext {
    state: QueryEngineState,
}

impl QueryContext {
    pub fn new(state: QueryEngineState) -> Self {
        Self { state }
    }

    #[inline]
    pub fn state(&self) -> &QueryEngineState {
        &self.state
    }
}
