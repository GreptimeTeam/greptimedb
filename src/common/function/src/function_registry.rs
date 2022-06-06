//! functions registry
use std::sync::RwLock;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, Arc<dyn Function>>>,
}
