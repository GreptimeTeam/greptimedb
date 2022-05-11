use crate::error::Result;

pub trait Serializable: Send + Sync {
    /// Serialize a column of value with given type to JSON value
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>>;
}
