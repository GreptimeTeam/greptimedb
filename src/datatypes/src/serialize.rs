use crate::errors::Result;

pub trait Serializable: Send + Sync {
    // serialize a column of value with given type to JSON value
    fn serialize_to_json(&self) -> Result<Vec<serde_json::Value>>;
}
