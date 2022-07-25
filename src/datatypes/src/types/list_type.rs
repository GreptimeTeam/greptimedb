use arrow::datatypes::{DataType as ArrowDataType, Field};
use serde::{Deserialize, Serialize};

use crate::prelude::*;
use crate::value::ListValue;

/// Used to represent the List datatype.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListType {
    /// The type of List's inner data.
    inner: Box<ConcreteDataType>,
}

impl Default for ListType {
    fn default() -> Self {
        ListType::new(ConcreteDataType::null_datatype())
    }
}

impl ListType {
    pub fn new(datatype: ConcreteDataType) -> Self {
        ListType {
            inner: Box::new(datatype),
        }
    }
}

impl DataType for ListType {
    fn name(&self) -> &str {
        "List"
    }

    fn logical_type_id(&self) -> LogicalTypeId {
        LogicalTypeId::List
    }

    fn default_value(&self) -> Value {
        Value::List(ListValue::new(None, *self.inner.clone()))
    }

    fn as_arrow_type(&self) -> ArrowDataType {
        let field = Box::new(Field::new("item", self.inner.as_arrow_type(), true));
        ArrowDataType::List(field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ListValue;

    #[test]
    fn test_list_type() {
        let t = ListType::new(ConcreteDataType::boolean_datatype());
        assert_eq!("List", t.name());
        assert_eq!(LogicalTypeId::List, t.logical_type_id());
        assert_eq!(
            Value::List(ListValue::new(None, ConcreteDataType::boolean_datatype())),
            t.default_value()
        );
        assert_eq!(
            ArrowDataType::List(Box::new(Field::new("item", ArrowDataType::Boolean, true))),
            t.as_arrow_type()
        );
    }
}
