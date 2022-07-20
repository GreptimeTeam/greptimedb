use arrow::datatypes::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnotationInfo {
    /// if None, use types infered by PyVector
    pub datatype: Option<DataType>,
    pub is_nullable: bool,
    /// if the result type need to be coerced to given type in `into(<datatype>)`
    pub coerce_into: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Coprocessor {
    pub name: String,
    // get from python decorator args&returns
    pub args: Vec<String>,
    pub returns: Vec<String>,
    // get from python function args& returns' annotation, first is type, second is is_nullable
    pub arg_types: Vec<Option<AnnotationInfo>>,
    pub return_types: Vec<Option<AnnotationInfo>>,
}