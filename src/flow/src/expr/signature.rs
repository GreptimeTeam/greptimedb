use datatypes::data_type::ConcreteDataType;

pub struct Signature {
    pub input: ConcreteDataType,
    pub output: ConcreteDataType,
    pub generic_fn: GenericFn,
}

/// Generic function category
#[derive(Debug, PartialEq, Eq)]
pub enum GenericFn {
    // aggregate func
    Max,
    Min,
    Sum,
    Count,
    Any,
    All,
    // binary func
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
    Mod
}
