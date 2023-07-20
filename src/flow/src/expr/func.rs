use crate::repr::Row;

/// Stateless functions
#[derive(Debug, Clone)]
pub enum Func {
    BuiltIn(BuiltInFunc),
    Custom(fn(Row) -> Row),
}

#[derive(Debug, Clone)]
pub enum BuiltInFunc {
    Not,
    IsNull,
    IsTrue,
    IsFalse,
}
