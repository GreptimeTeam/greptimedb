use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum GlobalId {
    /// System namespace.
    System(u64),
    /// User namespace.
    User(u64),
    /// Transient namespace.
    Transient(u64),
    /// Dummy id for query being explained
    Explain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LocalId(pub(crate) u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Id {
    /// An identifier that refers to a local component of a dataflow.
    Local(LocalId),
    /// An identifier that refers to a global dataflow.
    Global(GlobalId),
}
