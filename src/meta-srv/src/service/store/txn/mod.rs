
#[derive(Debug, Clone)]
pub enum TxnOp {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Delete(Vec<u8>),
    Txn(Txn),
}

#[derive(Debug, Default, Clone)]
pub struct Txn {
    req: TxnOp,
    c_when: bool,
    c_then: bool,
    c_else: bool,
}

impl Txn {
    pub fn new() -> Self {
        Self::default()
    }

}

pub enum CompareOp {
    Equal,
    Greater,
    Less,
    NotEqual
}