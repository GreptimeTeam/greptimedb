use std::fmt::Debug;

use crate::remote::{KvBackend, ValueIter};

#[derive(Debug)]
pub struct MetaKvBackend {}

#[async_trait::async_trait]
impl KvBackend for MetaKvBackend {
    type Error = crate::error::Error;

    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Self::Error>
    where
        'a: 'b,
    {
        let _ = key;
        todo!()
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Self::Error> {
        let _ = key;
        let _ = val;
        todo!()
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Self::Error> {
        let _ = key;
        let _ = end;
        todo!()
    }
}
