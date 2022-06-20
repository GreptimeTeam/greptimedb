#[derive(Debug)]
pub struct WriteResponse {}

#[derive(Debug)]
pub struct ScanResponse<R> {
    /// Reader to read result chunks.
    pub reader: R,
}

#[derive(Debug)]
pub struct GetResponse {}
