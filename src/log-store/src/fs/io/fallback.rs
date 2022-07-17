use std::convert::TryFrom;
use std::fs::File;

use snafu::ResultExt;

use crate::error::Error;

// TODO(hl): Implement pread/pwrite for non-Unix platforms
pub fn pread_exact(file: &File, _buf: &mut [u8],_ offset: u64) -> Result<(), Error> {
    unimplemented!()
}

pub fn pwrite_all(file: &File, _buf: &[u8], _offset: u64) -> Result<(), Error> {
    unimplemented!()
}
