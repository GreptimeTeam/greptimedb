use std::fs::File;
use std::os::unix::fs::FileExt;

use snafu::ResultExt;

use crate::error::Error;
use crate::error::IoSnafu;

#[allow(dead_code)]
pub fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> Result<(), Error> {
    file.read_exact_at(buf, offset as u64).context(IoSnafu)
}

pub fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> Result<(), Error> {
    file.write_all_at(buf, offset as u64).context(IoSnafu)
}
