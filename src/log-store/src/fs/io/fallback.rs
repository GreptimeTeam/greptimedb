use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;

use snafu::{Backtrace, ResultExt};

pub(crate) fn pread_exact_or_eof(
    file: &File,
    mut buf: &mut [u8],
    offset: u64,
) -> Result<usize, Error> {
    let mut total = 0_usize;
    while !buf.is_empty() {
        match file.read_at(buf, offset + u64::try_from(total).unwrap()) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                return Err(Error::Io {
                    source: e,
                    backtrace: Backtrace::generate(),
                })
            }
        }
    }
    Ok(total)
}

pub(crate) fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> Result<(), Error> {
    file.read_exact_at(buf, offset).context(IOSnafu)
}

pub(crate) fn pwrite_all(file: &File, buf: &[u8], offset: u64) -> Result<(), Error> {
    file.write_all_at(buf, offset).context(IOSnafu)
}
