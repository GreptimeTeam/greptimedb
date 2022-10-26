use api::v1::meta::ResponseHeader;

use crate::error;
use crate::error::Result;

#[inline]
pub(crate) fn check_response_header(header: Option<&ResponseHeader>) -> Result<()> {
    if let Some(header) = header {
        if let Some(ref error) = header.error {
            let code = error.code;
            let err_msg = &error.err_msg;
            return error::IllegalServerStateSnafu { code, err_msg }.fail();
        }
    }

    Ok(())
}

/// Get prefix end key of `key`.
#[inline]
pub fn get_prefix(key: &[u8]) -> Vec<u8> {
    for (i, v) in key.iter().enumerate().rev() {
        if *v < 0xFF {
            let mut end = Vec::from(&key[..=i]);
            end[i] = *v + 1;
            return end;
        }
    }

    // next prefix does not exist (e.g., 0xffff);
    vec![0]
}
