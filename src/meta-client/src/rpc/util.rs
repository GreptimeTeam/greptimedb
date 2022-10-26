use api::v1::meta::ResponseHeader;

use crate::error;
use crate::error::Result;

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
