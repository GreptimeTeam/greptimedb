use api::v1::{
    codec::SelectResult, object_result, MutateResult, ObjectResult, ResultHeader,
    SelectResult as SelectResultRaw,
};
use common_error::prelude::ErrorExt;

pub const PROTOCOL_VERSION: u32 = 1;

pub type Success = u32;
pub type Failure = u32;

#[derive(Default)]
pub(crate) struct ObjectResultBuilder {
    version: u32,
    code: u32,
    err_msg: Option<String>,
    result: Option<Body>,
}

pub(crate) enum Body {
    Mutate((Success, Failure)),
    Select(SelectResult),
}

impl ObjectResultBuilder {
    pub fn new() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            ..Default::default()
        }
    }

    #[allow(dead_code)]
    pub fn version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    pub fn status_code(mut self, code: u32) -> Self {
        self.code = code;
        self
    }

    pub fn err_msg(mut self, err_msg: String) -> Self {
        self.err_msg = Some(err_msg);
        self
    }

    pub fn mutate_result(mut self, success: u32, failure: u32) -> Self {
        self.result = Some(Body::Mutate((success, failure)));
        self
    }

    pub fn select_result(mut self, select_result: SelectResult) -> Self {
        self.result = Some(Body::Select(select_result));
        self
    }

    pub fn build(self) -> ObjectResult {
        let header = Some(ResultHeader {
            version: self.version,
            code: self.code,
            err_msg: self.err_msg.unwrap_or_default(),
        });

        let result = match self.result {
            Some(Body::Mutate((success, failure))) => {
                Some(object_result::Result::Mutate(MutateResult {
                    success,
                    failure,
                }))
            }
            Some(Body::Select(select)) => Some(object_result::Result::Select(SelectResultRaw {
                raw_data: select.into(),
            })),
            None => None,
        };

        ObjectResult { header, result }
    }
}

pub(crate) fn build_err_result(err: &impl ErrorExt) -> ObjectResult {
    ObjectResultBuilder::new()
        .status_code(err.status_code() as u32)
        .err_msg(err.to_string())
        .build()
}

#[cfg(test)]
mod tests {
    use api::v1::{object_result, MutateResult};
    use common_error::status_code::StatusCode;

    use super::*;
    use crate::error::UnsupportedExprSnafu;

    #[test]
    fn test_object_result_builder() {
        let obj_result = ObjectResultBuilder::new()
            .version(101)
            .status_code(500)
            .err_msg("Failed to read this file!".to_string())
            .mutate_result(100, 20)
            .build();
        let header = obj_result.header.unwrap();
        assert_eq!(101, header.version);
        assert_eq!(500, header.code);
        assert_eq!("Failed to read this file!", header.err_msg);

        let result = obj_result.result.unwrap();
        assert_eq!(
            object_result::Result::Mutate(MutateResult {
                success: 100,
                failure: 20,
            }),
            result
        );
    }

    #[test]
    fn test_build_err_result() {
        let err = UnsupportedExprSnafu { name: "select" }.build();
        let err_result = build_err_result(&err);
        let header = err_result.header.unwrap();
        let result = err_result.result;

        assert_eq!(PROTOCOL_VERSION, header.version);
        assert_eq!(StatusCode::Internal as u32, header.code);
        assert_eq!("Unsupported expr type: select", header.err_msg);
        assert!(result.is_none());
    }
}
