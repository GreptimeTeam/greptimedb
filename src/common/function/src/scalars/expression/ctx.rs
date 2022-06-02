use chrono_tz::Tz;

use crate::error::Error;

#[derive(Debug)]
pub struct EvalContext {
    tz: Tz,
    pub error: Option<Error>,
}

impl Default for EvalContext {
    fn default() -> Self {
        let tz = "UTC".parse::<Tz>().unwrap();
        Self { error: None, tz }
    }
}

impl EvalContext {
    pub fn new(error: Option<Error>, tz: Tz) -> Self {
        Self { error, tz }
    }

    pub fn set_error(&mut self, e: Error) {
        if self.error.is_none() {
            self.error = Some(e);
        }
    }
}
