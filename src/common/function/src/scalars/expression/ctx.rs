use chrono_tz::Tz;

use crate::error::Error;

pub struct EvalContext {
    _tz: Tz,
    pub error: Option<Error>,
}

impl Default for EvalContext {
    fn default() -> Self {
        let tz = "UTC".parse::<Tz>().unwrap();
        Self {
            error: None,
            _tz: tz,
        }
    }
}

impl EvalContext {
    pub fn set_error(&mut self, e: Error) {
        if self.error.is_none() {
            self.error = Some(e);
        }
    }
}
