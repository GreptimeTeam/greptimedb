// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// The standard logging macro.
#[macro_export]
macro_rules! log {
    // log!(target: "my_target", Level::INFO, "a {} event", "log");
    (target: $target:expr, $lvl:expr, $($arg:tt)+) => {{
        $crate::tracing::event!(target: $target, $lvl, $($arg)+)
    }};

    // log!(Level::INFO, "a log event")
    ($lvl:expr, $($arg:tt)+) => {{
        $crate::tracing::event!($lvl, $($arg)+)
    }};
}

/// Logs a message at the error level.
#[macro_export]
macro_rules! error {
    // error!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => ({
        $crate::log!(target: $target, $crate::tracing::Level::ERROR, $($arg)+)
    });

    // error!(e; target: "my_target", "a {} event", "log")
    ($e:expr; target: $target:expr, $($arg:tt)+) => ({
        $crate::log!(
            target: $target,
            $crate::tracing::Level::ERROR,
            err = ?$e,
            $($arg)+
        )
    });

    // error!(%e; target: "my_target", "a {} event", "log")
    (%$e:expr; target: $target:expr, $($arg:tt)+) => ({
        $crate::log!(
            target: $target,
            $crate::tracing::Level::ERROR,
            err = %$e,
            $($arg)+
        )
    });

    // error!(e; "a {} event", "log")
    ($e:expr; $($arg:tt)+) => ({
        $crate::log!(
            $crate::tracing::Level::ERROR,
            err = ?$e,
            $($arg)+
        )
    });

    // error!(%e; "a {} event", "log")
    (%$e:expr; $($arg:tt)+) => ({
        $crate::log!(
            $crate::tracing::Level::ERROR,
            err = %$e,
            $($arg)+
        )
    });

    // error!("a {} event", "log")
    ($($arg:tt)+) => ({
        $crate::log!($crate::tracing::Level::ERROR, $($arg)+)
    });
}

/// Logs a message at the warn level.
#[macro_export]
macro_rules! warn {
    // warn!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::tracing::Level::WARN, $($arg)+)
    };

    // warn!(e; "a {} event", "log")
    ($e:expr; $($arg:tt)+) => ({
        $crate::log!(
            $crate::tracing::Level::WARN,
            err = ?$e,
            $($arg)+
        )
    });

    // warn!(%e; "a {} event", "log")
    (%$e:expr; $($arg:tt)+) => ({
        $crate::log!(
            $crate::tracing::Level::WARN,
            err = %$e,
            $($arg)+
        )
    });

    // warn!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::tracing::Level::WARN, $($arg)+)
    };
}

/// Logs a message at the info level.
#[macro_export]
macro_rules! info {
    // info!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::tracing::Level::INFO, $($arg)+)
    };

    // info!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::tracing::Level::INFO, $($arg)+)
    };
}

/// Logs a message at the debug level.
#[macro_export]
macro_rules! debug {
    // debug!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::tracing::Level::DEBUG, $($arg)+)
    };

    // debug!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::tracing::Level::DEBUG, $($arg)+)
    };
}

/// Logs a message at the trace level.
#[macro_export]
macro_rules! trace {
    // trace!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => {
        $crate::log!(target: $target, $crate::tracing::Level::TRACE, $($arg)+)
    };

    // trace!("a {} event", "log")
    ($($arg:tt)+) => {
        $crate::log!($crate::tracing::Level::TRACE, $($arg)+)
    };
}

#[cfg(test)]
mod tests {
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use tracing::Level;

    macro_rules! all_log_macros {
        ($($arg:tt)*) => {
            trace!($($arg)*);
            debug!($($arg)*);
            info!($($arg)*);
            warn!($($arg)*);
            error!($($arg)*);
        };
    }

    #[test]
    fn test_log_args() {
        log!(target: "my_target", Level::TRACE, "foo");
        log!(target: "my_target", Level::DEBUG, "foo",);

        log!(target: "my_target", Level::INFO, "foo: {}", 3);
        log!(target: "my_target", Level::WARN, "foo: {}", 3,);

        log!(target: "my_target", Level::ERROR, "hello {world}", world = "world");
        log!(target: "my_target", Level::DEBUG, "hello {world}", world = "world",);

        all_log_macros!(target: "my_target", "foo");
        all_log_macros!(target: "my_target", "foo",);

        all_log_macros!(target: "my_target", "foo: {}", 3);
        all_log_macros!(target: "my_target", "foo: {}", 3,);

        all_log_macros!(target: "my_target", "hello {world}", world = "world");
        all_log_macros!(target: "my_target", "hello {world}", world = "world",);
    }

    #[test]
    fn test_log_no_target() {
        log!(Level::DEBUG, "foo");
        log!(Level::DEBUG, "foo: {}", 3);

        all_log_macros!("foo");
        all_log_macros!("foo: {}", 3);
    }

    #[test]
    fn test_log_ref_scope_args() {
        let bar = 35;
        let world = "world";
        log!(target: "my_target", Level::DEBUG, "bar: {bar}");
        log!(target: "my_target", Level::DEBUG, "bar: {bar}, hello {}", world);
        log!(target: "my_target", Level::DEBUG, "bar: {bar}, hello {world}",);

        all_log_macros!(target: "my_target", "bar: {bar}");
        all_log_macros!(target: "my_target", "bar: {bar}, hello {}", world);
        all_log_macros!(target: "my_target", "bar: {bar}, hello {world}",);
    }

    #[test]
    fn test_log_error() {
        crate::logging::init_default_ut_logging();

        let err = MockError::new(StatusCode::Unknown);
        let err_ref = &err;
        let err_ref2 = &err_ref;

        error!(target: "my_target", "hello {}", "world");
        // Supports both owned and reference type.
        error!(err; target: "my_target", "hello {}", "world");
        error!(%err; target: "my_target", "hello {}", "world");
        error!(err_ref; target: "my_target", "hello {}", "world");
        error!(err_ref2; "hello {}", "world");
        error!(%err_ref2; "hello {}", "world");
        error!("hello {}", "world");

        let root_err = MockError::with_source(err);
        error!(root_err; "Error with source hello {}", "world");
    }
}
