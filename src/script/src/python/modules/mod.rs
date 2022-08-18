//! Python udf module, that is udf function that you can use in
//! python script(in Python Coprocessor more precisely)

mod builtins;

pub(crate) use builtins::greptime_builtin;
#[cfg(test)]
#[allow(clippy::print_stdout)]
mod test;
