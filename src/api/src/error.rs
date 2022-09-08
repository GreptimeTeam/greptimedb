use datatypes::prelude::ConcreteDataType;
use snafu::prelude::*;
use snafu::Backtrace;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unknown proto column datatype: {}", datatype))]
    UnknownColumnDataType { datatype: i32, backtrace: Backtrace },

    #[snafu(display("Failed to create column datatype from {:?}", from))]
    IntoColumnDataType {
        from: ConcreteDataType,
        backtrace: Backtrace,
    },
}
