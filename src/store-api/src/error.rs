pub enum Error {
    WalIO,
}

pub type Result<T> = std::result::Result<T, Error>;
