pub mod admin;
mod client;
mod database;
mod error;

pub use api;

pub use self::{
    client::Client,
    database::{Database, ObjectResult, Select},
    error::{Error, Result},
};
