pub mod admin;
mod client;
mod database;
mod error;

pub use self::{
    client::Client,
    database::{Database, ObjectResult, Select},
    error::{Error, Result},
};
use crate::client::LB;

#[derive(Default)]
pub struct Options {
    catalog: Option<String>,
    schema: Option<String>,
    load_balance: Option<LB>,
}
