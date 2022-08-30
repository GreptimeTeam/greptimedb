pub mod error;
pub mod physical;

pub use error::Error;
pub use physical::{
    plan::{DefaultAsPlanImpl, MockExecution},
    AsExcutionPlan,
};
