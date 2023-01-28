#![feature(assert_matches)]

use std::sync::Arc;

use crate::partition::PartitionRule;

pub mod columns;
pub mod error;
pub mod manager;
pub mod partition;
pub mod range;
pub mod route;
pub mod splitter;

pub type PartitionRuleRef = Arc<dyn PartitionRule<Error = error::Error>>;
