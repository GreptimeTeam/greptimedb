//! This is the python coprocessor's engine, which should:
//! 1. store a parsed `Coprocessor` struct
//! 2. store other metadata so other module can call it when needed
//! TODO(discord9): resolve cyclic package dependency of `query` crate

use query::QueryEngineRef;

use super::copr_parse::parse_copr;
use crate::python::{coprocessor::Coprocessor, error::Result};

pub struct CoprEngine {
    copr: Coprocessor,
    query_engine: QueryEngineRef
}

impl CoprEngine {
    pub fn try_new(script: &str, query_engine: QueryEngineRef) -> Result<Self> {
        Ok(Self {
            copr: parse_copr(script)?,
            query_engine
        })
    }
    pub fn update_script(&mut self, script: &str) -> Result<()> {
        let copr = parse_copr(script)?;
        self.copr = copr;
        Ok(())
    }
    pub fn evaluate(&self) {
        todo!()
    }
}
