//! This is the python coprocessor's engine, which should:
//! 1. store a parsed `Coprocessor` struct
//! 2. store other metadata so other module can call it when needed
//! TODO(discord9): resolve cyclic package dependency of `query` crate

use super::copr_parse::parse_copr;
use crate::scalars::python::{coprocessor::Coprocessor, error::Result};

pub struct CoprEngine {
    copr: Coprocessor
}

impl CoprEngine {
    fn try_new(script: &str) -> Result<Self> {
        Ok(Self {
            copr: parse_copr(script)?,
        })
    }
    fn update_script(&mut self, script: &str) -> Result<()> {
        let copr = parse_copr(script)?;
        self.copr = copr;
        Ok(())
    }
    fn evaluate(&self) {
        todo!()
    }
}
