//! This is the python coprocessor's engine, which should:
//! 1. store a parsed `Coprocessor` struct
//! 2. store other metadata so other module can call it when needed
//! TODO(discord9): resolve cyclic package dependency of `query` crate

use std::task::Context;
use std::result::Result as StdResult;
use futures_util::StreamExt;

use common_recordbatch::{SendableRecordBatchStream, RecordBatchStream, RecordBatch, error::Error as RbError};
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

    /// this function use sql query to return a async iterator of (a stream) Resulting RecordBatch
    /// computed from input record batch from sql and execute python code
    pub async fn evaluate(&self) -> Result<SendableRecordBatchStream> {
        if let Some(sql)  = &self.copr.deco_args.sql {
            let plan = self.query_engine.sql_to_plan(sql)?;
            let res = self.query_engine.execute(&plan).await?;
            match res{
                query::Output::AffectedRows(_) => todo!(),
                query::Output::RecordBatch(stream) => {
                    stream.then(|rb: StdResult<RecordBatch, RbError>|async{
                        rb.map(|rb|{
                            todo!()
                        });
                        todo!()
                    });
                },
            }
        }else{
            // what do you mean when no sql query was given?Then where the record batch come from?
            todo!()
        }
        todo!()
    }
}
