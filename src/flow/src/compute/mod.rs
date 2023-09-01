//! for generate dataflow from logical plan and computing the dataflow
mod compute_state;
mod context;
mod plan;
mod render;
mod typedefs;
mod types;

pub use context::Context;

// TODO(discord9): make a simplified version of source/sink
// sink: simply get rows out of sinked collection/err collection and put it somewhere
// (R, T, D) row of course with since/until frontier to limit

// source: simply insert stuff into it
