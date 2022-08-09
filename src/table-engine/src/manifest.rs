//! Table manifest service
mod action;

use storage::manifest::ManifestImpl;

use crate::manifest::action::*;

pub type TableManifest = ManifestImpl<TableMetaActionList>;

#[cfg(test)]
mod tests {}
