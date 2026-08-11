// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The built-in derivation conventions of the entity graph, embedded as
//! [`conventions.yaml`](./conventions.yaml): the co-declared edge vocabulary,
//! the virtual-destination candidates, and the implicit declarations of
//! well-known Prometheus entity-descriptor metrics. The file is data shipped
//! with the binary, not an operator-editable configuration surface; explicit
//! `greptime.semantic.entity.*` declarations always override it.

use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;

use serde::Deserialize;
use table::requests::is_valid_entity_type;

/// A same-row co-declaration rule: a source-table row carrying both entity
/// identities witnesses `src -rel-> dst`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeRule {
    pub src: String,
    pub dst: String,
    pub rel: String,
}

/// A span-attribute column that may name an uninstrumented peer, and the
/// `connection_type` its match implies.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VirtualDstCandidate {
    pub column: String,
    pub connection_type: String,
}

/// One implicit entity declaration of a whitelisted Prometheus info metric.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImplicitEntity {
    pub entity: String,
    /// Identifying label columns; every one must exist as a tag on the table
    /// for the declaration to apply.
    pub id: Vec<String>,
    /// Descriptive label columns, filtered to those present (kube-state-metrics
    /// label sets vary across versions).
    #[serde(default)]
    pub descriptive: Vec<String>,
    /// Snapshot every tag column except the id columns as descriptive —
    /// `target_info`-style enrichment over an open attribute set.
    #[serde(default)]
    pub descriptive_rest: bool,
}

/// The parsed, validated conventions file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conventions {
    pub edge_vocabulary: Vec<EdgeRule>,
    pub agent_edge_vocabulary: Vec<EdgeRule>,
    pub virtual_dst_candidates: Vec<VirtualDstCandidate>,
    pub prometheus_info_metrics: BTreeMap<String, Vec<ImplicitEntity>>,
}

/// The closed relationship vocabulary (the RFC's rel_type table). A
/// conventions rule naming anything else is a defect in the embedded file.
const REL_TYPES: [&str; 8] = [
    "calls",
    "runs_on",
    "contains",
    "part_of",
    "uses",
    "invokes",
    "depends_on",
    "owns",
];

static CONVENTIONS: LazyLock<Result<Conventions, String>> =
    LazyLock::new(|| parse(include_str!("conventions.yaml")));

/// The embedded conventions. `Err` means the embedded file is broken — pinned
/// by unit test, and propagated by the derivation paths rather than panicking.
pub fn conventions() -> Result<&'static Conventions, String> {
    CONVENTIONS.as_ref().map_err(Clone::clone)
}

fn parse(yaml: &str) -> Result<Conventions, String> {
    let conventions: Conventions =
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("malformed conventions: {e}"))?;
    validate(&conventions)?;
    Ok(conventions)
}

fn validate(conventions: &Conventions) -> Result<(), String> {
    let mut seen_edges = HashSet::new();
    for rule in conventions
        .edge_vocabulary
        .iter()
        .chain(&conventions.agent_edge_vocabulary)
    {
        for ty in [&rule.src, &rule.dst] {
            if !is_valid_entity_type(ty) {
                return Err(format!("invalid entity type `{ty}` in edge vocabulary"));
            }
        }
        if !REL_TYPES.contains(&rule.rel.as_str()) {
            return Err(format!("unknown rel_type `{}` in edge vocabulary", rule.rel));
        }
        if !seen_edges.insert((&rule.src, &rule.dst, &rule.rel)) {
            return Err(format!(
                "duplicate edge rule `{} -{}-> {}`",
                rule.src, rule.rel, rule.dst
            ));
        }
    }

    let mut seen_columns = HashSet::new();
    for candidate in &conventions.virtual_dst_candidates {
        if candidate.column.is_empty() || candidate.connection_type.is_empty() {
            return Err("empty virtual destination candidate".to_string());
        }
        if !seen_columns.insert(&candidate.column) {
            return Err(format!(
                "duplicate virtual destination column `{}`",
                candidate.column
            ));
        }
    }

    for (table, entities) in &conventions.prometheus_info_metrics {
        let mut seen_types = HashSet::new();
        for implicit in entities {
            if !is_valid_entity_type(&implicit.entity) {
                return Err(format!(
                    "invalid entity type `{}` for info metric `{table}`",
                    implicit.entity
                ));
            }
            if !seen_types.insert(&implicit.entity) {
                return Err(format!(
                    "duplicate entity type `{}` for info metric `{table}`",
                    implicit.entity
                ));
            }
            if implicit.id.is_empty() || implicit.id.iter().any(String::is_empty) {
                return Err(format!(
                    "entity `{}` of info metric `{table}` needs non-empty id columns",
                    implicit.entity
                ));
            }
            if implicit.descriptive_rest && !implicit.descriptive.is_empty() {
                return Err(format!(
                    "entity `{}` of info metric `{table}` sets both descriptive and \
                     descriptive_rest",
                    implicit.entity
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_conventions_parse_and_validate() {
        let conventions = conventions().unwrap();
        assert!(!conventions.edge_vocabulary.is_empty());
        assert!(!conventions.agent_edge_vocabulary.is_empty());
        assert!(!conventions.virtual_dst_candidates.is_empty());
        assert!(conventions.prometheus_info_metrics.contains_key("target_info"));
    }

    #[test]
    fn validation_rejects_broken_conventions() {
        // Unknown rel_type.
        assert!(
            parse("edge_vocabulary: [{src: a, dst: b, rel: pets}]\nagent_edge_vocabulary: []\nvirtual_dst_candidates: []\nprometheus_info_metrics: {}")
                .is_err()
        );
        // Entity type outside the grammar.
        assert!(
            parse("edge_vocabulary: [{src: A, dst: b, rel: uses}]\nagent_edge_vocabulary: []\nvirtual_dst_candidates: []\nprometheus_info_metrics: {}")
                .is_err()
        );
        // Duplicate rule.
        assert!(
            parse("edge_vocabulary: [{src: a, dst: b, rel: uses}, {src: a, dst: b, rel: uses}]\nagent_edge_vocabulary: []\nvirtual_dst_candidates: []\nprometheus_info_metrics: {}")
                .is_err()
        );
        // descriptive and descriptive_rest are mutually exclusive.
        assert!(
            parse("edge_vocabulary: []\nagent_edge_vocabulary: []\nvirtual_dst_candidates: []\nprometheus_info_metrics: {t: [{entity: a, id: [x], descriptive: [y], descriptive_rest: true}]}")
                .is_err()
        );
        // Unknown YAML keys are rejected, catching typos in the embedded file.
        assert!(
            parse("edge_vocabulary: [{src: a, dst: b, rel: uses, direction: down}]\nagent_edge_vocabulary: []\nvirtual_dst_candidates: []\nprometheus_info_metrics: {}")
                .is_err()
        );
    }
}
