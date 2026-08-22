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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::LazyLock;

use serde::Deserialize;

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

/// One implicit entity declaration: of a whitelisted Prometheus or OTel info
/// metric, or of a trace-v1 table's flattened resource attributes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImplicitEntity {
    pub entity: String,
    /// Identifying label columns, ordered broad to narrow; every one must
    /// exist on the table for the declaration to apply.
    pub id: Vec<String>,
    /// Column qualifying `id[0]` as `<qualifier>/<id[0]>`, so a source
    /// carrying the parts separately matches one carrying them pre-composed.
    /// Skipped when the column is absent or empty.
    #[serde(default)]
    pub qualified_by: Option<String>,
    /// Columns whose presence on a row suppresses this declaration *for that
    /// row*. The judgement has to be row-level: one descriptor table holds both
    /// Kubernetes and bare-runtime rows, so a schema-level test cannot separate
    /// them.
    #[serde(default)]
    pub suppressed_by: Vec<String>,
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
    pub co_declared_edges: Vec<EdgeRule>,
    pub trace_co_declared_edges: Vec<EdgeRule>,
    pub virtual_dst_candidates: Vec<VirtualDstCandidate>,
    pub otlp_trace_entities: Vec<ImplicitEntity>,
    /// Table name -> the entities that table declares, for Prometheus-sourced
    /// descriptor metrics (`source = prometheus`).
    pub prometheus_info_metrics: BTreeMap<String, Vec<ImplicitEntity>>,
    /// Table name -> the entities that table declares, for OTLP-sourced
    /// descriptor tables (`source = opentelemetry`).
    pub otel_info_metrics: BTreeMap<String, Vec<ImplicitEntity>>,
}

/// The built-in entity-type vocabulary. User-declared types are open-ended;
/// the embedded conventions file must stay inside this set.
pub const ENTITY_TYPE_SERVICE: &str = "service";
pub const ENTITY_TYPE_SERVICE_INSTANCE: &str = "service.instance";
pub const ENTITY_TYPE_HOST: &str = "host";
pub const ENTITY_TYPE_CONTAINER: &str = "container";
pub const ENTITY_TYPE_PROCESS: &str = "process";
pub const ENTITY_TYPE_K8S_POD: &str = "k8s.pod";
pub const ENTITY_TYPE_K8S_NODE: &str = "k8s.node";
pub const ENTITY_TYPE_K8S_CONTAINER: &str = "k8s.container";
pub const ENTITY_TYPE_K8S_WORKLOAD: &str = "k8s.workload";
pub const ENTITY_TYPE_K8S_SERVICE: &str = "k8s.service";
pub const ENTITY_TYPE_GEN_AI_AGENT: &str = "gen_ai.agent";
pub const ENTITY_TYPE_GEN_AI_MODEL: &str = "gen_ai.model";
pub const ENTITY_TYPE_GEN_AI_TOOL: &str = "gen_ai.tool";

const ENTITY_TYPES: [&str; 13] = [
    ENTITY_TYPE_SERVICE,
    ENTITY_TYPE_SERVICE_INSTANCE,
    ENTITY_TYPE_HOST,
    ENTITY_TYPE_CONTAINER,
    ENTITY_TYPE_PROCESS,
    ENTITY_TYPE_K8S_POD,
    ENTITY_TYPE_K8S_NODE,
    ENTITY_TYPE_K8S_CONTAINER,
    ENTITY_TYPE_K8S_WORKLOAD,
    ENTITY_TYPE_K8S_SERVICE,
    ENTITY_TYPE_GEN_AI_AGENT,
    ENTITY_TYPE_GEN_AI_MODEL,
    ENTITY_TYPE_GEN_AI_TOOL,
];

/// The relationship vocabulary (the RFC's rel_type table) and the edge
/// provenances.
pub const REL_TYPE_CALLS: &str = "calls";
pub const REL_TYPE_RUNS_ON: &str = "runs_on";
pub const REL_TYPE_CONTAINS: &str = "contains";
pub const REL_TYPE_PART_OF: &str = "part_of";
pub const REL_TYPE_USES: &str = "uses";
pub const REL_TYPE_INVOKES: &str = "invokes";
pub const REL_TYPE_DEPENDS_ON: &str = "depends_on";
pub const REL_TYPE_OWNS: &str = "owns";
pub const PROVENANCE_TRACE: &str = "trace";
pub const PROVENANCE_ATTRIBUTE: &str = "attribute";
pub const PROVENANCE_DECLARED: &str = "declared";
pub const PROVENANCE_AGENT: &str = "agent";

const REL_TYPES: [&str; 8] = [
    REL_TYPE_CALLS,
    REL_TYPE_RUNS_ON,
    REL_TYPE_CONTAINS,
    REL_TYPE_PART_OF,
    REL_TYPE_USES,
    REL_TYPE_INVOKES,
    REL_TYPE_DEPENDS_ON,
    REL_TYPE_OWNS,
];

/// The `connection_type` values a virtual-destination candidate may imply;
/// the calls derivation branches on them when building edge attributes.
pub const CONNECTION_TYPE_DATABASE: &str = "database";
pub const CONNECTION_TYPE_VIRTUAL_NODE: &str = "virtual_node";

const CONNECTION_TYPES: [&str; 2] = [CONNECTION_TYPE_DATABASE, CONNECTION_TYPE_VIRTUAL_NODE];

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
        .co_declared_edges
        .iter()
        .chain(&conventions.trace_co_declared_edges)
    {
        for ty in [&rule.src, &rule.dst] {
            if !ENTITY_TYPES.contains(&ty.as_str()) {
                return Err(format!("unknown entity type `{ty}` in edge vocabulary"));
            }
        }
        if !REL_TYPES.contains(&rule.rel.as_str()) {
            return Err(format!(
                "unknown rel_type `{}` in edge vocabulary",
                rule.rel
            ));
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
        if candidate.column.is_empty() {
            return Err("empty virtual destination column".to_string());
        }
        if !CONNECTION_TYPES.contains(&candidate.connection_type.as_str()) {
            return Err(format!(
                "unknown connection_type `{}` for virtual destination `{}`",
                candidate.connection_type, candidate.column
            ));
        }
        if !seen_columns.insert(&candidate.column) {
            return Err(format!(
                "duplicate virtual destination column `{}`",
                candidate.column
            ));
        }
    }

    let per_table = conventions
        .prometheus_info_metrics
        .iter()
        .chain(&conventions.otel_info_metrics)
        .map(|(table, entities)| (table.as_str(), entities))
        .chain(std::iter::once((
            "otlp traces",
            &conventions.otlp_trace_entities,
        )));
    // An entity type declared with a different number of id columns by two
    // sources yields ids that can never match, silently splitting one entity
    // in two.
    let mut arity = HashMap::new();
    for (table, entities) in per_table {
        let mut seen_types = HashSet::new();
        for implicit in entities {
            if !ENTITY_TYPES.contains(&implicit.entity.as_str()) {
                return Err(format!(
                    "unknown entity type `{}` for info metric `{table}`",
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
            if implicit.suppressed_by.iter().any(String::is_empty) {
                return Err(format!(
                    "entity `{}` of info metric `{table}` has an empty suppressed_by column",
                    implicit.entity
                ));
            }
            if implicit.qualified_by.as_ref().is_some_and(String::is_empty) {
                return Err(format!(
                    "entity `{}` of info metric `{table}` has an empty qualified_by",
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
            match arity.insert(implicit.entity.as_str(), implicit.id.len()) {
                Some(previous) if previous != implicit.id.len() => {
                    return Err(format!(
                        "entity `{}` is declared with {previous} id columns elsewhere but \
                         {} for `{table}`",
                        implicit.entity,
                        implicit.id.len()
                    ));
                }
                _ => {}
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
        conventions().unwrap();
    }

    /// Each case must fail on exactly the rule it names, so the shared
    /// boilerplate is valid and the mutated part is inside the vocabulary.
    fn broken(edges: &str, info_metrics: &str, otel_metrics: &str) -> String {
        format!(
            "co_declared_edges: [{edges}]\ntrace_co_declared_edges: []\n\
             virtual_dst_candidates: []\notlp_trace_entities: []\n\
             prometheus_info_metrics: {{{info_metrics}}}\n\
             otel_info_metrics: {{{otel_metrics}}}"
        )
    }

    #[test]
    fn validation_rejects_broken_conventions() {
        let err = |edges, info, otel| parse(&broken(edges, info, otel)).unwrap_err();

        assert!(err("{src: host, dst: service, rel: pets}", "", "").contains("unknown rel_type"));
        assert!(
            err("{src: k8s.pods, dst: k8s.node, rel: runs_on}", "", "")
                .contains("unknown entity type")
        );
        assert!(
            err(
                "{src: host, dst: service, rel: uses}, {src: host, dst: service, rel: uses}",
                "",
                ""
            )
            .contains("duplicate edge rule")
        );
        assert!(
            err(
                "",
                "t: [{entity: host, id: [x], descriptive: [y], descriptive_rest: true}]",
                ""
            )
            .contains("descriptive_rest")
        );
        assert!(
            err("", "t: [{entity: host, id: [x], suppressed_by: ['']}]", "")
                .contains("suppressed_by")
        );
        // the otel map runs through the same per-table validation
        assert!(err("", "", "t: [{entity: hosts, id: [x]}]").contains("unknown entity type"));
        // ids of a different arity for one type can never match each other
        assert!(
            err(
                "",
                "a: [{entity: host, id: [x]}]",
                "b: [{entity: host, id: [x, y]}]"
            )
            .contains("id columns")
        );
        // Unknown YAML keys are rejected, catching typos in the embedded file.
        assert!(
            parse(&broken(
                "{src: host, dst: service, rel: uses, direction: down}",
                "",
                ""
            ))
            .is_err()
        );
    }
}
