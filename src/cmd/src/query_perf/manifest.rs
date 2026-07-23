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

use std::collections::{BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::query_perf::case::{QueryKind, QueryThresholds};
use crate::query_perf::error::{Error, Result};

/// Schema version shared by every phase-one manifest.
pub const MANIFEST_VERSION: u32 = 1;

/// Canonical SHA-256 digest.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    /// Computes a SHA-256 digest over bytes.
    pub fn compute(bytes: impl AsRef<[u8]>) -> Self {
        Self(format!("sha256:{:x}", Sha256::digest(bytes)))
    }

    /// Parses a canonical SHA-256 digest.
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.len() == 71
            && value.starts_with("sha256:")
            && value[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            Ok(Self(value))
        } else {
            Err(Error::new("digest must be sha256:<64 hex characters>"))
        }
    }

    /// Verifies content against this digest.
    pub fn verify(&self, bytes: impl AsRef<[u8]>) -> Result<()> {
        if *self == Self::compute(bytes) {
            Ok(())
        } else {
            Err(Error::new("SHA-256 digest does not match content"))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// HTTP(S) endpoint with no embedded credentials or request parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct EndpointUrl(String);

impl EndpointUrl {
    /// Parses a safe, host-qualified endpoint URL.
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let parsed = url::Url::parse(&value)
            .map_err(|err| Error::new(format!("invalid endpoint URL: {err}")))?;
        if !matches!(parsed.scheme(), "http" | "https")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(Error::new(
                "endpoint URL must be http(s), host-qualified, and contain no userinfo/query/fragment",
            ));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for EndpointUrl {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// The two and only two comparison roles.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum TargetRole {
    Base,
    Candidate,
}

/// Release-equivalent target profile.
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BuildProfile {
    Release,
    ReleaseWithDebugInfo,
    Profile,
}

/// Explicit runner endpoint capability.
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Sql,
    RemoteWrite,
    FooterObservation,
    ReadBench,
}

/// Target manifest sealed after endpoint resolution.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetEnvironmentManifest {
    pub version: u32,
    pub run_id: String,
    pub controller_id: String,
    pub targets: Vec<TargetBinding>,
    pub completion_digest: Sha256Digest,
}

/// Immutable target identity and endpoint binding.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetBinding {
    pub role: TargetRole,
    pub target_id: String,
    pub identity: Sha256Digest,
    pub git_identity: Sha256Digest,
    pub build_profile: BuildProfile,
    pub features: Vec<String>,
    pub capabilities: Vec<Capability>,
    pub health_endpoint: EndpointUrl,
    pub sql_endpoint: EndpointUrl,
    #[serde(default)]
    pub remote_write_endpoint: Option<EndpointUrl>,
    pub namespace: String,
    pub topology: String,
    pub fixture_bindings: Vec<TargetFixtureBinding>,
}

/// Target-local fixture identity. It must agree with the controller fixture
/// binding before the runner consumes either artifact.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetFixtureBinding {
    pub fixture_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    pub database: String,
    /// The table exposed to SQL/TQL queries. For Prometheus API writes this is
    /// the logical metric table, never the storage table.
    pub logical_table: String,
    /// The physical Prometheus storage table. It is present only for ApiWrite.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_table: Option<String>,
    pub region: String,
}

/// Only providers visible to the endpoint runner.
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunnerFixtureProvider {
    ApiWrite,
    ExternalPrepared,
}

/// Controller-only LocalFs input. It cannot deserialize as a runner provider.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalFsFixtureSpec {
    pub source: String,
    pub conversion_id: String,
}

/// Explicit future controller conversion boundary.
pub trait LocalFsFixtureConverter {
    /// Converts a controller-local spec into a runner fixture manifest.
    fn convert(&self, spec: LocalFsFixtureSpec) -> Result<FixtureManifest>;
}

/// Logical fixture evidence independent of local filesystem paths.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalFixtureSummary {
    pub rows: u64,
    pub series: u64,
    pub time_start_unix_nanos: i64,
    pub time_end_unix_nanos: i64,
}

/// Immutable runner fixture manifest.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureManifest {
    pub version: u32,
    pub run_id: String,
    pub fixture_id: String,
    pub provider: RunnerFixtureProvider,
    pub data_plan_digest: Sha256Digest,
    pub generator_identity: Sha256Digest,
    pub logical_summary: LogicalFixtureSummary,
    pub storage_prefix: String,
    pub files: Vec<FixtureFile>,
    pub target_bindings: Vec<FixtureTargetBinding>,
    pub completion_digest: Sha256Digest,
}

/// Controller-attested immutable external file.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureFile {
    pub file_id: String,
    pub uri: String,
    pub size_bytes: u64,
    pub digest: Sha256Digest,
}

/// Server-visible table, region, and file binding.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureTargetBinding {
    pub role: TargetRole,
    pub target_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,
    pub database: String,
    pub logical_table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_table: Option<String>,
    pub region: String,
    pub file_ids: Vec<String>,
    #[serde(default)]
    pub external: Option<ExternalPreparedTargetBinding>,
}

/// Server-visible identity needed to verify an externally prepared fixture over SQL.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalPreparedTargetBinding {
    pub catalog: String,
    /// This is the SQL schema/database; `FixtureTargetBinding.database` remains the
    /// single canonical database field and must match it exactly.
    pub schema: String,
    pub table_id: u64,
    pub region_id: u64,
    pub time_index: String,
    pub time_unit: TimestampUnit,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    Nanosecond,
    Millisecond,
}

/// Exact query identity resolved from the validated case plan.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedQueryIdentity {
    pub name: String,
    pub kind: QueryKind,
    pub database: String,
    pub query: String,
    pub warmup: u32,
    pub iterations: u32,
}

/// Query execution result status.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum QueryStatus {
    Passed,
    Failed,
}

/// One finite client-observed latency sample.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LatencySample {
    pub milliseconds: f64,
}

/// Measurement record for one exact query.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryMeasurement {
    #[serde(flatten)]
    pub identity: ResolvedQueryIdentity,
    pub status: QueryStatus,
    pub samples: Vec<LatencySample>,
    pub result_digest: Sha256Digest,
}

/// Target measurement artifact.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementManifest {
    pub version: u32,
    pub run_id: String,
    pub measurement_id: String,
    pub target_id: String,
    pub fixture_id: String,
    pub queries: Vec<QueryMeasurement>,
    pub completion_digest: Sha256Digest,
}

/// One immutable pair of target measurements produced by `measure`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MeasurementBundle {
    pub version: u32,
    pub run_id: String,
    pub fixture_id: String,
    pub case_digest: Sha256Digest,
    pub target_manifest_digest: Sha256Digest,
    pub fixture_manifest_digest: Sha256Digest,
    pub measurements: Vec<MeasurementManifest>,
    pub observation_request: ArtifactReference,
    pub completion_digest: Sha256Digest,
}

/// Sealed failed endpoint attempt. It deliberately has no successful bundle.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FailedMeasurementAttempt {
    pub version: u32,
    pub run_id: String,
    pub fixture_id: String,
    pub case_digest: Sha256Digest,
    pub target_manifest_digest: Sha256Digest,
    pub fixture_manifest_digest: Sha256Digest,
    pub failure_detail: String,
    pub completion_digest: Sha256Digest,
}

/// Controller work requested by an endpoint measurement run.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ObservationKind {
    Footer,
    ReadBench,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationRequest {
    pub role: TargetRole,
    pub target_id: String,
    pub kind: ObservationKind,
    #[serde(default)]
    pub read_bench: Option<ReadBenchRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadBenchRequest {
    pub parquetbench: bool,
    pub scanbench: bool,
}

/// Sealed observation request emitted together with a measurement bundle.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationRequestManifest {
    pub version: u32,
    pub run_id: String,
    pub request_id: String,
    pub fixture_id: String,
    pub case_digest: Sha256Digest,
    pub target_manifest_digest: Sha256Digest,
    pub fixture_manifest_digest: Sha256Digest,
    pub requests: Vec<ObservationRequest>,
    pub completion_digest: Sha256Digest,
}

/// Observation status with an explicit failure path.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ObservationStatus {
    Passed,
    Failed,
}

/// Typed footer details.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FooterObservation {
    pub file_count: u64,
    pub total_file_size_bytes: u64,
    pub files_with_column: u64,
    pub column_compressed_size_bytes: u64,
    pub column_uncompressed_size_bytes: u64,
    pub encodings: Vec<String>,
}

/// Typed read-bench details.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadBenchObservation {
    pub parquet_median_milliseconds: Option<f64>,
    pub scan_median_milliseconds: Option<f64>,
}

/// Typed endpoint/controller observation payload.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObservationPayload {
    Visibility { visible_rows: u64 },
    Footer(FooterObservation),
    ReadBench(ReadBenchObservation),
}

/// Observation artifact bound to one target and fixture.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationManifest {
    pub version: u32,
    pub run_id: String,
    pub observation_id: String,
    pub target_id: String,
    pub fixture_id: String,
    pub status: ObservationStatus,
    pub payload: ObservationPayload,
    #[serde(default)]
    pub failure_detail: Option<String>,
    pub completion_digest: Sha256Digest,
}

/// Sealed controller output for exactly the observations requested by measure.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationBundle {
    pub version: u32,
    pub run_id: String,
    pub fixture_id: String,
    pub request: ArtifactReference,
    pub target_manifest_digest: Sha256Digest,
    pub fixture_manifest_digest: Sha256Digest,
    pub observations: Vec<ObservationManifest>,
    pub completion_digest: Sha256Digest,
}

/// Result of a configured query threshold.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdResult {
    pub query_name: String,
    pub threshold: QueryThresholds,
    pub passed: bool,
    pub detail: String,
}

/// Typed threshold category, shared by query and controller observations.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ThresholdKind {
    QueryLatency,
    Footer,
    ReadBench,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdOutcome {
    pub key: ThresholdOutcomeKey,
    pub passed: bool,
    pub evidence: ThresholdEvidence,
}

/// Stable identity for one configured threshold evaluation.
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThresholdOutcomeKey {
    QueryLatency {
        query_name: String,
    },
    FooterTarget {
        rule: FooterRule,
        role: TargetRole,
        target_id: String,
        encoding: Option<String>,
    },
    FooterComparison {
        rule: FooterRule,
        base_target_id: String,
        candidate_target_id: String,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FooterRule {
    MinFiles,
    MinFilesWithColumn,
    RequiredEncoding,
    ForbiddenEncoding,
    MaxTotalFileSizeBytes,
    MaxColumnCompressedSizeBytes,
    MaxColumnUncompressedSizeBytes,
    MaxCandidateTotalFileSizeRegressionPct,
    MaxCandidateColumnCompressedSizeRegressionPct,
    MaxCandidateColumnUncompressedSizeRegressionPct,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ThresholdEvidence {
    QueryLatency {
        base_milliseconds: f64,
        candidate_milliseconds: f64,
        limit_pct: f64,
        regression_pct: f64,
    },
    FooterValue {
        actual: u64,
        limit: u64,
    },
    FooterEncoding {
        encoding_present: bool,
    },
    FooterComparison {
        base_actual: u64,
        candidate_actual: u64,
        limit_pct: f64,
        regression_pct: f64,
    },
}

/// Artifact reference embedded in a final report.
#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReference {
    pub id: String,
    pub digest: Sha256Digest,
}

/// Final evaluation report with explicit provenance and all artifact references.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReportManifest {
    pub version: u32,
    pub run_id: String,
    pub report_id: String,
    pub target_ids: Vec<String>,
    pub fixture_id: String,
    pub queries: Vec<ResolvedQueryIdentity>,
    pub threshold_results: Vec<ThresholdResult>,
    #[serde(default)]
    pub threshold_outcomes: Vec<ThresholdOutcome>,
    /// Sealed finalization progress. It prevents an evaluation error from being
    /// mistaken for a completed threshold evaluation with an empty outcome set.
    pub evaluation_state: EvaluationState,
    #[serde(default)]
    pub outcome: ReportOutcome,
    pub failures: Vec<String>,
    #[serde(default)]
    pub failure_details: Vec<ReportFailure>,
    pub measurements: Vec<ArtifactReference>,
    pub observations: Vec<ArtifactReference>,
    /// A sealed bundle is only claimed when its seal and all cross-references were trusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement_bundle: Option<ArtifactReference>,
    /// A sealed bundle is only claimed when its seal and all cross-references were trusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_bundle: Option<ArtifactReference>,
    pub target_manifest_digest: Sha256Digest,
    pub fixture_manifest_digest: Sha256Digest,
    pub provenance: Sha256Digest,
    pub summary_digest: Sha256Digest,
    pub completion_digest: Sha256Digest,
}

/// A final report is structurally either successful or failed.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReportOutcome {
    Success,
    #[default]
    Failed,
}

/// Whether threshold evaluation did not start, stopped with a trusted error, or
/// completed the full configured outcome set.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationState {
    NotStarted,
    Incomplete,
    Complete,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReportFailure {
    pub stage: ReportFailureStage,
    pub detail: String,
}

/// The trusted finalization boundary at which a failed report was produced.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReportFailureStage {
    MeasurementArtifact,
    MeasurementAttempt,
    ObservationRequest,
    MeasurementBundle,
    ObservationArtifact,
    ObservationBundle,
    Threshold,
    Evaluation,
    ReportInvariant,
}

macro_rules! sealed_manifest {
    ($type:ty) => {
        impl $type {
            /// Computes the canonical completion digest over the manifest body.
            pub fn seal(&mut self) -> Result<()> {
                self.completion_digest = Sha256Digest::compute(canonical_body(self)?);
                Ok(())
            }

            /// Validates the canonical completion digest.
            fn verify_seal(&self) -> Result<()> {
                self.completion_digest.verify(canonical_body(self)?)
            }
        }
    };
}

sealed_manifest!(TargetEnvironmentManifest);
sealed_manifest!(FixtureManifest);
sealed_manifest!(MeasurementManifest);
sealed_manifest!(MeasurementBundle);
sealed_manifest!(FailedMeasurementAttempt);
sealed_manifest!(ObservationRequestManifest);
sealed_manifest!(ObservationManifest);
sealed_manifest!(ObservationBundle);
sealed_manifest!(ReportManifest);

impl TargetEnvironmentManifest {
    /// Validates target roles, release identities, endpoints, and the canonical seal.
    pub fn validate(&self) -> Result<()> {
        version(self.version)?;
        nonempty("run_id", &self.run_id)?;
        nonempty("controller_id", &self.controller_id)?;
        self.verify_seal()?;
        if self.targets.len() != 2 {
            return Err(Error::new(
                "targets must contain exactly base and candidate",
            ));
        }
        let mut roles = HashSet::new();
        let mut ids = HashSet::new();
        for target in &self.targets {
            roles.insert(target.role);
            if !ids.insert(target.target_id.as_str()) {
                return Err(Error::new("target IDs must be unique"));
            }
            for value in [&target.target_id, &target.namespace, &target.topology] {
                nonempty("target binding value", value)?;
            }
            if target.fixture_bindings.is_empty() {
                return Err(Error::new("target fixture_bindings must be non-empty"));
            }
            let mut fixture_tables = HashSet::new();
            for fixture in &target.fixture_bindings {
                for value in [
                    &fixture.fixture_id,
                    &fixture.database,
                    &fixture.logical_table,
                    &fixture.region,
                ] {
                    nonempty("target fixture binding value", value)?;
                }
                if let Some(catalog) = &fixture.catalog {
                    nonempty("target fixture binding catalog", catalog)?;
                }
                if let Some(physical_table) = &fixture.physical_table {
                    nonempty("target fixture binding physical table", physical_table)?;
                }
                if !fixture_tables.insert((
                    fixture.fixture_id.as_str(),
                    fixture.database.as_str(),
                    fixture.logical_table.as_str(),
                )) {
                    return Err(Error::new(
                        "target fixture bindings must be uniquely keyed by table",
                    ));
                }
            }
            valid_digest(&target.identity)?;
            valid_digest(&target.git_identity)?;
            if target
                .features
                .iter()
                .any(|feature| feature.trim().is_empty())
            {
                return Err(Error::new("target feature must be non-empty"));
            }
            if target.capabilities.contains(&Capability::RemoteWrite)
                != target.remote_write_endpoint.is_some()
            {
                return Err(Error::new(
                    "remote-write capability and endpoint must agree",
                ));
            }
        }
        exact_roles(&roles)
    }
}

impl FixtureManifest {
    /// Validates fixture/provider/file/binding identity against sealed targets.
    pub fn validate_against(&self, targets: &TargetEnvironmentManifest) -> Result<()> {
        self.validate()?;
        targets.validate()?;
        if self.run_id != targets.run_id {
            return Err(Error::new("fixture run_id does not match targets"));
        }
        let target_by_role: HashMap<_, _> = targets
            .targets
            .iter()
            .map(|target| (target.role, target))
            .collect();
        let required: HashSet<_> = targets
            .targets
            .iter()
            .flat_map(|target| {
                target.fixture_bindings.iter().map(move |fixture| {
                    (
                        target.role,
                        target.target_id.as_str(),
                        fixture.database.as_str(),
                        fixture.logical_table.as_str(),
                    )
                })
            })
            .collect();
        if required.is_empty() || self.target_bindings.len() != required.len() {
            return Err(Error::new(
                "fixture bindings do not cover target table bindings",
            ));
        }
        let file_ids: HashSet<_> = self
            .files
            .iter()
            .map(|file| file.file_id.as_str())
            .collect();
        let mut present = HashSet::new();
        for binding in &self.target_bindings {
            let target = target_by_role
                .get(&binding.role)
                .ok_or_else(|| Error::new("missing target role"))?;
            if binding.target_id != target.target_id {
                return Err(Error::new(
                    "fixture role-to-target mapping differs from target manifest",
                ));
            }
            for value in [&binding.database, &binding.logical_table, &binding.region] {
                nonempty("fixture binding", value)?;
            }
            if let Some(catalog) = &binding.catalog {
                nonempty("fixture binding catalog", catalog)?;
            }
            if let Some(physical_table) = &binding.physical_table {
                nonempty("fixture binding physical table", physical_table)?;
            }
            let target_fixture = target.fixture_bindings.iter().find(|target_fixture| {
                target_fixture.fixture_id == self.fixture_id
                    && target_fixture.database == binding.database
                    && target_fixture.logical_table == binding.logical_table
            });
            if target_fixture.is_none_or(|target_fixture| {
                target_fixture.catalog != binding.catalog
                    || target_fixture.physical_table != binding.physical_table
                    || target_fixture.region != binding.region
            }) {
                return Err(Error::new(
                    "target fixture_bindings do not match fixture target binding",
                ));
            }
            if !present.insert((
                binding.role,
                binding.target_id.as_str(),
                binding.database.as_str(),
                binding.logical_table.as_str(),
            )) {
                return Err(Error::new(
                    "fixture bindings must be uniquely keyed by target and table",
                ));
            }
            match self.provider {
                RunnerFixtureProvider::ApiWrite
                    if !binding.file_ids.is_empty()
                        || binding.external.is_some()
                        || binding.catalog.is_some()
                        || binding.physical_table.is_none() =>
                {
                    return Err(Error::new(
                        "api_write fixture binding must not contain files or external evidence",
                    ));
                }
                RunnerFixtureProvider::ExternalPrepared
                    if binding.file_ids.is_empty()
                        || binding.external.is_none()
                        || binding.catalog.is_none()
                        || binding.physical_table.is_some() =>
                {
                    return Err(Error::new(
                        "external_prepared fixture binding requires file_ids and external evidence",
                    ));
                }
                _ => {}
            }
            if let Some(external) = &binding.external {
                for value in [&external.catalog, &external.schema, &external.time_index] {
                    nonempty("external fixture binding", value)?;
                }
                if binding.catalog.as_deref() != Some(external.catalog.as_str())
                    || external.schema != binding.database
                    || external.region_id.to_string() != binding.region
                {
                    return Err(Error::new(
                        "external fixture binding schema or region does not match target binding",
                    ));
                }
            }
            if binding
                .file_ids
                .iter()
                .any(|id| !file_ids.contains(id.as_str()))
            {
                return Err(Error::new("fixture binding must reference existing files"));
            }
            if binding.file_ids.iter().collect::<HashSet<_>>().len() != binding.file_ids.len() {
                return Err(Error::new("fixture binding file IDs must be unique"));
            }
        }
        if present == required {
            Ok(())
        } else {
            Err(Error::new(
                "fixture bindings do not exactly match target table bindings",
            ))
        }
    }

    /// Adds case-derived provider constraints that cannot be safely supplied by
    /// a resealed fixture manifest.
    pub fn validate_against_case(
        &self,
        targets: &TargetEnvironmentManifest,
        case: &crate::query_perf::case::ValidatedCase,
    ) -> Result<()> {
        self.validate_against(targets)?;
        match self.provider {
            RunnerFixtureProvider::ApiWrite => {
                let expected = case.remote_write_fixture_expectation()?;
                if self.logical_summary.rows != expected.logical_summary().rows()
                    || self.logical_summary.series != expected.logical_summary().series()
                    || self.logical_summary.time_start_unix_nanos
                        != expected.logical_summary().time_start_unix_nanos()
                    || self.logical_summary.time_end_unix_nanos
                        != expected.logical_summary().time_end_unix_nanos()
                {
                    return Err(Error::new(
                        "api_write logical summary differs from remote-write case",
                    ));
                }
                for binding in &self.target_bindings {
                    if binding.database != expected.database()
                        || binding.logical_table != expected.logical_metric_table()
                        || binding.physical_table.as_deref() != Some(expected.physical_table())
                    {
                        return Err(Error::new(
                            "api_write fixture binding differs from remote-write case",
                        ));
                    }
                }
                let expected_roles = [TargetRole::Base, TargetRole::Candidate];
                if self
                    .target_bindings
                    .iter()
                    .map(|binding| binding.role)
                    .ne(expected_roles)
                    || targets.targets.iter().any(|target| {
                        target.fixture_bindings.len() != 1
                            || target.fixture_bindings[0].database != expected.database()
                            || target.fixture_bindings[0].logical_table
                                != expected.logical_metric_table()
                            || target.fixture_bindings[0].physical_table.as_deref()
                                != Some(expected.physical_table())
                    })
                {
                    return Err(Error::new(
                        "api_write fixture bindings must be deterministically ordered and unique per target",
                    ));
                }
            }
            RunnerFixtureProvider::ExternalPrepared => {
                let expected = case.direct_fixture_logical_summary().map_err(|_| {
                    Error::new("external_prepared requires a direct-SST case table contract")
                })?;
                if self.logical_summary.rows != expected.rows()
                    || self.logical_summary.series != expected.series()
                    || self.logical_summary.time_start_unix_nanos
                        != expected.time_start_unix_nanos()
                    || self.logical_summary.time_end_unix_nanos != expected.time_end_unix_nanos()
                {
                    return Err(Error::new(
                        "external_prepared logical summary differs from per-table direct-SST layout",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Validates fixture-local fields and canonical seal.
    pub fn validate(&self) -> Result<()> {
        version(self.version)?;
        for value in [&self.run_id, &self.fixture_id, &self.storage_prefix] {
            nonempty("fixture value", value)?;
        }
        valid_digest(&self.data_plan_digest)?;
        valid_digest(&self.generator_identity)?;
        self.verify_seal()?;
        if self.logical_summary.rows == 0
            || self.logical_summary.series == 0
            || self.logical_summary.time_end_unix_nanos < self.logical_summary.time_start_unix_nanos
        {
            return Err(Error::new("fixture logical summary is invalid"));
        }
        match self.provider {
            RunnerFixtureProvider::ApiWrite if !self.files.is_empty() => {
                return Err(Error::new("api_write fixture must not contain files"));
            }
            RunnerFixtureProvider::ExternalPrepared if self.files.is_empty() => {
                return Err(Error::new("external_prepared fixture requires files"));
            }
            _ => {}
        }
        let mut ids = HashSet::new();
        for file in &self.files {
            nonempty("file ID", &file.file_id)?;
            nonempty("file URI", &file.uri)?;
            valid_digest(&file.digest)?;
            if file.size_bytes == 0 || !ids.insert(file.file_id.as_str()) {
                return Err(Error::new(
                    "fixture files must be positive-sized and uniquely named",
                ));
            }
        }
        Ok(())
    }
}

impl MeasurementManifest {
    /// Validates exact query identity/order, finite samples, and target/fixture links.
    pub fn validate_against(
        &self,
        expected: &[ResolvedQueryIdentity],
        targets: &TargetEnvironmentManifest,
        fixture: &FixtureManifest,
    ) -> Result<()> {
        version(self.version)?;
        nonempty("measurement_id", &self.measurement_id)?;
        self.verify_seal()?;
        targets.validate()?;
        fixture.validate_against(targets)?;
        if self.run_id != targets.run_id
            || self.fixture_id != fixture.fixture_id
            || !targets
                .targets
                .iter()
                .any(|target| target.target_id == self.target_id)
        {
            return Err(Error::new(
                "measurement run/fixture/target identity mismatch",
            ));
        }
        if self.queries.len() != expected.len()
            || !self
                .queries
                .iter()
                .map(|query| &query.identity)
                .zip(expected)
                .all(|(actual, expected)| same_query(actual, expected))
        {
            return Err(Error::new(
                "measurement queries must exactly match resolved case query order",
            ));
        }
        for query in &self.queries {
            valid_digest(&query.result_digest)?;
            if query.status != QueryStatus::Passed
                || query.samples.len() != query.identity.iterations as usize
                || query
                    .samples
                    .iter()
                    .any(|sample| !sample.milliseconds.is_finite() || sample.milliseconds < 0.0)
            {
                return Err(Error::new("measurement sample count or latency is invalid"));
            }
        }
        Ok(())
    }
}

impl FailedMeasurementAttempt {
    pub fn validate(&self) -> Result<()> {
        version(self.version)?;
        nonempty("failed measurement detail", &self.failure_detail)?;
        self.verify_seal()
    }
}

impl MeasurementBundle {
    pub fn validate_against(
        &self,
        expected: &[ResolvedQueryIdentity],
        case_digest: &Sha256Digest,
        targets: &TargetEnvironmentManifest,
        fixture: &FixtureManifest,
        request: &ObservationRequestManifest,
    ) -> Result<()> {
        version(self.version)?;
        self.verify_seal()?;
        if self.run_id != targets.run_id
            || self.fixture_id != fixture.fixture_id
            || &self.case_digest != case_digest
            || self.target_manifest_digest != targets.completion_digest
            || self.fixture_manifest_digest != fixture.completion_digest
            || self.observation_request.id != request.request_id
            || self.observation_request.digest != request.completion_digest
        {
            return Err(Error::new("measurement bundle identity mismatch"));
        }
        request.validate_against(case_digest, targets, fixture)?;
        if self.measurements.len() != 2 {
            return Err(Error::new(
                "measurement bundle requires base and candidate measurements",
            ));
        }
        let mut roles = HashSet::new();
        for measurement in &self.measurements {
            measurement.validate_against(expected, targets, fixture)?;
            let role = targets
                .targets
                .iter()
                .find(|target| target.target_id == measurement.target_id)
                .ok_or_else(|| Error::new("measurement target is unknown"))?
                .role;
            if !roles.insert(role) {
                return Err(Error::new("measurement bundle has duplicate target role"));
            }
        }
        exact_roles(&roles)
    }
}

impl ObservationRequestManifest {
    pub fn validate_against(
        &self,
        case_digest: &Sha256Digest,
        targets: &TargetEnvironmentManifest,
        fixture: &FixtureManifest,
    ) -> Result<()> {
        version(self.version)?;
        nonempty("observation request ID", &self.request_id)?;
        self.verify_seal()?;
        fixture.validate_against(targets)?;
        if self.run_id != targets.run_id
            || self.fixture_id != fixture.fixture_id
            || &self.case_digest != case_digest
            || self.target_manifest_digest != targets.completion_digest
            || self.fixture_manifest_digest != fixture.completion_digest
        {
            return Err(Error::new("observation request identity mismatch"));
        }
        let mut entries = HashSet::new();
        for request in &self.requests {
            if !targets
                .targets
                .iter()
                .any(|target| target.role == request.role && target.target_id == request.target_id)
                || !entries.insert((request.role, request.target_id.as_str(), request.kind))
            {
                return Err(Error::new("observation request target coverage is invalid"));
            }
            match (request.kind, &request.read_bench) {
                (ObservationKind::Footer, None)
                | (
                    ObservationKind::ReadBench,
                    Some(ReadBenchRequest {
                        parquetbench: true, ..
                    }),
                )
                | (
                    ObservationKind::ReadBench,
                    Some(ReadBenchRequest {
                        scanbench: true, ..
                    }),
                ) => {}
                _ => return Err(Error::new("observation request payload is invalid")),
            }
        }
        Ok(())
    }
}

impl ObservationManifest {
    /// Validates observation status/payload and target/fixture links.
    pub fn validate_against(
        &self,
        targets: &TargetEnvironmentManifest,
        fixture: &FixtureManifest,
    ) -> Result<()> {
        version(self.version)?;
        nonempty("observation_id", &self.observation_id)?;
        self.verify_seal()?;
        fixture.validate_against(targets)?;
        if self.run_id != targets.run_id
            || self.fixture_id != fixture.fixture_id
            || !targets
                .targets
                .iter()
                .any(|target| target.target_id == self.target_id)
        {
            return Err(Error::new(
                "observation run/fixture/target identity mismatch",
            ));
        }
        match (&self.status, &self.failure_detail) {
            (ObservationStatus::Passed, Some(_)) | (ObservationStatus::Failed, None) => {
                return Err(Error::new("observation status and failure detail disagree"));
            }
            (ObservationStatus::Failed, Some(detail)) if detail.trim().is_empty() => {
                return Err(Error::new("observation status and failure detail disagree"));
            }
            _ => {}
        }
        match &self.payload {
            ObservationPayload::Footer(value)
                if value.files_with_column > value.file_count
                    || value
                        .encodings
                        .iter()
                        .any(|encoding| encoding.trim().is_empty()) =>
            {
                Err(Error::new("footer observation is invalid"))
            }
            ObservationPayload::ReadBench(value)
                if [
                    value.parquet_median_milliseconds,
                    value.scan_median_milliseconds,
                ]
                .into_iter()
                .flatten()
                .any(|latency| !latency.is_finite() || latency < 0.0) =>
            {
                Err(Error::new("read-bench latency is invalid"))
            }
            _ => Ok(()),
        }
    }
}

impl ObservationBundle {
    pub fn validate_against(
        &self,
        case_digest: &Sha256Digest,
        targets: &TargetEnvironmentManifest,
        fixture: &FixtureManifest,
        request: &ObservationRequestManifest,
    ) -> Result<()> {
        version(self.version)?;
        self.verify_seal()?;
        request.validate_against(case_digest, targets, fixture)?;
        if self.run_id != targets.run_id
            || self.fixture_id != fixture.fixture_id
            || self.request.id != request.request_id
            || self.request.digest != request.completion_digest
            || self.target_manifest_digest != targets.completion_digest
            || self.fixture_manifest_digest != fixture.completion_digest
        {
            return Err(Error::new("observation bundle identity mismatch"));
        }
        let requested: HashSet<_> = request
            .requests
            .iter()
            .map(|value| (value.target_id.as_str(), value.kind))
            .collect();
        let mut present = HashSet::new();
        for observation in &self.observations {
            observation.validate_against(targets, fixture)?;
            if observation.status != ObservationStatus::Passed {
                return Err(Error::new(
                    "successful finalization cannot consume failed observation",
                ));
            }
            let kind = match &observation.payload {
                ObservationPayload::Footer(_) => ObservationKind::Footer,
                ObservationPayload::ReadBench(_) => ObservationKind::ReadBench,
                ObservationPayload::Visibility { .. } => {
                    return Err(Error::new(
                        "observation bundle contains unrequested visibility payload",
                    ));
                }
            };
            if !requested.contains(&(observation.target_id.as_str(), kind))
                || !present.insert((observation.target_id.as_str(), kind))
            {
                return Err(Error::new("observation bundle coverage is invalid"));
            }
            if let ObservationPayload::ReadBench(value) = &observation.payload {
                let request = request
                    .requests
                    .iter()
                    .find(|request| {
                        request.target_id == observation.target_id
                            && request.kind == ObservationKind::ReadBench
                    })
                    .ok_or_else(|| Error::new("read-bench observation was not requested"))?;
                let modes = request
                    .read_bench
                    .as_ref()
                    .ok_or_else(|| Error::new("read-bench request lacks modes"))?;
                if (modes.parquetbench != value.parquet_median_milliseconds.is_some())
                    || (modes.scanbench != value.scan_median_milliseconds.is_some())
                    || [
                        value.parquet_median_milliseconds,
                        value.scan_median_milliseconds,
                    ]
                    .into_iter()
                    .flatten()
                    .any(|value| !value.is_finite() || value < 0.0)
                {
                    return Err(Error::new(
                        "read-bench observation modes are incomplete or invalid",
                    ));
                }
            }
        }
        if present != requested {
            return Err(Error::new(
                "observation bundle is missing requested observations",
            ));
        }
        Ok(())
    }
}

impl ReportManifest {
    /// Verifies the report is bound to the already validated executable case.
    pub fn validate_provenance(&self, case_digest: &Sha256Digest) -> Result<()> {
        if &self.provenance != case_digest {
            return Err(Error::new("report provenance does not match case digest"));
        }
        Ok(())
    }

    pub fn validate_summary(&self, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.summary_digest.verify(bytes)
    }
    pub fn validate_bundle_references(
        &self,
        bundle: Option<&MeasurementBundle>,
        observations: Option<&ObservationBundle>,
    ) -> Result<()> {
        let measurement_bundle = bundle.map(|bundle| ArtifactReference {
            id: "measurement_bundle".to_string(),
            digest: bundle.completion_digest.clone(),
        });
        let observation_bundle = observations.map(|bundle| ArtifactReference {
            id: "observation_bundle".to_string(),
            digest: bundle.completion_digest.clone(),
        });
        if self.measurement_bundle != measurement_bundle
            || self.observation_bundle != observation_bundle
        {
            return Err(Error::new(
                "report bundle references do not match sealed inputs",
            ));
        }
        Ok(())
    }
    /// Validates every input artifact reference, target/query set, and report seal.
    pub fn validate_against(
        &self,
        expected: &[ResolvedQueryIdentity],
        targets: &TargetEnvironmentManifest,
        fixture: &FixtureManifest,
        measurements: &[MeasurementManifest],
        observations: &[ObservationManifest],
    ) -> Result<()> {
        version(self.version)?;
        nonempty("report_id", &self.report_id)?;
        self.verify_seal()?;
        fixture.validate_against(targets)?;
        if self.run_id != targets.run_id || self.fixture_id != fixture.fixture_id {
            return Err(Error::new("report run/fixture identity mismatch"));
        }
        if self.target_manifest_digest != targets.completion_digest
            || self.fixture_manifest_digest != fixture.completion_digest
        {
            return Err(Error::new("report target or fixture seal mismatch"));
        }
        let expected_targets: BTreeSet<_> = targets
            .targets
            .iter()
            .map(|target| target.target_id.as_str())
            .collect();
        if self.target_ids.len() != expected_targets.len()
            || self
                .target_ids
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                != expected_targets
            || self.queries.len() != expected.len()
            || !self
                .queries
                .iter()
                .zip(expected)
                .all(|(actual, expected)| same_query(actual, expected))
        {
            return Err(Error::new(
                "report targets or queries do not exactly match inputs",
            ));
        }
        let target_roles: HashMap<_, _> = targets
            .targets
            .iter()
            .map(|target| (target.target_id.as_str(), target.role))
            .collect();
        let mut measurement_ids = HashSet::new();
        let mut measurement_roles = HashSet::new();
        for measurement in measurements {
            measurement.validate_against(expected, targets, fixture)?;
            if !measurement_ids.insert(measurement.measurement_id.as_str())
                || !measurement_roles.insert(
                    *target_roles
                        .get(measurement.target_id.as_str())
                        .ok_or_else(|| Error::new("measurement references unknown target"))?,
                )
            {
                return Err(Error::new(
                    "measurements must have unique IDs and exactly one base and candidate entry",
                ));
            }
        }
        if self.outcome == ReportOutcome::Success
            && measurement_roles != HashSet::from([TargetRole::Base, TargetRole::Candidate])
        {
            return Err(Error::new(
                "measurements must contain exactly one base and candidate entry",
            ));
        }
        let mut observation_ids = HashSet::new();
        for observation in observations {
            observation.validate_against(targets, fixture)?;
            if !observation_ids.insert(observation.observation_id.as_str()) {
                return Err(Error::new("observation IDs must be unique"));
            }
        }
        let expected_measurements =
            artifact_references(measurements.iter().map(|value| ArtifactReference {
                id: value.measurement_id.clone(),
                digest: value.completion_digest.clone(),
            }))?;
        let expected_observations =
            artifact_references(observations.iter().map(|value| ArtifactReference {
                id: value.observation_id.clone(),
                digest: value.completion_digest.clone(),
            }))?;
        if !same_artifact_references(&self.measurements, &expected_measurements)
            || !same_artifact_references(&self.observations, &expected_observations)
        {
            return Err(Error::new(
                "report has missing or extra artifact references",
            ));
        }
        let mut threshold_queries = HashSet::new();
        for threshold in &self.threshold_results {
            nonempty("threshold query", &threshold.query_name)?;
            nonempty("threshold detail", &threshold.detail)?;
            let query = expected
                .iter()
                .find(|query| query.name == threshold.query_name);
            if query.is_none() || !threshold_queries.insert(threshold.query_name.as_str()) {
                return Err(Error::new("threshold references unknown query"));
            }
        }
        for outcome in &self.threshold_outcomes {
            validate_threshold_outcome(outcome)?;
        }
        for failure in &self.failure_details {
            nonempty("report failure detail", &failure.detail)?;
        }
        match self.evaluation_state {
            EvaluationState::NotStarted
                if self.outcome != ReportOutcome::Failed
                    || !self.threshold_results.is_empty()
                    || !self.threshold_outcomes.is_empty() =>
            {
                return Err(Error::new(
                    "not-started evaluation must be a failed report without threshold results",
                ));
            }
            EvaluationState::Incomplete
                if self.outcome != ReportOutcome::Failed
                    || self.measurement_bundle.is_none()
                    || self.observation_bundle.is_none()
                    || !self
                        .failure_details
                        .iter()
                        .any(|failure| failure.stage == ReportFailureStage::Evaluation) =>
            {
                return Err(Error::new(
                    "incomplete evaluation requires trusted bundles and a typed evaluation failure",
                ));
            }
            EvaluationState::Complete => {}
            _ => {}
        }
        match self.outcome {
            ReportOutcome::Success
                if !self.failures.is_empty()
                    || !self.failure_details.is_empty()
                    || self.evaluation_state != EvaluationState::Complete
                    || self.measurement_bundle.is_none()
                    || self.observation_bundle.is_none()
                    || self
                        .threshold_outcomes
                        .iter()
                        .any(|outcome| !outcome.passed)
                    || self.threshold_results.iter().any(|outcome| !outcome.passed)
                    || observations
                        .iter()
                        .any(|observation| observation.status != ObservationStatus::Passed) =>
            {
                return Err(Error::new("successful report contains failures"));
            }
            ReportOutcome::Failed if self.failure_details.is_empty() => {
                return Err(Error::new("failed report requires failure details"));
            }
            _ => {}
        }
        valid_digest(&self.provenance)
    }

    /// Requires all and only the threshold outcomes configured by a complete case.
    pub fn validate_threshold_coverage(
        &self,
        case: &crate::query_perf::case::ValidatedCase,
        targets: &TargetEnvironmentManifest,
    ) -> Result<()> {
        let expected = expected_threshold_keys(case, targets)?;
        let actual: HashSet<_> = self
            .threshold_outcomes
            .iter()
            .map(|outcome| outcome.key.clone())
            .collect();
        let expected_queries: HashSet<_> = case
            .queries()
            .iter()
            .filter_map(|query| {
                query
                    .max_candidate_latency_regression_pct()
                    .map(|_| query.resolved_identity().name().to_string())
            })
            .collect();
        let actual_queries: HashSet<_> = self
            .threshold_results
            .iter()
            .map(|result| result.query_name.clone())
            .collect();
        let unique_outcomes = actual.len() == self.threshold_outcomes.len();
        let unique_results = actual_queries.len() == self.threshold_results.len();
        match self.evaluation_state {
            EvaluationState::Complete
                if !unique_outcomes
                    || actual != expected
                    || !unique_results
                    || actual_queries != expected_queries =>
            {
                return Err(Error::new(
                    "completed evaluation threshold coverage is not exact",
                ));
            }
            EvaluationState::Incomplete
                if !unique_outcomes
                    || !actual.is_subset(&expected)
                    || !unique_results
                    || !actual_queries.is_subset(&expected_queries) =>
            {
                return Err(Error::new(
                    "incomplete evaluation contains duplicate or unconfigured threshold outcomes",
                ));
            }
            EvaluationState::NotStarted
                if !self.threshold_outcomes.is_empty() || !self.threshold_results.is_empty() =>
            {
                return Err(Error::new(
                    "not-started evaluation cannot contain threshold outcomes",
                ));
            }
            _ => {}
        }
        for result in &self.threshold_results {
            let limit = case
                .queries()
                .iter()
                .find(|query| query.resolved_identity().name() == result.query_name)
                .and_then(|query| query.max_candidate_latency_regression_pct());
            if result.threshold.max_candidate_latency_regression_pct != limit {
                return Err(Error::new(
                    "query threshold result limit does not match configured threshold",
                ));
            }
        }
        Ok(())
    }
}

fn validate_threshold_outcome(outcome: &ThresholdOutcome) -> Result<()> {
    match (&outcome.key, &outcome.evidence) {
        (
            ThresholdOutcomeKey::QueryLatency { query_name },
            ThresholdEvidence::QueryLatency {
                base_milliseconds,
                candidate_milliseconds,
                limit_pct,
                regression_pct,
            },
        ) if !query_name.trim().is_empty()
            && [
                base_milliseconds,
                candidate_milliseconds,
                limit_pct,
                regression_pct,
            ]
            .into_iter()
            .all(|value| value.is_finite())
            && *base_milliseconds > 0.0
            && *limit_pct >= 0.0 =>
        {
            Ok(())
        }
        (
            ThresholdOutcomeKey::FooterTarget {
                target_id,
                encoding,
                rule,
                ..
            },
            ThresholdEvidence::FooterValue { .. },
        ) if !target_id.trim().is_empty()
            && encoding.is_none()
            && !matches!(
                rule,
                FooterRule::RequiredEncoding | FooterRule::ForbiddenEncoding
            ) =>
        {
            Ok(())
        }
        (
            ThresholdOutcomeKey::FooterTarget {
                target_id,
                encoding: Some(encoding),
                rule: FooterRule::RequiredEncoding | FooterRule::ForbiddenEncoding,
                ..
            },
            ThresholdEvidence::FooterEncoding { .. },
        ) if !target_id.trim().is_empty() && !encoding.trim().is_empty() => Ok(()),
        (
            ThresholdOutcomeKey::FooterComparison {
                base_target_id,
                candidate_target_id,
                rule,
            },
            ThresholdEvidence::FooterComparison {
                base_actual,
                limit_pct,
                regression_pct,
                ..
            },
        ) if !base_target_id.trim().is_empty()
            && !candidate_target_id.trim().is_empty()
            && *base_actual > 0
            && *limit_pct >= 0.0
            && limit_pct.is_finite()
            && regression_pct.is_finite()
            && matches!(
                rule,
                FooterRule::MaxCandidateTotalFileSizeRegressionPct
                    | FooterRule::MaxCandidateColumnCompressedSizeRegressionPct
                    | FooterRule::MaxCandidateColumnUncompressedSizeRegressionPct
            ) =>
        {
            Ok(())
        }
        _ => Err(Error::new("threshold outcome key and evidence disagree")),
    }
}

fn expected_threshold_keys(
    case: &crate::query_perf::case::ValidatedCase,
    targets: &TargetEnvironmentManifest,
) -> Result<HashSet<ThresholdOutcomeKey>> {
    let mut keys: HashSet<_> = case
        .queries()
        .iter()
        .filter_map(|query| {
            query.max_candidate_latency_regression_pct().map(|_| {
                ThresholdOutcomeKey::QueryLatency {
                    query_name: query.resolved_identity().name().to_string(),
                }
            })
        })
        .collect();
    let Some(thresholds) = case.storage_thresholds() else {
        return Ok(keys);
    };
    for target in &targets.targets {
        for rule in [FooterRule::MinFiles, FooterRule::MinFilesWithColumn] {
            keys.insert(ThresholdOutcomeKey::FooterTarget {
                rule,
                role: target.role,
                target_id: target.target_id.clone(),
                encoding: None,
            });
        }
        for (rule, enabled) in [
            (
                FooterRule::MaxTotalFileSizeBytes,
                thresholds.max_total_file_size_bytes().is_some(),
            ),
            (
                FooterRule::MaxColumnCompressedSizeBytes,
                thresholds.max_column_compressed_size_bytes().is_some(),
            ),
            (
                FooterRule::MaxColumnUncompressedSizeBytes,
                thresholds.max_column_uncompressed_size_bytes().is_some(),
            ),
        ] {
            if enabled {
                keys.insert(ThresholdOutcomeKey::FooterTarget {
                    rule,
                    role: target.role,
                    target_id: target.target_id.clone(),
                    encoding: None,
                });
            }
        }
        for encoding in thresholds.require_encodings() {
            keys.insert(ThresholdOutcomeKey::FooterTarget {
                rule: FooterRule::RequiredEncoding,
                role: target.role,
                target_id: target.target_id.clone(),
                encoding: Some(encoding.clone()),
            });
        }
        for encoding in thresholds.forbid_encodings() {
            keys.insert(ThresholdOutcomeKey::FooterTarget {
                rule: FooterRule::ForbiddenEncoding,
                role: target.role,
                target_id: target.target_id.clone(),
                encoding: Some(encoding.clone()),
            });
        }
    }
    let targets = targets
        .targets
        .iter()
        .map(|target| (target.role, target.target_id.clone()))
        .collect::<HashMap<_, _>>();
    let base = targets
        .get(&TargetRole::Base)
        .ok_or_else(|| Error::new("base target missing"))?;
    let candidate = targets
        .get(&TargetRole::Candidate)
        .ok_or_else(|| Error::new("candidate target missing"))?;
    for (rule, enabled) in [
        (
            FooterRule::MaxCandidateTotalFileSizeRegressionPct,
            thresholds
                .max_candidate_total_file_size_regression_pct()
                .is_some(),
        ),
        (
            FooterRule::MaxCandidateColumnCompressedSizeRegressionPct,
            thresholds
                .max_candidate_column_compressed_size_regression_pct()
                .is_some(),
        ),
        (
            FooterRule::MaxCandidateColumnUncompressedSizeRegressionPct,
            thresholds
                .max_candidate_column_uncompressed_size_regression_pct()
                .is_some(),
        ),
    ] {
        if enabled {
            keys.insert(ThresholdOutcomeKey::FooterComparison {
                rule,
                base_target_id: base.clone(),
                candidate_target_id: candidate.clone(),
            });
        }
    }
    Ok(keys)
}

fn canonical_body<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut body = serde_json::to_value(value)
        .map_err(|err| Error::new(format!("failed to serialize canonical body: {err}")))?;
    body.as_object_mut()
        .ok_or_else(|| Error::new("manifest must serialize as an object"))?
        .remove("completion_digest");
    serde_json::to_vec(&body)
        .map_err(|err| Error::new(format!("failed to serialize canonical body: {err}")))
}
fn version(value: u32) -> Result<()> {
    if value == MANIFEST_VERSION {
        Ok(())
    } else {
        Err(Error::new(format!("unsupported manifest version {value}")))
    }
}
fn valid_digest(value: &Sha256Digest) -> Result<()> {
    Sha256Digest::parse(value.0.clone()).map(|_| ())
}
fn nonempty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::new(format!("{field} must be non-empty")))
    } else {
        Ok(())
    }
}
fn exact_roles(roles: &HashSet<TargetRole>) -> Result<()> {
    if roles == &HashSet::from([TargetRole::Base, TargetRole::Candidate]) {
        Ok(())
    } else {
        Err(Error::new("roles must be exactly base and candidate"))
    }
}
fn same_query(left: &ResolvedQueryIdentity, right: &ResolvedQueryIdentity) -> bool {
    serde_json::to_vec(left).ok() == serde_json::to_vec(right).ok()
}
fn artifact_references(
    references: impl IntoIterator<Item = ArtifactReference>,
) -> Result<HashSet<ArtifactReference>> {
    let mut unique = HashSet::new();
    for reference in references {
        nonempty("artifact reference ID", &reference.id)?;
        valid_digest(&reference.digest)?;
        if !unique.insert(reference) {
            return Err(Error::new("artifact references must be unique"));
        }
    }
    Ok(unique)
}
fn same_artifact_references(
    actual: &[ArtifactReference],
    expected: &HashSet<ArtifactReference>,
) -> bool {
    actual.len() == expected.len()
        && artifact_references(actual.iter().cloned()).is_ok_and(|unique| &unique == expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(value: &str) -> Sha256Digest {
        Sha256Digest::compute(value)
    }
    fn endpoint() -> EndpointUrl {
        EndpointUrl::parse("https://example.com/v1/sql")
            .unwrap_or_else(|err| panic!("endpoint: {err}"))
    }
    fn target(role: TargetRole, id: &str) -> TargetBinding {
        TargetBinding {
            role,
            target_id: id.into(),
            identity: digest(id),
            git_identity: digest("git"),
            build_profile: BuildProfile::Release,
            features: vec![],
            capabilities: vec![Capability::Sql],
            health_endpoint: endpoint(),
            sql_endpoint: endpoint(),
            remote_write_endpoint: None,
            namespace: "ns".into(),
            topology: "single".into(),
            fixture_bindings: vec![TargetFixtureBinding {
                fixture_id: "fixture".into(),
                catalog: Some("greptime".into()),
                database: "public".into(),
                logical_table: "t".into(),
                physical_table: None,
                region: "0".into(),
            }],
        }
    }
    fn targets() -> TargetEnvironmentManifest {
        let mut value = TargetEnvironmentManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            controller_id: "controller".into(),
            targets: vec![
                target(TargetRole::Base, "base"),
                target(TargetRole::Candidate, "candidate"),
            ],
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
    }
    fn queries() -> Vec<ResolvedQueryIdentity> {
        vec![ResolvedQueryIdentity {
            name: "q".into(),
            kind: QueryKind::Sql,
            database: "public".into(),
            query: "select 1".into(),
            warmup: 1,
            iterations: 2,
        }]
    }
    fn fixture(targets: &TargetEnvironmentManifest) -> FixtureManifest {
        let mut value = FixtureManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            fixture_id: "fixture".into(),
            provider: RunnerFixtureProvider::ExternalPrepared,
            data_plan_digest: digest("plan"),
            generator_identity: digest("generator"),
            logical_summary: LogicalFixtureSummary {
                rows: 1,
                series: 1,
                time_start_unix_nanos: 0,
                time_end_unix_nanos: 1,
            },
            storage_prefix: "prefix".into(),
            files: vec![FixtureFile {
                file_id: "file".into(),
                uri: "s3://bucket/file".into(),
                size_bytes: 1,
                digest: digest("file"),
            }],
            target_bindings: vec![
                FixtureTargetBinding {
                    role: TargetRole::Base,
                    target_id: "base".into(),
                    catalog: Some("greptime".into()),
                    database: "public".into(),
                    logical_table: "t".into(),
                    physical_table: None,
                    region: "0".into(),
                    file_ids: vec!["file".into()],
                    external: Some(ExternalPreparedTargetBinding {
                        catalog: "greptime".into(),
                        schema: "public".into(),
                        table_id: 1,
                        region_id: 0,
                        time_index: "ts".into(),
                        time_unit: TimestampUnit::Nanosecond,
                    }),
                },
                FixtureTargetBinding {
                    role: TargetRole::Candidate,
                    target_id: "candidate".into(),
                    catalog: Some("greptime".into()),
                    database: "public".into(),
                    logical_table: "t".into(),
                    physical_table: None,
                    region: "0".into(),
                    file_ids: vec!["file".into()],
                    external: Some(ExternalPreparedTargetBinding {
                        catalog: "greptime".into(),
                        schema: "public".into(),
                        table_id: 1,
                        region_id: 0,
                        time_index: "ts".into(),
                        time_unit: TimestampUnit::Nanosecond,
                    }),
                },
            ],
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
            .validate_against(targets)
            .unwrap_or_else(|err| panic!("fixture: {err}"));
        value
    }
    fn measurement(
        id: &str,
        target_id: &str,
        query: &[ResolvedQueryIdentity],
    ) -> MeasurementManifest {
        let mut value = MeasurementManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            measurement_id: id.into(),
            target_id: target_id.into(),
            fixture_id: "fixture".into(),
            queries: query
                .iter()
                .cloned()
                .map(|identity| QueryMeasurement {
                    samples: (0..identity.iterations)
                        .map(|_| LatencySample { milliseconds: 1.0 })
                        .collect(),
                    identity,
                    status: QueryStatus::Passed,
                    result_digest: digest("result"),
                })
                .collect(),
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
    }
    fn observation(id: &str, target_id: &str) -> ObservationManifest {
        let mut value = ObservationManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            observation_id: id.into(),
            target_id: target_id.into(),
            fixture_id: "fixture".into(),
            status: ObservationStatus::Passed,
            payload: ObservationPayload::Footer(FooterObservation {
                file_count: 1,
                total_file_size_bytes: 2,
                files_with_column: 1,
                column_compressed_size_bytes: 1,
                column_uncompressed_size_bytes: 3,
                encodings: vec!["PLAIN".into()],
            }),
            failure_detail: None,
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
    }
    fn read_bench_observation(
        id: &str,
        target_id: &str,
        parquet: Option<f64>,
        scan: Option<f64>,
    ) -> ObservationManifest {
        let mut value = ObservationManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            observation_id: id.into(),
            target_id: target_id.into(),
            fixture_id: "fixture".into(),
            status: ObservationStatus::Passed,
            payload: ObservationPayload::ReadBench(ReadBenchObservation {
                parquet_median_milliseconds: parquet,
                scan_median_milliseconds: scan,
            }),
            failure_detail: None,
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
    }
    fn request(
        targets: &TargetEnvironmentManifest,
        requests: Vec<ObservationRequest>,
    ) -> ObservationRequestManifest {
        let fixture = fixture(targets);
        let mut value = ObservationRequestManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            request_id: "request".into(),
            fixture_id: "fixture".into(),
            case_digest: digest("case"),
            target_manifest_digest: targets.completion_digest.clone(),
            fixture_manifest_digest: fixture.completion_digest,
            requests,
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
    }
    fn observation_bundle(
        targets: &TargetEnvironmentManifest,
        request: &ObservationRequestManifest,
        observations: Vec<ObservationManifest>,
    ) -> ObservationBundle {
        let fixture = fixture(targets);
        let mut value = ObservationBundle {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            fixture_id: "fixture".into(),
            request: ArtifactReference {
                id: request.request_id.clone(),
                digest: request.completion_digest.clone(),
            },
            target_manifest_digest: targets.completion_digest.clone(),
            fixture_manifest_digest: fixture.completion_digest,
            observations,
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
    }
    fn request_entry(
        role: TargetRole,
        target_id: &str,
        kind: ObservationKind,
        read_bench: Option<ReadBenchRequest>,
    ) -> ObservationRequest {
        ObservationRequest {
            role,
            target_id: target_id.into(),
            kind,
            read_bench,
        }
    }
    fn report(
        query: &[ResolvedQueryIdentity],
        measurements: &[MeasurementManifest],
        observations: &[ObservationManifest],
    ) -> ReportManifest {
        let expected_targets = targets();
        let expected_fixture = fixture(&expected_targets);
        let mut value = ReportManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            report_id: "report".into(),
            target_ids: vec!["base".into(), "candidate".into()],
            fixture_id: "fixture".into(),
            queries: query.to_vec(),
            threshold_results: vec![],
            threshold_outcomes: vec![],
            evaluation_state: EvaluationState::Complete,
            outcome: ReportOutcome::Success,
            failures: vec![],
            failure_details: vec![],
            measurements: measurements
                .iter()
                .map(|value| ArtifactReference {
                    id: value.measurement_id.clone(),
                    digest: value.completion_digest.clone(),
                })
                .collect(),
            observations: observations
                .iter()
                .map(|value| ArtifactReference {
                    id: value.observation_id.clone(),
                    digest: value.completion_digest.clone(),
                })
                .collect(),
            measurement_bundle: Some(ArtifactReference {
                id: "bundle".into(),
                digest: digest("bundle"),
            }),
            observation_bundle: Some(ArtifactReference {
                id: "observations".into(),
                digest: digest("observations"),
            }),
            target_manifest_digest: expected_targets.completion_digest,
            fixture_manifest_digest: expected_fixture.completion_digest,
            provenance: digest("provenance"),
            summary_digest: digest("summary"),
            completion_digest: digest("placeholder"),
        };
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
    }
    #[test]
    fn endpoint_deserialize_and_digest_tamper_fail() {
        assert!(serde_json::from_str::<EndpointUrl>("\"https://u:p@example.com/sql\"").is_err());
        let mut value = targets();
        value.targets[0].namespace = "changed".into();
        assert!(value.validate().is_err());
        assert!(Sha256Digest::parse("sha256:bad").is_err());
    }
    #[test]
    fn target_and_fixture_roundtrip_and_role_crossing_fail() {
        let targets = targets();
        targets
            .validate()
            .unwrap_or_else(|err| panic!("targets: {err}"));
        let text = serde_json::to_string(&targets).unwrap_or_else(|err| panic!("serialize: {err}"));
        let round: TargetEnvironmentManifest =
            serde_json::from_str(&text).unwrap_or_else(|err| panic!("deserialize: {err}"));
        round
            .validate()
            .unwrap_or_else(|err| panic!("round: {err}"));
        let mut bad = fixture(&targets);
        bad.target_bindings[1].target_id = "base".into();
        bad.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(bad.validate_against(&targets).is_err());
    }

    #[test]
    fn api_write_fixture_allows_empty_files_and_file_ids() {
        let mut targets = targets();
        let mut value = fixture(&targets);
        for target in &mut targets.targets {
            target.fixture_bindings[0].catalog = None;
            target.fixture_bindings[0].physical_table = Some("p".into());
        }
        targets
            .seal()
            .unwrap_or_else(|err| panic!("seal targets: {err}"));
        value.provider = RunnerFixtureProvider::ApiWrite;
        value.files.clear();
        for binding in &mut value.target_bindings {
            binding.file_ids.clear();
            binding.external = None;
            binding.catalog = None;
            binding.physical_table = Some("p".into());
        }
        value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        value
            .validate_against(&targets)
            .unwrap_or_else(|err| panic!("api-write fixture: {err}"));

        let mut with_file = value.clone();
        with_file.files.push(FixtureFile {
            file_id: "unexpected".into(),
            uri: "file:///unexpected".into(),
            size_bytes: 1,
            digest: digest("unexpected"),
        });
        with_file.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(with_file.validate_against(&targets).is_err());

        let mut with_file_id = value;
        with_file_id.target_bindings[0]
            .file_ids
            .push("unexpected".into());
        with_file_id
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(with_file_id.validate_against(&targets).is_err());
    }

    #[test]
    fn fixture_target_binding_crossing_and_input_seal_tamper_fail() {
        let mut crossed_targets = targets();
        let sealed_fixture = fixture(&crossed_targets);
        crossed_targets.targets[1]
            .fixture_bindings
            .first_mut()
            .unwrap_or_else(|| panic!("fixture binding"))
            .region = "other".into();
        crossed_targets
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(sealed_fixture.validate_against(&crossed_targets).is_err());

        let targets = targets();
        let mut fixture = fixture(&targets);
        fixture.logical_summary.rows = 2;
        assert!(fixture.validate_against(&targets).is_err());
    }
    #[test]
    fn measurement_query_order_samples_and_report_refs_are_exact() {
        let targets = targets();
        let fixture = fixture(&targets);
        let query = queries();
        let mut measurement = MeasurementManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            measurement_id: "m".into(),
            target_id: "base".into(),
            fixture_id: "fixture".into(),
            queries: vec![QueryMeasurement {
                identity: query[0].clone(),
                status: QueryStatus::Passed,
                samples: vec![
                    LatencySample { milliseconds: 1.0 },
                    LatencySample { milliseconds: 2.0 },
                ],
                result_digest: digest("result"),
            }],
            completion_digest: digest("placeholder"),
        };
        measurement
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        measurement
            .validate_against(&query, &targets, &fixture)
            .unwrap_or_else(|err| panic!("measurement: {err}"));
        measurement.queries[0].samples[0].milliseconds = f64::NAN;
        measurement
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            measurement
                .validate_against(&query, &targets, &fixture)
                .is_err()
        );
    }

    #[test]
    fn observation_and_report_roundtrip_and_references_are_exact() {
        let targets = targets();
        let fixture = fixture(&targets);
        let query = queries();
        let measurements = vec![
            measurement("base-measurement", "base", &query),
            measurement("candidate-measurement", "candidate", &query),
        ];
        let observations = vec![observation("base-observation", "base")];
        let valid_report = report(&query, &measurements, &observations);
        valid_report
            .validate_against(&query, &targets, &fixture, &measurements, &observations)
            .unwrap_or_else(|err| panic!("report: {err}"));
        let observation_roundtrip: ObservationManifest = serde_json::from_str(
            &serde_json::to_string(&observations[0])
                .unwrap_or_else(|err| panic!("serialize: {err}")),
        )
        .unwrap_or_else(|err| panic!("deserialize: {err}"));
        observation_roundtrip
            .validate_against(&targets, &fixture)
            .unwrap_or_else(|err| panic!("observation: {err}"));
        let report_roundtrip: ReportManifest = serde_json::from_str(
            &serde_json::to_string(&valid_report).unwrap_or_else(|err| panic!("serialize: {err}")),
        )
        .unwrap_or_else(|err| panic!("deserialize: {err}"));
        report_roundtrip
            .validate_against(&query, &targets, &fixture, &measurements, &observations)
            .unwrap_or_else(|err| panic!("roundtrip report: {err}"));

        let mut empty = valid_report.clone();
        empty.measurements.clear();
        empty.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            empty
                .validate_against(&query, &targets, &fixture, &measurements, &observations)
                .is_err()
        );
        let mut duplicate = valid_report.clone();
        duplicate
            .measurements
            .push(duplicate.measurements[0].clone());
        duplicate.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            duplicate
                .validate_against(&query, &targets, &fixture, &measurements, &observations)
                .is_err()
        );
        let missing_base = vec![measurements[1].clone()];
        let mut missing_base_report = report(&query, &missing_base, &observations);
        missing_base_report
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            missing_base_report
                .validate_against(&query, &targets, &fixture, &missing_base, &observations)
                .is_err()
        );
    }

    #[test]
    fn report_rejects_query_add_drop_and_reorder() {
        let targets = targets();
        let fixture = fixture(&targets);
        let mut query = queries();
        query.push(ResolvedQueryIdentity {
            name: "q2".into(),
            kind: QueryKind::Sql,
            database: "public".into(),
            query: "select 2".into(),
            warmup: 0,
            iterations: 1,
        });
        let measurements = vec![
            measurement("base-measurement", "base", &query),
            measurement("candidate-measurement", "candidate", &query),
        ];
        let observations = vec![observation("base-observation", "base")];
        for queries in [
            vec![query[0].clone()],
            vec![query[0].clone(), query[1].clone(), query[0].clone()],
            vec![query[1].clone(), query[0].clone()],
        ] {
            let mut value = report(&queries, &measurements, &observations);
            value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
            assert!(
                value
                    .validate_against(&query, &targets, &fixture, &measurements, &observations)
                    .is_err()
            );
        }
    }

    #[test]
    fn failed_report_requires_detail_and_success_requires_complete_evidence() {
        let targets = targets();
        let fixture = fixture(&targets);
        let query = queries();
        let measurements = vec![
            measurement("base-measurement", "base", &query),
            measurement("candidate-measurement", "candidate", &query),
        ];
        let observations = vec![observation("base-observation", "base")];
        let mut failed = report(&query, &measurements, &observations);
        failed.outcome = ReportOutcome::Failed;
        failed.failures.clear();
        failed.failure_details.clear();
        failed.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            failed
                .validate_against(&query, &targets, &fixture, &measurements, &observations)
                .is_err()
        );

        let mut successful = report(&query, &measurements, &observations);
        successful.failures.push("unexpected".into());
        successful
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            successful
                .validate_against(&query, &targets, &fixture, &measurements, &observations)
                .is_err()
        );

        let mut missing_bundle = report(&query, &measurements, &observations);
        missing_bundle.measurement_bundle = None;
        missing_bundle
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            missing_bundle
                .validate_against(&query, &targets, &fixture, &measurements, &observations)
                .is_err()
        );
    }

    #[test]
    fn threshold_keys_are_typed_and_encoding_specific() {
        let required = ThresholdOutcomeKey::FooterTarget {
            rule: FooterRule::RequiredEncoding,
            role: TargetRole::Base,
            target_id: "base".into(),
            encoding: Some("PLAIN".into()),
        };
        let forbidden = ThresholdOutcomeKey::FooterTarget {
            rule: FooterRule::ForbiddenEncoding,
            role: TargetRole::Base,
            target_id: "base".into(),
            encoding: Some("PLAIN".into()),
        };
        assert_ne!(required, forbidden);
        assert!(
            validate_threshold_outcome(&ThresholdOutcome {
                key: required,
                passed: true,
                evidence: ThresholdEvidence::FooterEncoding {
                    encoding_present: true
                },
            })
            .is_ok()
        );
        assert!(
            validate_threshold_outcome(&ThresholdOutcome {
                key: forbidden,
                passed: true,
                evidence: ThresholdEvidence::FooterValue {
                    actual: 1,
                    limit: 1
                },
            })
            .is_err()
        );
    }

    #[test]
    fn identity_and_seal_mutations_reject_stale_or_crossed_artifacts() {
        let targets = targets();
        let fixture = fixture(&targets);
        let query = queries();
        let request = request(&targets, vec![]);
        let measurements = vec![
            measurement("base-measurement", "base", &query),
            measurement("candidate-measurement", "candidate", &query),
        ];
        let mut bundle = MeasurementBundle {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            fixture_id: "fixture".into(),
            case_digest: digest("case"),
            target_manifest_digest: targets.completion_digest.clone(),
            fixture_manifest_digest: fixture.completion_digest.clone(),
            measurements: measurements.clone(),
            observation_request: ArtifactReference {
                id: request.request_id.clone(),
                digest: request.completion_digest.clone(),
            },
            completion_digest: digest("placeholder"),
        };
        bundle.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        bundle
            .validate_against(&query, &digest("case"), &targets, &fixture, &request)
            .unwrap_or_else(|err| panic!("bundle: {err}"));

        let mut stale_target = targets.clone();
        stale_target.targets[0].identity = digest("other identity");
        assert!(stale_target.validate().is_err());
        let mut crossed_target = targets.clone();
        crossed_target.targets[0].target_id = "candidate".into();
        crossed_target
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(crossed_target.validate().is_err());

        let mutations: [(&str, fn(&mut MeasurementBundle)); 5] = [
            ("case", |value: &mut MeasurementBundle| {
                value.case_digest = digest("other")
            }),
            ("target seal", |value: &mut MeasurementBundle| {
                value.target_manifest_digest = digest("other")
            }),
            ("fixture seal", |value: &mut MeasurementBundle| {
                value.fixture_manifest_digest = digest("other")
            }),
            ("request ID", |value: &mut MeasurementBundle| {
                value.observation_request.id = "other".into()
            }),
            ("request seal", |value: &mut MeasurementBundle| {
                value.observation_request.digest = digest("other")
            }),
        ];
        for (name, mutate) in mutations {
            let mut changed = bundle.clone();
            mutate(&mut changed);
            changed.seal().unwrap_or_else(|err| panic!("{name}: {err}"));
            assert!(
                changed
                    .validate_against(&query, &digest("case"), &targets, &fixture, &request)
                    .is_err(),
                "{name} cross-reference must fail"
            );
        }

        let mut duplicate_file = fixture.clone();
        duplicate_file.target_bindings[0]
            .file_ids
            .push("file".into());
        duplicate_file
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(duplicate_file.validate_against(&targets).is_err());
    }

    #[test]
    fn observation_request_and_read_bench_matrix_is_exact() {
        let targets = targets();
        let fixture = fixture(&targets);
        let case_digest = digest("case");
        for (name, entries) in [
            (
                "footer has read-bench payload",
                vec![request_entry(
                    TargetRole::Base,
                    "base",
                    ObservationKind::Footer,
                    Some(ReadBenchRequest {
                        parquetbench: true,
                        scanbench: false,
                    }),
                )],
            ),
            (
                "read-bench lacks modes",
                vec![request_entry(
                    TargetRole::Base,
                    "base",
                    ObservationKind::ReadBench,
                    None,
                )],
            ),
            (
                "read-bench enables neither mode",
                vec![request_entry(
                    TargetRole::Base,
                    "base",
                    ObservationKind::ReadBench,
                    Some(ReadBenchRequest {
                        parquetbench: false,
                        scanbench: false,
                    }),
                )],
            ),
            (
                "duplicate request",
                vec![
                    request_entry(TargetRole::Base, "base", ObservationKind::Footer, None),
                    request_entry(TargetRole::Base, "base", ObservationKind::Footer, None),
                ],
            ),
            (
                "wrong role target",
                vec![request_entry(
                    TargetRole::Base,
                    "candidate",
                    ObservationKind::Footer,
                    None,
                )],
            ),
        ] {
            let value = request(&targets, entries);
            assert!(
                value
                    .validate_against(&case_digest, &targets, &fixture)
                    .is_err(),
                "{name} must fail"
            );
        }

        for (name, parquet, scan) in [
            ("parquet only", true, false),
            ("scan only", false, true),
            ("both", true, true),
        ] {
            let request = request(
                &targets,
                vec![request_entry(
                    TargetRole::Base,
                    "base",
                    ObservationKind::ReadBench,
                    Some(ReadBenchRequest {
                        parquetbench: parquet,
                        scanbench: scan,
                    }),
                )],
            );
            let bundle = observation_bundle(
                &targets,
                &request,
                vec![read_bench_observation(
                    name,
                    "base",
                    parquet.then_some(1.0),
                    scan.then_some(2.0),
                )],
            );
            bundle
                .validate_against(&case_digest, &targets, &fixture, &request)
                .unwrap_or_else(|err| panic!("{name}: {err}"));
        }

        let footer_request = request(
            &targets,
            vec![request_entry(
                TargetRole::Base,
                "base",
                ObservationKind::Footer,
                None,
            )],
        );
        for (name, observation) in [
            ("visibility", {
                let mut value = observation("visible", "base");
                value.payload = ObservationPayload::Visibility { visible_rows: 1 };
                value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
                value
            }),
            ("failed", {
                let mut value = observation("failed", "base");
                value.status = ObservationStatus::Failed;
                value.failure_detail = Some("controller failed".into());
                value.seal().unwrap_or_else(|err| panic!("seal: {err}"));
                value
            }),
        ] {
            let bundle = observation_bundle(&targets, &footer_request, vec![observation]);
            assert!(
                bundle
                    .validate_against(&case_digest, &targets, &fixture, &footer_request)
                    .is_err(),
                "{name} observation must fail"
            );
        }

        let read_request = request(
            &targets,
            vec![request_entry(
                TargetRole::Base,
                "base",
                ObservationKind::ReadBench,
                Some(ReadBenchRequest {
                    parquetbench: true,
                    scanbench: false,
                }),
            )],
        );
        for (name, observations) in [
            ("missing", vec![]),
            (
                "wrong payload kind",
                vec![observation("footer-extra", "base")],
            ),
            (
                "duplicate",
                vec![
                    read_bench_observation("read-one", "base", Some(1.0), None),
                    read_bench_observation("read-two", "base", Some(1.0), None),
                ],
            ),
        ] {
            let bundle = observation_bundle(&targets, &read_request, observations);
            assert!(
                bundle
                    .validate_against(&case_digest, &targets, &fixture, &read_request)
                    .is_err(),
                "{name} observation coverage must fail"
            );
        }
    }

    #[test]
    fn report_status_requires_typed_failures_and_successful_evidence() {
        let targets = targets();
        let fixture = fixture(&targets);
        let query = queries();
        let measurements = vec![
            measurement("base-measurement", "base", &query),
            measurement("candidate-measurement", "candidate", &query),
        ];
        let mut failed_measurement = measurements[0].clone();
        failed_measurement.queries[0].status = QueryStatus::Failed;
        failed_measurement
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            failed_measurement
                .validate_against(&query, &targets, &fixture)
                .is_err()
        );

        let mut failed_observation = observation("failed-observation", "base");
        failed_observation.status = ObservationStatus::Failed;
        failed_observation.failure_detail = Some("controller failure".into());
        failed_observation
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        let failed_observations = [failed_observation.clone()];
        let mut success = report(&query, &measurements, &failed_observations);
        success.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            success
                .validate_against(
                    &query,
                    &targets,
                    &fixture,
                    &measurements,
                    &failed_observations,
                )
                .is_err()
        );

        let mut failed = report(&query, &measurements, &[]);
        failed.outcome = ReportOutcome::Failed;
        failed.failures = vec!["untyped failure".into()];
        failed.failure_details.clear();
        failed.measurements.clear();
        failed.observations.clear();
        failed.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            failed
                .validate_against(&query, &targets, &fixture, &[], &[])
                .is_err()
        );

        let mut blank_failure = failed_observation;
        blank_failure.failure_detail = Some(" ".into());
        blank_failure
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(blank_failure.validate_against(&targets, &fixture).is_err());
    }

    #[test]
    fn threshold_coverage_rejects_every_non_exact_key_set() {
        let targets = targets();
        let case = crate::query_perf::case::load_case(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../tests/perf/query_cases/prom_remote_write_mixed_every/case.toml"),
        )
        .unwrap_or_else(|err| panic!("case: {err}"));
        let query = case
            .manifest_queries()
            .unwrap_or_else(|err| panic!("queries: {err}"));
        let measurements = vec![
            measurement("base-measurement", "base", &query),
            measurement("candidate-measurement", "candidate", &query),
        ];
        let mut value = report(&query, &measurements, &[]);
        let keys =
            expected_threshold_keys(&case, &targets).unwrap_or_else(|err| panic!("keys: {err}"));
        value.threshold_outcomes = keys
            .iter()
            .cloned()
            .map(|key| ThresholdOutcome {
                evidence: match &key {
                    ThresholdOutcomeKey::QueryLatency { .. } => ThresholdEvidence::QueryLatency {
                        base_milliseconds: 1.0,
                        candidate_milliseconds: 1.0,
                        limit_pct: 75.0,
                        regression_pct: 0.0,
                    },
                    ThresholdOutcomeKey::FooterTarget {
                        rule: FooterRule::RequiredEncoding | FooterRule::ForbiddenEncoding,
                        ..
                    } => ThresholdEvidence::FooterEncoding {
                        encoding_present: true,
                    },
                    ThresholdOutcomeKey::FooterTarget { .. } => ThresholdEvidence::FooterValue {
                        actual: 1,
                        limit: 1,
                    },
                    ThresholdOutcomeKey::FooterComparison { .. } => {
                        ThresholdEvidence::FooterComparison {
                            base_actual: 1,
                            candidate_actual: 1,
                            limit_pct: 75.0,
                            regression_pct: 0.0,
                        }
                    }
                },
                key,
                passed: true,
            })
            .collect();
        value.threshold_results = case
            .queries()
            .iter()
            .filter_map(|query| {
                query
                    .max_candidate_latency_regression_pct()
                    .map(|limit| ThresholdResult {
                        query_name: query.resolved_identity().name().into(),
                        threshold: QueryThresholds {
                            max_candidate_latency_regression_pct: Some(limit),
                        },
                        passed: true,
                        detail: "pass".into(),
                    })
            })
            .collect();
        value
            .validate_threshold_coverage(&case, &targets)
            .unwrap_or_else(|err| panic!("coverage: {err}"));

        let mutations: [(&str, fn(&mut Vec<ThresholdOutcome>)); 4] = [
            ("missing", |outcomes: &mut Vec<ThresholdOutcome>| {
                outcomes.pop();
            }),
            ("duplicate", |outcomes: &mut Vec<ThresholdOutcome>| {
                outcomes.push(outcomes[0].clone());
            }),
            ("extra", |outcomes: &mut Vec<ThresholdOutcome>| {
                outcomes.push(ThresholdOutcome {
                    key: ThresholdOutcomeKey::QueryLatency {
                        query_name: "extra".into(),
                    },
                    passed: true,
                    evidence: ThresholdEvidence::QueryLatency {
                        base_milliseconds: 1.0,
                        candidate_milliseconds: 1.0,
                        limit_pct: 1.0,
                        regression_pct: 0.0,
                    },
                });
            }),
            ("wrong target", |outcomes: &mut Vec<ThresholdOutcome>| {
                if let Some(ThresholdOutcome {
                    key: ThresholdOutcomeKey::FooterTarget { target_id, .. },
                    ..
                }) = outcomes.iter_mut().find(|outcome| {
                    matches!(&outcome.key, ThresholdOutcomeKey::FooterTarget { .. })
                }) {
                    *target_id = "wrong".into();
                }
            }),
        ];
        for (name, mutate) in mutations {
            for outcome in [ReportOutcome::Success, ReportOutcome::Failed] {
                let mut changed = value.clone();
                changed.outcome = outcome;
                if outcome == ReportOutcome::Failed {
                    changed.failures = vec!["threshold failed".into()];
                    changed.failure_details = vec![ReportFailure {
                        stage: ReportFailureStage::Threshold,
                        detail: "threshold failed".into(),
                    }];
                }
                mutate(&mut changed.threshold_outcomes);
                assert!(
                    changed
                        .validate_threshold_coverage(&case, &targets)
                        .is_err(),
                    "{outcome:?} {name} threshold keys must fail"
                );
            }
        }

        let query_mutations: [(&str, fn(&mut ReportManifest)); 3] = [
            ("missing query", |report: &mut ReportManifest| {
                report.threshold_results.clear();
            }),
            ("duplicate query", |report: &mut ReportManifest| {
                report
                    .threshold_results
                    .push(report.threshold_results[0].clone());
            }),
            ("wrong query limit", |report: &mut ReportManifest| {
                report.threshold_results[0]
                    .threshold
                    .max_candidate_latency_regression_pct = Some(1.0);
            }),
        ];
        for (name, mutate) in query_mutations {
            let mut changed = value.clone();
            mutate(&mut changed);
            assert!(
                changed
                    .validate_threshold_coverage(&case, &targets)
                    .is_err(),
                "{name} query threshold must fail"
            );
        }

        let mut early_failed = value.clone();
        early_failed.outcome = ReportOutcome::Failed;
        early_failed.evaluation_state = EvaluationState::NotStarted;
        early_failed.measurement_bundle = None;
        early_failed.threshold_outcomes.clear();
        early_failed.threshold_results.clear();
        early_failed
            .validate_threshold_coverage(&case, &targets)
            .unwrap_or_else(|err| panic!("early failure: {err}"));
    }

    #[test]
    fn evaluation_state_rejects_successful_or_untyped_incomplete_reports() {
        let targets = targets();
        let fixture = fixture(&targets);
        let query = queries();
        let measurements = vec![
            measurement("base-measurement", "base", &query),
            measurement("candidate-measurement", "candidate", &query),
        ];
        let mut successful_incomplete = report(&query, &measurements, &[]);
        successful_incomplete.evaluation_state = EvaluationState::Incomplete;
        successful_incomplete
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            successful_incomplete
                .validate_against(&query, &targets, &fixture, &measurements, &[])
                .is_err()
        );

        let mut missing_evaluation_detail = report(&query, &measurements, &[]);
        missing_evaluation_detail.outcome = ReportOutcome::Failed;
        missing_evaluation_detail.evaluation_state = EvaluationState::Incomplete;
        missing_evaluation_detail.failures = vec!["failed".into()];
        missing_evaluation_detail.failure_details = vec![ReportFailure {
            stage: ReportFailureStage::Threshold,
            detail: "not an evaluation failure".into(),
        }];
        missing_evaluation_detail
            .seal()
            .unwrap_or_else(|err| panic!("seal: {err}"));
        assert!(
            missing_evaluation_detail
                .validate_against(&query, &targets, &fixture, &measurements, &[])
                .is_err()
        );
    }
}
