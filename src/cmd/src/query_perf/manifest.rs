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
    pub fixture_binding: Option<TargetFixtureBinding>,
}

/// Target-local fixture identity. It must agree with the controller fixture
/// binding before the runner consumes either artifact.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TargetFixtureBinding {
    pub fixture_id: String,
    pub database: String,
    pub table: String,
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
    pub database: String,
    pub table: String,
    pub region: String,
    pub file_ids: Vec<String>,
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

/// Result of a configured query threshold.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdResult {
    pub query_name: String,
    pub threshold: QueryThresholds,
    pub passed: bool,
    pub detail: String,
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
    pub failures: Vec<String>,
    pub measurements: Vec<ArtifactReference>,
    pub observations: Vec<ArtifactReference>,
    pub provenance: Sha256Digest,
    pub completion_digest: Sha256Digest,
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
sealed_manifest!(ObservationManifest);
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
            let fixture = target
                .fixture_binding
                .as_ref()
                .ok_or_else(|| Error::new("target fixture_binding must be present"))?;
            for value in [
                &fixture.fixture_id,
                &fixture.database,
                &fixture.table,
                &fixture.region,
            ] {
                nonempty("target fixture binding value", value)?;
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
        if self.target_bindings.len() != 2 {
            return Err(Error::new(
                "fixture bindings must be exactly base and candidate",
            ));
        }
        let file_ids: HashSet<_> = self
            .files
            .iter()
            .map(|file| file.file_id.as_str())
            .collect();
        let mut roles = HashSet::new();
        for binding in &self.target_bindings {
            roles.insert(binding.role);
            let target = target_by_role
                .get(&binding.role)
                .ok_or_else(|| Error::new("missing target role"))?;
            if binding.target_id != target.target_id {
                return Err(Error::new(
                    "fixture role-to-target mapping differs from target manifest",
                ));
            }
            for value in [&binding.database, &binding.table, &binding.region] {
                nonempty("fixture binding", value)?;
            }
            let target_fixture = target
                .fixture_binding
                .as_ref()
                .ok_or_else(|| Error::new("target fixture_binding must be present"))?;
            if target_fixture.fixture_id != self.fixture_id
                || target_fixture.database != binding.database
                || target_fixture.table != binding.table
                || target_fixture.region != binding.region
            {
                return Err(Error::new(
                    "target fixture_binding does not match fixture target binding",
                ));
            }
            match self.provider {
                RunnerFixtureProvider::ApiWrite if !binding.file_ids.is_empty() => {
                    return Err(Error::new(
                        "api_write fixture binding must not contain file_ids",
                    ));
                }
                RunnerFixtureProvider::ExternalPrepared if binding.file_ids.is_empty() => {
                    return Err(Error::new(
                        "external_prepared fixture binding requires file_ids",
                    ));
                }
                _ => {}
            }
            if binding
                .file_ids
                .iter()
                .any(|id| !file_ids.contains(id.as_str()))
            {
                return Err(Error::new("fixture binding must reference existing files"));
            }
        }
        exact_roles(&roles)
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
            if query.samples.len() != query.identity.iterations as usize
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

impl ReportManifest {
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
        if measurement_roles != HashSet::from([TargetRole::Base, TargetRole::Candidate]) {
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
        for threshold in &self.threshold_results {
            nonempty("threshold query", &threshold.query_name)?;
            nonempty("threshold detail", &threshold.detail)?;
            if !expected
                .iter()
                .any(|query| query.name == threshold.query_name)
            {
                return Err(Error::new("threshold references unknown query"));
            }
        }
        valid_digest(&self.provenance)
    }
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
            fixture_binding: Some(TargetFixtureBinding {
                fixture_id: "fixture".into(),
                database: "public".into(),
                table: "t".into(),
                region: "0".into(),
            }),
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
                    database: "public".into(),
                    table: "t".into(),
                    region: "0".into(),
                    file_ids: vec!["file".into()],
                },
                FixtureTargetBinding {
                    role: TargetRole::Candidate,
                    target_id: "candidate".into(),
                    database: "public".into(),
                    table: "t".into(),
                    region: "0".into(),
                    file_ids: vec!["file".into()],
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
    fn report(
        query: &[ResolvedQueryIdentity],
        measurements: &[MeasurementManifest],
        observations: &[ObservationManifest],
    ) -> ReportManifest {
        let mut value = ReportManifest {
            version: MANIFEST_VERSION,
            run_id: "run".into(),
            report_id: "report".into(),
            target_ids: vec!["base".into(), "candidate".into()],
            fixture_id: "fixture".into(),
            queries: query.to_vec(),
            threshold_results: vec![],
            failures: vec![],
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
            provenance: digest("provenance"),
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
        let targets = targets();
        let mut value = fixture(&targets);
        value.provider = RunnerFixtureProvider::ApiWrite;
        value.files.clear();
        for binding in &mut value.target_bindings {
            binding.file_ids.clear();
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
            .fixture_binding
            .as_mut()
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
}
