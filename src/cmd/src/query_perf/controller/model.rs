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

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::query_perf::error::{Error, Result};
use crate::query_perf::manifest::{ArtifactReference, BuildProfile, Sha256Digest, TargetRole};

pub const BUILD_ATTESTATION_VERSION: u32 = 1;
pub const SUITE_MANIFEST_VERSION: u32 = 1;

/// Inputs accepted by the local-controller CLI. All paths are explicit because
/// the controller may never infer a binary, runner, or output location.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RunSuiteArgs {
    pub suite: PathBuf,
    pub base_binary: PathBuf,
    pub candidate_binary: PathBuf,
    pub base_attestation: PathBuf,
    pub candidate_attestation: PathBuf,
    pub endpoint_runner: PathBuf,
    pub work_root: PathBuf,
    pub fixture_cache_root: Option<PathBuf>,
    pub report: PathBuf,
    pub summary: PathBuf,
    pub artifact: PathBuf,
}

/// Validated immutable controller specification. It deliberately contains no
/// process handles or cache/materialization state.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LocalControllerRunSpec {
    pub suite: PathBuf,
    pub base: LocalTargetSpec,
    pub candidate: LocalTargetSpec,
    pub endpoint_runner: PathBuf,
    pub work_root: PathBuf,
    pub fixture_cache_root: Option<PathBuf>,
    pub report: PathBuf,
    pub summary: PathBuf,
    pub artifact: PathBuf,
}

impl LocalControllerRunSpec {
    pub fn from_args(args: RunSuiteArgs) -> Result<Self> {
        for (name, path) in [
            ("suite", &args.suite),
            ("base binary", &args.base_binary),
            ("candidate binary", &args.candidate_binary),
            ("base attestation", &args.base_attestation),
            ("candidate attestation", &args.candidate_attestation),
            ("endpoint runner", &args.endpoint_runner),
            ("work root", &args.work_root),
            ("report", &args.report),
            ("summary", &args.summary),
            ("artifact", &args.artifact),
        ] {
            usable_path(name, path)?;
        }
        if args.report == args.summary
            || args.report == args.artifact
            || args.summary == args.artifact
        {
            return Err(Error::new(
                "report, summary, and artifact paths must differ",
            ));
        }
        if let Some(root) = &args.fixture_cache_root {
            usable_path("fixture cache root", root)?;
        }
        Ok(Self {
            suite: args.suite,
            base: LocalTargetSpec::new(TargetRole::Base, args.base_binary, args.base_attestation),
            candidate: LocalTargetSpec::new(
                TargetRole::Candidate,
                args.candidate_binary,
                args.candidate_attestation,
            ),
            endpoint_runner: args.endpoint_runner,
            work_root: args.work_root,
            fixture_cache_root: args.fixture_cache_root,
            report: args.report,
            summary: args.summary,
            artifact: args.artifact,
        })
    }
}

/// Strict, ordered selection of query-regression cases for one controller run.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteManifest {
    pub version: u32,
    pub cases: Vec<SuiteCaseSpec>,
}

impl SuiteManifest {
    pub fn validate(&self) -> Result<()> {
        if self.version != SUITE_MANIFEST_VERSION {
            return Err(Error::new(format!(
                "unsupported suite manifest version {}",
                self.version
            )));
        }
        if self.cases.is_empty() {
            return Err(Error::new("suite manifest must contain at least one case"));
        }
        let mut paths = HashSet::new();
        for case in &self.cases {
            usable_path("suite case", &case.case)?;
            if !paths.insert(&case.case) {
                return Err(Error::new("suite manifest cases must be unique"));
            }
        }
        Ok(())
    }
}

/// One case in a suite. The vector order in [`SuiteManifest`] is execution order.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteCaseSpec {
    pub case: PathBuf,
}

/// Role-specific local target input, before attestation and port preflight.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalTargetSpec {
    pub role: TargetRole,
    pub binary: PathBuf,
    pub attestation: PathBuf,
}

impl LocalTargetSpec {
    pub fn new(role: TargetRole, binary: PathBuf, attestation: PathBuf) -> Self {
        Self {
            role,
            binary,
            attestation,
        }
    }
}

/// Build-produced sidecar that binds a release-equivalent binary to its source
/// and feature graph. This is intentionally stricter than runtime --version.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildAttestation {
    pub version: u32,
    pub build_id: String,
    pub binary_sha256: Sha256Digest,
    pub commit: String,
    pub git_identity: Sha256Digest,
    pub profile: AttestedBuildProfile,
    pub features: Vec<String>,
    pub tool_identity: String,
    pub runtime_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<BuildProvenance>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildProvenance {
    pub builder: String,
    pub source: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttestedBuildProfile {
    Release,
    ReleaseWithDebugInfo,
    Profile,
    Debug,
    Dev,
}

impl AttestedBuildProfile {
    pub fn target_profile(self) -> Result<BuildProfile> {
        match self {
            Self::Release => Ok(BuildProfile::Release),
            Self::ReleaseWithDebugInfo => Ok(BuildProfile::ReleaseWithDebugInfo),
            Self::Profile => Ok(BuildProfile::Profile),
            Self::Debug | Self::Dev => Err(Error::new(
                "build attestation profile must be release-equivalent, not debug/dev",
            )),
        }
    }
}

/// Identity fields that map directly into the existing TargetBinding contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TargetIdentity {
    pub identity: Sha256Digest,
    pub git_identity: Sha256Digest,
    pub build_profile: BuildProfile,
    pub features: Vec<String>,
    pub build_id: String,
    pub commit: String,
}

impl BuildAttestation {
    pub fn validate(&self) -> Result<()> {
        if self.version != BUILD_ATTESTATION_VERSION {
            return Err(Error::new(format!(
                "unsupported build attestation version {}",
                self.version
            )));
        }
        for (name, value) in [
            ("build_id", &self.build_id),
            ("commit", &self.commit),
            ("tool_identity", &self.tool_identity),
            ("runtime_version", &self.runtime_version),
        ] {
            nonempty(name, value)?;
        }
        if !canonical_commit(&self.commit) {
            return Err(Error::new(
                "attestation commit must be 40 lowercase hexadecimal characters",
            ));
        }
        Sha256Digest::parse(self.binary_sha256.as_str().to_string())?;
        Sha256Digest::parse(self.git_identity.as_str().to_string())?;
        if self.git_identity != Sha256Digest::compute(&self.commit) {
            return Err(Error::new(
                "attestation git_identity must be the SHA-256 digest of commit",
            ));
        }
        self.profile.target_profile()?;
        let mut previous = None;
        for feature in &self.features {
            valid_identifier("attestation feature", feature)?;
            if previous.is_some_and(|value: &String| value >= feature) {
                return Err(Error::new(
                    "attestation features must be sorted and deduplicated",
                ));
            }
            previous = Some(feature);
        }
        if let Some(provenance) = &self.provenance {
            nonempty("provenance builder", &provenance.builder)?;
            nonempty("provenance source", &provenance.source)?;
        }
        Ok(())
    }

    pub fn target_identity(&self) -> Result<TargetIdentity> {
        self.validate()?;
        Ok(TargetIdentity {
            identity: self.binary_sha256.clone(),
            git_identity: self.git_identity.clone(),
            build_profile: self.profile.target_profile()?,
            features: self.features.clone(),
            build_id: self.build_id.clone(),
            commit: self.commit.clone(),
        })
    }
}

pub fn load_build_attestation(path: &Path) -> Result<BuildAttestation> {
    let bytes = std::fs::read(path).map_err(|err| {
        Error::new(format!(
            "failed to read attestation {}: {err}",
            path.display()
        ))
    })?;
    let attestation = serde_json::from_slice::<BuildAttestation>(&bytes).map_err(|err| {
        Error::new(format!(
            "failed to parse attestation {}: {err}",
            path.display()
        ))
    })?;
    attestation.validate()?;
    Ok(attestation)
}

pub fn hash_binary(path: &Path) -> Result<Sha256Digest> {
    let file = File::open(path)
        .map_err(|err| Error::new(format!("failed to open binary {}: {err}", path.display())))?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(|err| {
            Error::new(format!("failed to read binary {}: {err}", path.display()))
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Sha256Digest::parse(format!("sha256:{:x}", hasher.finalize()))
}

/// Runtime output supplied by a later command-execution lane. Keeping this
/// interface pure makes sidecar/binary identity tests independent of spawning.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RuntimeVersionEvidence {
    pub version: String,
    pub commit: String,
}

impl RuntimeVersionEvidence {
    /// Parses Greptime's long-version output emitted by Clap.
    ///
    /// `greptime --long-version` renders an optional product banner followed by
    /// the `common_version::verbose_version()` fields: `branch`, `commit`,
    /// `clean`, and `version`, each as `name: value` on its own line.
    pub fn parse(text: &str) -> Result<Self> {
        let mut version = None;
        let mut commit = None;
        let mut branch = None;
        let mut clean = None;
        let mut banner = None;
        for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let Some((key, value)) = line.split_once(':') else {
                // Clap prefixes the verbose version with the product and short
                // version. It is not an attestation field.
                if banner.replace(line).is_some()
                    || version.is_some()
                    || commit.is_some()
                    || branch.is_some()
                    || clean.is_some()
                {
                    return Err(Error::new(
                        "runtime version evidence has invalid product banner",
                    ));
                }
                continue;
            };
            match key.trim() {
                "version" if version.is_none() => version = Some(value.trim().to_string()),
                "commit" if commit.is_none() => commit = Some(value.trim().to_string()),
                "branch" if branch.is_none() => branch = Some(value.trim().to_string()),
                "clean" if clean.is_none() => clean = Some(value.trim().to_string()),
                "version" | "commit" | "branch" | "clean" => {
                    return Err(Error::new(
                        "runtime version evidence has duplicate identity field",
                    ));
                }
                _ => return Err(Error::new("runtime version evidence has unknown field")),
            }
        }
        let evidence = Self {
            version: version.ok_or_else(|| Error::new("runtime version evidence lacks version"))?,
            commit: commit.ok_or_else(|| Error::new("runtime version evidence lacks commit"))?,
        };
        nonempty("runtime version", &evidence.version)?;
        // A detached checkout has an intentionally empty branch in Greptime's
        // build metadata, but the field itself must still be present.
        branch.ok_or_else(|| Error::new("runtime version evidence lacks branch"))?;
        if clean.as_deref() != Some("true") && clean.as_deref() != Some("false") {
            return Err(Error::new(
                "runtime version evidence clean must be true or false",
            ));
        }
        if !canonical_commit(&evidence.commit) {
            return Err(Error::new("runtime version evidence commit is invalid"));
        }
        Ok(evidence)
    }

    pub fn validate(&self) -> Result<()> {
        nonempty("runtime version", &self.version)?;
        if !canonical_commit(&self.commit) {
            return Err(Error::new("runtime version evidence commit is invalid"));
        }
        Ok(())
    }
}

pub trait VersionEvidenceProvider: Send + Sync {
    fn version_evidence(&self, binary: &Path) -> Result<RuntimeVersionEvidence>;
}

pub fn validate_attested_binary(
    binary: &Path,
    attestation: &BuildAttestation,
    provider: &dyn VersionEvidenceProvider,
) -> Result<TargetIdentity> {
    attestation.validate()?;
    let actual = hash_binary(binary)?;
    if actual != attestation.binary_sha256 {
        return Err(Error::new(
            "binary SHA-256 does not match build attestation",
        ));
    }
    let runtime = provider.version_evidence(binary)?;
    runtime.validate()?;
    if runtime.version != attestation.runtime_version || runtime.commit != attestation.commit {
        return Err(Error::new(
            "runtime --version evidence does not match build attestation",
        ));
    }
    attestation.target_identity()
}

/// Exactly eight loopback ports reserved for one local target role.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortSet {
    pub metasrv_grpc: u16,
    pub metasrv_http: u16,
    pub datanode_grpc: u16,
    pub datanode_http: u16,
    pub frontend_http: u16,
    pub frontend_grpc: u16,
    pub frontend_mysql: u16,
    pub frontend_postgres: u16,
}

impl PortSet {
    pub fn validate(&self) -> Result<()> {
        let ports = [
            self.metasrv_grpc,
            self.metasrv_http,
            self.datanode_grpc,
            self.datanode_http,
            self.frontend_http,
            self.frontend_grpc,
            self.frontend_mysql,
            self.frontend_postgres,
        ];
        if ports.contains(&0) || ports.iter().collect::<HashSet<_>>().len() != ports.len() {
            return Err(Error::new("PortSet requires eight unique nonzero ports"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessComponent {
    MetaSrv,
    DataNode,
    Frontend,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessExit {
    Exited { code: i32 },
    Signaled { signal: i32 },
    StillRunning,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuiesceOutcome {
    Graceful {
        role: TargetRole,
        component: ProcessComponent,
        exit: ProcessExit,
    },
    Forced {
        role: TargetRole,
        component: ProcessComponent,
        exit: ProcessExit,
        reason: ForcedCleanupReason,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForcedCleanupReason {
    Timeout,
    Signal,
    Error,
}

impl QuiesceOutcome {
    pub fn trusted_for_observation(&self) -> bool {
        matches!(
            self,
            Self::Graceful {
                exit: ProcessExit::Exited { code: 0 },
                ..
            }
        )
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheOutcome {
    Hit,
    Miss,
    InvalidRebuild,
    Disabled,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationMethod {
    Reflink,
    Hardlink,
    Copy,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaterializationSummary {
    pub cache_outcome: CacheOutcome,
    pub reflink_files: u64,
    pub hardlink_files: u64,
    pub copied_files: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandOutcome {
    pub role: TargetRole,
    pub component: ProcessComponent,
    pub exit: ProcessExit,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerRunOutcome {
    pub run_id: String,
    pub outcome: ControllerOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_manifest: Option<ArtifactReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixture_manifest: Option<ArtifactReference>,
    #[serde(default)]
    pub commands: Vec<CommandOutcome>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControllerOutcome {
    Succeeded,
    Failed,
    NotImplemented,
}

fn usable_path(name: &str, path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        Err(Error::new(format!("{name} path must be non-empty")))
    } else {
        Ok(())
    }
}
fn nonempty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::new(format!("{name} must be non-empty")))
    } else {
        Ok(())
    }
}
fn valid_identifier(name: &str, value: &str) -> Result<()> {
    nonempty(name, value)?;
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        Ok(())
    } else {
        Err(Error::new(format!("{name} is not an identifier")))
    }
}

fn canonical_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_perf::manifest::{
        Capability, EndpointUrl, TargetBinding, TargetEnvironmentManifest, TargetFixtureBinding,
    };

    fn digest(value: &str) -> Sha256Digest {
        Sha256Digest::compute(value)
    }
    fn attestation(binary: Sha256Digest) -> BuildAttestation {
        BuildAttestation {
            version: BUILD_ATTESTATION_VERSION,
            build_id: "build-1".into(),
            binary_sha256: binary,
            commit: "0123456789abcdef0123456789abcdef01234567".into(),
            git_identity: digest("0123456789abcdef0123456789abcdef01234567"),
            profile: AttestedBuildProfile::Release,
            features: vec!["dev-tools".into()],
            tool_identity: "rustc-nightly".into(),
            runtime_version: "v1".into(),
            provenance: None,
        }
    }
    struct Evidence {
        version: String,
        commit: String,
    }
    impl VersionEvidenceProvider for Evidence {
        fn version_evidence(&self, _: &Path) -> Result<RuntimeVersionEvidence> {
            Ok(RuntimeVersionEvidence {
                version: self.version.clone(),
                commit: self.commit.clone(),
            })
        }
    }
    #[test]
    fn attestation_is_strict_and_rejects_debug_feature_disorder_and_unknown_fields() {
        let valid = attestation(digest("binary"));
        assert!(valid.validate().is_ok());
        let mut debug = valid.clone();
        debug.profile = AttestedBuildProfile::Debug;
        assert!(debug.validate().is_err());
        let mut unordered = valid.clone();
        unordered.features = vec!["z".into(), "a".into()];
        assert!(unordered.validate().is_err());
        let mut duplicate = valid.clone();
        duplicate.features = vec!["a".into(), "a".into()];
        assert!(duplicate.validate().is_err());
        let json = serde_json::json!({"version":1,"build_id":"b","binary_sha256":digest("b"),"commit":"0123456789abcdef0123456789abcdef01234567","git_identity":digest("0123456789abcdef0123456789abcdef01234567"),"profile":"release","features":[],"tool_identity":"t","runtime_version":"v","extra":true});
        assert!(serde_json::from_value::<BuildAttestation>(json).is_err());
    }
    #[test]
    fn attested_binary_hash_and_runtime_evidence_must_match() {
        let directory = tempfile::tempdir().unwrap_or_else(|err| panic!("tempdir: {err}"));
        let binary = directory.path().join("binary");
        std::fs::write(&binary, b"binary").unwrap_or_else(|err| panic!("write: {err}"));
        let valid = attestation(hash_binary(&binary).unwrap_or_else(|err| panic!("hash: {err}")));
        let evidence = Evidence {
            version: "v1".into(),
            commit: valid.commit.clone(),
        };
        let identity = validate_attested_binary(&binary, &valid, &evidence)
            .unwrap_or_else(|err| panic!("validate: {err}"));
        assert_eq!(identity.build_profile, BuildProfile::Release);
        assert_eq!(identity.features, vec!["dev-tools"]);
        let mut mismatch = valid.clone();
        mismatch.binary_sha256 = digest("other");
        assert!(validate_attested_binary(&binary, &mismatch, &evidence).is_err());
        let mut mismatched_git_identity = valid.clone();
        mismatched_git_identity.git_identity = digest("other-git");
        assert!(mismatched_git_identity.validate().is_err());
        assert!(
            validate_attested_binary(
                &binary,
                &valid,
                &Evidence {
                    version: "other".into(),
                    commit: valid.commit.clone()
                }
            )
            .is_err()
        );
        assert!(
            validate_attested_binary(
                &binary,
                &valid,
                &Evidence {
                    version: "v1".into(),
                    commit: "abcdef0123456789abcdef0123456789abcdef0123".into()
                }
            )
            .is_err()
        );
        assert_eq!(
            RuntimeVersionEvidence::parse("GreptimeDB v1\nbranch: main\ncommit: 0123456789abcdef0123456789abcdef01234567\nclean: true\nversion: v1")
                .unwrap_or_else(|err| panic!("evidence: {err}"))
                .version,
            "v1"
        );
        assert!(
            RuntimeVersionEvidence::parse("branch: main\ncommit: 0123456789abcdef0123456789abcdef01234567\nclean: true\nversion: v1\ncommit: 0123456789abcdef0123456789abcdef01234567").is_err()
        );
    }
    #[test]
    fn target_identity_converts_to_the_existing_target_manifest_contract() {
        let identity = attestation(digest("binary"))
            .target_identity()
            .unwrap_or_else(|err| panic!("identity: {err}"));
        let binding = |role, target_id: &str| TargetBinding {
            role,
            target_id: target_id.into(),
            identity: identity.identity.clone(),
            git_identity: identity.git_identity.clone(),
            build_profile: identity.build_profile.clone(),
            features: identity.features.clone(),
            capabilities: vec![Capability::Sql],
            health_endpoint: EndpointUrl::parse("http://127.0.0.1:4000/health")
                .unwrap_or_else(|err| panic!("health: {err}")),
            sql_endpoint: EndpointUrl::parse("http://127.0.0.1:4000/sql")
                .unwrap_or_else(|err| panic!("sql: {err}")),
            remote_write_endpoint: None,
            namespace: "public".into(),
            topology: "single".into(),
            fixture_bindings: vec![TargetFixtureBinding {
                fixture_id: "fixture".into(),
                catalog: None,
                database: "public".into(),
                logical_table: "metric".into(),
                physical_table: Some("physical".into()),
                region: "0".into(),
            }],
        };
        let mut manifest = TargetEnvironmentManifest {
            version: crate::query_perf::manifest::MANIFEST_VERSION,
            run_id: "run".into(),
            controller_id: "controller".into(),
            targets: vec![
                binding(TargetRole::Base, "base"),
                binding(TargetRole::Candidate, "candidate"),
            ],
            completion_digest: digest("unsealed"),
        };
        manifest.seal().unwrap_or_else(|err| panic!("seal: {err}"));
        manifest
            .validate()
            .unwrap_or_else(|err| panic!("manifest: {err}"));
    }
    #[test]
    fn port_roles_and_quiesce_outcomes_are_typed_and_serializable() {
        let ports = PortSet {
            metasrv_grpc: 1,
            metasrv_http: 2,
            datanode_grpc: 3,
            datanode_http: 4,
            frontend_http: 5,
            frontend_grpc: 6,
            frontend_mysql: 7,
            frontend_postgres: 8,
        };
        assert!(ports.validate().is_ok());
        assert!(
            PortSet {
                frontend_http: 1,
                ..ports
            }
            .validate()
            .is_err()
        );
        assert!(
            PortSet {
                frontend_http: 0,
                ..ports
            }
            .validate()
            .is_err()
        );
        let graceful = QuiesceOutcome::Graceful {
            role: TargetRole::Base,
            component: ProcessComponent::DataNode,
            exit: ProcessExit::Exited { code: 0 },
        };
        let forced = QuiesceOutcome::Forced {
            role: TargetRole::Candidate,
            component: ProcessComponent::DataNode,
            exit: ProcessExit::Signaled { signal: 9 },
            reason: ForcedCleanupReason::Timeout,
        };
        assert!(graceful.trusted_for_observation());
        assert!(!forced.trusted_for_observation());
        assert_eq!(
            serde_json::from_str::<QuiesceOutcome>(
                &serde_json::to_string(&forced).unwrap_or_default()
            )
            .unwrap_or(graceful),
            forced
        );
        assert_eq!(
            LocalTargetSpec::new(TargetRole::Base, "base".into(), "base.json".into()).role,
            TargetRole::Base
        );
        assert!(
            SuiteManifest {
                version: SUITE_MANIFEST_VERSION,
                cases: vec![
                    SuiteCaseSpec {
                        case: "first.toml".into(),
                    },
                    SuiteCaseSpec {
                        case: "second.toml".into(),
                    }
                ],
            }
            .validate()
            .is_ok()
        );
        assert!(
            serde_json::from_str::<SuiteManifest>(
                r#"{"version":1,"cases":[{"case":"first.toml"}],"extra":true}"#
            )
            .is_err()
        );
        let command = CommandOutcome {
            role: TargetRole::Base,
            component: ProcessComponent::Frontend,
            exit: ProcessExit::Exited { code: 0 },
        };
        assert_eq!(
            serde_json::from_str::<CommandOutcome>(
                &serde_json::to_string(&command).unwrap_or_default()
            )
            .unwrap_or(command.clone()),
            command
        );
    }
}
