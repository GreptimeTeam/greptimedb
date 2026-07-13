---
name: greptimedb-development-docker-image
description: Builds a development-only GreptimeDB Docker image from a local debug binary for local-cluster testing, and optionally pushes it to a development registry. Use when the user asks to package, build, tag, publish, or cross-build a non-release GreptimeDB or GreptimeDB Enterprise image for debugging.
compatibility: Requires Docker with Buildx for cross-platform builds; Podman is supported for native-platform builds.
metadata:
  protocols: docker buildx podman
  platforms: linux-amd64 linux-arm64
---

# GreptimeDB Development Docker Image

## Goal

Package a locally built GreptimeDB binary into a **development-only** Docker
image for debugging and local-cluster testing, optionally push it to a
development registry, and retain the non-secret image settings in `.env` for
the next build. It is **not** a release-image workflow and must not be used to
publish a production or release artifact.

This skill follows the documented development-image build procedure:

- Build `greptime` with Cargo's `nightly` profile.
- Copy the target binary into the Docker build context as `./greptime`.
- Build the supplied `Dockerfile`, which uses Ubuntu 24.04 only as the runtime
  base image and exposes the `greptime` binary as its entrypoint.

## Inputs

Collect or discover the following before any build or push:

| Input | Discovery and rule |
| --- | --- |
| Source repository | Treat the directory in which this skill is invoked as the default workspace. Include it as an editable field in the batch configuration; do not ask a separate confirmation. Do not assume open-source versus Enterprise. |
| Edition and binary | Default to `greptime`; use the binary name the user requests for Enterprise builds. The copied Docker-context filename must always be `greptime`. |
| Cargo profile and binary source | Include this in the batch configuration. Default to rebuilding `nightly`; reuse an existing binary only after the user explicitly accepts that its freshness is unverified. |
| Target platform | Preselect `linux/amd64` unless Docker's server is `linux/arm64`, then preselect `linux/arm64`. Let the user override it in the batch configuration. Ubuntu 24.04 is the runtime base image, not a target-platform choice. Each image has exactly one target platform, but it may differ from the host platform. |
| Build mode | Ask whether the user wants a locally loadable debug image or a registry push. |
| Registry/repository | Read `IMAGE_REGISTRY` and `IMAGE_REPOSITORY` only from the selected workspace's `.env` and prefill them in the batch configuration. Example: `registry.example.com/team` + `greptimedb-dev`. |
| Tag | Read `IMAGE_TAG` only from the selected workspace's `.env` and prefill it in the batch configuration. When the tag exists in the selected registry, preselect an incremented version. |

Use the image reference `${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}:${IMAGE_TAG}`.
If the registry is intentionally empty, omit its slash rather than producing a
leading slash.

## Interactive Workflow

Use the platform's interactive prompt component for every question, selection,
and confirmation. Do not ask an open-ended text question when a single-choice,
multi-select, or confirmation component can represent the decision. If the
platform does not provide an interactive component, use the equivalent numbered
or lettered prompt below and wait for input before continuing.

### Batch configuration

Minimize user round trips: run the collector first against the invocation
directory, then use one interactive form or batched prompt to collect workspace,
edition/binary, profile/reuse-or-rebuild choice, target platform, build mode,
registry/repository, tag, and whether to inspect the configured registry tag.
Use the invocation directory as the editable workspace default. Prefill
collector values and mark recommended defaults. If the user changes workspace,
rerun the collector for that workspace without asking another configuration
question. Only ask a follow-up if information is missing or invalid, or a safety
gate is required. Treat unchanged prefilled values as accepted.

Keep separate confirmation components only for elevated privileges: `sudo`,
package installation, and privileged QEMU setup. Use one final confirmation for
all non-privileged selected work.

Always offer **Cancel** for an action that can write state, build, push, or
require elevated privileges. Display the relevant preview before the user
confirms it: source/binary path, `file` architecture result, platform(s), image
reference, build mode, and non-secret `.env` values.

### Batch configuration fields

Use single-choice controls inside the one batch form for mutually exclusive
choices:

| Decision | Required choices |
| --- | --- |
| Cargo output | Rebuild `nightly` (recommended), reuse an existing binary only with a freshness-unverified acknowledgement, choose a profile/binary |
| Target platform | `linux/amd64` (default unless Docker server is arm64), `linux/arm64` (default when Docker server is arm64). The selected image may be cross-built with Buildx. |
| Build mode | Load locally (recommended), push to development registry |
| Registry/repository | Use `.env` defaults, enter new values |
| Tag | Use proposed increment when the configured tag exists, enter a version |

When push is selected and the image configuration is complete, automatically
run the read-only registry-tag check. It may return `unknown`; do not treat that
as a missing tag.

Use one final confirmation for all selected non-privileged operations:

- persist changed non-secret image settings to `.env`;
- run the selected Cargo build or reuse the resolved binary;
- prepare the Docker context;
- build, verify, and, when selected, push the image.

The final confirmation must clearly state: **“This creates a development and
local-cluster test image, not a release or production artifact.”**

For a real multi-select only, use lettered choices (`[A]`, `[B]`) and accept
`all`, `none`, or `cancel`; otherwise use single-choice components.

### Preflight context collection

Before asking the profile, registry, or tag questions, run the bundled,
read-only collector. It works on macOS and Linux, does not invoke `cargo build`,
and emits JSON that can be shown or summarized to the user:

```bash
python3 <skill-dir>/scripts/collect_context.py \
  --source <source-checkout> \
  --bin greptime \
  --profile nightly \
  [--target <rust-target>]
```

Use its report to identify:

- missing `IMAGE_REGISTRY`, `IMAGE_REPOSITORY`, or `IMAGE_TAG` values in the
  selected workspace's `.env`;
- Cargo binary targets available in the workspace and whether the requested
  `--bin` exists;
- the resolved Cargo target directory, expected profile output path, and whether
  that binary already exists; and
- macOS/Linux host architecture plus Docker's server platform when Docker is
  available.

When push is selected and image configuration is complete, rerun the collector
with `--check-registry-tag` and all selected image values. It uses the selected
engine's read-only manifest inspection and returns `exists` or `unknown`;
`unknown` includes an unavailable registry, an absent tag, or missing
authentication and must not be treated as an absent tag.

```bash
python3 <skill-dir>/scripts/collect_context.py \
  --source <source-checkout> \
  --bin <binary> \
  --profile <profile> \
  --engine <docker|podman> \
  --registry <registry> \
  --repository <repository> \
  --tag <tag> \
  --check-registry-tag
```

When the status is `exists` and the tag ends in a number, preselect the
collector's `candidate_tag` as the next development tag. The user must still
confirm it before `.env` is updated or an image is built. Never use automatic
incrementing as permission to overwrite or push an existing image.

If Cargo metadata or the requested binary target is unavailable, stop before
building and ask the user to correct the source path or binary target.

The profile/binary choice is a field in the batch configuration, not a separate
question. Its default is the existing `nightly` output when present:

```text
Question: Which Cargo build output should package this image?
Options:
- Rebuild nightly (Recommended): run Cargo with `--profile nightly`, then use its output.
- Reuse nightly output: use the existing output only after showing its path, modification time, size, and matching platform; freshness is unverified.
- Choose profile or binary: provide a different Cargo profile or an explicit binary path.
```

Do not infer that a binary under `target/debug`, `target/release`, or another
profile is suitable. For an existing binary, show its resolved path and run
`file` before asking for final build confirmation.

## Environment State

The image configuration lives only in the selected workspace's `.env`. The
collector reads only the following keys and never reads or displays other `.env`
values:

```dotenv
IMAGE_REGISTRY=registry.example.com/team
IMAGE_REPOSITORY=greptimedb-dev
IMAGE_TAG=dev-001
```

1. Read `.env` if it exists. Ignore blank lines and `#` comments.
2. Show the discovered registry, repository, and proposed tag to the user.
3. Include registry/repository and tag in the batch configuration even when
   values already exist; saved values are defaults, not authorization to reuse
   them.
4. Compare the selected configuration with the three current managed values. If
   all match, skip `.env` confirmation and do not update the file. Otherwise,
   preview only the changed `IMAGE_*` values, request confirmation, and update
   the file while preserving unrelated entries and comments.
5. Never put registry credentials, access tokens, passwords, or `docker login`
   output in `.env`, build commands, logs, or responses. Ask the user to log in
   themselves if a push needs authentication.

For a repeated build, offer an incremented tag before asking. Use
`scripts/next_image_tag.py --tag <existing-tag>` to calculate it. It increments
the final numeric component while preserving zero padding:

```text
weny-2025-0715-01 -> weny-2025-0715-02
v0.1.4 -> v0.1.5
debug-009 -> debug-010
```

If the saved tag has no trailing number, ask the user for the next tag rather
than inventing a versioning convention. Do not overwrite an existing image tag
without explicit user confirmation.

Only if a managed value is missing or differs, persist the values with the
bundled helper after confirmation instead of hand-editing the file:

```bash
python3 <skill-dir>/scripts/update_image_env.py \
  --env <source-checkout>/.env \
  --registry <registry> \
  --repository <repository> \
  --tag <tag>
```

## Build Procedure

### 1. Inspect the host and container tooling

Run these checks and report the selected path:

```bash
uname -s
uname -m
docker version --format '{{.Server.Os}}/{{.Server.Arch}}'
docker buildx version
docker buildx inspect --bootstrap
```

For a native-platform build, Podman can be used if Docker is unavailable. For
any non-native request, require Docker Buildx. Do not silently fall back to a
native build when the requested image platform differs.

If the requested target is unavailable, stop and ask the user to configure a
Buildx builder and cross-compilation toolchain externally. Do not automate
privileged QEMU or binfmt setup, and never run an unpinned privileged image.

### 2. Build the executable for the image platform

From the source checkout, build the requested binary. For open-source amd64:

```bash
<skill-dir>/scripts/build_binary.sh \
  --source <source-checkout> --package cmd --bin greptime --profile nightly
```

For open-source arm64:

```bash
<skill-dir>/scripts/build_binary.sh \
  --source <source-checkout> \
  --package cmd \
  --bin greptime \
  --profile nightly \
  --target aarch64-unknown-linux-gnu
```

For Enterprise, use the user-provided executable target, for example:

```bash
<skill-dir>/scripts/build_binary.sh \
  --source <source-checkout> --bin greptime-ent-cloud --profile nightly
```

For a user-selected profile, replace `nightly` in the helper call and resolve
the binary under `target/<profile>/<binary>` (or
`target/<rust-target>/<profile>/<binary>` for cross-compilation). Reuse the
existing target file only when the interactive profile question selected reuse;
otherwise invoke the helper to rebuild it.

Do not use a binary compiled for the host architecture in an image intended for
another architecture. Determine the binary path from the Cargo target and
profile, then copy it into the Docker context as `greptime`:

```bash
cp <source-target-binary> <build-context>/greptime
```

Verify it before building the image with `file <build-context>/greptime`; its
reported architecture must match the requested target platform.

Create a fresh isolated build context (never use the repository root) and use
the bundled preparation script. The helper rejects an existing Dockerfile or
binary output to prevent accidental context reuse:

```bash
<skill-dir>/scripts/prepare_context.sh \
  --context "$(mktemp -d)" \
  --binary <source-target-binary> \
  --dockerfile <skill-dir>/assets/Dockerfile \
  --platform <platform>
```

### 3. Build or push the image

Run the command only after the user confirms the final image reference and
whether it should be pushed.

Use the bundled build helper rather than spelling out individual Docker or
Podman commands. It follows the existing scripts' Docker-first/Podman-fallback
behavior, uses Buildx for a non-native Docker request, and accepts exactly one
target platform per image:

```bash
<skill-dir>/scripts/build_image.sh \
  --context <build-context> \
  --image <image-reference> \
  --platform <platform> \
  --mode local|push
```

`--mode local` builds a native image locally or uses `docker buildx --load` for
a non-native image. `--mode push` pushes the selected single-platform image.
The helper never pushes in `local` mode.

This is intentionally a runtime image built from a precompiled binary, not a
multi-stage Dockerfile. Ubuntu 24.04 is the runtime base image only; select one
target platform separately as `linux/amd64` or `linux/arm64`. Only introduce a
multi-stage Dockerfile if the user asks to compile within Docker or the local
Rust toolchain cannot build the required target. Keep the final Ubuntu 24.04
runtime stage and existing entrypoint unless the user asks to change runtime
behavior.

At the final confirmation, repeat that the selected reference is a
**development/local-cluster test image**, not a release artifact. If the user
requests a release or production image, stop and direct them to the release
process instead.

### 4. Verify the result

For a local image, use the selected engine to inspect its architecture and
start it with `--help`:

```bash
docker image inspect <image-reference> --format '{{.Os}}/{{.Architecture}}'
docker run --rm <image-reference> --help
```

For Podman, use `podman image inspect <image-reference>` and
`podman run --rm <image-reference> --help`. For a pushed Podman image, use
`podman manifest inspect <image-reference>`.

For a pushed image, inspect its remote manifest:

```bash
docker buildx imagetools inspect <image-reference>
```

Report the final image reference, target platform, source binary path, and
whether the image was loaded locally or pushed.

## Safety Rules

- This skill is only for development and local-cluster testing. Never present
  its image as a production, release, or officially published artifact.
- Never push by default. Local debug builds should use a local tag and load the
  image unless the user explicitly requests a push.
- Do not run `docker login`, privileged QEMU setup, package installation, or
  overwrite a tag without confirmation.
- Do not delete existing images, builders, binaries, or `.env` entries as part
  of this workflow.
- Stop and explain if Docker's server is unavailable, Buildx cannot support the
  requested platform, or the copied binary architecture does not match the
  image platform.
