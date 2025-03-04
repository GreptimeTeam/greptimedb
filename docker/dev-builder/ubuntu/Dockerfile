FROM ubuntu:20.04

# The root path under which contains all the dependencies to build this Dockerfile.
ARG DOCKER_BUILD_ROOT=.

ENV LANG en_US.utf8
WORKDIR /greptimedb

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common
# Install dependencies.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    libssl-dev \
    tzdata \
    curl \
    unzip \
    ca-certificates \
    git \
    build-essential \
    pkg-config

ARG TARGETPLATFORM
RUN echo "target platform: $TARGETPLATFORM"

ARG PROTOBUF_VERSION=29.3

# Install protobuf, because the one in the apt is too old (v3.12).
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
    curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-aarch_64.zip && \
    unzip protoc-${PROTOBUF_VERSION}-linux-aarch_64.zip -d protoc3; \
elif [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
    curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    unzip protoc-${PROTOBUF_VERSION}-linux-x86_64.zip -d protoc3; \
fi
RUN mv protoc3/bin/* /usr/local/bin/
RUN mv protoc3/include/* /usr/local/include/

# Silence all `safe.directory` warnings, to avoid the "detect dubious repository" error when building with submodules.
# Disabling the safe directory check here won't pose extra security issues, because in our usage for this dev build
# image, we use it solely on our own environment (that github action's VM, or ECS created dynamically by ourselves),
# and the repositories are pulled from trusted sources (still us, of course). Doing so does not violate the intention
# of the Git's addition to the "safe.directory" at the first place (see the commit message here:
# https://github.com/git/git/commit/8959555cee7ec045958f9b6dd62e541affb7e7d9).
# There's also another solution to this, that we add the desired submodules to the safe directory, instead of using
# wildcard here. However, that requires the git's config files and the submodules all owned by the very same user.
# It's troublesome to do this since the dev build runs in Docker, which is under user "root"; while outside the Docker,
# it can be a different user that have prepared the submodules.
RUN git config --global --add safe.directory '*'

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Install Rust toolchains.
ARG RUST_TOOLCHAIN
RUN rustup toolchain install ${RUST_TOOLCHAIN}

# Install cargo-binstall with a specific version to adapt the current rust toolchain.
# Note: if we use the latest version, we may encounter the following `use of unstable library feature 'io_error_downcast'` error.
# compile from source take too long, so we use the precompiled binary instead
COPY $DOCKER_BUILD_ROOT/docker/dev-builder/binstall/pull_binstall.sh /usr/local/bin/pull_binstall.sh
RUN chmod +x /usr/local/bin/pull_binstall.sh && /usr/local/bin/pull_binstall.sh

# Install nextest.
RUN cargo binstall cargo-nextest --no-confirm
