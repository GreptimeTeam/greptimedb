# Cross-build environment for producing riscv64 (riscv64gc-unknown-linux-gnu)
# greptime binaries on x86_64/aarch64 hosts.
ARG BASE_IMAGE=ubuntu:22.04
FROM ${BASE_IMAGE}

ENV LANG=en_US.utf8
WORKDIR /greptimedb

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common
# Install dependencies, including the riscv64 cross toolchain.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    libssl-dev \
    libatomic1 \
    tzdata \
    curl \
    unzip \
    ca-certificates \
    git \
    build-essential \
    pkg-config \
    gcc-riscv64-linux-gnu \
    g++-riscv64-linux-gnu

ARG TARGETPLATFORM
RUN echo "host platform: $TARGETPLATFORM"

ARG PROTOBUF_VERSION=29.3

# Install protobuf for the host arch (build scripts and protoc run on the host),
# because the one in the apt is too old (v3.12).
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
    curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-aarch_64.zip && \
    unzip -o protoc-${PROTOBUF_VERSION}-linux-aarch_64.zip -d protoc3; \
else \
    curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    unzip -o protoc-${PROTOBUF_VERSION}-linux-x86_64.zip -d protoc3; \
fi
RUN mv -f protoc3/bin/* /usr/local/bin/
# Use cp -r instead of mv so that it also works when the base image already
# contains protobuf headers under /usr/local/include.
RUN cp -r protoc3/include/* /usr/local/include/

# Install mold and make it the default linker (the equivalent of
# rui314/setup-mold with its default options on GitHub-hosted runners) for
# lower peak memory usage during linking. The host `ld` symlink covers
# host-side linking (build scripts, proc macros); the riscv64 symlink covers
# the cross target -- the cross gcc driver passes -m elf64lriscv explicitly,
# so mold picks the right emulation.
ARG MOLD_VERSION=2.42.1
RUN arch="$(uname -m)" && \
    curl -fsSL -o /tmp/mold.tar.gz https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-${arch}-linux.tar.gz && \
    tar -C /usr/local --strip-components=1 -xzf /tmp/mold.tar.gz && \
    rm /tmp/mold.tar.gz && \
    ln -sf /usr/local/bin/mold "$(readlink -f /usr/bin/ld)" && \
    ln -sf /usr/local/bin/mold "$(readlink -f "$(command -v riscv64-linux-gnu-ld)")"

# Silence all `safe.directory` warnings, to avoid the "detect dubious repository"
# error when building with the repository mounted from a different user.
RUN git config --global --add safe.directory '*'

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH=/root/.cargo/bin/:$PATH

# Install Rust toolchains and the riscv64 target std.
ARG RUST_TOOLCHAIN
RUN rustup toolchain install ${RUST_TOOLCHAIN}
RUN rustup target add --toolchain ${RUST_TOOLCHAIN} riscv64gc-unknown-linux-gnu
