FROM ubuntu:22.04 AS builder

ARG CARGO_PROFILE
ARG FEATURES
ARG OUTPUT_DIR

ENV LANG=en_US.utf8
WORKDIR /greptimedb

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common

# Install dependencies.
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    git \
    build-essential \
    pkg-config

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH=/root/.cargo/bin/:$PATH

# Build the project in release mode.
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cargo/registry \
    make build \
    CARGO_PROFILE=${CARGO_PROFILE} \
    FEATURES=${FEATURES} \
    TARGET_DIR=/out/target

# Export the binary to the clean image.
# TODO(zyy17): Maybe should use the more secure container image.
FROM ubuntu:22.04 AS base

ARG OUTPUT_DIR

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get \
    -y install ca-certificates \
    curl

WORKDIR /greptime
COPY --from=builder /out/target/${OUTPUT_DIR}/greptime /greptime/bin/
ENV PATH=/greptime/bin/:$PATH

ENV MALLOC_CONF="prof:true,prof_active:false"

ENTRYPOINT ["greptime"]
