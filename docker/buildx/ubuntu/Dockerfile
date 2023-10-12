FROM ubuntu:20.04 as builder

ARG CARGO_PROFILE
ARG FEATURES
ARG OUTPUT_DIR

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Add PPA for Python 3.10.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa -y

# Install dependencies.
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    git \
    build-essential \
    pkg-config \
    python3.10 \
    python3.10-dev \
    python3-pip

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Build the project in release mode.
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cargo/registry \
    make build \
    CARGO_PROFILE=${CARGO_PROFILE} \
    FEATURES=${FEATURES} \
    TARGET_DIR=/out/target

# Export the binary to the clean image.
# TODO(zyy17): Maybe should use the more secure container image.
FROM ubuntu:22.04 as base

ARG OUTPUT_DIR

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get \
    -y install ca-certificates \
    python3.10 \
    python3.10-dev \
    python3-pip \
    curl

COPY ./docker/python/requirements.txt /etc/greptime/requirements.txt

RUN python3 -m pip install -r /etc/greptime/requirements.txt

WORKDIR /greptime
COPY --from=builder /out/target/${OUTPUT_DIR}/greptime /greptime/bin/
ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
