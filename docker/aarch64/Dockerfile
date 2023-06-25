FROM ubuntu:22.04 as builder

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Install dependencies.
RUN apt-get update && apt-get install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    git \
    build-essential \
    pkg-config \
    wget

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Install cross platform toolchain
RUN apt-get -y update && \
    apt-get -y install g++-aarch64-linux-gnu gcc-aarch64-linux-gnu && \
    apt-get install binutils-aarch64-linux-gnu

COPY ./docker/aarch64/compile-python.sh ./docker/aarch64/
RUN chmod +x ./docker/aarch64/compile-python.sh && \
    ./docker/aarch64/compile-python.sh

COPY ./rust-toolchain.toml .
# Install rustup target for cross compiling.
RUN rustup target add aarch64-unknown-linux-gnu
COPY . .
# Update dependency, using separate `RUN` to separate cache
RUN cargo fetch

# This three env var is set in script, so I set it manually in dockerfile.
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/
ENV LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib/
ENV PY_INSTALL_PATH=/greptimedb/python_arm64_build

# Set the environment variable for cross compiling and compile it
# cross compiled python is `python3` in path, but pyo3 need `python` in path so alias it
# Build the project in release mode.
RUN export PYO3_CROSS_LIB_DIR=$PY_INSTALL_PATH/lib && \ 
    alias python=python3 && \
    cargo build --target aarch64-unknown-linux-gnu --release -F pyo3_backend

# Exporting the binary to the clean image
FROM ubuntu:22.04 as base

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get -y install ca-certificates

WORKDIR /greptime
COPY --from=builder /greptimedb/target/aarch64-unknown-linux-gnu/release/greptime /greptime/bin/
ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
