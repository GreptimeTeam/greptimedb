FROM ubuntu:20.04

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Add PPA for Python 3.10.
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa -y

# Install dependencies.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    libssl-dev \
    tzdata \
    protobuf-compiler \
    curl \
    ca-certificates \
    git \
    build-essential \
    pkg-config \
    python3.10 \
    python3.10-dev \
    python3-pip

RUN git config --global --add safe.directory /greptimedb

# Install Python dependencies.
COPY ./docker/python/requirements.txt /etc/greptime/requirements.txt
RUN python3 -m pip install -r /etc/greptime/requirements.txt

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Install Rust toolchains.
ARG RUST_TOOLCHAIN
RUN rustup toolchain install ${RUST_TOOLCHAIN}

# Install nextest.
RUN cargo install cargo-nextest --locked
