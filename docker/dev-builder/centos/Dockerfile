FROM centos:7 as builder

ENV LANG en_US.utf8

# Install dependencies
RUN ulimit -n 1024000 && yum groupinstall -y 'Development Tools'
RUN yum install -y epel-release  \
    openssl \
    openssl-devel  \
    centos-release-scl  \
    rh-python38  \
    rh-python38-python-devel \
    which

# Install protoc
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
RUN unzip protoc-3.15.8-linux-x86_64.zip -d /usr/local/

# Install Rust
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /opt/rh/rh-python38/root/usr/bin:/usr/local/bin:/root/.cargo/bin/:$PATH

# Install Rust toolchains.
ARG RUST_TOOLCHAIN
RUN rustup toolchain install ${RUST_TOOLCHAIN}

# Install nextest.
RUN cargo install cargo-binstall --locked
RUN cargo binstall cargo-nextest --no-confirm
