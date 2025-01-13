FROM centos:7 as builder

ENV LANG en_US.utf8

# Note: CentOS 7 has reached EOL since 2024-07-01 thus `mirror.centos.org` is no longer available and we need to use `vault.centos.org` instead.
RUN sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo
RUN sed -i s/^#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo

# Install dependencies
RUN ulimit -n 1024000 && yum groupinstall -y 'Development Tools'
RUN yum install -y epel-release  \
    openssl \
    openssl-devel  \
    centos-release-scl  \
    which

# Install protoc
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
RUN unzip protoc-3.15.8-linux-x86_64.zip -d /usr/local/

# Install Rust
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /usr/local/bin:/root/.cargo/bin/:$PATH

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
