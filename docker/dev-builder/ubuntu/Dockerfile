FROM ubuntu:20.04

# The root path under which contains all the dependencies to build this Dockerfile.
ARG DOCKER_BUILD_ROOT=.

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
    python3.10-dev

# https://github.com/GreptimeTeam/greptimedb/actions/runs/10935485852/job/30357457188#step:3:7106
# `aws-lc-sys` require gcc >= 10.3.0 to work, hence alias to use gcc-10
RUN alias gcc="gcc-10" && alias cc="gcc-10"

# Remove Python 3.8 and install pip.
RUN apt-get -y purge python3.8 && \
    apt-get -y autoremove && \
    ln -s /usr/bin/python3.10 /usr/bin/python3 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

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
RUN git config --global --add safe.directory *

# Install Python dependencies.
COPY $DOCKER_BUILD_ROOT/docker/python/requirements.txt /etc/greptime/requirements.txt
RUN python3 -m pip install -r /etc/greptime/requirements.txt

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Install Rust toolchains.
ARG RUST_TOOLCHAIN
RUN rustup toolchain install ${RUST_TOOLCHAIN}

# Install cargo-binstall with a specific version to adapt the current rust toolchain.
# Note: if we use the latest version, we may encounter the following `use of unstable library feature 'io_error_downcast'` error.
RUN cargo install cargo-binstall --version 1.6.6 --locked

# Install nextest.
RUN cargo binstall cargo-nextest --no-confirm
