FROM ubuntu:22.04 as builder

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Install dependencies.
RUN apt-get update && apt-get install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    build-essential \
    pkg-config \
    git

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Build the project in release mode.
COPY . .
# git used in build.rs seems need to be initialized first
# TODO(discord9): find a better way to init git
RUN git init && git config user.name "AnyName" \
    && git config user.email "any@email.com" && \
    git commit --allow-empty -n -m "Initial commit." && \
    cargo build --release

# Export the binary to the clean image.
# TODO(zyy17): Maybe should use the more secure container image.
FROM ubuntu:22.04 as base

WORKDIR /greptime
COPY --from=builder /greptimedb/target/release/greptime /greptime/bin/
ENV PATH /greptime/bin/:$PATH

ENTRYPOINT ["greptime"]
