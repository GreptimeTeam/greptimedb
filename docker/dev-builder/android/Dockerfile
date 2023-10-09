FROM --platform=linux/amd64 saschpe/android-ndk:34-jdk17.0.8_7-ndk25.2.9519653-cmake3.22.1

ENV LANG en_US.utf8
WORKDIR /greptimedb

# Rename libunwind to libgcc
RUN cp ${NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/lib64/clang/14.0.7/lib/linux/aarch64/libunwind.a ${NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/lib64/clang/14.0.7/lib/linux/aarch64/libgcc.a

# Install dependencies.
RUN apt-get update && apt-get install -y \
    libssl-dev \
    protobuf-compiler \
    curl \
    git \
    build-essential \
    pkg-config \
    python3 \
    python3-dev \
    python3-pip \
    && pip3 install --upgrade pip \
    && pip3 install pyarrow

# Trust workdir
RUN git config --global --add safe.directory /greptimedb

# Install Rust.
SHELL ["/bin/bash", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Add android toolchains
ARG RUST_TOOLCHAIN
RUN rustup toolchain install ${RUST_TOOLCHAIN}
RUN rustup target add aarch64-linux-android

# Install cargo-ndk
RUN cargo install cargo-ndk
ENV ANDROID_NDK_HOME $NDK_ROOT

# Builder entrypoint.
CMD ["cargo", "ndk", "--platform", "23", "-t", "aarch64-linux-android", "build", "--bin", "greptime", "--profile", "release", "--no-default-features"]
