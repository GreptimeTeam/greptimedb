FROM mcr.microsoft.com/devcontainers/base:ubuntu-22.04

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=en_US.UTF-8
ENV LC_ALL=C.UTF-8

# Configure apt and install packages
RUN apt-get update \
    && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
    && apt-get -y install \
        # Essential build tools
        build-essential \
        pkg-config \
        cmake \
        # SSL and crypto libraries
        libssl-dev \
        # Timezone data
        tzdata \
        # Network and download tools
        curl \
        wget \
        unzip \
        ca-certificates \
        # Version control
        git \
        # Additional development tools
        vim \
        nano \
        htop \
        jq \
        # Python for scripts
        python3 \
        python3-pip \
        # PostgreSQL client for testing
        postgresql-client \
        # MySQL client for testing
        mysql-client \
        # Additional libraries that may be needed
        libclang-dev \
        clang \
        # For running tests and benchmarks
        time \
        # For debugging
        gdb \
        lldb \
        # For container health checks
        netcat \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Install Protocol Buffers compiler with retry logic
ARG PROTOBUF_VERSION=29.3
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        PROTOC_ARCH="linux-aarch_64"; \
    elif [ "$ARCH" = "x86_64" ]; then \
        PROTOC_ARCH="linux-x86_64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    echo "Downloading protoc for $ARCH architecture..." && \
    for i in 1 2 3; do \
        if curl -L --retry 3 --retry-delay 5 -o "protoc.zip" \
            "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-${PROTOC_ARCH}.zip"; then \
            echo "Download successful on attempt $i"; \
            break; \
        else \
            echo "Download attempt $i failed, retrying..."; \
            sleep 5; \
        fi; \
    done && \
    unzip "protoc.zip" -d protoc3 && \
    mv protoc3/bin/* /usr/local/bin/ && \
    mv protoc3/include/* /usr/local/include/ && \
    rm -rf protoc3 "protoc.zip"

# Install additional tools that might be needed for development
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g @apidevtools/swagger-cli

# Install etcd for testing distributed features
RUN ETCD_VERSION="v3.5.9" && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        ETCD_ARCH="arm64"; \
    elif [ "$ARCH" = "x86_64" ]; then \
        ETCD_ARCH="amd64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl -L "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-${ETCD_ARCH}.tar.gz" -o etcd.tar.gz && \
    tar xzf etcd.tar.gz && \
    mv etcd-*/etcd /usr/local/bin/ && \
    mv etcd-*/etcdctl /usr/local/bin/ && \
    rm -rf etcd*

# Install MinIO client for object storage testing
RUN curl -L "https://dl.min.io/client/mc/release/linux-$(uname -m)/mc" -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Note: Rust toolchain is installed via devcontainer features
# See devcontainer.json for the Rust version configuration

# Create a non-root user (vscode is the default for devcontainers)
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create the user (handle case where user already exists)
RUN (groupadd --gid $USER_GID $USERNAME || true) \
    && (useradd --uid $USER_UID --gid $USER_GID -m $USERNAME || true) \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=dialog

# Set up git configuration to avoid safe directory warnings
RUN git config --global --add safe.directory '*'

# Set the default user
USER $USERNAME

# Set working directory
WORKDIR /workspace

# Set environment variables for Rust
ENV RUST_BACKTRACE=1
ENV CARGO_TERM_COLOR=always
ENV PROTOC=/usr/local/bin/protoc

# Verify installations
RUN protoc --version && \
    rustc --version && \
    cargo --version