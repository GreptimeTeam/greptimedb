# The arguments for building images.
CARGO_PROFILE ?=
FEATURES ?=
TARGET_DIR ?=
TARGET ?=
BUILD_BIN ?= greptime
CARGO_BUILD_OPTS := --locked
IMAGE_REGISTRY ?= docker.io
IMAGE_NAMESPACE ?= greptime
IMAGE_TAG ?= latest
DEV_BUILDER_IMAGE_TAG ?= 2024-06-06-1acda74c-20240919113454
BUILDX_MULTI_PLATFORM_BUILD ?= false
BUILDX_BUILDER_NAME ?= gtbuilder
BASE_IMAGE ?= ubuntu
RUST_TOOLCHAIN ?= $(shell cat rust-toolchain.toml | grep channel | cut -d'"' -f2)
CARGO_REGISTRY_CACHE ?= ${HOME}/.cargo/registry
ARCH := $(shell uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')
OUTPUT_DIR := $(shell if [ "$(RELEASE)" = "true" ]; then echo "release"; elif [ ! -z "$(CARGO_PROFILE)" ]; then echo "$(CARGO_PROFILE)" ; else echo "debug"; fi)
SQLNESS_OPTS ?=

# The arguments for running integration tests.
ETCD_VERSION ?= v3.5.9
ETCD_IMAGE ?= quay.io/coreos/etcd:${ETCD_VERSION}
RETRY_COUNT ?= 3
NEXTEST_OPTS := --retries ${RETRY_COUNT}
BUILD_JOBS ?= $(shell which nproc 1>/dev/null && expr $$(nproc) / 2) # If nproc is not available, we don't set the build jobs.
ifeq ($(BUILD_JOBS), 0) # If the number of cores is less than 2, set the build jobs to 1.
  BUILD_JOBS := 1
endif

ifneq ($(strip $(BUILD_JOBS)),)
	NEXTEST_OPTS += --build-jobs=${BUILD_JOBS}
endif

ifneq ($(strip $(CARGO_PROFILE)),)
	CARGO_BUILD_OPTS += --profile ${CARGO_PROFILE}
endif

ifneq ($(strip $(FEATURES)),)
	CARGO_BUILD_OPTS += --features ${FEATURES}
endif

ifneq ($(strip $(TARGET_DIR)),)
	CARGO_BUILD_OPTS += --target-dir ${TARGET_DIR}
endif

ifneq ($(strip $(TARGET)),)
	CARGO_BUILD_OPTS += --target ${TARGET}
endif

ifneq ($(strip $(BUILD_BIN)),)
	CARGO_BUILD_OPTS += --bin ${BUILD_BIN}
endif

ifneq ($(strip $(RELEASE)),)
	CARGO_BUILD_OPTS += --release
endif

ifeq ($(BUILDX_MULTI_PLATFORM_BUILD), all)
	BUILDX_MULTI_PLATFORM_BUILD_OPTS := --platform linux/amd64,linux/arm64 --push
else ifeq ($(BUILDX_MULTI_PLATFORM_BUILD), amd64)
	BUILDX_MULTI_PLATFORM_BUILD_OPTS := --platform linux/amd64 --push
else
	BUILDX_MULTI_PLATFORM_BUILD_OPTS := -o type=docker
endif

ifneq ($(strip $(CARGO_BUILD_EXTRA_OPTS)),)
	CARGO_BUILD_OPTS += ${CARGO_BUILD_EXTRA_OPTS}
endif

##@ Build

.PHONY: build
build: ## Build debug version greptime.
	cargo ${CARGO_EXTENSION} build ${CARGO_BUILD_OPTS}

.PHONY: build-by-dev-builder
build-by-dev-builder: ## Build greptime by dev-builder.
	docker run --network=host \
	-v ${PWD}:/greptimedb -v ${CARGO_REGISTRY_CACHE}:/root/.cargo/registry \
	-w /greptimedb ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder-${BASE_IMAGE}:${DEV_BUILDER_IMAGE_TAG} \
	make build \
	CARGO_EXTENSION="${CARGO_EXTENSION}" \
	CARGO_PROFILE=${CARGO_PROFILE} \
	FEATURES=${FEATURES} \
	TARGET_DIR=${TARGET_DIR} \
	TARGET=${TARGET} \
	RELEASE=${RELEASE} \
	CARGO_BUILD_EXTRA_OPTS="${CARGO_BUILD_EXTRA_OPTS}"

.PHONY: build-android-bin
build-android-bin: ## Build greptime binary for android.
	docker run --network=host \
	-v ${PWD}:/greptimedb -v ${CARGO_REGISTRY_CACHE}:/root/.cargo/registry \
	-w /greptimedb ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder-android:${DEV_BUILDER_IMAGE_TAG} \
	make build \
	CARGO_EXTENSION="ndk --platform 23 -t aarch64-linux-android" \
	CARGO_PROFILE=release \
	FEATURES="${FEATURES}" \
	TARGET_DIR="${TARGET_DIR}" \
	TARGET="${TARGET}" \
	RELEASE="${RELEASE}" \
	CARGO_BUILD_EXTRA_OPTS="--bin greptime --no-default-features"

.PHONY: strip-android-bin
strip-android-bin: build-android-bin ## Strip greptime binary for android.
	docker run --network=host \
	-v ${PWD}:/greptimedb \
	-w /greptimedb ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder-android:${DEV_BUILDER_IMAGE_TAG} \
	bash -c '$${NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-strip --strip-debug /greptimedb/target/aarch64-linux-android/release/greptime'

.PHONY: clean
clean: ## Clean the project.
	cargo clean

.PHONY: fmt
fmt: ## Format all the Rust code.
	cargo fmt --all

.PHONY: fmt-toml
fmt-toml: ## Format all TOML files.
	taplo format

.PHONY: check-toml
check-toml: ## Check all TOML files.
	taplo format --check

.PHONY: docker-image
docker-image: build-by-dev-builder ## Build docker image.
	mkdir -p ${ARCH} && \
	cp ./target/${OUTPUT_DIR}/greptime ${ARCH}/greptime && \
	docker build -f docker/ci/${BASE_IMAGE}/Dockerfile -t ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/greptimedb:${IMAGE_TAG} . && \
	rm -r ${ARCH}

.PHONY: docker-image-buildx
docker-image-buildx: multi-platform-buildx ## Build docker image by buildx.
	docker buildx build --builder ${BUILDX_BUILDER_NAME} \
	  --build-arg="CARGO_PROFILE=${CARGO_PROFILE}" \
	  --build-arg="FEATURES=${FEATURES}" \
	  --build-arg="OUTPUT_DIR=${OUTPUT_DIR}" \
	  -f docker/buildx/${BASE_IMAGE}/Dockerfile \
	  -t ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/greptimedb:${IMAGE_TAG} ${BUILDX_MULTI_PLATFORM_BUILD_OPTS} .

.PHONY: dev-builder
dev-builder: multi-platform-buildx ## Build dev-builder image.
	docker buildx build --builder ${BUILDX_BUILDER_NAME} \
	--build-arg="RUST_TOOLCHAIN=${RUST_TOOLCHAIN}" \
	-f docker/dev-builder/${BASE_IMAGE}/Dockerfile \
	-t ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder-${BASE_IMAGE}:${DEV_BUILDER_IMAGE_TAG} ${BUILDX_MULTI_PLATFORM_BUILD_OPTS} .

.PHONY: multi-platform-buildx
multi-platform-buildx: ## Create buildx multi-platform builder.
	docker buildx inspect ${BUILDX_BUILDER_NAME} || docker buildx create --name ${BUILDX_BUILDER_NAME} --driver docker-container --bootstrap --use

##@ Test
.PHONY: test
test: nextest ## Run unit and integration tests.
	cargo nextest run ${NEXTEST_OPTS}

.PHONY: nextest
nextest: ## Install nextest tools.
	cargo --list | grep nextest || cargo install cargo-nextest --locked

.PHONY: sqlness-test
sqlness-test: ## Run sqlness test.
	cargo sqlness ${SQLNESS_OPTS}

# Run fuzz test ${FUZZ_TARGET}.
RUNS ?= 1
FUZZ_TARGET ?= fuzz_alter_table
.PHONY: fuzz
fuzz:
	cargo fuzz run ${FUZZ_TARGET} --fuzz-dir tests-fuzz -D -s none -- -runs=${RUNS}

.PHONY: fuzz-ls
fuzz-ls:
	cargo fuzz list --fuzz-dir tests-fuzz

.PHONY: check
check: ## Cargo check all the targets.
	cargo check --workspace --all-targets --all-features

.PHONY: clippy
clippy: ## Check clippy rules.
	cargo clippy --workspace --all-targets --all-features -- -D warnings

.PHONY: fix-clippy
fix-clippy: ## Fix clippy violations.
	cargo clippy --workspace --all-targets --all-features --fix

.PHONY: fmt-check
fmt-check: ## Check code format.
	cargo fmt --all -- --check
	python3 scripts/check-snafu.py

.PHONY: start-etcd
start-etcd: ## Start single node etcd for testing purpose.
	docker run --rm -d --network=host -p 2379-2380:2379-2380 ${ETCD_IMAGE}

.PHONY: stop-etcd
stop-etcd: ## Stop single node etcd for testing purpose.
	docker stop $$(docker ps -q --filter ancestor=${ETCD_IMAGE})

.PHONY: run-it-in-container
run-it-in-container: start-etcd ## Run integration tests in dev-builder.
	docker run --network=host \
	-v ${PWD}:/greptimedb -v ${CARGO_REGISTRY_CACHE}:/root/.cargo/registry -v /tmp:/tmp \
	-w /greptimedb ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder-${BASE_IMAGE}:${DEV_BUILDER_IMAGE_TAG} \
	make test sqlness-test BUILD_JOBS=${BUILD_JOBS}

.PHONY: start-cluster
start-cluster: ## Start the greptimedb cluster with etcd by using docker compose.
	 docker compose -f ./docker/docker-compose/cluster-with-etcd.yaml up

.PHONY: stop-cluster
stop-cluster: ## Stop the greptimedb cluster that created by docker compose.
	docker compose -f ./docker/docker-compose/cluster-with-etcd.yaml stop

##@ Docs
config-docs: ## Generate configuration documentation from toml files.
	docker run --rm \
    -v ${PWD}:/greptimedb \
    -w /greptimedb/config \
    toml2docs/toml2docs:v0.1.3 \
    -p '##' \
    -t ./config-docs-template.md \
    -o ./config.md

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# https://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display help messages.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
