# The arguments for building images.
CARGO_PROFILE ?=
FEATURES ?=
TARGET_DIR ?=
CARGO_BUILD_OPTS := --locked
IMAGE_REGISTRY ?= docker.io
IMAGE_NAMESPACE ?= greptime
IMAGE_TAG ?= latest
BUILDX_MULTI_PLATFORM_BUILD ?= false
BUILDX_BUILDER_NAME ?= gtbuilder
BASE_IMAGE ?= ubuntu
RUST_TOOLCHAIN ?= $(shell cat rust-toolchain.toml | grep channel | cut -d'"' -f2)
CARGO_REGISTRY_CACHE ?= ${HOME}/.cargo/registry

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

ifeq ($(BUILDX_MULTI_PLATFORM_BUILD), true)
	BUILDX_MULTI_PLATFORM_BUILD_OPTS := --platform linux/amd64,linux/arm64 --push
else
	BUILDX_MULTI_PLATFORM_BUILD_OPTS := -o type=docker
endif

##@ Build

.PHONY: build
build: ## Build debug version greptime. If USE_DEV_BUILDER is true, the binary will be built in dev-builder.
ifeq ($(USE_DEV_BUILDER), true)
	docker run --network=host \
	-v ${PWD}:/greptimedb -v ${CARGO_REGISTRY_CACHE}:/root/.cargo/registry \
	-w /greptimedb ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder:latest \
	make build CARGO_PROFILE=${CARGO_PROFILE} FEATURES=${FEATURES} TARGET_DIR=${TARGET_DIR}
else
	cargo build ${CARGO_BUILD_OPTS}
endif

.PHONY: release
release:  ## Build release version greptime. If USE_DEV_BUILDER is true, the binary will be built in dev-builder.
ifeq ($(USE_DEV_BUILDER), true)
	docker run --network=host \
	-v ${PWD}:/greptimedb -v ${CARGO_REGISTRY_CACHE}:/root/.cargo/registry \
	-w /greptimedb ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder:latest \
	make release CARGO_PROFILE=${CARGO_PROFILE} FEATURES=${FEATURES} TARGET_DIR=${TARGET_DIR}
else
	cargo build --release ${CARGO_BUILD_OPTS}
endif

.PHONY: clean
clean: ## Clean the project.
	cargo clean

.PHONY: fmt
fmt: ## Format all the Rust code.
	cargo fmt --all

.PHONY: fmt-toml
fmt-toml: ## Format all TOML files.
	taplo format --option "indent_string=    "

.PHONY: check-toml
check-toml: ## Check all TOML files.
	taplo format --check --option "indent_string=    "

.PHONY: docker-image
docker-image: multi-platform-buildx ## Build docker image.
	docker buildx build --builder ${BUILDX_BUILDER_NAME} \
	  --build-arg="CARGO_PROFILE=${CARGO_PROFILE}" --build-arg="FEATURES=${FEATURES}" \
	  -f docker/${BASE_IMAGE}/Dockerfile \
	  -t ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/greptimedb:${IMAGE_TAG} ${BUILDX_MULTI_PLATFORM_BUILD_OPTS} .

.PHONY: build-greptime-by-buildx
build-greptime-by-buildx: multi-platform-buildx ## Build greptime binary by docker buildx. The binary will be copied to the current directory.
	docker buildx build --builder ${BUILDX_BUILDER_NAME} \
	  --target=builder \
	  --build-arg="CARGO_PROFILE=${CARGO_PROFILE}" --build-arg="FEATURES=${FEATURES}" \
	  -f docker/${BASE_IMAGE}/Dockerfile \
	  -t ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/greptimedb-builder:${IMAGE_TAG} ${BUILDX_MULTI_PLATFORM_BUILD_OPTS} .

	docker run --rm -v ${PWD}:/data \
      --entrypoint cp ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/greptimedb-builder:${IMAGE_TAG} \
      /out/target/${CARGO_PROFILE}/greptime /data/greptime

.PHONY: dev-builder
dev-builder: multi-platform-buildx ## Build dev-builder image.
	docker buildx build --builder ${BUILDX_BUILDER_NAME} \
	--build-arg="RUST_TOOLCHAIN=${RUST_TOOLCHAIN}" \
	-f docker/dev-builder/Dockerfile \
	-t ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder:${IMAGE_TAG} ${BUILDX_MULTI_PLATFORM_BUILD_OPTS} .

.PHONY: multi-platform-buildx
multi-platform-buildx: ## Create buildx multi-platform builder.
	docker buildx inspect ${BUILDX_BUILDER_NAME} || docker buildx create --name ${BUILDX_BUILDER_NAME} --driver docker-container --bootstrap --use

##@ Test
test: nextest ## Run unit and integration tests.
	cargo nextest run ${NEXTEST_OPTS}

.PHONY: nextest ## Install nextest tools.
nextest:
	cargo --list | grep nextest || cargo install cargo-nextest --locked

.PHONY: sqlness-test
sqlness-test: ## Run sqlness test.
	cargo sqlness

.PHONY: check
check: ## Cargo check all the targets.
	cargo check --workspace --all-targets

.PHONY: clippy
clippy: ## Check clippy rules.
	cargo clippy --workspace --all-targets -- -D warnings

.PHONY: fmt-check
fmt-check: ## Check code format.
	cargo fmt --all -- --check

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
	-w /greptimedb ${IMAGE_REGISTRY}/${IMAGE_NAMESPACE}/dev-builder:latest \
	make test sqlness-test BUILD_JOBS=${BUILD_JOBS}

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
