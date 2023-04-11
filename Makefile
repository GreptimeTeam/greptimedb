IMAGE_REGISTRY ?= greptimedb
IMAGE_TAG ?= latest

##@ Build

.PHONY: build
build: ## Build debug version greptime.
	cargo build

.PHONY: release
release:  ## Build release version greptime.
	cargo build --release

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
docker-image: ## Build docker image.
	docker build --network host -f docker/Dockerfile -t ${IMAGE_REGISTRY}:${IMAGE_TAG} .

##@ Test

.PHONY: unit-test
unit-test: ## Run unit test.
	cargo test --workspace

.PHONY: integration-test
integration-test: ## Run integation test.
	cargo test integration

.PHONY: sqlness-test
sqlness-test: ## Run sqlness test.
	cargo sqlness

.PHONY: check
check: ## Cargo check all the targets.
	cargo check --workspace --all-targets

.PHONY: clippy
clippy: ## Check clippy rules.
	cargo clippy --workspace --all-targets -- -D warnings -D clippy::print_stdout -D clippy::print_stderr

.PHONY: fmt-check
fmt-check: ## Check code format.
	cargo fmt --all -- --check

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
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
