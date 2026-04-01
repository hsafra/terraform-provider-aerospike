.EXPORT_ALL_VARIABLES:

default: testacc

AEROSPIKE_USER ?= admin
AEROSPIKE_PASSWORD ?= admin
AEROSPIKE_HOST ?= localhost
AEROSPIKE_PORT ?= 3000

# Aerospike version matrix — update these when new patch versions are released
AEROSPIKE_V6 ?= 6.2.0.7
AEROSPIKE_V7 ?= 7.0.0.3
AEROSPIKE_V8 ?= 8.1.1.1

# Run acceptance tests (no local docker)
.PHONY: testacc
testacc:
	TF_ACC=1 go test ./... -v -cover $(TESTARGS) -timeout 120m

# Run acceptance tests against a specific Aerospike version using docker compose.
# Usage: make localtestacc-6  or  make localtestacc-7  or  make localtestacc-8
define run_local_test
	AEROSPIKE_VERSION=$(1) AEROSPIKE_MAJOR_VERSION=$(2) docker compose -f tests/docker-compose.yml up -d --wait
	TF_ACC=1 go test ./... -v -cover $(TESTARGS) -timeout 120m; \
	ret=$$?; \
	AEROSPIKE_VERSION=$(1) AEROSPIKE_MAJOR_VERSION=$(2) docker compose -f tests/docker-compose.yml down; \
	exit $$ret
endef

.PHONY: localtestacc-6
localtestacc-6:
	$(call run_local_test,$(AEROSPIKE_V6),6)

.PHONY: localtestacc-7
localtestacc-7:
	$(call run_local_test,$(AEROSPIKE_V7),7)

.PHONY: localtestacc-8
localtestacc-8:
	$(call run_local_test,$(AEROSPIKE_V8),8)

.PHONY: localtestacc
localtestacc: localtestacc-6 localtestacc-7 localtestacc-8
