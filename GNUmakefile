.EXPORT_ALL_VARIABLES:

default: testacc

AEROSPIKE_USER ?= admin
AEROSPIKE_PASSWORD ?= admin
AEROSPIKE_HOST ?= localhost
AEROSPIKE_PORT ?= 3000

# Aerospike version matrix — update these when new patch versions are released
AEROSPIKE_V6 ?= 6.4.0.7
AEROSPIKE_V7 ?= 7.2.0.6
AEROSPIKE_V8 ?= 8.1.1.1

# Run acceptance tests (no local docker)
.PHONY: testacc
testacc:
	TF_ACC=1 go test ./... -v -cover -count=1 -p 1 $(TESTARGS) -timeout 120m

# Run acceptance tests against a specific Aerospike version using docker compose.
# Each version starts a 3-node source cluster + 2 target nodes.
# Usage: make localtestacc-6  or  make localtestacc-7  or  make localtestacc-8
# $(1) = full version, $(2) = major version, $(3) = base host port
# Source nodes use ports: $(3), $(3)+10, $(3)+20
define run_local_test
	AEROSPIKE_VERSION=$(1) AEROSPIKE_MAJOR_VERSION=$(2) \
		AEROSPIKE_HOST_PORT=$(3) \
		AEROSPIKE_HOST_PORT_2=$$(($(3)+10)) \
		AEROSPIKE_HOST_PORT_3=$$(($(3)+20)) \
		COMPOSE_PROJECT_NAME=aerospike-test-v$(2) \
		docker compose -f tests/docker-compose.yml up -d --wait
	AEROSPIKE_PORT=$(3) TF_ACC=1 go test ./... -v -cover -count=1 -p 1 $(TESTARGS) -timeout 120m; \
	ret=$$?; \
	AEROSPIKE_VERSION=$(1) AEROSPIKE_MAJOR_VERSION=$(2) \
		AEROSPIKE_HOST_PORT=$(3) \
		AEROSPIKE_HOST_PORT_2=$$(($(3)+10)) \
		AEROSPIKE_HOST_PORT_3=$$(($(3)+20)) \
		COMPOSE_PROJECT_NAME=aerospike-test-v$(2) \
		docker compose -f tests/docker-compose.yml down; \
	exit $$ret
endef

.PHONY: localtestacc-6
localtestacc-6:
	$(call run_local_test,$(AEROSPIKE_V6),6,3000)

.PHONY: localtestacc-7
localtestacc-7:
	$(call run_local_test,$(AEROSPIKE_V7),7,3000)

.PHONY: localtestacc-8
localtestacc-8:
	$(call run_local_test,$(AEROSPIKE_V8),8,3000)

# Run all versions sequentially (original behavior)
.PHONY: localtestacc
localtestacc: localtestacc-6 localtestacc-7 localtestacc-8

# Run all versions in parallel — each version gets its own port range and compose project
# v6: 3100-3120, v7: 3200-3220, v8: 3300-3320
.PHONY: localtestacc-parallel
localtestacc-parallel:
	@echo "Starting parallel tests for v6 (ports 3100-3120), v7 (ports 3200-3220), v8 (ports 3300-3320)..."
	@mkdir -p /tmp/localtestacc-results
	@$(MAKE) _parallel-v6 _parallel-v7 _parallel-v8 -j3
	@echo ""; echo "=== Parallel Test Summary ==="
	@for v in 6 7 8; do \
		if [ -f /tmp/localtestacc-results/v$$v.exit ]; then \
			code=$$(cat /tmp/localtestacc-results/v$$v.exit); \
			if [ "$$code" = "0" ]; then \
				echo "  v$$v: PASS"; \
			else \
				echo "  v$$v: FAIL (exit $$code)"; \
			fi; \
		else \
			echo "  v$$v: NOT RUN"; \
		fi; \
	done
	@failed=0; for v in 6 7 8; do \
		code=$$(cat /tmp/localtestacc-results/v$$v.exit 2>/dev/null || echo 1); \
		if [ "$$code" != "0" ]; then failed=1; fi; \
	done; \
	if [ "$$failed" = "1" ]; then \
		echo ""; echo "Some tests failed. Logs in /tmp/localtestacc-results/v{6,7,8}.log"; \
		exit 1; \
	fi

.PHONY: _parallel-v6
_parallel-v6:
	@$(MAKE) _run-parallel PVERSION=$(AEROSPIKE_V6) PMAJOR=6 PPORT=3100 2>&1 | tee /tmp/localtestacc-results/v6.log; \
	echo $${PIPESTATUS[0]} > /tmp/localtestacc-results/v6.exit

.PHONY: _parallel-v7
_parallel-v7:
	@$(MAKE) _run-parallel PVERSION=$(AEROSPIKE_V7) PMAJOR=7 PPORT=3200 2>&1 | tee /tmp/localtestacc-results/v7.log; \
	echo $${PIPESTATUS[0]} > /tmp/localtestacc-results/v7.exit

.PHONY: _parallel-v8
_parallel-v8:
	@$(MAKE) _run-parallel PVERSION=$(AEROSPIKE_V8) PMAJOR=8 PPORT=3300 2>&1 | tee /tmp/localtestacc-results/v8.log; \
	echo $${PIPESTATUS[0]} > /tmp/localtestacc-results/v8.exit

.PHONY: _run-parallel
_run-parallel:
	$(call run_local_test,$(PVERSION),$(PMAJOR),$(PPORT))
