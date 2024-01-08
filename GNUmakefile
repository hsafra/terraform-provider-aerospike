.EXPORT_ALL_VARIABLES:

default: testacc

AEROSPIKE_USER ?= admin
AEROSPIKE_PASSWORD ?= admin
AEROSPIKE_HOST ?= localhost
AEROSPIKE_PORT ?= 3000

# Run acceptance tests
.PHONY: testacc
testacc:
	TF_ACC=1 go test ./... -v $(TESTARGS) -timeout 120m
