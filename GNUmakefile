.EXPORT_ALL_VARIABLES:

default: testacc

AEROSPIKE_USER ?= admin
AEROSPIKE_PASSWORD ?= admin
AEROSPIKE_HOST ?= localhost
AEROSPIKE_PORT ?= 3000

AEROSPIKE_LOCAL_VERSION ?= 6.2.0.7
AEROSPIKE_LOCAL_MAJOR_VERSION ?= 6

# Run acceptance tests
.PHONY: testacc
testacc:
	TF_ACC=1 go test ./... -v -cover $(TESTARGS) -timeout 120m

localtestacc:
	docker run -d -v ./tests\:/opt/aerospike/etc/ -e FEATURE_KEY_FILE=/opt/aerospike/etc/features.conf --name aerospike -p 3000-3002\:3000-3002 aerospike\:ee-${AEROSPIKE_LOCAL_VERSION} --config-file /opt/aerospike/etc/aerospike-ee-${AEROSPIKE_LOCAL_MAJOR_VERSION}.conf
	TF_ACC=1 go test ./... -v -cover $(TESTARGS) -timeout 120m
	docker rm -f aerospike
