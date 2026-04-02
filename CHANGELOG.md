## 0.5.2
BUG FIXES:
* Fix set-config commands only reaching a single cluster node — all write commands now fan out to every node, matching asadm behavior
* Fix `ignore_sets`/`ship_sets` removal not detected by plan when server state drifts across nodes
* Exclude internal set-removal mechanism commands (move-to-opposite-list) from `info_commands` output
* Tolerate SMD propagation race errors for structural XDR commands (create/delete DC, add/remove node/namespace) in multi-node clusters

ENHANCEMENTS:
* Import documentation auto-generated via tfplugindocs for all resources
* Multi-node (3-node) source cluster test infrastructure for all Aerospike versions
* Multi-node verification tests confirming config consistency across all cluster nodes
* `UseServicesAlternate` enabled on client for Docker/NAT compatibility
* CI workflow updated to run tests against 3-node clusters

## 0.5.0
FEATURES:
* New `aerospike_service_config` resource for managing dynamic service-level configuration parameters
* Singleton enforcement — only one `aerospike_service_config` instance allowed per provider
* New `aerospike_namespace_config` resource for managing dynamic namespace and set-level configuration parameters
* New `aerospike_xdr_dc_config` resource for managing XDR datacenter configuration — full DC lifecycle, node management, namespace assignment with rewind support, and set shipping policies (`ship_sets`, `ignore_sets`, `ship_only_specified_sets`)
* Set policy params (`ship-only-specified-sets`, `ship-set`, `ignore-set`) are fenced into a dedicated `set_policy` block to prevent misconfiguration

ENHANCEMENTS:
* Upgraded Aerospike Go client from v7.10.2 to v8.6.0
* Updated Terraform test matrix to versions 1.8 through 1.14
* Multi-version test infrastructure with Aerospike v6, v7, and v8 source/target containers
* Comprehensive acceptance tests for user and role resources
* General-purpose parameter incompatibility validation framework

## 0.4.2
BUG FIXES:
* Fix Terraform semantics issues with role privilege handling (#50)
* Upgrade Aerospike client library

## 0.4.0
BUG FIXES:
* Disallow empty strings for namespace and set in role privileges — the Aerospike API treats empty strings and nulls the same
* Return empty namespace and set attributes as `""` instead of `null`

ENHANCEMENTS:
* CI/CD workflow improvements
* Acceptance test improvements

## 0.3.0
Bug fixes

## 0.2.0
FEATURES:
* TLS support

## 0.1.0
FEATURES:
* Aerospike users
* Aerospike roles
