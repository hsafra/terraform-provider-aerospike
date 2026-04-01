## 0.5.0
FEATURES:
* New `aerospike_service_config` resource for managing dynamic service-level configuration parameters
* Singleton enforcement — only one `aerospike_service_config` instance allowed per provider
* New `aerospike_namespace_config` resource for managing dynamic namespace and set-level configuration parameters

ENHANCEMENTS:
* Updated Terraform test matrix to versions 1.8 through 1.14
* Multi-version test infrastructure with Aerospike v6, v7, and v8 source/target containers
* Comprehensive acceptance tests for user and role resources

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
