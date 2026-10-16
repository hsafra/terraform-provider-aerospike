## 0.7.0
BREAKING CHANGES:
* **`info_commands` changes meaning and behaviour** on `aerospike_service_config`, `aerospike_namespace_config` and `aerospike_sindex`. It used to be a log of the commands sent by the last create or update. It is now the list of commands that reproduce the resource's managed config:
  * It is rebuilt from the server on every refresh, not only when an apply runs. If a value is changed directly on the cluster and the config is then updated to match, nothing is applied, but `info_commands` still updates, so anything consuming it (for example an S3 object holding the commands to replay on new servers) picks up the new value. Before, the consumer stayed stale.
  * It is always the full list for the managed config, not just what the last apply happened to send.
  * It is sorted: namespace params, then sets by name, each by key. Before, the order was random on every apply, which could rewrite consumers even when nothing changed.
  * It is known at plan time when the planned params are known, so a PR plan shows the exact new list instead of "known after apply". The exception is `aerospike_namespace_config` with `enable-index` in `set_config` on 8.1.2+, which stays unknown until apply because SMD ownership is only decided then.
  * `enable-index` is left out for sets whose index is owned by `aerospike_sindex`, matching what is actually sent.
* **Expect a one-time diff after upgrading.** The first refresh rewrites `info_commands` in existing state, so anything consuming it will show an in-place update on the first plan.

BUG FIXES:
* An invalid service, namespace or set parameter key no longer leaves some of the valid keys applied — all keys are validated before any command is sent

## 0.6.1
BUG FIXES:
* The provider no longer crashes on a root CA file that holds no valid certificate (it dereferenced a nil error), or on any connection error other than a timeout, such as wrong credentials or an unreachable host. Both are now reported as errors.
* `aerospike_user` and `aerospike_role` no longer crash the provider when a server call fails. Creating a user that already exists now reports "User already exists", matching roles.
* An asinfo response is treated as an error only when it starts with `ERROR`. Before, any response containing "error" anywhere failed, so a set index or set named, say, `consumer-errors-idx` was created and then broke every refresh of its namespace.
* `aerospike_xdr_dc_config`: a create that fails after the datacenter exists (for example on an invalid DC or namespace parameter) now removes the datacenter again. Before, it stayed on the server outside state, and every later apply failed trying to create it.
* `aerospike_xdr_dc_config`: refresh now detects namespaces and nodes detached from the datacenter outside Terraform, and the next apply re-adds them. Before, they were never noticed.
* `aerospike_role` keeps the configured order of `white_list` on refresh, so a non-sorted whitelist no longer risks a permanent diff.
* Removing the whole `params` map from `aerospike_service_config`, `aerospike_namespace_config` or `aerospike_xdr_dc_config` now warns that the values stay on the server, as removing a single key already did.

ENHANCEMENTS:
* `aerospike_user` `roles` and `aerospike_role` `white_list` reject duplicate entries at plan time. The server keeps only one copy, which caused a permanent diff.
* Much wider test coverage: unit tests for environment overrides, privilege conversion, XDR validation and helpers, set-index ownership decisions, and response parsing; acceptance tests for provider connection errors, out-of-band drift (XDR membership, `ship-only-specified-sets`, deleted set indexes), import errors, identifier validation, and version gating of `aerospike_sindex`.

## 0.6.0
FEATURES:
* New `aerospike_sindex` resource for set indexes on Aerospike Database 8.1.2+ (`sindex-create` / `sindex-delete` with `indextype=set`). Creating a set index on a config-owned index (`enable-index`) converts ownership in place — no rebuild. Changing `name` renames the SMD index rather than deleting and recreating it. Destroy uses `sindex-delete` only.

ENHANCEMENTS:
* `aerospike_namespace_config` keeps `enable-index` in `set_config` for older clusters and config-owned indexes, but treats it as deprecated on 8.1.2+. After SMD conversion, `enable-index=true` is a no-op and `enable-index=false` is rejected so mixed clusters and consumer migrations do not disable or fail. Removing the key from HCL stops managing it and does not send `enable-index=false`.

## 0.5.5
BUG FIXES:
* Fix the client silently managing only a subset of a multi-node cluster. `UseServicesAlternate` was hard-coded to `true`, which forces peer discovery through each node's alternate-access address. When nodes are reached directly (e.g. behind AWS Cloud Map / a load balancer that returns a capped, rotating subset of seed addresses) and advertise no alternate address, peer discovery finds nothing and the client only ever sees the seed nodes — so service/namespace/set/XDR config reads and writes reach just that subset while still reporting success.

ENHANCEMENTS:
* New `use_services_alternate` provider attribute (default `false`, overridable via `AEROSPIKE_USE_SERVICES_ALTERNATE`) — enable only for NAT/Docker topologies where every node has an alternate-access address; leave disabled so peer discovery can tend the whole cluster
* Cluster-coverage guard — on connect, compare the number of discovered nodes against the server-reported `cluster_size` and fail when the client only sees a subset, so partial-cluster applies can no longer proceed silently

## 0.5.4
BUG FIXES:
* Detect cross-node config drift — reads now fan out to every node and prefer a value differing from prior state when nodes disagree, so `terraform refresh`/`plan` sees the drift and the next apply converges the cluster; per-node values surface as warning diagnostics
* Absorb momentary network blips — each per-node asinfo call retries once after 500ms before failing

ENHANCEMENTS:
* Parallelize fan-out reads and non-SMD writes across cluster nodes for faster plan/apply
* Raise CI acceptance test timeouts to accommodate the larger multi-node test suite

## 0.5.3
BUG FIXES:
* Strip `storage-engine.` subcontext prefix from namespace parameters in `set-config` commands — Aerospike's `get-config` returns these params with the prefix but `set-config` rejects it

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
