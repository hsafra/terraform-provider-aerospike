#!/bin/sh
set -e

# Builds a runtime Aerospike config from the base source config, injecting
# mesh-seed and alternate-access settings for multi-node Docker clusters.
#
# Environment variables:
#   AEROSPIKE_MAJOR_VERSION - required: selects the base config (6, 7, or 8)
#   FEATURE_KEY_FILE        - optional: path to the feature key file
#   MESH_SEED_HOST          - optional: hostname for mesh-seed-address-port
#   ALT_ACCESS_PORT         - optional: alternate-access-port for Docker Desktop compatibility

BASE_CONFIG="/opt/aerospike/etc/aerospike-ee-${AEROSPIKE_MAJOR_VERSION}-source.conf"
RUNTIME_CONFIG="/tmp/aerospike.conf"

awk -v feature_key="${FEATURE_KEY_FILE}" -v mesh_seed="${MESH_SEED_HOST}" -v alt_port="${ALT_ACCESS_PORT}" '
  # After the opening "service {" block, inject feature-key-file
  /^service \{/ && !fk_done {
    print
    if (feature_key != "") {
      print "\tfeature-key-file " feature_key
    }
    fk_done = 1
    next
  }
  # After "port 3000" in the service section, inject alternate-access settings
  /^[[:space:]]*port 3000/ && !svc_done {
    print
    if (alt_port != "") {
      print "\t\talternate-access-address 127.0.0.1"
      print "\t\talternate-access-port " alt_port
    }
    svc_done = 1
    next
  }
  # After "port 3002" in the heartbeat section, inject mesh-seed
  /^[[:space:]]*port 3002/ && !hb_done {
    print
    if (mesh_seed != "") {
      print "\t\tmesh-seed-address-port " mesh_seed " 3002"
    }
    hb_done = 1
    next
  }
  { print }
' "${BASE_CONFIG}" > "${RUNTIME_CONFIG}"

exec asd --foreground --config-file "${RUNTIME_CONFIG}"
