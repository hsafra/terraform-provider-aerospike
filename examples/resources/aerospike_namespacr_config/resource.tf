resource "aerospike_namespace_config" "aerospike_ns" {
  namespace = "aerospike"
  default_set_ttl = {
    "set1" = "100"
    "set2" = "10M"
  }
  migartion_threads = 2
  xdr_datacenter    = "dc1"
  xdr_exclude       = ["set3"]
}


