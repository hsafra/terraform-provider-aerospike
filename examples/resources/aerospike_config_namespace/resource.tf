resource "aerospike_namespace_config" "aerospike_ns" {
  namespace = "aerospike"
  default_set_ttl = {
    "set1" = "100"
    "set2" = "10M"
  }
  xdr_config = {
    datacenter               = "dc1"
    ship_only_specified_sets = false
    exclude_sets             = ["set3"]
  }
}


