# Basic XDR datacenter configuration
resource "aerospike_xdr_dc_config" "example" {
  dc = "dc-west"

  node_address_ports = ["10.0.0.2:3000", "10.0.0.3:3000"]

  namespace {
    name   = "production"
    rewind = "all"
  }
}

# XDR datacenter with set policy — ship only specific sets
resource "aerospike_xdr_dc_config" "selective" {
  dc = "dc-east"

  node_address_ports = ["10.1.0.2:3000"]

  namespace {
    name = "production"

    set_policy {
      ship_only_specified_sets = true
      ship_sets                = ["users", "orders"]
    }
  }
}

# XDR datacenter with set policy — ignore specific sets
resource "aerospike_xdr_dc_config" "filtered" {
  dc = "dc-south"

  node_address_ports = ["10.2.0.2:3000"]

  namespace {
    name = "production"

    set_policy {
      ship_only_specified_sets = false
      ignore_sets              = ["temp_data", "cache"]
    }
  }
}
