# Manage namespace-level configuration parameters
resource "aerospike_namespace_config" "example" {
  namespace = "test"

  params = {
    "default-ttl"           = "0"
    "high-water-memory-pct" = "80"
  }

  set_config = {
    "users" = {
      "default-ttl" = "3600"
    }
    "orders" = {
      "default-ttl" = "7200"
    }
  }
}

# Access the list of asinfo commands that were executed
output "applied_commands" {
  value = aerospike_namespace_config.example.info_commands
}
